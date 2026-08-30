[CmdletBinding()]
param(
    [string]$PostgresImage = 'postgres:16-alpine'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$root = [System.IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$runnerPath = Join-Path $PSScriptRoot 'run-milestone-7-dr-evidence.ps1'
$migrationDirectory = Join-Path $root 'migrations'
$runnerSource = Get-Content -Raw -LiteralPath $runnerPath
$healthyQueryMatch = [regex]::Match(
    $runnerSource,
    '(?ms)\$m7HealthyTrain\s*=\s*Get-M7Scalar\b.*?-SQL\s+@"\r?\n(?<sql>SELECT\b.*?)\r?\n"@'
)
if (-not $healthyQueryMatch.Success) {
    throw 'Milestone 7 healthy physical-shard assignment query was not found exactly once'
}
$healthyQuery = $healthyQueryMatch.Groups['sql'].Value.Trim()
if ($healthyQuery.Length -gt 4096) { throw 'Milestone 7 healthy assignment query exceeds the focused SQL bound' }

$suffix = [guid]::NewGuid().ToString('N').Substring(0, 12)
$container = "railway-m7-assignment-contract-$suffix"
$containerCreationAttempted = $false
$contractFailed = $false
$contractResult = $null

function Invoke-AssignmentContractDocker {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [string]$Label = 'docker-command',
        [ValidateRange(1, 300)][int]$TimeoutSeconds = 60,
        [switch]$AllowFailure
    )

    $dockerCommand = Get-Command docker -ErrorAction Stop
    $start = [System.Diagnostics.ProcessStartInfo]::new()
    $start.FileName = $dockerCommand.Source
    $start.UseShellExecute = $false
    $start.CreateNoWindow = $true
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    foreach ($argument in $Arguments) { $start.ArgumentList.Add($argument) }

    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $start
    try {
        if (-not $process.Start()) { throw "$Label did not start" }
        $stdoutTask = $process.StandardOutput.ReadToEndAsync()
        $stderrTask = $process.StandardError.ReadToEndAsync()
        if (-not $process.WaitForExit($TimeoutSeconds * 1000)) {
            try { $process.Kill($true); $process.WaitForExit() } catch { }
            if ($AllowFailure) {
                return [pscustomobject]@{ Output = [string[]]@("$Label timed out"); ExitCode = 124 }
            }
            throw "$Label timed out"
        }
        $stdout = $stdoutTask.GetAwaiter().GetResult()
        $stderr = $stderrTask.GetAwaiter().GetResult()
        $lines = @($stdout, $stderr) |
            ForEach-Object { [string]$_ -split "`r?`n" } |
            Where-Object { $_ -ne '' }
        if ($process.ExitCode -ne 0 -and -not $AllowFailure) {
            $detail = [string](@($lines | Select-Object -Last 20) -join '; ')
            throw "$Label failed with exit code $($process.ExitCode): $detail"
        }
        return [pscustomobject]@{ Output = [string[]]$lines; ExitCode = $process.ExitCode }
    } finally {
        $process.Dispose()
    }
}

function Invoke-AssignmentContractPSQL {
    param(
        [string]$SQL,
        [string]$Label = 'assignment-contract-sql'
    )
    if ($SQL.Length -gt 20000) { throw "$Label exceeds the focused SQL bound" }
    return Invoke-AssignmentContractDocker -Label $Label -TimeoutSeconds 120 -Arguments @(
        'exec', $container,
        'psql', '-X', '-q', '-A', '-t', '-v', 'ON_ERROR_STOP=1',
        '-U', 'postgres', '-d', 'railway_contract', '-c', $SQL
    )
}

function Get-AssignmentContractScalar {
    param(
        [string]$SQL,
        [string]$Label = 'assignment-contract-scalar'
    )
    $result = Invoke-AssignmentContractPSQL -SQL $SQL -Label $Label
    return [string](@($result.Output | Where-Object { $_.Trim() }) | Select-Object -Last 1).Trim()
}

try {
    Invoke-AssignmentContractDocker -Label 'docker-server-version' -TimeoutSeconds 15 -Arguments @(
        'version', '--format', '{{.Server.Version}}'
    ) | Out-Null
    $preflight = Invoke-AssignmentContractDocker -AllowFailure -Label 'assignment-contract-name-preflight' -TimeoutSeconds 15 -Arguments @(
        'inspect', '--format', '{{.Id}}', $container
    )
    if ($preflight.ExitCode -eq 0) { throw 'Disposable PostgreSQL name is already in use' }
    if ($preflight.ExitCode -ne 1) { throw "Disposable PostgreSQL name preflight failed with exit code $($preflight.ExitCode)" }
    $containerCreationAttempted = $true
    Invoke-AssignmentContractDocker -Label 'assignment-contract-container-start' -TimeoutSeconds 120 -Arguments @(
        'run', '--detach', '--name', $container,
        '--label', 'railway.m7.assignment-contract=true',
        '--env', 'POSTGRES_PASSWORD=assignment-contract-local-only',
        '--env', 'POSTGRES_DB=railway_contract',
        $PostgresImage
    ) | Out-Null

    $ready = $false
    for ($attempt = 1; $attempt -le 80; $attempt++) {
        $probe = Invoke-AssignmentContractDocker -AllowFailure -Label 'assignment-contract-readiness' -TimeoutSeconds 10 -Arguments @(
            'exec', $container, 'pg_isready', '-U', 'postgres', '-d', 'railway_contract'
        )
        if ($probe.ExitCode -eq 0) {
            $ready = $true
            break
        }
        Start-Sleep -Milliseconds 250
    }
    if (-not $ready) { throw 'Disposable PostgreSQL did not become ready within the focused bound' }

    Invoke-AssignmentContractDocker -Label 'assignment-contract-migration-copy' -TimeoutSeconds 30 -Arguments @(
        'cp', "$migrationDirectory\.", "${container}:/migrations"
    ) | Out-Null
    $migrationFiles = @(Get-ChildItem -LiteralPath $migrationDirectory -Filter '*.up.sql' | Sort-Object Name)
    if ($migrationFiles.Count -ne 11) { throw "Expected 11 up migrations, found $($migrationFiles.Count)" }
    foreach ($migration in $migrationFiles) {
        Invoke-AssignmentContractDocker -Label "apply-$($migration.BaseName)" -TimeoutSeconds 120 -Arguments @(
            'exec', $container,
            'psql', '-X', '-q', '-v', 'ON_ERROR_STOP=1',
            '-U', 'postgres', '-d', 'railway_contract',
            '-f', "/migrations/$($migration.Name)"
        ) | Out-Null
    }

    $columnResult = Invoke-AssignmentContractPSQL -Label 'assignment-schema-columns' -SQL @'
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'train_run_shard_assignments'
ORDER BY ordinal_position;
'@
    $columns = [string[]]@($columnResult.Output | Where-Object { $_.Trim() } | ForEach-Object { $_.Trim() })
    foreach ($requiredColumn in @(
        'train_run_id', 'shard_id', 'assignment_generation', 'assignment_state',
        'active_migration_id', 'availability_generation', 'created_at', 'updated_at'
    )) {
        if ($requiredColumn -notin $columns) { throw "Assignment schema omits required column: $requiredColumn" }
    }
    if ('is_current' -in $columns) { throw 'Assignment schema unexpectedly exposes is_current' }

    $primaryKey = Get-AssignmentContractScalar -Label 'assignment-primary-key' -SQL @'
SELECT string_agg(a.attname, ',' ORDER BY key_column.ordinality)
FROM pg_index AS i
CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS key_column(attnum, ordinality)
JOIN pg_attribute AS a
  ON a.attrelid = i.indrelid
 AND a.attnum = key_column.attnum
WHERE i.indrelid = 'public.train_run_shard_assignments'::regclass
  AND i.indisprimary;
'@
    if ($primaryKey -cne 'train_run_id') { throw "Unexpected assignment primary key: $primaryKey" }

    Invoke-AssignmentContractPSQL -Label 'assignment-contract-fixture' -SQL @'
BEGIN;
SELECT set_config('railway.deployment_region', 'region-a', true);
SELECT set_config('railway.deployment_role', 'active', true);
SELECT set_config('railway.region_epoch', '1', true);
SELECT set_config('railway.regional_writes_enabled', 'true', true);
INSERT INTO public.stations (id, code, name, timezone) VALUES
    ('91000000-0000-4000-8000-000000000001', 'M7C1', 'M7 contract origin', 'UTC'),
    ('91000000-0000-4000-8000-000000000002', 'M7C2', 'M7 contract destination', 'UTC');
INSERT INTO public.routes (id, code, name, operating_timezone)
VALUES ('91000000-0000-4000-8000-000000000003', 'M7_CONTRACT', 'M7 assignment contract', 'UTC');
INSERT INTO public.route_stops (
    route_id, station_id, stop_index, arrival_offset_minutes, departure_offset_minutes
) VALUES
    ('91000000-0000-4000-8000-000000000003', '91000000-0000-4000-8000-000000000001', 0, 0, 0),
    ('91000000-0000-4000-8000-000000000003', '91000000-0000-4000-8000-000000000002', 1, 10, 10);
INSERT INTO public.trains (id, code, name)
VALUES ('91000000-0000-4000-8000-000000000004', 'M7CT', 'M7 contract train');
INSERT INTO public.train_runs (
    id, train_id, route_id, service_date, scheduled_departure_at, status, segment_count
) VALUES
    ('21000000-0000-4000-8000-000000000402', '91000000-0000-4000-8000-000000000004', '91000000-0000-4000-8000-000000000003', DATE '2099-01-02', TIMESTAMPTZ '2099-01-02 08:00:00+00', 'scheduled', 1),
    ('21000000-0000-4000-8000-000000000403', '91000000-0000-4000-8000-000000000004', '91000000-0000-4000-8000-000000000003', DATE '2099-01-03', TIMESTAMPTZ '2099-01-03 08:00:00+00', 'scheduled', 1),
    ('21000000-0000-4000-8000-000000000404', '91000000-0000-4000-8000-000000000004', '91000000-0000-4000-8000-000000000003', DATE '2099-01-04', TIMESTAMPTZ '2099-01-04 08:00:00+00', 'scheduled', 1),
    ('21000000-0000-4000-8000-000000000405', '91000000-0000-4000-8000-000000000004', '91000000-0000-4000-8000-000000000003', DATE '2099-01-05', TIMESTAMPTZ '2099-01-05 08:00:00+00', 'scheduled', 1);
UPDATE public.booking_shards
SET enabled = true,
    write_enabled = true,
    state = 'active',
    health_state = 'healthy',
    last_health_checked_at = clock_timestamp(),
    write_disabled_reason = NULL
WHERE shard_id IN ('physical-shard-0', 'physical-shard-1');
INSERT INTO public.physical_shard_migrations (
    migration_id, train_run_id, source_shard_id, target_shard_id,
    source_generation, target_generation, state, source_fenced_at,
    target_enabled_at, assignment_switched_at, completed_at
) VALUES
    ('91000000-0000-4000-8000-000000000010', '21000000-0000-4000-8000-000000000402', 'legacy', 'physical-shard-1', 1, 2, 'completed', clock_timestamp(), clock_timestamp(), clock_timestamp(), clock_timestamp()),
    ('91000000-0000-4000-8000-000000000011', '21000000-0000-4000-8000-000000000403', 'legacy', 'physical-shard-0', 1, 2, 'completed', clock_timestamp(), clock_timestamp(), clock_timestamp(), clock_timestamp());
UPDATE public.train_run_write_fences
SET write_enabled = false
WHERE train_run_id IN (
    '21000000-0000-4000-8000-000000000402',
    '21000000-0000-4000-8000-000000000403'
);
UPDATE public.train_run_shard_assignments
SET shard_id = CASE train_run_id
        WHEN '21000000-0000-4000-8000-000000000402'::uuid THEN 'physical-shard-1'
        ELSE 'physical-shard-0'
    END,
    assignment_generation = 2,
    assignment_state = 'stable'
WHERE train_run_id IN (
    '21000000-0000-4000-8000-000000000402',
    '21000000-0000-4000-8000-000000000403'
);
COMMIT;
'@ | Out-Null

    $selectedTrainRun = Get-AssignmentContractScalar -Label 'runner-healthy-assignment-query' -SQL $healthyQuery
    if ($selectedTrainRun -cne '21000000-0000-4000-8000-000000000402') {
        throw "Healthy assignment query selected an unexpected train run: $selectedTrainRun"
    }
    $selectedCount = Get-AssignmentContractScalar -Label 'runner-healthy-assignment-cardinality' -SQL "SELECT count(*) FROM ($healthyQuery) AS selected"
    if ($selectedCount -cne '1') { throw "Healthy assignment query returned unexpected cardinality: $selectedCount" }
    $duplicateCandidates = Get-AssignmentContractScalar -Label 'assignment-duplicate-candidates' -SQL @'
SELECT count(*)
FROM (
    SELECT train_run_id
    FROM public.train_run_shard_assignments
    GROUP BY train_run_id
    HAVING count(*) > 1
) AS duplicate_assignment;
'@
    if ($duplicateCandidates -cne '0') { throw "Assignment primary-key contract allowed duplicate candidates: $duplicateCandidates" }

    $contractResult = [ordered]@{
        status = 'passed'
        migrations_applied = $migrationFiles.Count
        columns = $columns
        primary_key = $primaryKey
        selected_train_run_id = $selectedTrainRun
        selected_cardinality = [int]$selectedCount
        duplicate_candidates = [int]$duplicateCandidates
        physical_shard_0_excluded = ($selectedTrainRun -cne '21000000-0000-4000-8000-000000000403')
        is_current_required = $false
    }
} catch {
    $contractFailed = $true
    throw
} finally {
    if ($containerCreationAttempted) {
        $cleanupFailure = ''
        $ownedContainer = Invoke-AssignmentContractDocker -AllowFailure -Label 'assignment-contract-ownership-inspect' -TimeoutSeconds 15 -Arguments @(
            'inspect', '--format', '{{index .Config.Labels "railway.m7.assignment-contract"}}', $container
        )
        if ($ownedContainer.ExitCode -eq 0) {
            $ownershipLabel = [string](@($ownedContainer.Output | Where-Object { $_.Trim() }) | Select-Object -Last 1).Trim()
            if ($ownershipLabel -cne 'true') {
                $cleanupFailure = 'Disposable PostgreSQL ownership label did not match; exact container was preserved'
            } else {
                $removed = Invoke-AssignmentContractDocker -AllowFailure -Label 'assignment-contract-container-remove' -TimeoutSeconds 30 -Arguments @(
                    'rm', '--force', '--volumes', $container
                )
                if ($removed.ExitCode -ne 0) {
                    $cleanupFailure = "Disposable PostgreSQL removal failed with exit code $($removed.ExitCode)"
                } else {
                    $postCleanup = Invoke-AssignmentContractDocker -AllowFailure -Label 'assignment-contract-cleanup-verify' -TimeoutSeconds 15 -Arguments @(
                        'inspect', '--format', '{{.Id}}', $container
                    )
                    if ($postCleanup.ExitCode -eq 0) {
                        $cleanupFailure = 'Disposable PostgreSQL still exists after removal'
                    } elseif ($postCleanup.ExitCode -ne 1) {
                        $cleanupFailure = "Disposable PostgreSQL cleanup verification failed with exit code $($postCleanup.ExitCode)"
                    }
                }
            }
        } elseif ($ownedContainer.ExitCode -ne 1) {
            $cleanupFailure = "Disposable PostgreSQL ownership inspection failed with exit code $($ownedContainer.ExitCode)"
        }
        if ($cleanupFailure) {
            if ($contractFailed) {
                Write-Warning $cleanupFailure
            } else {
                throw $cleanupFailure
            }
        }
    }
}

$contractResult | ConvertTo-Json -Compress
