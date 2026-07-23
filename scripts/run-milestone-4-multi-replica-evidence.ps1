[CmdletBinding()]
param(
    [ValidateRange(6, 30)]
    [int]$CustomerCount = 12,

    [ValidatePattern('^[1-9][0-9]*(s|m)$')]
    [string]$LoadDuration = '15s',

    [string]$ProjectName = '',

    [string]$EvidenceDirectory = '',

    [switch]$KeepEnvironment
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$root = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $root 'docker-compose.multi-replica.yml'
$fixtureFile = Join-Path $root 'loadtest/fixtures/milestone-2-multi-replica.sql'
$suffix = [guid]::NewGuid().ToString('N').Substring(0, 10)
if ([string]::IsNullOrWhiteSpace($ProjectName)) {
    $ProjectName = "railway-m4-evidence-$suffix"
}
if ($ProjectName -notmatch '^[a-z0-9][a-z0-9_-]{2,62}$') {
    throw 'ProjectName must be a bounded lowercase Compose project name'
}
if ([string]::IsNullOrWhiteSpace($EvidenceDirectory)) {
    $EvidenceDirectory = Join-Path ([System.IO.Path]::GetTempPath()) "railway-m4-evidence-$suffix"
}
$EvidenceDirectory = [System.IO.Path]::GetFullPath($EvidenceDirectory)
$repositoryPath = [System.IO.Path]::GetFullPath($root).TrimEnd('\', '/')
$isWindowsHost = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
    [System.Runtime.InteropServices.OSPlatform]::Windows
)
$comparison = if ($isWindowsHost) {
    [StringComparison]::OrdinalIgnoreCase
} else {
    [StringComparison]::Ordinal
}
if ($EvidenceDirectory.Equals($repositoryPath, $comparison) -or
    $EvidenceDirectory.StartsWith($repositoryPath + [System.IO.Path]::DirectorySeparatorChar, $comparison)) {
    throw 'EvidenceDirectory must be outside the repository'
}
New-Item -ItemType Directory -Path $EvidenceDirectory -Force | Out-Null

$compose = @('compose', '-p', $ProjectName, '-f', $composeFile)
$fixtureTrainA = '21000000-0000-4000-8000-000000000401'
$fixtureTrainB = '21000000-0000-4000-8000-000000000402'
$migrationA = '41000000-0000-4000-8000-000000000401'
$migrationB = '41000000-0000-4000-8000-000000000402'
$origin = 'M2A'
$destination = 'M2B'
$seatClass = 'standard'
$started = $false
$succeeded = $false
$operatorCLI = 'unrun'
$failureCategory = 'not_started'
$customers = @()
$secretValues = [System.Collections.Generic.List[string]]::new()
$savedEnvironment = @{}
$environmentNames = @(
    'BASE_URL', 'CUSTOMER_TOKEN', 'CUSTOMER_TOKENS', 'PASSENGER_IDS', 'RESERVATION_IDS',
    'TRAIN_RUN_ID', 'TRAIN_RUN_IDS',
    'LEGACY_TRAIN_RUN_ID', 'SCHEMA_TRAIN_RUN_ID', 'API_URLS', 'ORIGIN_CODE',
    'DESTINATION_CODE', 'SEAT_CLASS', 'VUS', 'VUS_PER_SHARD', 'ITERATIONS',
    'DURATION', 'MAX_DURATION', 'ALLOW_REBALANCING_503', 'EXPECT_PARTIAL'
)
foreach ($name in $environmentNames) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

function Write-EvidenceStatus {
    param(
        [Parameter(Mandatory = $true)][string]$Status,
        [Parameter(Mandatory = $true)][string]$Reason
    )
    [ordered]@{
        milestone = 4
        status = $Status
        reason = $Reason
        operator_cli = $script:operatorCLI
        project = $script:ProjectName
        generated_at = [DateTimeOffset]::UtcNow.ToString('o')
    } | ConvertTo-Json -Depth 4 |
        Out-File -LiteralPath (Join-Path $script:EvidenceDirectory 'evidence-status.json') -Encoding utf8
}

function Invoke-Native {
    param(
        [Parameter(Mandatory = $true)][scriptblock]$Command,
        [switch]$AllowFailure
    )
    $previous = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = & $Command 2>&1
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    if ($exitCode -ne 0 -and -not $AllowFailure) {
        throw "native command failed with exit code $exitCode"
    }
    return [pscustomobject]@{ Output = @($output); ExitCode = $exitCode }
}

function Invoke-Compose {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [string]$CapturePath = '',
        [switch]$AllowFailure
    )
    $result = Invoke-Native -AllowFailure -Command { & docker @script:compose @Arguments }
    if (-not [string]::IsNullOrWhiteSpace($CapturePath)) {
        $result.Output | Out-File -LiteralPath $CapturePath -Encoding utf8
    }
    if ($result.ExitCode -ne 0 -and -not $AllowFailure) {
        throw "docker compose command failed with exit code $($result.ExitCode)"
    }
    return $result
}

function Invoke-PSQL {
    param(
        [Parameter(Mandatory = $true)][string]$SQL,
        [Parameter(Mandatory = $true)][string]$Artifact
    )
    return Invoke-Compose -Arguments @(
        'exec', '-T', 'postgres', 'psql', '-U', 'railway', '-d', 'railway',
        '-v', 'ON_ERROR_STOP=1', '-At', '-c', $SQL
    ) -CapturePath (Join-Path $script:EvidenceDirectory $Artifact)
}

function Get-PublishedURL {
    $result = Invoke-Compose -Arguments @('port', 'load-balancer', '8080')
    $endpoint = [string](@($result.Output | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) }) | Select-Object -Last 1)
    $endpoint = $endpoint.Trim()
    if ($endpoint -notmatch '^(127\.0\.0\.1|0\.0\.0\.0|\[::\]|::):(?<port>[1-9][0-9]{1,4})$') {
        throw 'load-balancer published endpoint was not a bounded loopback address'
    }
    return "http://127.0.0.1:$($matches.port)"
}

function Invoke-API {
    param(
        [Parameter(Mandatory = $true)][ValidateSet('GET', 'POST')][string]$Method,
        [Parameter(Mandatory = $true)][string]$Path,
        [hashtable]$Body,
        [string]$Token = '',
        [string]$IdempotencyKey = '',
        [string]$ForwardedFor = ''
    )
    $headers = @{ Accept = 'application/json' }
    if (-not [string]::IsNullOrWhiteSpace($Token)) { $headers['Authorization'] = "Bearer $Token" }
    if (-not [string]::IsNullOrWhiteSpace($IdempotencyKey)) { $headers['Idempotency-Key'] = $IdempotencyKey }
    if (-not [string]::IsNullOrWhiteSpace($ForwardedFor)) { $headers['X-Forwarded-For'] = $ForwardedFor }
    $parameters = @{
        Uri = "$script:baseURL$Path"
        Method = $Method
        Headers = $headers
        UseBasicParsing = $true
        TimeoutSec = 15
    }
    if ($null -ne $Body) {
        $parameters['ContentType'] = 'application/json'
        $parameters['Body'] = ($Body | ConvertTo-Json -Compress -Depth 6)
    }
    $response = Invoke-WebRequest @parameters
    $decoded = $null
    if (-not [string]::IsNullOrWhiteSpace([string]$response.Content)) {
        $decoded = $response.Content | ConvertFrom-Json
    }
    return [pscustomobject]@{ StatusCode = [int]$response.StatusCode; Body = $decoded }
}

function Wait-Ready {
    param([string]$Service, [int]$Port, [int]$Attempts = 90)
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $probe = Invoke-Compose -AllowFailure -Arguments @(
            'exec', '-T', $Service, 'wget', '-q', '-T', '2', '-O', '/dev/null',
            "http://127.0.0.1:$Port/readyz"
        )
        if ($probe.ExitCode -eq 0) { return }
        Start-Sleep -Seconds 1
    }
    throw "$Service did not become ready in the bounded startup window"
}

function Wait-HotPoliciesInitialized {
    param([int]$Attempts = 80)
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $result = Invoke-Compose -AllowFailure -Arguments @(
            'exec', '-T', 'postgres', 'psql', '-U', 'railway', '-d', 'railway', '-At', '-c',
            "SELECT count(*) FROM public.hot_train_policies WHERE train_run_id IN ('$fixtureTrainA','$fixtureTrainB') AND enabled AND redis_initialized_version=version;"
        )
        $value = [string](@($result.Output | Where-Object {
            ([string]$_).Trim() -match '^[0-9]+$'
        }) | Select-Object -Last 1)
        if ($result.ExitCode -eq 0 -and $value.Trim() -eq '2') { return }
        Start-Sleep -Milliseconds 250
    }
    throw 'two hot-train policies were not initialized by the bounded worker barrier'
}

function Invoke-ShardAdmin {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$Artifact,
        [switch]$AllowFailure
    )
    $result = Invoke-Compose -AllowFailure:$AllowFailure -Arguments (@(
        '--profile', 'tools', 'run', '--rm', '-T', '--no-deps', 'shard-admin'
    ) + $Arguments) -CapturePath (Join-Path $script:EvidenceDirectory $Artifact)
    $jsonLine = @($result.Output | ForEach-Object { [string]$_ } | Where-Object {
        $_.TrimStart().StartsWith('{') -and $_.TrimEnd().EndsWith('}')
    }) | Select-Object -Last 1
    $envelope = $null
    if (-not [string]::IsNullOrWhiteSpace([string]$jsonLine)) {
        try { $envelope = $jsonLine | ConvertFrom-Json } catch { $envelope = $null }
    }
    if (-not $AllowFailure -and ($result.ExitCode -ne 0 -or $null -eq $envelope)) {
        throw 'shard-admin invocation failed or omitted its structured envelope'
    }
    return [pscustomobject]@{ ExitCode = $result.ExitCode; Envelope = $envelope }
}

function Set-K6Environment {
    param([hashtable]$Values)
    foreach ($entry in $Values.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable([string]$entry.Key, [string]$entry.Value, 'Process')
    }
}

function Invoke-K6 {
    param(
        [Parameter(Mandatory = $true)][string]$Script,
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][hashtable]$Environment
    )
    Set-K6Environment -Values $Environment
    $environmentArguments = @()
    foreach ($name in ($Environment.Keys | Sort-Object)) { $environmentArguments += @('-e', [string]$name) }
    $arguments = @(
        '--profile', 'tools', 'run', '--rm', '-T', '--no-deps', '-v', "${EvidenceDirectory}:/evidence"
    ) + $environmentArguments + @(
        'k6', 'run', '--summary-export', "/evidence/$Name-summary.json", "/scripts/$Script"
    )
    Invoke-Compose -Arguments $arguments -CapturePath (Join-Path $script:EvidenceDirectory "$Name.log") | Out-Null
}

function Start-K6 {
    param(
        [Parameter(Mandatory = $true)][string]$Script,
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][hashtable]$Environment
    )
    Set-K6Environment -Values $Environment
    $containerName = "$ProjectName-$Name"
    $environmentArguments = @()
    foreach ($variable in ($Environment.Keys | Sort-Object)) { $environmentArguments += @('-e', [string]$variable) }
    $arguments = @(
        '--profile', 'tools', 'run', '-d', '--name', $containerName, '--no-deps', '-v', "${EvidenceDirectory}:/evidence"
    ) + $environmentArguments + @(
        'k6', 'run', '--summary-export', "/evidence/$Name-summary.json", "/scripts/$Script"
    )
    $result = Invoke-Compose -Arguments $arguments
    if ($result.ExitCode -ne 0) { throw 'background k6 container failed to start' }
    return $containerName
}

function Wait-K6 {
    param([Parameter(Mandatory = $true)][string]$ContainerName, [Parameter(Mandatory = $true)][string]$Name)
    $wait = Invoke-Native -Command { & docker wait $ContainerName }
    $logs = Invoke-Native -AllowFailure -Command { & docker logs $ContainerName }
    $logs.Output | Out-File -LiteralPath (Join-Path $script:EvidenceDirectory "$Name.log") -Encoding utf8
    Invoke-Native -AllowFailure -Command { & docker rm $ContainerName } | Out-Null
    $exitText = [string](@($wait.Output) | Select-Object -Last 1)
    if ($exitText.Trim() -ne '0') { throw "$Name k6 workload failed its bounded thresholds" }
}

function Save-Metrics {
    param([Parameter(Mandatory = $true)][string]$Label)
    foreach ($service in @('api-1', 'api-2', 'api-3')) {
        Invoke-Compose -Arguments @(
            'exec', '-T', $service, 'wget', '-q', '-T', '3', '-O', '-', 'http://127.0.0.1:8080/metrics'
        ) -CapturePath (Join-Path $script:EvidenceDirectory "$Label-$service.prom") | Out-Null
    }
}

function Get-APIMetricTotal {
    param(
        [Parameter(Mandatory = $true)][string]$Family,
        [string]$RequiredLabel = ''
    )
    $total = 0.0
    foreach ($service in @('api-1', 'api-2', 'api-3')) {
        $result = Invoke-Compose -Arguments @(
            'exec', '-T', $service, 'wget', '-q', '-T', '3', '-O', '-', 'http://127.0.0.1:8080/metrics'
        )
        foreach ($line in $result.Output) {
            if ([string]$line -match "^$([regex]::Escape($Family))\{(?<labels>[^}]*)\}\s+(?<value>[0-9eE+.-]+)$" -and
                ([string]::IsNullOrWhiteSpace($RequiredLabel) -or $matches.labels.Contains($RequiredLabel))) {
                $total += [double]::Parse($matches.value, [Globalization.CultureInfo]::InvariantCulture)
            }
        }
    }
    return $total
}

function Wait-WorkloadBarrier {
    param([double]$Before, [string]$ContainerName, [int]$Attempts = 30)
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $inspect = Invoke-Native -AllowFailure -Command { & docker inspect -f '{{.State.Running}}' $ContainerName }
        if ($inspect.ExitCode -ne 0 -or ([string](@($inspect.Output) | Select-Object -Last 1)).Trim() -ne 'true') {
            throw 'cutover workload exited before reaching the explicit route-metric barrier'
        }
        if ((Get-APIMetricTotal -Family 'shard_route_total') -gt $Before) { return }
        Start-Sleep -Milliseconds 500
    }
    throw 'cutover workload did not cross the explicit route-metric barrier'
}

function Get-MigrationState {
    param([Parameter(Mandatory = $true)][object]$Envelope)
    $result = $Envelope.result
    if ($null -ne $result) {
        $recordProperty = $result.PSObject.Properties |
            Where-Object { $_.Name -ieq 'record' } | Select-Object -First 1
        if ($null -ne $recordProperty) { $result = $recordProperty.Value }
    }
    $stateProperty = if ($null -ne $result) {
        $result.PSObject.Properties | Where-Object { $_.Name -ieq 'state' } | Select-Object -First 1
    } else { $null }
    if ($null -eq $stateProperty -or [string]::IsNullOrWhiteSpace([string]$stateProperty.Value)) {
        throw 'shard-admin result omitted migration state'
    }
    return ([string]$stateProperty.Value).ToLowerInvariant()
}

function Invoke-MigrationToCutoverReady {
    param(
        [Parameter(Mandatory = $true)][string]$TrainRunID,
        [Parameter(Mandatory = $true)][string]$TargetShard,
        [Parameter(Mandatory = $true)][string]$MigrationID,
        [Parameter(Mandatory = $true)][string]$Prefix
    )
    Invoke-ShardAdmin -Arguments @(
        'plan-migration', '--train-run-id', $TrainRunID, '--target-shard', $TargetShard,
        '--migration-id', $MigrationID, '--rollback-window', '5m', '--confirm', '--timeout', '30s'
    ) -Artifact "$Prefix-plan.json" | Out-Null
    $state = 'planned'
    for ($page = 1; $page -le 100 -and $state -notin @('validating', 'cutover_ready'); $page++) {
        $command = if ($page -eq 1) { 'start-migration' } else { 'resume-migration' }
        $copy = Invoke-ShardAdmin -Arguments @(
            $command, '--migration-id', $MigrationID, '--batch-size', '100', '--confirm', '--timeout', '30s'
        ) -Artifact "$Prefix-copy-$page.json"
        $state = Get-MigrationState -Envelope $copy.Envelope
    }
    if ($state -notin @('validating', 'cutover_ready')) {
        throw 'migration copy did not converge within 100 bounded batches'
    }
    if ($state -eq 'validating') {
        $validation = Invoke-ShardAdmin -Arguments @(
            'validate-migration', '--migration-id', $MigrationID, '--row-cap', '10000',
            '--confirm', '--timeout', '30s'
        ) -Artifact "$Prefix-validate.json"
        $state = Get-MigrationState -Envelope $validation.Envelope
    }
    if ($state -ne 'cutover_ready') { throw 'migration validation did not reach cutover_ready' }
}

function Invoke-Cutover {
    param(
        [Parameter(Mandatory = $true)][string]$MigrationID,
        [Parameter(Mandatory = $true)][string]$Prefix
    )
    $cutover = Invoke-ShardAdmin -Arguments @(
        'cutover', '--migration-id', $MigrationID, '--row-cap', '10000',
        '--locator-row-cap', '10000', '--confirm', '--timeout', '30s'
    ) -Artifact "$Prefix-cutover.json"
    if ((Get-MigrationState -Envelope $cutover.Envelope) -ne 'rollback_window') {
        throw 'cutover did not reach rollback_window'
    }
}

function Get-ObjectPropertyValue {
    param([object]$Object, [Parameter(Mandatory = $true)][string]$Name)
    if ($null -eq $Object) { return $null }
    $property = $Object.PSObject.Properties |
        Where-Object { $_.Name -eq $Name } | Select-Object -First 1
    if ($null -eq $property) { return $null }
    return $property.Value
}

function Get-K6Summary {
    param([Parameter(Mandatory = $true)][string]$Name)
    $path = Join-Path $script:EvidenceDirectory "$Name-summary.json"
    if (-not (Test-Path -LiteralPath $path)) { return $null }
    $summary = Get-Content -Raw -LiteralPath $path | ConvertFrom-Json
    $metrics = Get-ObjectPropertyValue -Object $summary -Name 'metrics'
    if ($null -eq $metrics) { return $null }
    $durationMetric = Get-ObjectPropertyValue -Object $metrics -Name 'shard_request_duration'
    $unexpectedMetric = Get-ObjectPropertyValue -Object $metrics -Name 'unexpected_5xx'
    $rebalancingMetric = Get-ObjectPropertyValue -Object $metrics -Name 'expected_rebalancing_503'
    $outageMetric = Get-ObjectPropertyValue -Object $metrics -Name 'expected_outage_503'
    $durationValues = Get-ObjectPropertyValue -Object $durationMetric -Name 'values'
    $unexpectedValues = Get-ObjectPropertyValue -Object $unexpectedMetric -Name 'values'
    $rebalancingValues = Get-ObjectPropertyValue -Object $rebalancingMetric -Name 'values'
    $outageValues = Get-ObjectPropertyValue -Object $outageMetric -Name 'values'
    return [ordered]@{
        artifact = "$Name-summary.json"
        p95_ms = Get-ObjectPropertyValue -Object $durationValues -Name 'p(95)'
        p99_ms = Get-ObjectPropertyValue -Object $durationValues -Name 'p(99)'
        unexpected_5xx = if ($null -ne $unexpectedValues) { Get-ObjectPropertyValue -Object $unexpectedValues -Name 'count' } else { 0 }
        expected_rebalancing_503 = if ($null -ne $rebalancingValues) { Get-ObjectPropertyValue -Object $rebalancingValues -Name 'count' } else { 0 }
        expected_outage_503 = if ($null -ne $outageValues) { Get-ObjectPropertyValue -Object $outageValues -Name 'count' } else { 0 }
    }
}

function Assert-ArtifactsSanitized {
    foreach ($file in Get-ChildItem -LiteralPath $script:EvidenceDirectory -File) {
        $content = Get-Content -Raw -LiteralPath $file.FullName -ErrorAction SilentlyContinue
        if ([string]::IsNullOrEmpty($content)) { continue }
        if ($content -match 'postgres(?:ql)?://[^\s"'']+' -or $content -match 'Bearer\s+[A-Za-z0-9._-]+') {
            throw 'evidence artifact contained a forbidden credential-shaped value'
        }
        foreach ($secret in $script:secretValues) {
            if (-not [string]::IsNullOrWhiteSpace($secret) -and $content.Contains($secret)) {
                throw 'evidence artifact contained an in-memory synthetic credential'
            }
        }
    }
}

$baseURL = ''
Write-EvidenceStatus -Status 'running' -Reason 'evidence_in_progress'

try {
    Push-Location $root
    if ((Invoke-Native -AllowFailure -Command { & docker version }).ExitCode -ne 0) {
        $failureCategory = 'docker_unavailable'
        throw 'Docker Engine is unavailable'
    }
    if ((Invoke-Native -AllowFailure -Command { & docker compose version }).ExitCode -ne 0) {
        $failureCategory = 'compose_unavailable'
        throw 'Docker Compose v2 is unavailable'
    }

    $started = $true
    Invoke-Compose -Arguments @('up', '-d', '--build') `
        -CapturePath (Join-Path $EvidenceDirectory 'compose-up.log') | Out-Null
    $adminBuild = Invoke-Compose -AllowFailure -Arguments @('--profile', 'tools', 'build', 'shard-admin') `
        -CapturePath (Join-Path $EvidenceDirectory 'shard-admin-build.log')
    if ($adminBuild.ExitCode -ne 0) {
        $failureCategory = 'operator_cli_unrun'
        throw 'hardened shard-admin image could not be built'
    }
    foreach ($service in @('api-1', 'api-2', 'api-3')) { Wait-Ready -Service $service -Port 8080 }
    foreach ($service in @('admission-worker-1', 'admission-worker-2')) { Wait-Ready -Service $service -Port 9090 }
    $baseURL = Get-PublishedURL

    $fixtureSQL = Get-Content -Raw -LiteralPath $fixtureFile
    $fixtureLoad = Invoke-Native -Command {
        $fixtureSQL | & docker @script:compose exec -T postgres psql -U railway -d railway -v ON_ERROR_STOP=1
    }
    $fixtureLoad.Output | Out-File -LiteralPath (Join-Path $EvidenceDirectory 'fixture-load.log') -Encoding utf8
    Invoke-PSQL -Artifact 'fixture-m4-adjustments.log' -SQL @"
UPDATE public.hot_train_policies
SET enabled=false, updated_at=clock_timestamp()
WHERE train_run_id IN ('$fixtureTrainA'::uuid, '$fixtureTrainB'::uuid);
INSERT INTO public.hot_train_policies (
    id, train_run_id, seat_class, enabled, version, max_queue_size,
    admission_rate_per_second, max_inflight_admissions,
    admission_token_ttl_seconds, processing_lease_seconds, queue_entry_ttl_seconds
) VALUES (
    '21000000-0000-4000-8000-000000000501', '$fixtureTrainB', 'standard', false,
    1, 1000, 5, 5, 240, 5, 300
)
ON CONFLICT (train_run_id, seat_class) DO UPDATE SET enabled=false, updated_at=clock_timestamp();
"@ | Out-Null

    $health = Invoke-ShardAdmin -Arguments @('inspect-health', '--timeout', '30s') -Artifact 'operator-health-baseline.json' -AllowFailure
    if ($health.ExitCode -ne 0 -or $null -eq $health.Envelope) {
        $failureCategory = 'operator_cli_unrun'
        throw 'hardened shard-admin service is unavailable'
    }
    $operatorCLI = 'run'

    $password = "M4-$suffix-Aa1!Evidence"
    $secretValues.Add($password)
    for ($index = 1; $index -le $CustomerCount; $index++) {
        $email = "m4-$suffix-$index@example.test"
        $forwardedFor = "198.19.0.$($index + 10)"
        $register = Invoke-API -Method POST -Path '/api/v1/auth/register' -Body @{
            email = $email; password = $password; display_name = "M4 Synthetic Rider $index"
        } -ForwardedFor $forwardedFor
        if ($register.StatusCode -ne 202) { throw 'synthetic registration failed' }
        $login = Invoke-API -Method POST -Path '/api/v1/auth/login' `
            -Body @{ email = $email; password = $password } -ForwardedFor $forwardedFor
        $token = [string]$login.Body.access_token
        if ([string]::IsNullOrWhiteSpace($token)) { throw 'synthetic login omitted access token' }
        $secretValues.Add($token)
        $passenger = Invoke-API -Method POST -Path '/api/v1/passengers' -Token $token -Body @{
            display_name = "M4 Load Passenger $index"
        }
        if ($passenger.StatusCode -ne 201 -or [string]::IsNullOrWhiteSpace([string]$passenger.Body.id)) {
            throw 'synthetic passenger creation failed'
        }
        $customers += [pscustomobject]@{ Token = $token; PassengerID = [string]$passenger.Body.id }
    }
    $password = $null

    $seedReservations = @()
    foreach ($trainRunID in @($fixtureTrainA, $fixtureTrainB)) {
        $seed = Invoke-API -Method POST -Path '/api/v1/reservations' `
            -Token $customers[0].Token `
            -IdempotencyKey "m4-seed-$suffix-$($trainRunID.Substring(35))" `
            -Body @{
                train_run_id = $trainRunID
                origin_station_code = $origin
                destination_station_code = $destination
                seat_class = $seatClass
                passenger_ids = @($customers[0].PassengerID)
            }
        if ($seed.StatusCode -ne 201 -or [string]::IsNullOrWhiteSpace([string]$seed.Body.id)) {
            throw 'deterministic seed reservation creation failed'
        }
        $seedReservations += [string]$seed.Body.id
    }

    $commonK6 = @{
        BASE_URL = 'http://load-balancer:8080'
        CUSTOMER_TOKEN = $customers[0].Token
        CUSTOMER_TOKENS = ($customers.Token -join ',')
        PASSENGER_IDS = ($customers.PassengerID -join ',')
        RESERVATION_IDS = ($seedReservations -join ',')
        TRAIN_RUN_IDS = "$fixtureTrainA,$fixtureTrainB"
        ORIGIN_CODE = $origin
        DESTINATION_CODE = $destination
        SEAT_CLASS = $seatClass
    }

    Save-Metrics -Label 'baseline'
    Invoke-K6 -Script 'shard-routing.js' -Name 'shard-routing' -Environment ($commonK6 + @{
        VUS = '8'; ITERATIONS = '48'; MAX_DURATION = '2m'; ALLOW_REBALANCING_503 = 'no'
    })
    Invoke-K6 -Script 'shard-route-cache.js' -Name 'shard-route-cache' -Environment ($commonK6 + @{
        TRAIN_RUN_ID = $fixtureTrainA; VUS = '6'; DURATION = $LoadDuration
    })
    $routeCacheHits = Get-APIMetricTotal -Family 'shard_route_cache_total' -RequiredLabel 'result="hit"'
    if ($routeCacheHits -le 0) { throw 'route-cache workload produced no bounded cache-hit metric evidence' }

    foreach ($api in @('api-1', 'api-2', 'api-3')) {
        Invoke-K6 -Script 'shard-route-cache.js' -Name "prewarm-$api" -Environment ($commonK6 + @{
            BASE_URL = "http://${api}:8080"; TRAIN_RUN_ID = $fixtureTrainA; VUS = '1'; DURATION = '2s'
        })
    }
    Invoke-MigrationToCutoverReady -TrainRunID $fixtureTrainA -TargetShard 'shard-0' `
        -MigrationID $migrationA -Prefix 'train-a'

    $metricBarrier = Get-APIMetricTotal -Family 'shard_route_total'
    $staleBefore = Get-APIMetricTotal -Family 'shard_assignment_stale_total'
    $cutoverContainer = Start-K6 -Script 'shard-cutover.js' -Name 'shard-cutover' -Environment ($commonK6 + @{
        TRAIN_RUN_ID = $fixtureTrainA; VUS = '8'; DURATION = '20s'
    })
    Wait-WorkloadBarrier -Before $metricBarrier -ContainerName $cutoverContainer
    Invoke-Cutover -MigrationID $migrationA -Prefix 'train-a'
    Wait-K6 -ContainerName $cutoverContainer -Name 'shard-cutover'
    $staleAfter = Get-APIMetricTotal -Family 'shard_assignment_stale_total'
    if ($staleAfter -le $staleBefore) {
        throw 'cutover workload did not produce bounded stale-router refresh evidence'
    }

    Invoke-K6 -Script 'stale-router-refresh.js' -Name 'stale-router-refresh' -Environment ($commonK6 + @{
        TRAIN_RUN_ID = $fixtureTrainA
        API_URLS = 'http://api-1:8080,http://api-2:8080,http://api-3:8080'
        VUS = '9'; ITERATIONS = '36'; MAX_DURATION = '90s'
    })
    Invoke-K6 -Script 'legacy-vs-schema-shard.js' -Name 'legacy-vs-schema-shard' -Environment ($commonK6 + @{
        LEGACY_TRAIN_RUN_ID = $fixtureTrainB; SCHEMA_TRAIN_RUN_ID = $fixtureTrainA
        VUS_PER_SHARD = '4'; DURATION = $LoadDuration
    })

    Invoke-MigrationToCutoverReady -TrainRunID $fixtureTrainB -TargetShard 'shard-1' `
        -MigrationID $migrationB -Prefix 'train-b'
    Invoke-Cutover -MigrationID $migrationB -Prefix 'train-b'

    Invoke-PSQL -Artifact 'two-hot-policies-enable.log' -SQL @"
UPDATE public.hot_train_policies
SET enabled=true, updated_at=clock_timestamp()
WHERE train_run_id IN ('$fixtureTrainA'::uuid, '$fixtureTrainB'::uuid)
  AND seat_class='standard';
"@ | Out-Null
    Wait-HotPoliciesInitialized
    Invoke-K6 -Script 'two-hot-train-shards.js' -Name 'two-hot-train-shards' -Environment ($commonK6 + @{
        VUS_PER_SHARD = '5'; DURATION = $LoadDuration
    })
    Invoke-K6 -Script 'cross-shard-admin.js' -Name 'cross-shard-healthy' -Environment ($commonK6 + @{
        EXPECT_PARTIAL = 'no'; DURATION = '10s'
    })

    Invoke-PSQL -Artifact 'shard-outage-inject.log' -SQL @"
UPDATE public.booking_shards
SET enabled=false, write_enabled=false, state='disabled', updated_at=clock_timestamp()
WHERE shard_id='shard-0';
"@ | Out-Null
    try {
        Invoke-ShardAdmin -Arguments @('inspect-health', '--timeout', '30s') `
            -Artifact 'operator-health-partial.json' -AllowFailure | Out-Null
        Invoke-K6 -Script 'shard-outage-isolation.js' -Name 'shard-outage-isolation' -Environment ($commonK6 + @{
            DURATION = $LoadDuration
        })
        Invoke-K6 -Script 'cross-shard-admin.js' -Name 'cross-shard-partial' -Environment ($commonK6 + @{
            EXPECT_PARTIAL = 'yes'; DURATION = '10s'
        })
    } finally {
        Invoke-PSQL -Artifact 'shard-outage-restore.log' -SQL @"
UPDATE public.booking_shards
SET enabled=true, write_enabled=true, state='active', updated_at=clock_timestamp()
WHERE shard_id='shard-0';
"@ | Out-Null
    }
    foreach ($pair in @(
        @{ Run = $fixtureTrainA; Name = 'train-a' },
        @{ Run = $fixtureTrainB; Name = 'train-b' }
    )) {
        Invoke-ShardAdmin -Arguments @(
            'reconcile', '--train-run-id', $pair.Run, '--row-cap', '10000', '--timeout', '30s'
        ) -Artifact "$($pair.Name)-reconcile.json" | Out-Null
    }

    $integrityResult = Invoke-PSQL -Artifact 'integrity-evidence.json' -SQL @"
WITH authoritative_reservations AS (
    SELECT reservation.id, reservation.train_run_id, reservation.status
    FROM public.reservations AS reservation
    JOIN public.train_run_shard_assignments AS assignment
      ON assignment.train_run_id=reservation.train_run_id AND assignment.shard_id='legacy'
    UNION ALL
    SELECT reservation.id, reservation.train_run_id, reservation.status
    FROM booking_shard_0.reservations AS reservation
    JOIN public.train_run_shard_assignments AS assignment
      ON assignment.train_run_id=reservation.train_run_id AND assignment.shard_id='shard-0'
    UNION ALL
    SELECT reservation.id, reservation.train_run_id, reservation.status
    FROM booking_shard_1.reservations AS reservation
    JOIN public.train_run_shard_assignments AS assignment
      ON assignment.train_run_id=reservation.train_run_id AND assignment.shard_id='shard-1'
), authoritative_seats AS (
    SELECT seat.id, seat.reservation_id, seat.train_run_id, seat.seat_id, seat.segment_mask
    FROM public.reservation_seats AS seat
    JOIN public.train_run_shard_assignments AS assignment
      ON assignment.train_run_id=seat.train_run_id AND assignment.shard_id='legacy'
    UNION ALL
    SELECT seat.id, seat.reservation_id, seat.train_run_id, seat.seat_id, seat.segment_mask
    FROM booking_shard_0.reservation_seats AS seat
    JOIN public.train_run_shard_assignments AS assignment
      ON assignment.train_run_id=seat.train_run_id AND assignment.shard_id='shard-0'
    UNION ALL
    SELECT seat.id, seat.reservation_id, seat.train_run_id, seat.seat_id, seat.segment_mask
    FROM booking_shard_1.reservation_seats AS seat
    JOIN public.train_run_shard_assignments AS assignment
      ON assignment.train_run_id=seat.train_run_id AND assignment.shard_id='shard-1'
), overlap_violations AS (
    SELECT left_seat.id
    FROM authoritative_seats AS left_seat
    JOIN authoritative_seats AS right_seat
      ON right_seat.train_run_id=left_seat.train_run_id
     AND right_seat.seat_id=left_seat.seat_id
     AND right_seat.id>left_seat.id
    JOIN authoritative_reservations AS left_reservation
      ON left_reservation.id=left_seat.reservation_id AND left_reservation.status IN ('held','confirmed')
    JOIN authoritative_reservations AS right_reservation
      ON right_reservation.id=right_seat.reservation_id AND right_reservation.status IN ('held','confirmed')
    WHERE bit_count(left_seat.segment_mask & right_seat.segment_mask)>0
)
SELECT json_build_object(
    'assignment_count', (SELECT count(*) FROM public.train_run_shard_assignments WHERE train_run_id IN ('$fixtureTrainA','$fixtureTrainB')),
    'authoritative_reservation_count', (SELECT count(*) FROM authoritative_reservations),
    'duplicate_authoritative_reservation_ids', (SELECT count(*) FROM (SELECT id FROM authoritative_reservations GROUP BY id HAVING count(*)>1) AS duplicate),
    'overlap_violations', (SELECT count(*) FROM overlap_violations),
    'missing_reservation_locators', (SELECT count(*) FROM authoritative_reservations AS reservation LEFT JOIN public.reservation_shard_locators AS locator ON locator.reservation_id=reservation.id WHERE locator.reservation_id IS NULL),
    'target_write_generations', (SELECT count(*) FROM public.train_run_generation_writes WHERE train_run_id IN ('$fixtureTrainA','$fixtureTrainB'))
)::text;
"@
    $integrityLine = [string](@($integrityResult.Output | Where-Object {
        ([string]$_).TrimStart().StartsWith('{')
    }) | Select-Object -Last 1)
    if ([string]::IsNullOrWhiteSpace($integrityLine)) { throw 'integrity evidence omitted its JSON result' }
    $integrity = $integrityLine | ConvertFrom-Json
    if ([int64]$integrity.assignment_count -ne 2 -or
        [int64]$integrity.duplicate_authoritative_reservation_ids -ne 0 -or
        [int64]$integrity.overlap_violations -ne 0 -or
        [int64]$integrity.missing_reservation_locators -ne 0 -or
        [int64]$integrity.target_write_generations -ne 2) {
        throw 'authoritative duplicate, overlap, locator, assignment, or target-write evidence check failed'
    }
    Save-Metrics -Label 'final'

    $names = @(
        'shard-routing', 'shard-route-cache', 'shard-cutover', 'stale-router-refresh',
        'legacy-vs-schema-shard', 'two-hot-train-shards', 'cross-shard-healthy',
        'shard-outage-isolation', 'cross-shard-partial'
    )
    $workloads = [ordered]@{}
    foreach ($name in $names) { $workloads[$name] = Get-K6Summary -Name $name }
    $commit = (& git rev-parse HEAD 2>$null | Out-String).Trim()
    [ordered]@{
        milestone = 4
        status = 'passed'
        commit_sha = $commit
        topology = [ordered]@{ api_replicas = 3; admission_workers = 2; logical_booking_shards = 3 }
        barriers = [ordered]@{
            train_a_copy = 'cutover_ready_before_overlap_workload'
            overlap_workload = 'route_metric_increment_observed_before_cutover'
            train_a_cutover = 'rollback_window'
            train_b_cutover = 'rollback_window'
            outage_restore = 'completed_before_reconciliation'
        }
        route_cache_hit_count = $routeCacheHits
        stale_router_refresh_count_delta = ($staleAfter - $staleBefore)
        workloads = $workloads
        integrity_artifact = 'integrity-evidence.json'
        reconciliation_artifacts = @('train-a-reconcile.json', 'train-b-reconcile.json')
        limitations = @(
            'Bounded local functional and latency smoke; it is not production capacity evidence.',
            'Schema shards share one PostgreSQL process, so outage injection proves logical catalog isolation rather than physical database-host isolation.',
            'Reported p95 and p99 values apply only to this synthetic fixture, topology, and bounded duration.'
        )
    } | ConvertTo-Json -Depth 8 |
        Out-File -LiteralPath (Join-Path $EvidenceDirectory 'milestone-4-summary.json') -Encoding utf8

    $succeeded = $true
    $failureCategory = 'none'
    Write-EvidenceStatus -Status 'passed' -Reason 'bounded_evidence_completed'
    Assert-ArtifactsSanitized
    Write-Host "Milestone 4 bounded evidence passed. Artifacts: $EvidenceDirectory"
}
catch {
    if ($failureCategory -eq 'not_started') { $failureCategory = 'evidence_step_failed' }
    $status = if ($failureCategory -eq 'operator_cli_unrun') { 'blocked' } else { 'failed' }
    Write-EvidenceStatus -Status $status -Reason $failureCategory
    try {
        Assert-ArtifactsSanitized
    } catch {
        $failureCategory = 'artifact_sanitization_failed'
        Write-EvidenceStatus -Status 'failed' -Reason $failureCategory
    }
    throw "Milestone 4 evidence did not complete; inspect sanitized artifacts (category=$failureCategory)"
}
finally {
    foreach ($name in $environmentNames) {
        [Environment]::SetEnvironmentVariable($name, $savedEnvironment[$name], 'Process')
    }
    $secretValues.Clear()
    if ($started -and -not $KeepEnvironment) {
        try {
            Invoke-Compose -AllowFailure -Arguments @('down', '--volumes', '--remove-orphans') `
                -CapturePath (Join-Path $EvidenceDirectory 'compose-down.log') | Out-Null
        } catch {
            # Preserve the primary evidence outcome; teardown status remains inspectable.
        }
    }
    if ((Get-Location).Path -eq $root) { Pop-Location }
}

if (-not $succeeded) { exit 1 }
