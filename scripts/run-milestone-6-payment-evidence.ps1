[CmdletBinding()]
param(
    [string]$ProjectName = '',
    [string]$EvidenceDirectory = '',
    [ValidateRange(1, 10)][int]$IterationsPerScenario = 1,
    [switch]$SkipBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$root = [System.IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$sourceDigestExclusions = @(
    'docs/benchmark-report-milestone-6.md',
    'docs/milestone-6-load-testing.md'
)
$composeFile = Join-Path $root 'docker-compose.payment.yml'
$driverPath = Join-Path $PSScriptRoot 'milestone-5-physical-shard-evidence-driver.ps1'
. $driverPath

$suffix = [guid]::NewGuid().ToString('N').Substring(0, 10)
if ([string]::IsNullOrWhiteSpace($ProjectName)) { $ProjectName = "railway-m6-evidence-$suffix" }
if ($ProjectName -notmatch '^[a-z0-9][a-z0-9_-]{2,62}$') { throw 'ProjectName is invalid' }
if ([string]::IsNullOrWhiteSpace($EvidenceDirectory)) {
    $EvidenceDirectory = Join-Path ([System.IO.Path]::GetTempPath()) "railway-m6-evidence-$suffix"
}
$EvidenceDirectory = [System.IO.Path]::GetFullPath($EvidenceDirectory)
$rootPrefix = $root.TrimEnd([System.IO.Path]::DirectorySeparatorChar) + [System.IO.Path]::DirectorySeparatorChar
if ($EvidenceDirectory.StartsWith($rootPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw 'EvidenceDirectory must be outside the source repository'
}
if (Test-Path -LiteralPath $EvidenceDirectory) { throw 'EvidenceDirectory must not already exist' }
New-Item -ItemType Directory -Path $EvidenceDirectory | Out-Null

$composeArguments = @('compose', '-p', $ProjectName, '-f', $composeFile)
$context = [pscustomobject]@{
    RepositoryPath = $root
    RawDirectory = $EvidenceDirectory
    ProjectName = $ProjectName
    ComposeFile = $composeFile
    ComposeArguments = [string[]]$composeArguments
}
$started = $false
$migrationJob = $null
$scenarioResults = [ordered]@{}
$sensitiveValues = [System.Collections.Generic.List[string]]::new()
$originalJWTSecret = $env:JWT_SECRET
try {
$runJWTSecret = "m6-$([guid]::NewGuid().ToString('N'))-$([guid]::NewGuid().ToString('N'))"
$env:JWT_SECRET = $runJWTSecret
$sensitiveValues.Add($runJWTSecret)
if ($env:GITHUB_ACTIONS -eq 'true') { Write-Output "::add-mask::$runJWTSecret" }
$latestPoolMetrics = @{}
$poolMetricSamples = 0
$maxObservedAcquiredConnections = 0.0
$paymentMetricSamples = 0
$runError = $null

function Invoke-M6Native {
    param([scriptblock]$Command, [switch]$AllowFailure)
    $previous = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = @(& $Command 2>&1 | ForEach-Object { [string]$_ })
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    if ($exitCode -ne 0 -and -not $AllowFailure) { throw "native command failed with exit code $exitCode" }
    [pscustomobject]@{ Output = $output; ExitCode = $exitCode }
}

function Save-M6NativeVersion {
    param([string]$Name, [scriptblock]$Command)
    $result = Invoke-M6Native -AllowFailure -Command $Command
    [ordered]@{ name=$Name; exit_code=$result.ExitCode; output=@($result.Output | Select-Object -First 8) }
}

function Get-M6TextSHA256 {
    param([Parameter(Mandatory=$true)][string]$Text)
    $sha256 = [System.Security.Cryptography.SHA256]::Create()
    try { $digest = $sha256.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($Text)) } finally { $sha256.Dispose() }
    return (($digest | ForEach-Object { $_.ToString('x2') }) -join '')
}

function New-M6CustomerReservations {
    param([string]$BaseURL, [string]$TrainRunID, [int]$Count, [int]$FixtureIndex)
    if ($Count -lt 1 -or $Count -gt 20) { throw 'reservation fixture count is outside the evidence bound' }
    $password = "M6-$([guid]::NewGuid().ToString('N').Substring(0, 14))-Aa1!"
    $email = "m6-evidence-$suffix-$FixtureIndex@example.test"
    $ip = "198.19.6.$($FixtureIndex + 20)"
    Invoke-Milestone5DriverAPI -BaseURL $BaseURL -Method POST -Path '/api/v1/auth/register' `
        -ForwardedFor $ip -Body @{ email=$email; password=$password; display_name="M6 Evidence Rider $FixtureIndex" } `
        -ExpectedStatus @(202) | Out-Null
    $login = Invoke-Milestone5DriverAPI -BaseURL $BaseURL -Method POST -Path '/api/v1/auth/login' `
        -ForwardedFor $ip -Body @{ email=$email; password=$password } -ExpectedStatus @(200)
    $token = [string]$login.Body.access_token
    if (-not $token) { throw 'synthetic customer login omitted its access token' }
    $sensitiveValues.Add($token)
    $reservations = [System.Collections.Generic.List[string]]::new()
    for ($slot = 0; $slot -lt $Count; $slot++) {
        $passenger = Invoke-Milestone5DriverAPI -BaseURL $BaseURL -Method POST -Path '/api/v1/passengers' `
            -Token $token -Body @{ display_name="M6 Passenger $FixtureIndex-$slot" } -ExpectedStatus @(201)
        $reservation = Invoke-Milestone5DriverAPI -BaseURL $BaseURL -Method POST -Path '/api/v1/reservations' `
            -Token $token -IdempotencyKey "m6-evidence-reservation-$suffix-$FixtureIndex-$slot" -Body @{
                train_run_id=$TrainRunID; origin_station_code='M2A'; destination_station_code='M2B'
                seat_class='standard'; passenger_ids=@([string]$passenger.Body.id)
            } -ExpectedStatus @(201)
        $reservations.Add([string]$reservation.Body.id)
    }
    $password = $null
    [pscustomobject]@{ Token=$token; ReservationIDs=[string[]]$reservations }
}

function Join-M6FixtureValues {
    param([string[]]$Values)
    return ($Values -join ',')
}

function Get-M6MetricValues {
    param($Metric)
    if ($null -eq $Metric) { throw 'required k6 metric is missing' }
    $valuesProperty = $Metric.PSObject.Properties['values']
    if ($null -ne $valuesProperty) { return $valuesProperty.Value }
    return $Metric
}

function Invoke-M6K6 {
    param([string]$Script, [hashtable]$Environment)
    $network = "${ProjectName}_backend"
    $arguments = @('run', '--rm', '--network', $network,
        '-v', "${root}:/repo:ro", '-v', "${EvidenceDirectory}:/evidence", '-w', '/repo')
    foreach ($entry in $Environment.GetEnumerator()) { $arguments += @('-e', "$($entry.Key)=$($entry.Value)") }
    $name = [System.IO.Path]::GetFileNameWithoutExtension($Script)
    $arguments += @('grafana/k6:0.55.0', 'run', '--quiet',
        '--summary-export', "/evidence/$name-summary.json", "/repo/loadtest/k6/$Script")
    $result = Invoke-M6Native -AllowFailure -Command { & docker @arguments }
    $result.Output | Set-Content -LiteralPath (Join-Path $EvidenceDirectory "$name.log") -Encoding utf8
    if ($result.ExitCode -ne 0) { throw "$Script failed" }
    $summaryPath = Join-Path $EvidenceDirectory "$name-summary.json"
    if (-not (Test-Path -LiteralPath $summaryPath)) { throw "$Script did not produce its bounded JSON summary" }
    $summary = Get-Content -Raw -LiteralPath $summaryPath | ConvertFrom-Json
    $checks = Get-M6MetricValues -Metric $summary.metrics.PSObject.Properties['checks'].Value
    if ($null -eq $checks -or [int64]$checks.passes -lt 1 -or [int64]$checks.fails -ne 0) {
        throw "$Script did not pass every k6 check"
    }
    $checkRate = [double]$checks.passes / ([double]$checks.passes + [double]$checks.fails)
    $iterationsValues = Get-M6MetricValues -Metric $summary.metrics.PSObject.Properties['iterations'].Value
    $requestsValues = Get-M6MetricValues -Metric $summary.metrics.PSObject.Properties['http_reqs'].Value
    $durationValues = Get-M6MetricValues -Metric $summary.metrics.PSObject.Properties['payment_http_request_duration'].Value
    $scenarioResult = [ordered]@{
        status='passed'
        checks=[ordered]@{ passes=[int64]$checks.passes; fails=[int64]$checks.fails; rate=$checkRate }
        iterations=[int64]$iterationsValues.count
        http_requests=[int64]$requestsValues.count
        http_request_duration_ms=[ordered]@{
            p50=[double]$durationValues.med
            p95=[double]$durationValues.'p(95)'
            p99=[double]$durationValues.'p(99)'
        }
    }
    $convergenceProperty = $summary.metrics.PSObject.Properties['payment_convergence_duration']
    if ($null -ne $convergenceProperty) {
        $convergenceValues = Get-M6MetricValues -Metric $convergenceProperty.Value
        $scenarioResult['convergence_duration_ms'] = [ordered]@{
            p50=[double]$convergenceValues.med
            p95=[double]$convergenceValues.'p(95)'
            p99=[double]$convergenceValues.'p(99)'
        }
    }
    $scenarioResults[$name] = $scenarioResult
}

function Save-M6ControlSnapshot {
    param([string]$Prefix)
    Invoke-Milestone5DriverPSQL -Context $context -Service 'control-postgres' `
        -Artifact "$Prefix-control-invariants.log" -SQL @"
SELECT 'intent_state|'||state||'|'||count(*) FROM public.payment_intents GROUP BY state
UNION ALL SELECT 'saga_state|'||state||'|'||current_step||'|'||count(*) FROM public.payment_sagas GROUP BY state,current_step
UNION ALL SELECT 'operation_state|'||operation_type||'|'||state||'|'||count(*) FROM public.payment_operations GROUP BY operation_type,state
UNION ALL SELECT 'webhook_state|'||state||'|'||count(*) FROM public.payment_webhook_inbox GROUP BY state
UNION ALL SELECT 'webhook_conflicts|'||count(*) FROM public.payment_provider_event_conflicts
UNION ALL SELECT 'manual_review|'||reason_category||'|'||count(*) FROM public.payment_manual_review_cases GROUP BY reason_category
UNION ALL SELECT 'reconciliation|'||scope||'|'||state||'|'||count(*) FROM public.payment_reconciliation_checkpoints GROUP BY scope,state
UNION ALL SELECT 'order_locators|'||status||'|'||count(*) FROM public.ticket_order_shard_locators GROUP BY status
UNION ALL SELECT 'ticket_locators|'||count(*) FROM public.ticket_shard_locators
UNION ALL SELECT 'ticket_code_directory|'||count(*) FROM public.ticket_code_directory
ORDER BY 1;
"@ | Out-Null
    Invoke-Milestone5DriverPSQL -Context $context -Service 'control-postgres' `
        -Artifact "$Prefix-control-pool.log" -SQL @"
SELECT coalesce(application_name,'unset')||'|'||state||'|'||count(*)
FROM pg_stat_activity WHERE datname=current_database()
GROUP BY application_name,state ORDER BY 1;
"@ | Out-Null
}

function Save-M6ShardSnapshot {
    param([string]$Prefix)
    foreach ($service in @('booking-shard-0-postgres','booking-shard-1-postgres')) {
        Invoke-Milestone5DriverPSQL -Context $context -Service $service `
            -Artifact "$Prefix-$service-invariants.log" -SQL @"
SELECT 'reservation|'||status||'|'||count(*) FROM public.reservations GROUP BY status
UNION ALL SELECT 'order|'||status||'|'||count(*) FROM public.ticket_orders GROUP BY status
UNION ALL SELECT 'ticket|'||status||'|'||count(*) FROM public.tickets GROUP BY status
UNION ALL SELECT 'issuance_receipt|'||count(*) FROM public.ticket_issuance_receipts
UNION ALL SELECT 'refund_receipt|'||count(*) FROM public.payment_refund_receipts
UNION ALL SELECT 'compensation_receipt|'||count(*) FROM public.payment_compensation_receipts
UNION ALL SELECT 'journal_gap|'||count(*) FROM (
  SELECT migration_id FROM public.train_run_mutation_journal
  GROUP BY migration_id HAVING max(mutation_sequence)-min(mutation_sequence)+1<>count(*)
) AS gaps
ORDER BY 1;
"@ | Out-Null
        Invoke-Milestone5DriverPSQL -Context $context -Service $service `
            -Artifact "$Prefix-$service-pool.log" -SQL @"
SELECT coalesce(application_name,'unset')||'|'||state||'|'||count(*)
FROM pg_stat_activity WHERE datname=current_database()
GROUP BY application_name,state ORDER BY 1;
"@ | Out-Null
    }
}

function Save-M6PoolMetricsSnapshot {
    param([string]$Prefix)
    $services = [ordered]@{
        'api-1'='http://127.0.0.1:8080/metrics'
        'api-2'='http://127.0.0.1:8080/metrics'
        'api-3'='http://127.0.0.1:8080/metrics'
        'payment-worker-1'='http://127.0.0.1:9090/metrics'
        'payment-worker-2'='http://127.0.0.1:9090/metrics'
    }
    $pattern = '^(database_pool_(?:acquired_connections|idle_connections|total_connections|max_connections|acquire_total|acquire_duration_seconds|empty_acquire_total|cancelled_acquire_total|peak_acquired_connections))\{database_role="(control|booking_shard)",shard_id="(none|physical-shard-[01])"\} ([0-9eE+.-]+)$'
    foreach ($entry in $services.GetEnumerator()) {
        $service = [string]$entry.Key
        $endpoint = [string]$entry.Value
        $result = Invoke-M6Native -AllowFailure -Command { & docker @composeArguments exec -T $service wget -qO- $endpoint }
        if ($result.ExitCode -ne 0) { throw "unable to scrape bounded pool metrics from $service" }
        $safeLines = [System.Collections.Generic.List[string]]::new()
        $paymentLines = [System.Collections.Generic.List[string]]::new()
        foreach ($rawLine in $result.Output) {
            $line = ([string]$rawLine).Trim()
            $match = [regex]::Match($line, $pattern)
            if ($match.Success) {
                $metric = $match.Groups[1].Value
                $role = $match.Groups[2].Value
                $shard = $match.Groups[3].Value
                if (($role -eq 'control' -and $shard -ne 'none') -or ($role -eq 'booking_shard' -and $shard -eq 'none')) {
                    throw "pool metric labels are inconsistent for $service"
                }
                $value = [double]::Parse($match.Groups[4].Value, [Globalization.CultureInfo]::InvariantCulture)
                $safeLines.Add($line)
                $latestPoolMetrics["$service|$role|$shard|$metric"] = $value
                $script:poolMetricSamples++
                if ($metric -eq 'database_pool_acquired_connections' -and $value -gt $script:maxObservedAcquiredConnections) {
                    $script:maxObservedAcquiredConnections = $value
                }
                continue
            }
            $paymentMatch = [regex]::Match($line, '^(payment_[a-z0-9_]+|ticket_issuance_[a-z0-9_]+)(\{[^}]+\})? ([0-9eE+.-]+)$')
            if (-not $paymentMatch.Success) { continue }
            $labels = $paymentMatch.Groups[2].Value
            if ($labels -and $labels -notmatch '^\{(?:(?:provider|operation|result|state|from_state|to_state|error_category|event_type|reconciliation_type|le)="[A-Za-z0-9._:+-]+"(?:,|\}))*$') {
                throw "payment metric labels escaped the bounded allowlist for $service"
            }
            $paymentLines.Add($line)
            $script:paymentMetricSamples++
        }
        if ($safeLines.Count -lt 9) { throw "bounded pool metrics are incomplete for $service" }
        $safeLines | Set-Content -LiteralPath (Join-Path $EvidenceDirectory "$Prefix-$service-pool-metrics.prom") -Encoding ascii
        if ($paymentLines.Count -gt 0) {
            $paymentLines | Set-Content -LiteralPath (Join-Path $EvidenceDirectory "$Prefix-$service-payment-metrics.prom") -Encoding ascii
        }
    }
}

function Get-M6PoolEvidence {
    $records = [System.Collections.Generic.List[object]]::new()
    $identities = @($latestPoolMetrics.Keys | ForEach-Object {
        $parts = ([string]$_).Split('|')
        "$($parts[0])|$($parts[1])|$($parts[2])"
    } | Sort-Object -Unique)
    foreach ($identity in $identities) {
        $parts = $identity.Split('|')
        $service = $parts[0]; $role = $parts[1]; $shard = $parts[2]
        $record = [ordered]@{ process=$service; database_role=$role; shard_id=$shard }
        foreach ($metric in @(
            'database_pool_acquired_connections','database_pool_idle_connections','database_pool_total_connections',
            'database_pool_max_connections','database_pool_acquire_total','database_pool_acquire_duration_seconds',
            'database_pool_empty_acquire_total','database_pool_cancelled_acquire_total','database_pool_peak_acquired_connections'
        )) {
            $key = "$service|$role|$shard|$metric"
            if (-not $latestPoolMetrics.ContainsKey($key)) { throw "final pool evidence is missing $key" }
            $record[$metric.Replace('database_pool_','')] = [double]$latestPoolMetrics[$key]
        }
        $records.Add([pscustomobject]$record)
    }
    $sumAcquireDuration = [double](($records | Measure-Object -Property acquire_duration_seconds -Sum).Sum)
    $sumAcquireCount = [double](($records | Measure-Object -Property acquire_total -Sum).Sum)
    $sumEmptyAcquire = [double](($records | Measure-Object -Property empty_acquire_total -Sum).Sum)
    $sumCancelledAcquire = [double](($records | Measure-Object -Property cancelled_acquire_total -Sum).Sum)
    $maxProcessPeak = [double](($records | Measure-Object -Property peak_acquired_connections -Maximum).Maximum)
    return [ordered]@{
        sample_count=$poolMetricSamples
        max_sampled_acquired_connections_per_pool=$maxObservedAcquiredConnections
        max_process_peak_acquired_connections_per_pool=$maxProcessPeak
        acquire_count=$sumAcquireCount
        acquire_duration_seconds=[Math]::Round($sumAcquireDuration,6)
        empty_acquire_count=$sumEmptyAcquire
        cancelled_acquire_count=$sumCancelledAcquire
        records=$records
        interpretation='bounded local per-process pgx observations; not a production capacity claim'
    }
}

function Save-M6Snapshot {
    param([string]$Prefix)
    Save-M6ControlSnapshot -Prefix $Prefix
    Save-M6ShardSnapshot -Prefix $Prefix
    Save-M6PoolMetricsSnapshot -Prefix $Prefix
    $stats = Invoke-M6Native -AllowFailure -Command { & docker stats --no-stream --format '{{json .}}' }
    @($stats.Output | Where-Object { $_ -match [regex]::Escape($ProjectName) }) |
        Set-Content -LiteralPath (Join-Path $EvidenceDirectory "$Prefix-container-stats.jsonl") -Encoding utf8
}

function Assert-M6FinalInvariants {
    $roleViolations = Get-Milestone5DriverScalar -Context $context -Service 'control-postgres' `
        -Artifact 'final-reconciler-control-role-violation-count.log' -SQL @"
SELECT
 (CASE WHEN has_table_privilege('payment_reconciler','public.users','SELECT') THEN 1 ELSE 0 END)
+(CASE WHEN has_table_privilege('payment_reconciler','public.payment_reconciliation_checkpoints','INSERT,UPDATE') THEN 0 ELSE 1 END)
+(CASE WHEN has_table_privilege('payment_reconciler','public.payment_manual_review_cases','INSERT') THEN 0 ELSE 1 END)
+(CASE WHEN has_table_privilege('payment_reconciler','public.payment_manual_review_cases','UPDATE') THEN 1 ELSE 0 END)
;
"@
    if ($roleViolations -ne 0) { throw "payment reconciler control role reported $roleViolations privilege violations" }
    $violations = Get-Milestone5DriverScalar -Context $context -Service 'control-postgres' `
        -Artifact 'final-control-violation-count.log' -SQL @"
SELECT
 (SELECT count(*) FROM (SELECT reservation_id FROM public.payment_intents WHERE state NOT IN ('completed','voided','refunded','cancelled','failed','expired') GROUP BY reservation_id HAVING count(*)>1) AS duplicate_active_intent)
+(SELECT count(*) FROM (SELECT payment_intent_id,operation_type FROM public.payment_operations WHERE operation_type IN ('authorize','capture','void','refund') GROUP BY payment_intent_id,operation_type HAVING count(*)>1) AS duplicate_financial_operation)
+(SELECT count(*) FROM public.payment_operations AS operation JOIN public.payment_intents AS intent USING(payment_intent_id) WHERE operation.amount_minor<>intent.amount_minor OR operation.currency<>intent.currency)
+(SELECT count(*) FROM public.payment_operations WHERE state IN ('claimed','in_flight') AND lease_until<clock_timestamp())
+(SELECT count(*) FROM public.payment_sagas WHERE lease_owner IS NOT NULL AND lease_until<clock_timestamp())
+(SELECT count(*) FROM public.payment_webhook_inbox WHERE state='processing' AND lease_until<clock_timestamp())
+(SELECT count(*) FROM public.payment_operations WHERE operation_type='refund' AND amount_minor<0)
+(SELECT count(*) FROM public.ticket_code_claim_readiness WHERE state<>'ready')
+(SELECT count(*) FROM (SELECT migration_id FROM public.physical_source_train_run_mutation_journal GROUP BY migration_id HAVING max(mutation_sequence)-min(mutation_sequence)+1<>count(*)) AS journal_gap)
+(SELECT CASE WHEN count(*)>140 THEN 1 ELSE 0 END FROM pg_stat_activity WHERE datname=current_database())
;
"@
    if ($violations -ne 0) { throw "final control invariants reported $violations violations" }
    foreach ($service in @('booking-shard-0-postgres','booking-shard-1-postgres')) {
        $roleViolations = Get-Milestone5DriverScalar -Context $context -Service $service `
            -Artifact "final-$service-reconciler-role-violation-count.log" -SQL @"
SELECT
 (CASE WHEN has_table_privilege('payment_reconciler','public.reservations','SELECT') THEN 0 ELSE 1 END)
+(CASE WHEN has_table_privilege('payment_reconciler','public.reservations','UPDATE') THEN 1 ELSE 0 END)
;
"@
        if ($roleViolations -ne 0) { throw "$service payment reconciler role reported $roleViolations privilege violations" }
        $shardViolations = Get-Milestone5DriverScalar -Context $context -Service $service `
            -Artifact "final-$service-violation-count.log" -SQL @"
SELECT
 (SELECT count(*) FROM public.ticket_orders WHERE refunded_amount_minor<0 OR refunded_amount_minor>captured_amount_minor)
+(SELECT count(*) FROM (SELECT ticket_order_id,reservation_seat_id FROM public.tickets GROUP BY ticket_order_id,reservation_seat_id HAVING count(*)>1) AS duplicate_ticket)
+(SELECT count(*) FROM (SELECT ticket_code FROM public.tickets GROUP BY ticket_code HAVING count(*)>1) AS duplicate_code)
+(SELECT count(*) FROM (SELECT migration_id FROM public.train_run_mutation_journal GROUP BY migration_id HAVING max(mutation_sequence)-min(mutation_sequence)+1<>count(*)) AS journal_gap)
+(SELECT CASE WHEN count(*)>140 THEN 1 ELSE 0 END FROM pg_stat_activity WHERE datname=current_database())
;
"@
        if ($shardViolations -ne 0) { throw "$service final invariants reported $shardViolations violations" }
    }
}

function Assert-M6EvidenceIsSecretSafe {
    $violations = [System.Collections.Generic.List[string]]::new()
    foreach ($file in Get-ChildItem -LiteralPath $EvidenceDirectory -Recurse -File) {
        if ($file.Length -gt 25MB) { $violations.Add("oversized:$($file.Name)"); continue }
        $rawContent = Get-Content -Raw -LiteralPath $file.FullName
        $content = if ($null -eq $rawContent) { '' } else { [string]$rawContent }
        foreach ($secret in $sensitiveValues) {
            if ($secret.Length -ge 16 -and $content.Contains($secret)) { $violations.Add("fixture-secret:$($file.Name)") }
        }
        if ($content -match '(?i)authorization:\s*bearer\s+\S+|payment_provider_api_key\s*[=:]|payment_webhook_keyring\s*[=:]|postgres(?:ql)?://[^\s]+:[^\s]+@') {
            $violations.Add("credential-pattern:$($file.Name)")
        }
    }
    [ordered]@{ status=if ($violations.Count -eq 0) {'passed'} else {'failed'}; violations=[string[]]$violations } |
        ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $EvidenceDirectory 'secret-scan.json') -Encoding utf8
    if ($violations.Count -ne 0) { throw 'evidence secret scan failed' }
}

function Write-M6EvidenceIndex {
    $items = [System.Collections.Generic.List[object]]::new()
    foreach ($file in Get-ChildItem -LiteralPath $EvidenceDirectory -Recurse -File | Sort-Object FullName) {
        if ($file.Name -eq 'evidence-index.json') { continue }
        $baseLength = $EvidenceDirectory.TrimEnd([System.IO.Path]::DirectorySeparatorChar).Length
        $relative = $file.FullName.Substring($baseLength).TrimStart([System.IO.Path]::DirectorySeparatorChar).Replace('\','/')
        $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $file.FullName).Hash.ToLowerInvariant()
        $items.Add([ordered]@{ path=$relative; bytes=$file.Length; sha256=$hash })
    }
    $canonical = (($items | ForEach-Object { "$($_.path)|$($_.bytes)|$($_.sha256)" }) -join "`n")
    $sha256 = [System.Security.Cryptography.SHA256]::Create()
    try { $digestBytes = $sha256.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($canonical)) } finally { $sha256.Dispose() }
    $bundleHash = (($digestBytes | ForEach-Object { $_.ToString('x2') }) -join '')
    [ordered]@{ status='complete'; file_count=$items.Count; bundle_sha256=$bundleHash; files=@($items) } |
        ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $EvidenceDirectory 'evidence-index.json') -Encoding utf8
}

function Get-M6SourceState {
    $paths = @(& git -C $root ls-files --cached --others --exclude-standard)
    if ($LASTEXITCODE -ne 0 -or $paths.Count -eq 0) { throw 'source-state inventory failed' }
    $entries = [System.Collections.Generic.List[string]]::new()
    foreach ($relative in @($paths | Sort-Object -Unique)) {
        $normalized = ([string]$relative).Replace('\','/')
        if ($sourceDigestExclusions -contains $normalized) { continue }
        $full = Join-Path $root ([string]$relative)
        if (-not [System.IO.File]::Exists($full)) {
            $entries.Add("$normalized|missing")
            continue
        }
        $file = [System.IO.FileInfo]::new($full)
        $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $full).Hash.ToLowerInvariant()
        $entries.Add("$normalized|$($file.Length)|$hash")
    }
    $canonical = ($entries -join "`n")
    $sha256 = [System.Security.Cryptography.SHA256]::Create()
    try { $digest = $sha256.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($canonical)) } finally { $sha256.Dispose() }
    return [pscustomobject]@{
        FileCount = $entries.Count
        SHA256 = (($digest | ForEach-Object { $_.ToString('x2') }) -join '')
    }
}

try {
    $projectLabel = "label=com.docker.compose.project=$ProjectName"
    foreach ($query in @(
        @('ps','-a','-q','--filter',$projectLabel),
        @('volume','ls','-q','--filter',$projectLabel),
        @('network','ls','-q','--filter',$projectLabel)
    )) {
        $owned = Invoke-M6Native -Command { & docker @query }
        if (@($owned.Output | Where-Object { $_ }).Count -ne 0) { throw 'ProjectName already owns Docker resources' }
    }

    $composeWrapperHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $composeFile).Hash.ToLowerInvariant()
    $sourceState = Get-M6SourceState
    $renderedConfig = Invoke-M6Native -Command { & docker @composeArguments config }
    if ($renderedConfig.Output.Count -lt 1) { throw 'rendered Compose config is empty' }
    $renderedComposeConfigHash = Get-M6TextSHA256 -Text ($renderedConfig.Output -join "`n")
    'status=passed' | Set-Content -LiteralPath (Join-Path $EvidenceDirectory 'compose-config-check.log') -Encoding utf8
    $dockerCapacityResult = Invoke-M6Native -Command { & docker info --format '{{.NCPU}}|{{.MemTotal}}|{{.OperatingSystem}}|{{.Architecture}}' }
    $dockerCapacity = ([string]$dockerCapacityResult.Output[0]).Split('|')
    if ($dockerCapacity.Count -ne 4) { throw 'Docker host capacity evidence is malformed' }
    $drive = [System.IO.DriveInfo]::new([System.IO.Path]::GetPathRoot($root))
    $start = (Get-Date).ToUniversalTime()
    [ordered]@{
        status='running'; source_commit=(git -C $root rev-parse HEAD).Trim()
        source_state_sha256=$sourceState.SHA256; source_file_count=$sourceState.FileCount
        source_digest_exclusions=[string[]]$sourceDigestExclusions
        worktree_status=@(git -C $root status --short)
        started_at=$start.ToString('o'); timezone=[System.TimeZoneInfo]::Local.Id
        compose_wrapper_sha256=$composeWrapperHash
        rendered_compose_config_sha256=$renderedComposeConfigHash
        fixture_seed=$suffix
        topology=[ordered]@{ api_replicas=3; payment_workers=2; booking_shards=2; reconciler=1; provider='sandbox' }
        build_mode=if ($SkipBuild) {'prebuilt-image-digests'} else {'source-build'}
        pool_caps=[ordered]@{ control_max_open_per_process=4; shard_max_open_per_process=3; shard_max_idle_per_process=2; shard_total_per_process=6; postgres_max_connections=140 }
        versions=@(
            (Save-M6NativeVersion -Name docker -Command { docker version --format '{{.Server.Version}}' }),
            (Save-M6NativeVersion -Name compose -Command { docker compose version --short }),
            (Save-M6NativeVersion -Name go -Command { go version })
        )
        host=[ordered]@{
            os=[System.Environment]::OSVersion.VersionString; processors=[Environment]::ProcessorCount
            repository_drive_total_bytes=$drive.TotalSize; repository_drive_free_bytes=$drive.AvailableFreeSpace
        }
        docker_engine=[ordered]@{
            cpus=[int]$dockerCapacity[0]; memory_bytes=[int64]$dockerCapacity[1]
            os=$dockerCapacity[2]; architecture=$dockerCapacity[3]
        }
    } | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $EvidenceDirectory 'run-manifest.json') -Encoding utf8

    $started = $true
    $upArguments = @('up','-d')
    if ($SkipBuild) { $upArguments += '--no-build' } else { $upArguments += '--build' }
    $upArguments += '--wait'
    $up = Invoke-M6Native -AllowFailure -Command { & docker @composeArguments @upArguments }
    $up.Output | Set-Content -LiteralPath (Join-Path $EvidenceDirectory 'compose-up.log') -Encoding utf8
    if ($up.ExitCode -ne 0) { throw 'payment topology did not start' }
    $images = Invoke-M6Native -AllowFailure -Command { & docker @composeArguments images --format json }
    $images.Output | Set-Content -LiteralPath (Join-Path $EvidenceDirectory 'compose-images.jsonl') -Encoding utf8
    if ($images.ExitCode -ne 0) { throw 'payment topology image digest inventory failed' }
    $running = Invoke-Milestone5DriverCompose -Context $context -Arguments @('ps','--status','running','--services') -Artifact 'topology-services.log'
    $runningServices = @($running.Output | ForEach-Object { ([string]$_).Trim() } | Where-Object { $_ })
    foreach ($service in @('api-1','api-2','api-3','payment-worker-1','payment-worker-2','payment-sandbox','payment-reconciler','redis','control-postgres','booking-shard-0-postgres','booking-shard-1-postgres')) {
        if ($service -notin $runningServices) { throw "required service $service is not running" }
    }
    Wait-Milestone5DriverReady -Context $context
    if (-not $SkipBuild) {
        Invoke-Milestone5DriverCompose -Context $context -Arguments @('--profile','tools','build','physical-shard-admin') -Artifact 'physical-shard-admin-build.log' | Out-Null
    } else {
        'prebuilt image required and verified by the subsequent tool invocation' |
            Set-Content -LiteralPath (Join-Path $EvidenceDirectory 'physical-shard-admin-build.log') -Encoding ascii
    }
    Initialize-Milestone5DriverFixture -Context $context
    Invoke-Milestone5DriverPSQL -Context $context -Service 'control-postgres' -Artifact 'postgres-settings.log' -SQL @"
SELECT 'server_version|'||current_setting('server_version')
UNION ALL SELECT 'max_connections|'||current_setting('max_connections')
UNION ALL SELECT 'shared_buffers|'||current_setting('shared_buffers')
UNION ALL SELECT 'work_mem|'||current_setting('work_mem')
UNION ALL SELECT 'wal_level|'||current_setting('wal_level')
ORDER BY 1;
"@ | Out-Null

    $trainA = '21000000-0000-4000-8000-000000000401'
    $trainB = '21000000-0000-4000-8000-000000000402'
    $trainC = '21000000-0000-4000-8000-000000000403'
    $migrationA = '61000000-0000-4000-8000-000000000411'
    $migrationB = '61000000-0000-4000-8000-000000000412'
    $migrationCInitial = '61000000-0000-4000-8000-000000000413'
    $migrationC = '61000000-0000-4000-8000-000000000414'
    New-Milestone5Migration -Context $context -TrainRunID $trainA -TargetShard 'physical-shard-0' -MigrationID $migrationA -Prefix 'm6-evidence-train-a'
    Move-Milestone5Migration -Context $context -MigrationID $migrationA -Target rollback_window -Prefix 'm6-evidence-train-a'
    New-Milestone5Migration -Context $context -TrainRunID $trainB -TargetShard 'physical-shard-1' -MigrationID $migrationB -Prefix 'm6-evidence-train-b'
    Move-Milestone5Migration -Context $context -MigrationID $migrationB -Target rollback_window -Prefix 'm6-evidence-train-b'
    New-Milestone5Migration -Context $context -TrainRunID $trainC -TargetShard 'physical-shard-1' -MigrationID $migrationCInitial -Prefix 'm6-evidence-train-c-initial'
    $bounded = Get-Milestone5DriverScalar -Context $context -Service 'control-postgres' -Artifact 'train-c-bounded-rollback.log' -SQL "WITH updated AS (UPDATE public.physical_shard_migrations SET rollback_window_seconds=1 WHERE migration_id='$migrationCInitial'::uuid AND state='planned' RETURNING 1) SELECT count(*) FROM updated;"
    if ($bounded -ne 1) { throw 'initial migration rollback window was not bounded' }
    Move-Milestone5Migration -Context $context -MigrationID $migrationCInitial -Target rollback_window -Prefix 'm6-evidence-train-c-initial'
    Complete-Milestone5MigrationAfterRollbackWindow -Context $context -MigrationID $migrationCInitial -Prefix 'm6-evidence-train-c-initial'

    $baseURL = Get-Milestone5DriverPublishedURL -Context $context
    $replicas = 'http://api-1:8080,http://api-2:8080,http://api-3:8080'
    $common = @{
        BASE_URL='http://api-1:8080'; SANDBOX_URL='http://payment-sandbox:8099'
        SANDBOX_CONTROL_TOKEN='synthetic-disposable-fault-token'
        VUS='1'; ITERATIONS_PER_VU=[string]$IterationsPerScenario; DURATION='3m'; POLL_ATTEMPTS='100'
        PAYMENT_POLL_ATTEMPTS='120'; REFUND_POLL_ATTEMPTS='120'
    }
    $sensitiveValues.Add([string]$common.SANDBOX_CONTROL_TOKEN)
    Save-M6Snapshot -Prefix '00-baseline'

    $fixtureIndex = 0
    $singleScripts = @('payment-intent-create.js','payment-capture-recovery.js','ticket-issuance.js','payment-refund.js','multi-replica-payment.js')
    foreach ($script in $singleScripts) {
        $fixture = New-M6CustomerReservations -BaseURL $baseURL -TrainRunID $trainA -Count $IterationsPerScenario -FixtureIndex $fixtureIndex
        $fixtureIndex++
        $env = @{} + $common
        $env.CUSTOMER_TOKENS = $fixture.Token
        $env.RESERVATION_IDS = Join-M6FixtureValues $fixture.ReservationIDs
        if ($script -in @('payment-intent-create.js','ticket-issuance.js','multi-replica-payment.js')) { $env.BASE_URLS = $replicas }
        Invoke-M6K6 -Script $script -Environment $env
        Save-M6Snapshot -Prefix ([System.IO.Path]::GetFileNameWithoutExtension($script))
    }

    $paired = New-M6CustomerReservations -BaseURL $baseURL -TrainRunID $trainA -Count ($IterationsPerScenario * 2) -FixtureIndex $fixtureIndex
    $fixtureIndex++
    $env = @{} + $common; $env.CUSTOMER_TOKENS=$paired.Token; $env.RESERVATION_IDS=Join-M6FixtureValues $paired.ReservationIDs
    Invoke-M6K6 -Script 'payment-idempotency.js' -Environment $env
    Save-M6Snapshot -Prefix 'payment-idempotency'

    $webhookIterations = [Math]::Max(2, $IterationsPerScenario)
    $webhook = New-M6CustomerReservations -BaseURL $baseURL -TrainRunID $trainA -Count $webhookIterations -FixtureIndex $fixtureIndex
    $fixtureIndex++
    $env = @{} + $common; $env.ITERATIONS_PER_VU=[string]$webhookIterations; $env.CUSTOMER_TOKENS=$webhook.Token; $env.RESERVATION_IDS=Join-M6FixtureValues $webhook.ReservationIDs
    Invoke-M6K6 -Script 'payment-webhook-burst.js' -Environment $env
    Save-M6Snapshot -Prefix 'payment-webhook-burst'

    $providerIterations = [Math]::Max(3, $IterationsPerScenario)
    $provider = New-M6CustomerReservations -BaseURL $baseURL -TrainRunID $trainA -Count $providerIterations -FixtureIndex $fixtureIndex
    $fixtureIndex++
    $env = @{} + $common; $env.ITERATIONS_PER_VU=[string]$providerIterations; $env.CUSTOMER_TOKENS=$provider.Token; $env.RESERVATION_IDS=Join-M6FixtureValues $provider.ReservationIDs
    $env.PROVIDER_FAULT_KINDS='outage,invalid_response,oversized_response'
    Invoke-M6K6 -Script 'payment-provider-outage.js' -Environment $env
    Save-M6Snapshot -Prefix 'payment-provider-outage'

    $outage = New-M6CustomerReservations -BaseURL $baseURL -TrainRunID $trainB -Count $IterationsPerScenario -FixtureIndex $fixtureIndex
    $fixtureIndex++
    $healthy = New-M6CustomerReservations -BaseURL $baseURL -TrainRunID $trainA -Count $IterationsPerScenario -FixtureIndex $fixtureIndex
    $fixtureIndex++
    Invoke-Milestone5DriverCompose -Context $context -Arguments @('stop','-t','15','booking-shard-1-postgres') -Artifact 'payment-shard-outage-stop.log' | Out-Null
    try {
        $env = @{} + $common; $env.OUTAGE_CUSTOMER_TOKENS=$outage.Token; $env.OUTAGE_RESERVATION_IDS=Join-M6FixtureValues $outage.ReservationIDs
        $env.HEALTHY_CUSTOMER_TOKENS=$healthy.Token; $env.HEALTHY_RESERVATION_IDS=Join-M6FixtureValues $healthy.ReservationIDs
        Invoke-M6K6 -Script 'payment-shard-outage.js' -Environment $env
    } finally {
        Invoke-Milestone5DriverCompose -Context $context -Arguments @('start','booking-shard-1-postgres') -Artifact 'payment-shard-outage-start.log' | Out-Null
        Wait-Milestone5DriverReady -Context $context
    }
    Save-M6Snapshot -Prefix 'payment-shard-outage'

    $migrating = New-M6CustomerReservations -BaseURL $baseURL -TrainRunID $trainC -Count $IterationsPerScenario -FixtureIndex $fixtureIndex
    New-Milestone5Migration -Context $context -TrainRunID $trainC -TargetShard 'physical-shard-0' -MigrationID $migrationC -Prefix 'm6-evidence-train-c'
    Move-Milestone5Migration -Context $context -MigrationID $migrationC -Target validating_online -Prefix 'm6-evidence-train-c'
    $jobContext = $context
    $migrationJob = Start-Job -ScriptBlock {
        param($trustedDriver, $ctx, $migrationID)
        . $trustedDriver
        Start-Sleep -Milliseconds 750
        for ($attempt = 1; $attempt -le 5; $attempt++) {
            try {
                Move-Milestone5Migration -Context $ctx -MigrationID $migrationID -Target rollback_window -Prefix "m6-evidence-train-c-cutover-$attempt"
                return
            } catch {
                if ($attempt -eq 5) { throw }
                Start-Sleep -Milliseconds 500
            }
        }
    } -ArgumentList $driverPath,$jobContext,$migrationC
    $env = @{} + $common; $env.CUSTOMER_TOKENS=$migrating.Token; $env.RESERVATION_IDS=Join-M6FixtureValues $migrating.ReservationIDs
    $env.MIGRATION_PHASE='validating_to_cutover'; $env.MIGRATION_POLL_ATTEMPTS='120'
    Invoke-M6K6 -Script 'payment-during-migration.js' -Environment $env
    if (-not (Wait-Job -Job $migrationJob -Timeout 180)) { throw 'migration cutover timed out' }
    Receive-Job -Job $migrationJob -ErrorAction Stop | Out-Null
    if ($migrationJob.State -ne 'Completed') { throw "migration job ended in $($migrationJob.State)" }
    Save-M6Snapshot -Prefix 'payment-during-migration'

    Start-Sleep -Seconds 35
    Invoke-Milestone5DriverPSQL -Context $context -Service 'control-postgres' `
        -Artifact 'final-reconciliation-control-candidates.log' -SQL @"
SELECT intent.payment_intent_id||'|'||intent.train_run_id||'|'||intent.state||'|'||
       saga.state||'|'||saga.current_step||'|'||assignment.shard_id||'|'||assignment.assignment_generation
FROM public.payment_intents AS intent
JOIN public.payment_sagas AS saga ON saga.payment_intent_id=intent.payment_intent_id
JOIN public.train_run_shard_assignments AS assignment ON assignment.train_run_id=intent.train_run_id
ORDER BY intent.payment_intent_id;
"@ | Out-Null
    foreach ($service in @('booking-shard-0-postgres','booking-shard-1-postgres')) {
        Invoke-Milestone5DriverPSQL -Context $context -Service $service `
            -Artifact "final-reconciliation-$service-snapshots.log" -SQL @"
SELECT reservation.payment_intent_id||'|'||reservation.train_run_id||'|'||
       reservation.assignment_generation||'|'||reservation.status||'|'||
       coalesce(ticket_order.status,'missing')||'|'||coalesce(ticket_order.assignment_generation::text,'missing')
FROM public.reservations AS reservation
LEFT JOIN public.ticket_orders AS ticket_order
  ON ticket_order.payment_intent_id=reservation.payment_intent_id
 AND ticket_order.train_run_id=reservation.train_run_id
WHERE reservation.payment_intent_id IS NOT NULL
ORDER BY reservation.payment_intent_id,reservation.assignment_generation;
"@ | Out-Null
        Invoke-Milestone5DriverPSQL -Context $context -Service $service `
            -Artifact "final-reconciliation-$service-role-snapshots.log" -SQL @"
SET ROLE payment_reconciler;
SELECT current_user||'|'||reservation.payment_intent_id||'|'||reservation.train_run_id||'|'||
       reservation.assignment_generation||'|'||reservation.status||'|'||
       coalesce(ticket_order.status,'missing')||'|'||coalesce(ticket_order.assignment_generation::text,'missing')
FROM public.reservations AS reservation
LEFT JOIN public.ticket_orders AS ticket_order
  ON ticket_order.payment_intent_id=reservation.payment_intent_id
 AND ticket_order.train_run_id=reservation.train_run_id
 AND ticket_order.assignment_generation=reservation.assignment_generation
WHERE reservation.payment_intent_id IS NOT NULL
ORDER BY reservation.payment_intent_id,reservation.assignment_generation;
RESET ROLE;
"@ | Out-Null
    }
    $reconciliation = Invoke-Milestone5DriverCompose -Context $context -AllowFailure `
        -Arguments @('run','--rm','-e','PAYMENT_PROCESSING_GRACE_SECONDS=1','payment-reconciler','--once','--scope','payment-all','--batch-size','100','--timeout','30s') `
        -Artifact 'final-detect-only-reconciliation.log'
    if ($reconciliation.ExitCode -ne 0) { throw 'final detect-only payment reconciliation failed' }
    $reconciliationLine = [string](@($reconciliation.Output | Where-Object {
        ([string]$_).TrimStart().StartsWith('{')
    }) | Select-Object -Last 1)
    if ([string]::IsNullOrWhiteSpace($reconciliationLine)) {
        throw 'final detect-only payment reconciliation omitted its JSON result'
    }
    $reconciliationResult = $reconciliationLine | ConvertFrom-Json
    if ([string]$reconciliationResult.status -ne 'completed' -or
        [int]$reconciliationResult.rows_examined -lt 1 -or
        [int]$reconciliationResult.shard_rows_found -lt 1 -or
        [int]$reconciliationResult.issued_orders -lt 1 -or
        [int]$reconciliationResult.mismatch_count -ne 0 -or
        [int]$reconciliationResult.manual_reviews -ne 0 -or
        [bool]$reconciliationResult.truncated -or
        -not [bool]$reconciliationResult.read_only) {
        throw 'final detect-only payment reconciliation did not prove a clean non-empty pass'
    }
    Assert-M6FinalInvariants
    Save-M6Snapshot -Prefix '99-final'
    $completedSourceState = Get-M6SourceState
    if ($completedSourceState.SHA256 -ne $sourceState.SHA256 -or
        $completedSourceState.FileCount -ne $sourceState.FileCount) {
        throw 'source state changed during the evidence run'
    }
    Assert-M6EvidenceIsSecretSafe
    $end = (Get-Date).ToUniversalTime()
    [ordered]@{
        status='passed'; started_at=$start.ToString('o'); completed_at=$end.ToString('o')
        duration_seconds=[Math]::Round(($end-$start).TotalSeconds,3)
        source_commit=(git -C $root rev-parse HEAD).Trim(); source_state_sha256=$sourceState.SHA256
        source_file_count=$sourceState.FileCount; worktree_dirty=(@(git -C $root status --short).Count -ne 0)
        source_digest_exclusions=[string[]]$sourceDigestExclusions
        compose_wrapper_sha256=$composeWrapperHash
        rendered_compose_config_sha256=$renderedComposeConfigHash
        build_mode=if ($SkipBuild) {'prebuilt-image-digests'} else {'source-build'}
        topology=[ordered]@{ api_replicas=3; payment_workers=2; reconciler=1; physical_shards=2 }
        scenarios=$scenarioResults; final_control_violations=0; final_shard_violations=0
        final_reconciliation=[ordered]@{
            scope=[string]$reconciliationResult.scope
            read_only=[bool]$reconciliationResult.read_only
            rows_examined=[int]$reconciliationResult.rows_examined
            shard_rows_found=[int]$reconciliationResult.shard_rows_found
            issued_orders=[int]$reconciliationResult.issued_orders
            mismatch_count=[int]$reconciliationResult.mismatch_count
            manual_reviews=[int]$reconciliationResult.manual_reviews
            truncated=[bool]$reconciliationResult.truncated
        }
        pool_pressure=(Get-M6PoolEvidence)
        operational_metrics=[ordered]@{
            sample_count=$paymentMetricSamples
            artifact_pattern='*-payment-metrics.prom'
            histogram_percentiles='Prometheus cumulative bucket upper-bound evidence; k6 convergence percentiles use exact Trend samples'
        }
        migration_state=(Get-Milestone5MigrationState -Context $context -MigrationID $migrationC -Artifact 'migration-final-state.log')
        evidence_secret_scan='passed'; teardown='pending'
        limitations=@('disposable synthetic sandbox only','single host and single region','bounded correctness and recovery evidence, not production capacity')
    } | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $EvidenceDirectory 'milestone-6-evidence-summary.json') -Encoding utf8
} catch {
    $runError = $_
    if ($started) {
        try {
            Save-M6ControlSnapshot -Prefix 'failure'
            foreach ($service in @('api-1','api-2','api-3','reverse-proxy','payment-sandbox','payment-worker-1','payment-worker-2','payment-reconciler','control-postgres','booking-shard-0-postgres','booking-shard-1-postgres')) {
                Invoke-Milestone5DriverCompose -Context $context -AllowFailure -Arguments @('logs','--no-color','--tail','200',$service) -Artifact "failure-$service.log" | Out-Null
            }
        } catch { }
    }
} finally {
    try {
        if ($null -ne $migrationJob) { Remove-Job -Job $migrationJob -Force -ErrorAction SilentlyContinue }
        if ($started) {
        $down = Invoke-M6Native -AllowFailure -Command { & docker @composeArguments down -v --remove-orphans }
        $down.Output | Set-Content -LiteralPath (Join-Path $EvidenceDirectory 'compose-down.log') -Encoding utf8
        $remainingCount = 0
        foreach ($query in @(
            @('ps','-a','-q','--filter',$projectLabel),
            @('volume','ls','-q','--filter',$projectLabel),
            @('network','ls','-q','--filter',$projectLabel)
        )) {
            $remaining = Invoke-M6Native -AllowFailure -Command { & docker @query }
            $remainingCount += @($remaining.Output | Where-Object { $_ }).Count
        }
        $teardown = [ordered]@{ status=if ($down.ExitCode -eq 0 -and $remainingCount -eq 0) {'passed'} else {'failed'}; remaining_resources=$remainingCount }
        $teardown | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $EvidenceDirectory 'teardown-summary.json') -Encoding utf8
        $summaryPath = Join-Path $EvidenceDirectory 'milestone-6-evidence-summary.json'
        if (Test-Path -LiteralPath $summaryPath) {
            $summary = Get-Content -Raw -LiteralPath $summaryPath | ConvertFrom-Json
            $summary.teardown = $teardown.status
            if ($teardown.status -ne 'passed') { $summary.status = 'failed' }
            $summary | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $summaryPath -Encoding utf8
        }
        $secretScanPassed = $false
        try {
            Assert-M6EvidenceIsSecretSafe
            $secretScanPassed = $true
        } catch {
            if ($null -eq $runError) { $runError = $_ } else { Write-Warning 'final evidence secret scan also failed' }
        }
        $manifestPath = Join-Path $EvidenceDirectory 'run-manifest.json'
        if (Test-Path -LiteralPath $manifestPath) {
            $manifest = Get-Content -Raw -LiteralPath $manifestPath | ConvertFrom-Json
            $endingSourceState = Get-M6SourceState
            $sourceStateVerified = (
                [string]$manifest.source_state_sha256 -eq $endingSourceState.SHA256 -and
                [int]$manifest.source_file_count -eq $endingSourceState.FileCount
            )
            if (-not $sourceStateVerified -and $null -eq $runError) {
                $runError = [System.Exception]::new('source state changed before evidence finalization')
            }
            $completedAt = (Get-Date).ToUniversalTime()
            $terminalPassed = ($null -eq $runError -and $teardown.status -eq 'passed' -and $secretScanPassed -and $sourceStateVerified)
            $scanStatus = if ($secretScanPassed) { 'passed' } else { 'failed' }
            $errorCategory = if ($terminalPassed) { $null } else { 'evidence_finalization_failed' }
            $manifest.status = if ($terminalPassed) { 'passed' } else { 'failed' }
            $manifest | Add-Member -NotePropertyName completed_at -NotePropertyValue $completedAt.ToString('o') -Force
            $manifest | Add-Member -NotePropertyName duration_seconds -NotePropertyValue ([Math]::Round($completedAt.Subtract($start).TotalSeconds,3)) -Force
            $manifest | Add-Member -NotePropertyName ending_source_state_sha256 -NotePropertyValue $endingSourceState.SHA256 -Force
            $manifest | Add-Member -NotePropertyName ending_source_file_count -NotePropertyValue $endingSourceState.FileCount -Force
            $manifest | Add-Member -NotePropertyName source_state_verified -NotePropertyValue $sourceStateVerified -Force
            $manifest | Add-Member -NotePropertyName evidence_secret_scan -NotePropertyValue $scanStatus -Force
            $manifest | Add-Member -NotePropertyName teardown -NotePropertyValue $teardown.status -Force
            $manifest | Add-Member -NotePropertyName error_category -NotePropertyValue $errorCategory -Force
            $manifest | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $manifestPath -Encoding utf8
        }
        Write-M6EvidenceIndex
        if ($teardown.status -ne 'passed' -and $null -eq $runError) {
            $runError = [System.Exception]::new('payment topology teardown was incomplete')
        }
        }
    } catch {
        if ($null -eq $runError) { $runError = $_ } else { Write-Warning 'evidence finalization also failed' }
    }
}

if ($null -ne $runError) { throw $runError }
} finally {
    if ($null -eq $originalJWTSecret) {
        Remove-Item Env:JWT_SECRET -ErrorAction SilentlyContinue
    } else {
        $env:JWT_SECRET = $originalJWTSecret
    }
}
