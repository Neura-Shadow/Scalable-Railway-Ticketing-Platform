[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Assert-True {
    param(
        [Parameter(Mandatory = $true)][bool]$Condition,
        [Parameter(Mandatory = $true)][string]$Message
    )
    if (-not $Condition) { throw $Message }
}

function Assert-Throws {
    param(
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][scriptblock]$Action
    )
    $threw = $false
    try { & $Action } catch { $threw = $true }
    if (-not $threw) { throw "$Label did not fail closed" }
}

$root = Split-Path -Parent $PSScriptRoot
$driverPath = Join-Path $PSScriptRoot 'milestone-5-physical-shard-evidence-driver.ps1'
$composePath = Join-Path $root 'docker-compose.physical-shards.yml'

Assert-True -Condition (Test-Path -LiteralPath $driverPath -PathType Leaf) `
    -Message 'trusted Milestone 5 evidence driver is missing'
. $driverPath

foreach ($name in @(
    'Initialize-Milestone5Evidence',
    'Start-Milestone5Scenario',
    'Stop-Milestone5Scenario',
    'Get-Milestone5DatabaseEvidence',
    'Get-Milestone5MigrationEvidence'
)) {
    Assert-True -Condition ($null -ne (Get-Command $name -CommandType Function -ErrorAction SilentlyContinue)) `
        -Message "driver omitted required hook $name"
}

$driver = Get-Content -Raw -LiteralPath $driverPath
foreach ($scenario in @(
    'physical-shard-routing', 'cross-shard-global-quota',
    'booking-command-recovery', 'physical-shard-outage',
    'online-base-copy', 'journal-catchup', 'physical-cutover',
    'stale-router-physical', 'reverse-migration', 'legacy-vs-physical'
)) {
    Assert-True -Condition $driver.Contains("'$scenario'") `
        -Message "driver omitted explicit control for $scenario"
}
foreach ($required in @(
    'control-postgres', 'booking-shard-0-postgres', 'booking-shard-1-postgres',
    'physical-shard-admin', 'Invoke-Milestone5DriverPSQL',
    'Invoke-Milestone5DriverAPI', 'Move-Milestone5Migration',
    'Invoke-Milestone5OperatorDurableSmoke', 'operator_booking_commands',
    'booking_command_receipts', 'expected_source_version', 'Idempotency-Key',
    'Stop-Milestone5DriverJob', 'target_write_observed_before_reverse',
    'target_write_preserved_after_reverse', 'M5EvidenceDriverPath',
    'stale-prewarm-$index.log', 'Complete-Milestone5MigrationAfterRollbackWindow',
    'rollback_deadline_at<=clock_timestamp()', '$Prefix-complete.log',
    'LEGACY_TRAIN_RUN_ID=$script:M5TrainF',
    'BaseCopyStartedAtUtc', 'base_copy_elapsed_ms',
    'JournalReplayStartedAtUtc', 'journal_replay_elapsed_ms',
    'published_physical_outbox_events', 'physical_read_model_receipts',
    'physical_same_idempotency_requests', 'physical_admission_journeys', 'physical_hold_expirations',
    'Assert-Milestone5ConcurrentPhysicalIdentity', 'Invoke-Milestone5PhysicalAdmissionExpiryJourney',
    'same-idempotency-command-count.log', 'physical-hold-expiry-proof.log',
    "expires_at=created_at+interval '1 microsecond'",
    'final_source_sequence IS NULL OR last_replayed_sequence IS NULL',
    "-Target capture_enabled -Prefix 'train-c'",
    'completed_at_utc=[DateTimeOffset]::UtcNow.ToString'
)) {
    Assert-True -Condition $driver.Contains($required) `
        -Message "driver omitted trusted evidence token $required"
}
Assert-True -Condition (-not $driver.Contains("checkpoint_kind='base_copy' AND object_name='migration_engine'")) `
    -Message 'base-copy throughput must not omit its first batch by using the late checkpoint insertion time'
Assert-True -Condition (-not $driver.Contains("checkpoint_kind='journal_replay' AND object_name='migration_engine'")) `
    -Message 'journal throughput must use the complete wall-clock replay phase rather than controller checkpoint timing'
Assert-True -Condition (-not $driver.Contains('$MyInvocation.MyCommand.Path')) `
    -Message 'background transition jobs must use the driver path captured while the script is dot-sourced'
Assert-True -Condition $driver.Contains('$MeasurePause.IsPresent') `
    -Message 'background transition jobs must serialize the switch presence as a primitive boolean'
Assert-True -Condition (-not $driver.Contains('--header=''Authorization: Bearer `$M5_TOKEN''')) `
    -Message 'stale prewarm must not single-quote the shell token expansion'
Assert-True -Condition (-not $driver.Contains('wget --server-response')) `
    -Message 'stale prewarm must use BusyBox-compatible wget response diagnostics'
Assert-True -Condition (-not $driver.Contains('--header=')) `
    -Message 'stale prewarm must pass BusyBox long-option values as separate arguments'
Assert-True -Condition (-not $driver.Contains('--post-data=')) `
    -Message 'stale prewarm must pass BusyBox post data as a separate argument'
Assert-True -Condition ($driver.Contains("PSObject.Properties['Content']") -and $driver.Contains('ReadAsStringAsync().GetAwaiter().GetResult()')) `
    -Message 'API error handling must read PowerShell 7 HttpResponseMessage bodies without assuming GetResponseStream'
Assert-True -Condition (-not $driver.Contains("'sh','-c',`$shell")) `
    -Message 'stale prewarm must preserve JSON and header argv boundaries without a shell command string'
Assert-True -Condition $driver.Contains("'--post-data',`$nativeBody") `
    -Message 'stale prewarm must preserve JSON quotes through Windows native argv marshalling'
foreach ($forbidden in @(
    'final_write_pause_ms = 1',
    'dual_writer_violations = 0',
    'journal_gaps = 0',
    'target_write_observed_before_reverse = $true',
    'target_write_preserved_after_reverse = $true'
)) {
    Assert-True -Condition (-not $driver.Contains($forbidden)) `
        -Message "driver contains synthetic evidence token: $forbidden"
}
Assert-True -Condition ($driver -notmatch '(?is)INSERT\s+INTO\s+(public\.)?operator_booking_commands') `
    -Message 'driver must not synthesize a durable operator command with SQL'
Assert-True -Condition ($driver -notmatch '(?is)INSERT\s+INTO\s+(public\.)?booking_command_receipts') `
    -Message 'driver must not synthesize a physical command receipt with SQL'

Assert-Throws -Label 'invalid initialization context' -Action {
    Initialize-Milestone5Evidence -Context ([pscustomobject]@{}) | Out-Null
}
Assert-Throws -Label 'unknown scenario start' -Action {
    Start-Milestone5Scenario -Context ([pscustomobject]@{}) -State ([pscustomobject]@{}) `
        -Scenario 'unknown' -Environment @{}
}

$fakeCustomers = @(0..25 | ForEach-Object {
    $customerIndex = [int]$_
    [pscustomobject]@{
        Email = "m5-contract-$customerIndex@example.test"
        Token = "synthetic-token-$customerIndex"
        PassengerIDs = [string[]]@(0..10 | ForEach-Object {
            "21000000-0000-4000-8$customerIndex$($_.ToString('00'))-$($customerIndex.ToString('00'))$($_.ToString('0000000000'))"
        })
    }
})
$environmentMap = New-Milestone5EnvironmentMap -State ([pscustomobject]@{
    Customers = $fakeCustomers
    Suffix = 'contract'
})
Assert-True -Condition ($environmentMap.Count -eq 10) `
    -Message 'driver did not construct all ten scenario environments'
Assert-True -Condition ($environmentMap['physical-shard-routing']['VUS'] -eq '6') `
    -Message 'physical routing must distribute replay requests across six bounded customer identities'
Assert-True -Condition ($environmentMap['physical-shard-routing']['CONCURRENT_CUSTOMER_TOKEN'] -eq 'synthetic-token-24' -and `
    $environmentMap['physical-shard-routing']['API_URLS'] -eq 'http://api-1:8080,http://api-2:8080,http://api-3:8080') `
    -Message 'physical routing must run the 100-way identity against all three API replicas'
Assert-True -Condition ($environmentMap['cross-shard-global-quota']['RATE_LIMIT_SETTLE_SECONDS'] -eq '61') `
    -Message 'global quota evidence must let its authenticated reservation-rate window expire before probing quota'
Assert-True -Condition ($environmentMap['physical-shard-outage']['VUS_PER_SHARD'] -eq '2') `
    -Message 'outage evidence requires two independent healthy-shard writers'
Assert-True -Condition ($environmentMap['physical-shard-outage']['ITERATIONS_PER_VU'] -eq '2') `
    -Message 'outage evidence must use bounded iterations below the public reservation-rate limit'
Assert-True -Condition ($environmentMap['online-base-copy']['ITERATIONS_PER_VU'] -eq '2') `
    -Message 'online base-copy evidence must use bounded source mutations'
Assert-True -Condition ($environmentMap['online-base-copy']['CUSTOMER_TOKENS'] -eq 'synthetic-token-11,synthetic-token-12') `
    -Message 'online base-copy evidence must use isolated public rate-limit identities'
Assert-True -Condition ($environmentMap['journal-catchup']['VUS'] -eq '3' -and `
    $environmentMap['journal-catchup']['ITERATIONS'] -eq '3') `
    -Message 'journal catch-up must use one bounded mutation identity per customer'
Assert-True -Condition ($environmentMap['journal-catchup']['CUSTOMER_TOKENS'] -eq 'synthetic-token-13,synthetic-token-14,synthetic-token-15') `
    -Message 'journal catch-up evidence must use isolated public rate-limit identities'
Assert-True -Condition ($environmentMap['physical-cutover']['ITERATIONS_PER_VU'] -eq '8') `
    -Message 'cutover evidence must poll through the bounded pause without exceeding the public rate limit'
Assert-True -Condition ($environmentMap['physical-cutover']['CUTOVER_INTERVAL_SECONDS'] -eq '3') `
    -Message 'cutover polling must span the full bounded state transition and observe recovery'
Assert-True -Condition ($environmentMap['physical-cutover']['CUSTOMER_TOKENS'] -eq 'synthetic-token-16,synthetic-token-17') `
    -Message 'cutover evidence must use dedicated synthetic customer rate-limit windows'
Assert-True -Condition ($environmentMap['legacy-vs-physical']['VUS_PER_PATH'] -eq '2') `
    -Message 'legacy/physical comparison requires two independent writers per path'
Assert-True -Condition ($environmentMap['legacy-vs-physical']['ITERATIONS_PER_VU'] -eq '2') `
    -Message 'legacy/physical comparison must use bounded replay iterations'
foreach ($scriptName in @('physical-shard-outage.js', 'legacy-vs-physical.js')) {
    $scriptSource = Get-Content -Raw -LiteralPath (Join-Path $root "loadtest/k6/$scriptName")
    Assert-True -Condition $scriptSource.Contains('__ITER === 0') `
        -Message "$scriptName lets replay loops inflate its independent-commit counter"
}
$outageSource = Get-Content -Raw -LiteralPath (Join-Path $root 'loadtest/k6/physical-shard-outage.js')
Assert-True -Condition $outageSource.Contains("executor: 'per-vu-iterations'") `
    -Message 'physical shard outage evidence must not turn a correctness probe into an unbounded rate-limit load loop'
Assert-True -Condition $outageSource.Contains("shard_request_duration: ['p(95)<3000', 'p(99)<5000']") `
    -Message 'physical shard outage latency must be bounded above the configured 2.5 second failure timeout'
foreach ($scriptName in @('online-base-copy.js', 'physical-cutover.js', 'legacy-vs-physical.js')) {
    $scriptSource = Get-Content -Raw -LiteralPath (Join-Path $root "loadtest/k6/$scriptName")
    Assert-True -Condition $scriptSource.Contains("executor: 'per-vu-iterations'") `
        -Message "$scriptName must use bounded iterations below the production reservation-rate limit"
}
$cutoverSource = Get-Content -Raw -LiteralPath (Join-Path $root 'loadtest/k6/physical-cutover.js')
Assert-True -Condition $cutoverSource.Contains("positiveNumber('CUTOVER_INTERVAL_SECONDS', 3)") `
    -Message 'physical cutover workload must use the configured bounded polling interval'

$compose = Get-Content -Raw -LiteralPath $composePath
foreach ($required in @(
    'physical-shard-admin:',
    'target: physical-shard-admin',
    'profiles: ["tools"]'
)) {
    Assert-True -Condition $compose.Contains($required) `
        -Message "Compose omitted evidence tool contract: $required"
}

Write-Output 'Milestone 5 evidence driver contract tests passed'
