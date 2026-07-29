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
    'target_write_preserved_after_reverse'
)) {
    Assert-True -Condition $driver.Contains($required) `
        -Message "driver omitted trusted evidence token $required"
}
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

$fakeCustomers = @(0..7 | ForEach-Object {
    $customerIndex = [int]$_
    [pscustomobject]@{
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
Assert-True -Condition ($environmentMap['physical-shard-outage']['VUS_PER_SHARD'] -eq '2') `
    -Message 'outage evidence requires two independent healthy-shard writers'
Assert-True -Condition ($environmentMap['legacy-vs-physical']['VUS_PER_PATH'] -eq '2') `
    -Message 'legacy/physical comparison requires two independent writers per path'
foreach ($scriptName in @('physical-shard-outage.js', 'legacy-vs-physical.js')) {
    $scriptSource = Get-Content -Raw -LiteralPath (Join-Path $root "loadtest/k6/$scriptName")
    Assert-True -Condition $scriptSource.Contains('__ITER === 0') `
        -Message "$scriptName lets replay loops inflate its independent-commit counter"
}

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
