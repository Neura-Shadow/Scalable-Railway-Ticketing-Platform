[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot 'milestone-5-evidence-guardrails.ps1')
$runnerSource = Get-Content -Raw -LiteralPath (Join-Path $PSScriptRoot 'run-milestone-5-physical-shard-evidence.ps1')

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

Assert-True -Condition $runnerSource.Contains("[string]`$ScenarioDuration = '45s'") `
    -Message 'formal Milestone 5 cutover window must default to the bounded 45 second evidence duration'

function New-StrictK6Summary {
    param(
        [int64]$Passes = 4,
        [int64]$Failures = 0,
        [bool]$ThresholdOK = $true
    )
    return [pscustomobject]@{
        metrics = [pscustomobject]@{
            checks = [pscustomobject]@{ values = [pscustomobject]@{
                passes = $Passes; fails = $Failures; value = if ($Failures -eq 0) { 1.0 } else { 0.5 }
            } }
            iterations = [pscustomobject]@{ values = [pscustomobject]@{ count = 2 } }
            http_reqs = [pscustomobject]@{ values = [pscustomobject]@{ count = 8; rate = 2.0 } }
            physical_route_success = [pscustomobject]@{
                values = [pscustomobject]@{ count = 2 }
                thresholds = [pscustomobject]@{ 'count>=2' = [pscustomobject]@{ ok = $ThresholdOK } }
            }
            physical_route_conflicts = [pscustomobject]@{ values = [pscustomobject]@{ count = 0 } }
            shard_rate_limited = [pscustomobject]@{ values = [pscustomobject]@{ count = 0 } }
        }
    }
}

$expectedScenarios = @(
    'physical-shard-routing',
    'cross-shard-global-quota',
    'booking-command-recovery',
    'physical-shard-outage',
    'online-base-copy',
    'journal-catchup',
    'physical-cutover',
    'stale-router-physical',
    'reverse-migration',
    'legacy-vs-physical'
)
$actualScenarios = @(Get-Milestone5ScenarioNames)
Assert-True -Condition ($actualScenarios.Count -eq 10) -Message 'runner must require exactly ten scenarios'
Assert-True -Condition (($actualScenarios -join ',') -eq ($expectedScenarios -join ',')) `
    -Message 'runner scenario order or identity drifted'

foreach ($status in @('passed', 'failed', 'blocked', 'not_run')) {
    Assert-Milestone5Status -Status $status
}
Assert-Throws -Label 'unknown evidence status' -Action { Assert-Milestone5Status -Status 'skipped' }
Assert-True -Condition ((Get-Milestone5EvidenceFailureStatus -Category 'docker_unavailable') -eq 'blocked') `
    -Message 'Docker unavailability must be blocked, not failed'
Assert-True -Condition ((Get-Milestone5EvidenceFailureStatus -Category 'scenario_physical_cutover') -eq 'failed') `
    -Message 'executed scenario failures must be failed'

$strict = ConvertFrom-Milestone5K6Summary -Summary (New-StrictK6Summary) `
    -Scenario 'physical-shard-routing'
Assert-True -Condition ($strict.status -eq 'passed' -and $strict.checks_failed -eq 0 -and
    $strict.iterations -eq 2 -and $strict.http_requests -eq 8) `
    -Message 'strict k6 conversion did not preserve non-vacuous execution evidence'
Assert-Throws -Label 'failed k6 checks' -Action {
    ConvertFrom-Milestone5K6Summary -Summary (New-StrictK6Summary -Failures 1) `
        -Scenario 'physical-shard-routing' | Out-Null
}
Assert-Throws -Label 'failed k6 threshold' -Action {
    ConvertFrom-Milestone5K6Summary -Summary (New-StrictK6Summary -ThresholdOK $false) `
        -Scenario 'physical-shard-routing' | Out-Null
}
Assert-Throws -Label 'unknown k6 scenario' -Action {
    ConvertFrom-Milestone5K6Summary -Summary (New-StrictK6Summary) -Scenario 'optional-smoke' | Out-Null
}
$vacuousConflictSummary = New-StrictK6Summary
$vacuousConflictSummary.metrics.physical_route_success.values.count = 1
$vacuousConflictSummary.metrics.physical_route_conflicts.values.count = 47
Assert-Throws -Label 'one success plus forty-seven conflicts' -Action {
    ConvertFrom-Milestone5K6Summary -Summary $vacuousConflictSummary `
        -Scenario 'physical-shard-routing' | Out-Null
}
$vacuousQuotaSummary = New-StrictK6Summary
$vacuousQuotaSummary.metrics.PSObject.Properties.Remove('physical_route_success')
$vacuousQuotaSummary.metrics.PSObject.Properties.Remove('physical_route_conflicts')
$vacuousQuotaSummary.metrics | Add-Member -NotePropertyName global_quota_holds_created `
    -NotePropertyValue ([pscustomobject]@{ values = [pscustomobject]@{ count = 1 } })
$vacuousQuotaSummary.metrics | Add-Member -NotePropertyName global_quota_rejections `
    -NotePropertyValue ([pscustomobject]@{ values = [pscustomobject]@{ count = 47 } })
Assert-Throws -Label 'one success plus forty-seven quota responses' -Action {
    ConvertFrom-Milestone5K6Summary -Summary $vacuousQuotaSummary `
        -Scenario 'cross-shard-global-quota' | Out-Null
}

$databaseInput = [pscustomobject]@{
    enabled_writer_fences = 2
    dual_writer_violations = 0
    assignment_ledger_mismatches = 0
    directory_mismatches = 0
    quota_violations = 0
    journal_gaps = 0
    apply_receipt_conflicts = 0
    command_receipt_conflicts = 0
    unreconciled_commands = 0
    online_copy_mutation_delta = 2
    online_copy_journal_delta = 4
}
$database = Assert-Milestone5DatabaseInvariants -Evidence $databaseInput
Assert-True -Condition ($database.status -eq 'passed' -and $database.enabled_writer_fences -eq 2) `
    -Message 'database invariant proof was not normalized'
$badDatabase = $databaseInput.PSObject.Copy()
$badDatabase.dual_writer_violations = 1
Assert-Throws -Label 'dual writer evidence' -Action {
    Assert-Milestone5DatabaseInvariants -Evidence $badDatabase | Out-Null
}
$noOnlineMutation = $databaseInput.PSObject.Copy()
$noOnlineMutation.online_copy_mutation_delta = 0
Assert-Throws -Label 'missing online-copy mutation delta' -Action {
    Assert-Milestone5DatabaseInvariants -Evidence $noOnlineMutation | Out-Null
}
$missingDatabase = $databaseInput.PSObject.Copy()
$missingDatabase.PSObject.Properties.Remove('journal_gaps')
Assert-Throws -Label 'missing journal evidence' -Action {
    Assert-Milestone5DatabaseInvariants -Evidence $missingDatabase | Out-Null
}

$measurementsInput = [pscustomobject]@{
    final_write_pause_ms = 275.25
    maximum_final_write_pause_ms = 30000
    target_write_observed_before_reverse = $true
    target_write_preserved_after_reverse = $true
    target_generation = 8
    reverse_generation = 9
}
$measurements = Assert-Milestone5MeasuredMigrationEvidence -Evidence $measurementsInput
Assert-True -Condition ($measurements.status -eq 'passed' -and
    $measurements.final_write_pause_ms -eq 275.25 -and $measurements.reverse_generation -eq 9) `
    -Message 'migration measurements were not normalized'
$overBudget = $measurementsInput.PSObject.Copy()
$overBudget.final_write_pause_ms = 30001
Assert-Throws -Label 'over-budget final pause' -Action {
    Assert-Milestone5MeasuredMigrationEvidence -Evidence $overBudget | Out-Null
}
$noTargetWrite = $measurementsInput.PSObject.Copy()
$noTargetWrite.target_write_observed_before_reverse = $false
Assert-Throws -Label 'missing target-era write' -Action {
    Assert-Milestone5MeasuredMigrationEvidence -Evidence $noTargetWrite | Out-Null
}
$staleReverse = $measurementsInput.PSObject.Copy()
$staleReverse.reverse_generation = 8
Assert-Throws -Label 'non-monotonic reverse generation' -Action {
    Assert-Milestone5MeasuredMigrationEvidence -Evidence $staleReverse | Out-Null
}

$passedScenarios = @($expectedScenarios | ForEach-Object {
    [pscustomobject]@{ scenario = $_; status = 'passed' }
})
Assert-True -Condition (Test-Milestone5CanonicalSummaryReady `
    -Scenarios $passedScenarios -DatabaseInvariantsPassed $true `
    -MigrationEvidencePassed $true -TeardownCompleted $true -SanitizationCompleted $true) `
    -Message 'complete evidence set was not publishable'
foreach ($case in @(
    @{ Name = 'missing scenario'; Scenarios = $passedScenarios[0..8]; DB = $true; Migration = $true; Down = $true; Sanitized = $true },
    @{ Name = 'database failure'; Scenarios = $passedScenarios; DB = $false; Migration = $true; Down = $true; Sanitized = $true },
    @{ Name = 'migration failure'; Scenarios = $passedScenarios; DB = $true; Migration = $false; Down = $true; Sanitized = $true },
    @{ Name = 'teardown failure'; Scenarios = $passedScenarios; DB = $true; Migration = $true; Down = $false; Sanitized = $true },
    @{ Name = 'sanitization failure'; Scenarios = $passedScenarios; DB = $true; Migration = $true; Down = $true; Sanitized = $false }
)) {
    Assert-True -Condition (-not (Test-Milestone5CanonicalSummaryReady `
        -Scenarios $case.Scenarios -DatabaseInvariantsPassed $case.DB `
        -MigrationEvidencePassed $case.Migration -TeardownCompleted $case.Down `
        -SanitizationCompleted $case.Sanitized)) `
        -Message "canonical publication accepted $($case.Name)"
}

$temporaryRoot = Join-Path ([System.IO.Path]::GetTempPath()) "m5-guardrail-test-$([guid]::NewGuid().ToString('N'))"
try {
    New-Item -ItemType Directory -Path $temporaryRoot | Out-Null
    $safe = Join-Path $temporaryRoot 'safe.json'
    Write-Milestone5JsonAtomic -Path $safe -Value ([ordered]@{ status = 'passed'; value = 1 })
    Assert-Milestone5ArtifactsSanitized -EvidenceDirectory $temporaryRoot
    $credentialShapedValue = [string]::Concat(
        'postgres', 'ql://operator:', 'synthetic@db.example.test/tickets'
    )
    Set-Content -LiteralPath (Join-Path $temporaryRoot 'dsn.log') -Value $credentialShapedValue
    Assert-Throws -Label 'DSN artifact' -Action {
        Assert-Milestone5ArtifactsSanitized -EvidenceDirectory $temporaryRoot
    }
    Remove-Item -LiteralPath (Join-Path $temporaryRoot 'dsn.log') -Force
    Set-Content -LiteralPath (Join-Path $temporaryRoot 'secret.log') -Value 'synthetic-secret-value'
    Assert-Throws -Label 'known secret artifact' -Action {
        Assert-Milestone5ArtifactsSanitized -EvidenceDirectory $temporaryRoot `
            -SecretValues @('synthetic-secret-value')
    }
} finally {
    if (Test-Path -LiteralPath $temporaryRoot) {
        Remove-Item -LiteralPath $temporaryRoot -Recurse -Force
    }
}

$runnerPath = Join-Path $PSScriptRoot 'run-milestone-5-physical-shard-evidence.ps1'
$runner = Get-Content -Raw -LiteralPath $runnerPath
$guardrails = Get-Content -Raw -LiteralPath (Join-Path $PSScriptRoot 'milestone-5-evidence-guardrails.ps1')
foreach ($required in @(
    'docker-compose.physical-shards.yml',
    "'up', '-d', '--build', '--wait'",
    "'down', '-v', '--remove-orphans'",
    'grafana/k6:0.54.0',
    'postgres_instances = 3',
    'redis_instances = 1',
    'Assert-Milestone5ArtifactsSanitized',
    'Test-Milestone5CanonicalSummaryReady',
    'Move-Item -LiteralPath $candidateSummary -Destination $canonicalSummary',
    'git status --porcelain=v1 --untracked-files=all',
    'compose_config_sha256',
    'SecretValues'
)) {
    Assert-True -Condition $runner.Contains($required) -Message "runner omitted contract token: $required"
}
foreach ($scenario in $expectedScenarios) {
    Assert-True -Condition $guardrails.Contains("'$scenario'") -Message "runner omitted $scenario"
    $scriptPath = Join-Path (Split-Path -Parent $PSScriptRoot) "loadtest/k6/$scenario.js"
    Assert-True -Condition (Test-Path -LiteralPath $scriptPath -PathType Leaf) `
        -Message "required k6 script is missing: $scenario"
}
$moveIndex = $runner.IndexOf('Move-Item -LiteralPath $candidateSummary -Destination $canonicalSummary')
$sanitizationIndex = $runner.LastIndexOf('Assert-Milestone5ArtifactsSanitized')
Assert-True -Condition ($moveIndex -gt $sanitizationIndex) `
    -Message 'canonical passed summary is published before sanitization'
Assert-True -Condition (-not $runner.Contains('KeepEnvironment')) `
    -Message 'runner must not permit canonical evidence while retaining disposable topology'
Assert-True -Condition (-not $runner.Contains('milestone-5-summary.json'' | Out-File')) `
    -Message 'runner contains an early direct canonical summary write'

Write-Output 'Milestone 5 evidence runner contract tests passed'
