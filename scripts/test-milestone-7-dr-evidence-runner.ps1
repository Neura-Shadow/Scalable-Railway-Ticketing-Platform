[CmdletBinding()]
param(
    [string]$EvidenceDirectory = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$root = [System.IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$runnerPath = Join-Path $PSScriptRoot 'run-milestone-7-dr-evidence.ps1'
$k6DiagnosticPath = Join-Path $PSScriptRoot 'milestone-7/k6-diagnostics.ps1'
$focusedRunnerPath = Join-Path $PSScriptRoot 'run-m7-provider-contract-focused.ps1'
$composePath = Join-Path $root 'docker-compose.dr.yml'
$replicationPath = Join-Path $root 'deploy/postgres/dr/10-replication.sh'
$standbyPath = Join-Path $root 'deploy/postgres/dr/start-standby.sh'
$workflowPath = Join-Path $root '.github/workflows/milestone-7-dr.yml'
$dockerfilePath = Join-Path $root 'Dockerfile'
foreach ($required in @($runnerPath, $k6DiagnosticPath, $focusedRunnerPath, $composePath, $replicationPath, $standbyPath, $workflowPath)) {
    if (-not (Test-Path -LiteralPath $required)) { throw "Milestone 7 DR artifact is missing: $(Split-Path -Leaf $required)" }
}

. $k6DiagnosticPath
$k6DiagnosticSource = Get-Content -Raw -LiteralPath $k6DiagnosticPath
foreach ($required in @(
    'New-M7K6TraversalState', 'Use-M7K6TraversalBudget', 'Get-M7K6TraversalProperty',
    "'object_node'", "'property'", "'check'", "'threshold'", "'array'", "'array_element'",
    '-TraversalState $traversalState', 'diagnostic_truncated', 'summary_inspection_complete'
)) {
    if (-not $k6DiagnosticSource.Contains($required)) { throw "k6 diagnostics omit shared traversal contract: $required" }
}

function Invoke-M7DiagnosticDocker {
    param(
        [Parameter(Mandatory=$true)][string[]]$Arguments,
        [ValidateRange(1,300)][int]$TimeoutSeconds = 90
    )
    $process = [System.Diagnostics.Process]::new()
    try {
        $dockerCommand = @(Get-Command docker -CommandType Application -ErrorAction Stop)[0]
        $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
        $startInfo.FileName = $dockerCommand.Source
        $startInfo.UseShellExecute = $false
        $startInfo.CreateNoWindow = $true
        $startInfo.RedirectStandardOutput = $true
        $startInfo.RedirectStandardError = $true
        foreach ($argument in $Arguments) { [void]$startInfo.ArgumentList.Add([string]$argument) }
        $process.StartInfo = $startInfo
        if (-not $process.Start()) { throw 'diagnostic docker process did not start' }
        $stdoutTask = $process.StandardOutput.ReadToEndAsync()
        $stderrTask = $process.StandardError.ReadToEndAsync()
        if (-not $process.WaitForExit($TimeoutSeconds * 1000)) {
            $process.Kill($true)
            [void]$process.WaitForExit(5000)
            return [pscustomobject]@{ ExitCode=124; Output=[string[]]@('diagnostic docker process timed out') }
        }
        $output = [string[]]@(
            [regex]::Split("$($stdoutTask.GetAwaiter().GetResult())`n$($stderrTask.GetAwaiter().GetResult())", '\r?\n') |
                Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
                Select-Object -Last 80
        )
        return [pscustomobject]@{ ExitCode=[int]$process.ExitCode; Output=$output }
    } finally {
        $process.Dispose()
    }
}

$containerFixtureDirectory = Join-Path ([System.IO.Path]::GetTempPath()) "railway-m7-k6-container-test-$([guid]::NewGuid().ToString('N'))"
[System.IO.Directory]::CreateDirectory($containerFixtureDirectory) | Out-Null
$containerFixtureToken = "railway-m7-k6-proof-$([guid]::NewGuid().ToString('N').Substring(0,12))"
$capabilityContainerName = "$containerFixtureToken-capability"
$negativeContainerName = "$containerFixtureToken-negative"
$containerSummaryPath = Join-Path $containerFixtureDirectory 'synthetic-negative-summary.json'
$containerSecrets = [string[]]@(
    'rk_test_should_never_leak', 'Bearer synthetic-wide-token',
    'postgresql://wide-user:wide-password@example.invalid/database',
    'synthetic-wide-webhook-secret', 'user@example.invalid', '+15555550123'
)
$containerCapabilityProof = $false
$negativeContainerSummaryCopied = $false
$negativeContainerRemoved = $false
try {
    $negativeScriptPath = Join-Path $containerFixtureDirectory 'synthetic-negative.js'
    [System.IO.File]::WriteAllText($negativeScriptPath, @'
import { check } from 'k6';
export const options = { vus: 1, iterations: 1, thresholds: { checks: ['rate==1'] } };
const neverLog = ['rk_test_should_never_leak', 'Bearer synthetic-wide-token', 'postgresql://wide-user:wide-password@example.invalid/database', 'synthetic-wide-webhook-secret', 'user@example.invalid', '+15555550123'];
export default function () {
  check(false, { 'stripe_adapter_payouts returned 200': (value) => value === true });
  if (neverLog.length !== 6) { throw new Error('invalid fixture'); }
}
'@, [System.Text.UTF8Encoding]::new($false))

    $repoScriptMount = "type=bind,src=$(Join-Path $root 'loadtest/k6'),dst=/scripts,readonly"
    $capability = Invoke-M7DiagnosticDocker -Arguments @(
        'run','--rm','--name',$capabilityContainerName,'--label',"m7.focused.project=$containerFixtureToken",
        '--network','none','--user','12345:12345','--read-only','--tmpfs','/tmp:rw,noexec,nosuid,size=16m',
        '--security-opt','no-new-privileges','--cap-drop','ALL','--mount',$repoScriptMount,
        '--entrypoint','sh','grafana/k6:0.57.0','-c',
        'set -eu; test "$(id -u)" = 12345; test "$(awk ''/^CapEff:/ {print $2}'' /proc/self/status)" = 0000000000000000; test -w /tmp; awk ''$5 == "/scripts" && $6 ~ /(^|,)ro(,|$)/ { found=1 } END { exit !found }'' /proc/self/mountinfo; : > /tmp/identity-proof; printf "uid=12345 cap_eff=0 tmp_writable=true scripts_read_only=true\n"'
    )
    if ($capability.ExitCode -ne 0 -or ([string]($capability.Output -join "`n")) -notmatch 'uid=12345 cap_eff=0 tmp_writable=true scripts_read_only=true') {
        throw 'pinned k6 image did not prove the unprivileged zero-capability writable-/tmp contract'
    }
    $containerCapabilityProof = $true

    $fixtureMount = "type=bind,src=$containerFixtureDirectory,dst=/scripts,readonly"
    $negative = Invoke-M7DiagnosticDocker -Arguments @(
        'run','--name',$negativeContainerName,'--label',"m7.focused.project=$containerFixtureToken",
        '--network','none','--user','12345:12345',
        '--security-opt','no-new-privileges','--cap-drop','ALL','--mount',$fixtureMount,
        '--entrypoint','sh','grafana/k6:0.57.0','-c',
        'umask 022; k6 "$@"; code=$?; test "$(stat -c %a /tmp/synthetic-negative-summary.json)" = 644; printf "summary_mode=644\n"; exit "$code"',
        'sh','run','--quiet','--summary-export','/tmp/synthetic-negative-summary.json','/scripts/synthetic-negative.js'
    )
    if ($negative.ExitCode -ne 99 -or ([string]($negative.Output -join "`n")) -notmatch 'summary_mode=644') {
        throw "synthetic stopped-container k6 exit/mode proof was invalid; exit=$($negative.ExitCode)"
    }
    $copy = Invoke-M7DiagnosticDocker -Arguments @('cp',"${negativeContainerName}:/tmp/synthetic-negative-summary.json",$containerSummaryPath)
    if ($copy.ExitCode -ne 0 -or -not [System.IO.File]::Exists($containerSummaryPath)) {
        throw 'synthetic stopped-container k6 summary was not copied to host evidence'
    }
    $negativeContainerSummaryCopied = $true
    $containerDiagnostic = Get-M7K6Diagnostic -Script 'production-provider-contract.js' -ExitCode $negative.ExitCode -SummaryPath $containerSummaryPath -LogLines ([string[]]$negative.Output) -SensitiveValues $containerSecrets
    $containerDiagnosticJSON = $containerDiagnostic | ConvertTo-Json -Depth 10 -Compress
    if ([int]$containerDiagnostic.exit_code -ne 99 -or @($containerDiagnostic.failed_checks).Count -ne 1 -or
        [string]$containerDiagnostic.failed_checks[0] -cne 'stripe_adapter_payouts returned 200') {
        throw 'synthetic stopped-container k6 diagnostic did not preserve its non-zero failed check'
    }
    foreach ($secret in $containerSecrets) {
        if ($containerDiagnosticJSON.Contains($secret)) { throw 'synthetic stopped-container diagnostic leaked a fixture secret' }
    }
} finally {
    [void](Invoke-M7DiagnosticDocker -Arguments @('rm','-f',$negativeContainerName))
    [void](Invoke-M7DiagnosticDocker -Arguments @('rm','-f',$capabilityContainerName))
    $remainingFixtureContainers = Invoke-M7DiagnosticDocker -Arguments @('ps','-aq','--filter',"label=m7.focused.project=$containerFixtureToken")
    $negativeContainerRemoved = ($remainingFixtureContainers.ExitCode -eq 0 -and @($remainingFixtureContainers.Output).Count -eq 0)
    if ([System.IO.Directory]::Exists($containerFixtureDirectory)) { [System.IO.Directory]::Delete($containerFixtureDirectory, $true) }
}
if (-not $containerCapabilityProof -or -not $negativeContainerSummaryCopied -or -not $negativeContainerRemoved) {
    throw 'unprivileged k6 stopped-container evidence regression did not complete or clean up'
}

$diagnosticFixtureDirectory = Join-Path ([System.IO.Path]::GetTempPath()) "railway-m7-k6-diagnostic-test-$([guid]::NewGuid().ToString('N'))"
[System.IO.Directory]::CreateDirectory($diagnosticFixtureDirectory) | Out-Null
try {
    $summaryPath = Join-Path $diagnosticFixtureDirectory 'failed-check-summary.json'
    [System.IO.File]::WriteAllText($summaryPath, (@{
        metrics = @{ checks = @{ passes = 12; fails = 1; value = 0.923; thresholds = @{ 'rate==1' = $true } }; iterations = @{ count = 1 } }
        root_group = @{ name = ''; groups = @{}; checks = @{ 'stripe_adapter_payouts returned 200' = @{ name = 'stripe_adapter_payouts returned 200'; passes = 0; fails = 1 } } }
    } | ConvertTo-Json -Depth 10), [System.Text.UTF8Encoding]::new($false))
    $diagnostic = Get-M7K6Diagnostic -Script 'production-provider-contract.js' -ExitCode 99 -SummaryPath $summaryPath -LogLines @()
    if (-not [bool]$diagnostic.summary_present -or [int]$diagnostic.check_passes -ne 12 -or [int]$diagnostic.check_failures -ne 1 -or
        @($diagnostic.failed_checks).Count -ne 1 -or [string]$diagnostic.failed_checks[0] -cne 'stripe_adapter_payouts returned 200') {
        throw 'bounded k6 diagnostic did not extract a failed check from a valid non-zero summary'
    }

    $thresholdSummaryPath = Join-Path $diagnosticFixtureDirectory 'failed-threshold-summary.json'
    [System.IO.File]::WriteAllText($thresholdSummaryPath, (@{
        metrics = @{
            checks = @{ passes = 19; fails = 0; value = 1; thresholds = @{ 'rate==1' = $false } }
            iterations = @{ count = 1 }
            payment_http_request_duration = @{ count = 0; 'p(99)' = 0; thresholds = @{ 'p(99)<10000' = $true } }
        }
        root_group = @{ name = ''; groups = @{}; checks = @{} }
    } | ConvertTo-Json -Depth 10), [System.Text.UTF8Encoding]::new($false))
    $thresholdDiagnostic = Get-M7K6Diagnostic -Script 'production-provider-contract.js' -ExitCode 99 -SummaryPath $thresholdSummaryPath -LogLines @()
    if (@($thresholdDiagnostic.failed_thresholds).Count -ne 1 -or [string]$thresholdDiagnostic.failed_thresholds[0] -cne 'payment_http_request_duration' -or
        [string]$thresholdDiagnostic.classification -cne 'k6_inherited_threshold_without_samples') {
        throw 'bounded k6 diagnostic did not extract and classify a crossed zero-sample threshold'
    }

    $missingDiagnostic = Get-M7K6Diagnostic -Script 'production-provider-contract.js' -ExitCode 1 `
        -SummaryPath (Join-Path $diagnosticFixtureDirectory 'missing-summary.json') `
        -LogLines @('level=error msg="failed to handle the end-of-test summary" error="could not open summary: permission denied"')
    if ([bool]$missingDiagnostic.summary_present -or [bool]$missingDiagnostic.summary_valid -or
        [string]$missingDiagnostic.classification -cne 'k6_summary_write_failure') {
        throw 'bounded k6 diagnostic did not classify a missing summary write failure'
    }
    $emptySummaryPath = Join-Path $diagnosticFixtureDirectory 'empty-summary.json'
    [System.IO.File]::WriteAllText($emptySummaryPath, '', [System.Text.UTF8Encoding]::new($false))
    $emptyDiagnostic = Get-M7K6Diagnostic -Script 'production-provider-contract.js' -ExitCode 107 `
        -SummaryPath $emptySummaryPath -LogLines @('level=error msg="ReferenceError: synthetic runtime failure"')
    if ([bool]$emptyDiagnostic.summary_present -or [bool]$emptyDiagnostic.summary_valid -or
        [string]$emptyDiagnostic.classification -cne 'k6_runtime_exception') {
        throw 'bounded k6 diagnostic treated a prepared but unwritten summary as malformed JSON'
    }

    $malformedSummaryPath = Join-Path $diagnosticFixtureDirectory 'malformed-summary.json'
    [System.IO.File]::WriteAllText($malformedSummaryPath, '{not-json', [System.Text.UTF8Encoding]::new($false))
    $malformedDiagnostic = Get-M7K6Diagnostic -Script 'production-provider-contract.js' -ExitCode 1 -SummaryPath $malformedSummaryPath -LogLines @()
    if (-not [bool]$malformedDiagnostic.summary_present -or [bool]$malformedDiagnostic.summary_valid -or
        [string]$malformedDiagnostic.classification -cne 'evidence_runner_diagnostic_failure') {
        throw 'bounded k6 diagnostic did not classify a malformed summary'
    }

    $syntheticSecrets = [string[]]@(
        'rk_test_must_never_appear',
        'synthetic-provider-secret-never-log',
        'postgresql://user:password@example.invalid/database',
        'Bearer synthetic-token-never-log'
    )
    $boundedLogLines = [System.Collections.Generic.List[string]]::new()
    foreach ($index in 1..55) { $boundedLogLines.Add(('time=synthetic level=error msg="bounded runtime line {0}"' -f $index)) }
    $boundedLogLines.Add(('time=synthetic level=error msg="ReferenceError {0}"' -f ($syntheticSecrets -join ' ')))
    $boundedLogLines.Add('time=synthetic level=error host=control-postgres.internal passenger_name=NeverExpose')
    $boundedLogLines.Add('{"passenger_name":"NeverExpose","response":"arbitrary body"}')
    $boundedLogLines.Add('time=synthetic level=error url=http://payment-stripe-contract:8100/private passenger@example.test +886-912-345-678')
    $boundedDiagnostic = Get-M7K6Diagnostic -Script 'production-provider-contract.js' -ExitCode 107 -SummaryPath (Join-Path $diagnosticFixtureDirectory 'runtime-missing-summary.json') -LogLines ([string[]]$boundedLogLines) -SensitiveValues $syntheticSecrets
    $boundedJSON = $boundedDiagnostic | ConvertTo-Json -Depth 10 -Compress
    if (@($boundedDiagnostic.log_tail).Count -gt 40 -or $boundedJSON.Length -gt 4096 -or
        [string]$boundedDiagnostic.runtime_error -cne 'javascript_exception') {
        throw 'bounded k6 diagnostic did not enforce log/character limits or detect a JavaScript runtime error'
    }
    foreach ($secret in $syntheticSecrets) {
        if ($boundedJSON.Contains($secret)) { throw 'bounded k6 diagnostic leaked a synthetic secret' }
    }
    foreach ($forbidden in @('control-postgres.internal','NeverExpose','arbitrary body','payment-stripe-contract','passenger@example.test','+886-912-345-678')) {
        if ($boundedJSON.Contains($forbidden)) { throw 'bounded k6 diagnostic leaked a host, passenger value, or arbitrary response body' }
    }

    $boundedChecks = @{}
    $boundedMetrics = @{ checks = @{ passes = 0; fails = 25; value = 0; thresholds = @{ 'rate==1' = $true } }; iterations = @{ count = 1 } }
    foreach ($index in 1..25) {
        $checkName = "failed check $index $($syntheticSecrets[$index % $syntheticSecrets.Count])"
        $boundedChecks[$checkName] = @{ name = $checkName; passes = 0; fails = 1 }
        $metricName = "synthetic_threshold_$index"
        $boundedMetrics[$metricName] = @{ count = 1; thresholds = @{ 'count==0' = $true } }
    }
    $boundedSummaryPath = Join-Path $diagnosticFixtureDirectory 'bounded-items-summary.json'
    [System.IO.File]::WriteAllText($boundedSummaryPath, (@{
        metrics = $boundedMetrics
        root_group = @{ name = ''; groups = @{}; checks = $boundedChecks }
    } | ConvertTo-Json -Depth 10), [System.Text.UTF8Encoding]::new($false))
    $boundedItemsDiagnostic = Get-M7K6Diagnostic -Script 'production-provider-contract.js' -ExitCode 99 -SummaryPath $boundedSummaryPath -LogLines @() -SensitiveValues $syntheticSecrets
    $boundedItemsJSON = $boundedItemsDiagnostic | ConvertTo-Json -Depth 10 -Compress
    if (@($boundedItemsDiagnostic.failed_checks).Count -gt 20 -or @($boundedItemsDiagnostic.failed_thresholds).Count -gt 20 -or
        $boundedItemsJSON.Length -gt 4096) {
        throw 'bounded k6 diagnostic exceeded failed-item or character limits'
    }
    foreach ($secret in $syntheticSecrets) {
        if ($boundedItemsJSON.Contains($secret)) { throw 'bounded k6 item extraction leaked a synthetic secret' }
    }

    $wideSecrets = [string[]]@(
        'rk_test_should_never_leak', 'Bearer synthetic-wide-token',
        'postgresql://wide-user:wide-password@example.invalid/database',
        'synthetic-wide-webhook-secret', 'user@example.invalid', '+15555550123'
    )
    $wideChecks = [ordered]@{}
    foreach ($index in 1..5000) {
        $checkName = 'check_{0:D4}' -f $index
        if ($index -eq 1000) { $checkName = "failed_around_boundary_$($wideSecrets[0])" }
        if ($index -eq 4500) { $checkName = "failed_after_boundary_$($wideSecrets[1])" }
        $failed = ($index -in @(1000,4500))
        $wideChecks[$checkName] = [ordered]@{ name=$checkName; passes=$(if($failed){0}else{1}); fails=$(if($failed){1}else{0}) }
    }
    $wideSummaryPath = Join-Path $diagnosticFixtureDirectory 'wide-5000-check-summary.json'
    [System.IO.File]::WriteAllText($wideSummaryPath, ([ordered]@{
        metrics=[ordered]@{ checks=[ordered]@{ passes=4998; fails=2; value=0.9996; thresholds=[ordered]@{'rate==1'=$true} }; iterations=[ordered]@{count=1} }
        root_group=[ordered]@{ name=''; groups=[ordered]@{}; checks=$wideChecks }
    } | ConvertTo-Json -Depth 12), [System.Text.UTF8Encoding]::new($false))
    $wideDiagnostic = Get-M7K6Diagnostic -Script 'production-provider-contract.js' -ExitCode 99 -SummaryPath $wideSummaryPath -LogLines @($wideSecrets) -SensitiveValues $wideSecrets
    $wideDiagnosticJSON = $wideDiagnostic | ConvertTo-Json -Depth 10 -Compress
    if (-not [bool]$wideDiagnostic.diagnostic_truncated -or [bool]$wideDiagnostic.summary_inspection_complete -or
        [string]$wideDiagnostic.classification -cne 'evidence_runner_diagnostic_failure' -or
        @($wideDiagnostic.failed_checks).Count -gt 20 -or @($wideDiagnostic.failed_thresholds).Count -gt 20 -or
        $wideDiagnosticJSON.Length -gt 4096) {
        throw 'public k6 diagnostic did not fail closed within the shared traversal budget for 5000 checks'
    }
    foreach ($secret in $wideSecrets) {
        if ($wideDiagnosticJSON.Contains($secret)) { throw 'wide truncated k6 diagnostic leaked a synthetic secret' }
    }
    $wideThrownException = "production-provider-contract.js failed: $wideDiagnosticJSON"
    $wideFocusedStdout = [ordered]@{
        status='failed'
        classification=[string]$wideDiagnostic.classification
        diagnostic_truncated=[bool]$wideDiagnostic.diagnostic_truncated
        failed_checks=[string[]]$wideDiagnostic.failed_checks
        failed_thresholds=[string[]]$wideDiagnostic.failed_thresholds
        runtime_error=[string]$wideDiagnostic.runtime_error
    } | ConvertTo-Json -Depth 10 -Compress
    $wideEvidenceLogPath = Join-Path $diagnosticFixtureDirectory 'wide-sanitized-evidence.log'
    [System.IO.File]::WriteAllLines($wideEvidenceLogPath, [string[]]$wideDiagnostic.log_tail, [System.Text.UTF8Encoding]::new($false))
    $wideSavedLog = Get-Content -Raw -LiteralPath $wideEvidenceLogPath
    foreach ($secret in $wideSecrets) {
        foreach ($sink in @($wideDiagnosticJSON,$wideThrownException,$wideFocusedStdout,$wideSavedLog)) {
            if (([string]$sink).Contains($secret)) { throw 'wide truncated k6 diagnostic leaked a synthetic secret through an output sink' }
        }
    }

    $wideChecks["failed_around_boundary_$($wideSecrets[0])"].passes = 1
    $wideChecks["failed_around_boundary_$($wideSecrets[0])"].fails = 0
    $wideChecks["failed_after_boundary_$($wideSecrets[1])"].passes = 1
    $wideChecks["failed_after_boundary_$($wideSecrets[1])"].fails = 0
    $wideExitZeroSummaryPath = Join-Path $diagnosticFixtureDirectory 'wide-5000-check-exit-zero-summary.json'
    [System.IO.File]::WriteAllText($wideExitZeroSummaryPath, ([ordered]@{
        metrics=[ordered]@{ checks=[ordered]@{ passes=5000; fails=0; value=1; thresholds=[ordered]@{'rate==1'=$false} }; iterations=[ordered]@{count=1} }
        root_group=[ordered]@{ name=''; groups=[ordered]@{}; checks=$wideChecks }
    } | ConvertTo-Json -Depth 12), [System.Text.UTF8Encoding]::new($false))
    $wideExitZeroDiagnostic = Get-M7K6Diagnostic -Script 'production-provider-contract.js' -ExitCode 0 -SummaryPath $wideExitZeroSummaryPath -LogLines @() -SensitiveValues $wideSecrets
    if (-not [bool]$wideExitZeroDiagnostic.diagnostic_truncated -or [bool]$wideExitZeroDiagnostic.summary_inspection_complete -or
        [string]$wideExitZeroDiagnostic.classification -cne 'evidence_runner_diagnostic_failure') {
        throw 'public k6 diagnostic did not fail closed for a misleading exit-zero wide summary'
    }

    $deepNode = [ordered]@{ thresholds=[ordered]@{} }
    foreach ($index in 1..14) { $deepNode = [ordered]@{ ("layer_{0:D2}" -f $index)=$deepNode } }
    $deepSummaryPath = Join-Path $diagnosticFixtureDirectory 'deep-exit-zero-summary.json'
    [System.IO.File]::WriteAllText($deepSummaryPath, ([ordered]@{
        metrics=[ordered]@{ checks=[ordered]@{ passes=1; fails=0; thresholds=[ordered]@{'rate==1'=$false} }; iterations=[ordered]@{count=1}; deep_metric=$deepNode }
        root_group=[ordered]@{ name=''; groups=[ordered]@{}; checks=[ordered]@{} }
    } | ConvertTo-Json -Depth 30), [System.Text.UTF8Encoding]::new($false))
    $deepDiagnostic = Get-M7K6Diagnostic -Script 'production-provider-contract.js' -ExitCode 0 -SummaryPath $deepSummaryPath -LogLines @()
    if (-not [bool]$deepDiagnostic.diagnostic_truncated -or [bool]$deepDiagnostic.summary_inspection_complete -or
        [string]$deepDiagnostic.classification -cne 'evidence_runner_diagnostic_failure') {
        throw 'public k6 diagnostic did not fail closed when its depth bound prevented complete inspection'
    }

    $successSummaryPath = Join-Path $diagnosticFixtureDirectory 'success-summary.json'
    [System.IO.File]::WriteAllText($successSummaryPath, (@{
        metrics = @{
            checks = @{ passes = 19; fails = 0; value = 1; thresholds = @{ 'rate==1' = $false } }
            iterations = @{ count = 1 }
            provider_contract_operation_duration = @{ count = 5; 'p(99)' = 10; thresholds = @{ 'p(99)<10000' = $false } }
        }
        root_group = @{ name = ''; groups = @{}; checks = @{} }
    } | ConvertTo-Json -Depth 10), [System.Text.UTF8Encoding]::new($false))
    $successDiagnostic = Get-M7K6Diagnostic -Script 'production-provider-contract.js' -ExitCode 0 -SummaryPath $successSummaryPath -LogLines @()
    if ([int]$successDiagnostic.check_passes -ne 19 -or [int]$successDiagnostic.check_failures -ne 0 -or
        @($successDiagnostic.failed_checks).Count -ne 0 -or @($successDiagnostic.failed_thresholds).Count -ne 0 -or
        [int]$successDiagnostic.iterations -ne 1 -or [bool]$successDiagnostic.diagnostic_truncated -or
        -not [bool]$successDiagnostic.summary_inspection_complete) {
        throw 'bounded k6 diagnostic did not preserve an all-green strict summary'
    }
} finally {
    foreach ($file in @(Get-ChildItem -LiteralPath $diagnosticFixtureDirectory -File)) { Remove-Item -LiteralPath $file.FullName -Force }
    Remove-Item -LiteralPath $diagnosticFixtureDirectory -Force
}

$workflow = Get-Content -Raw -LiteralPath $workflowPath
foreach ($required in @(
    'workflow_dispatch:', 'pull_request:', 'run-milestone-7-dr-evidence.ps1',
    'ConfirmDestructiveDrill', 'actions/setup-go@4a3601121dd01d1626a1e23e37211e3254c1c06c',
    'actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02',
    'actionlint .github/workflows/milestone-7-dr.yml',
    'test-milestone-7-dr-evidence-runner.ps1 -EvidenceDirectory',
    'if: success()', 'internal/payment/**', 'internal/regional/**', 'internal/platform/**', 'migrations/**'
    ,'cmd/payment-sandbox/**','cmd/admission-worker/**','cmd/read-model-worker/**','cmd/hold-expirer/**','cmd/outbox-worker/**','cmd/booking-command-reconciler/**'
)) {
    if (-not $workflow.Contains($required)) { throw "Milestone 7 DR workflow omits required token: $required" }
}
$tokens = $null
$parseErrors = $null
[System.Management.Automation.Language.Parser]::ParseFile(
    $runnerPath, [ref]$tokens, [ref]$parseErrors
) | Out-Null
if ($parseErrors.Count -ne 0) { throw "Milestone 7 DR runner has $($parseErrors.Count) PowerShell parse errors" }
$tokens = $null
$parseErrors = $null
[System.Management.Automation.Language.Parser]::ParseFile(
    $focusedRunnerPath, [ref]$tokens, [ref]$parseErrors
) | Out-Null
if ($parseErrors.Count -ne 0) { throw "focused provider-contract runner has $($parseErrors.Count) PowerShell parse errors" }

$source = Get-Content -Raw -LiteralPath $runnerPath
$focusedSource = Get-Content -Raw -LiteralPath $focusedRunnerPath
$diagnosticBuildIndex = $source.IndexOf('$diagnostic = Get-M7K6Diagnostic', [System.StringComparison]::Ordinal)
$diagnosticThrowIndex = $source.IndexOf('if ($result.ExitCode -ne 0 -or', [System.StringComparison]::Ordinal)
if ($diagnosticBuildIndex -lt 0 -or $diagnosticThrowIndex -le $diagnosticBuildIndex -or
    -not $source.Contains('-SensitiveValues ([string[]]$sensitiveValues)') -or
    -not $source.Contains('[string[]]$diagnostic.log_tail')) {
    throw 'Milestone 7 runner must build and persist a bounded sanitized k6 diagnostic before throwing'
}
if ($source.Contains('chown 12345:12345') -or $focusedSource.Contains('chown 12345:12345') -or
    $source -match '(?i)(?:--cap-add|cap_add|CAP_CHOWN)' -or $focusedSource -match '(?i)(?:--cap-add|cap_add|CAP_CHOWN)' -or
    -not $source.Contains("'--user','12345:12345'") -or -not $source.Contains('& docker cp') -or
    -not $source.Contains('/tmp/$name-summary.json')) {
    throw 'Milestone 7 k6 summary extraction must use unprivileged container-local storage plus docker cp without ownership capabilities'
}
if (-not $source.Contains('umask 022; exec k6 "$@"') -or -not $focusedSource.Contains('umask 022; exec k6 "$@"')) {
    throw 'Milestone 7 k6 one-off containers must establish a bounded 0644 summary mode without chown'
}
if (-not $source.Contains('[bool]$diagnostic.diagnostic_truncated') -or
    -not $focusedSource.Contains('[bool]$diagnostic.diagnostic_truncated') -or
    -not $focusedSource.Contains('diagnostic_truncated = $false')) {
    throw 'Milestone 7 runners must fail closed when shared diagnostic traversal is truncated'
}
if (-not $source.Contains('@($diagnostic.failed_thresholds).Count -ne 0') -or
    -not $source.Contains('[int64]$diagnostic.iterations -lt 1')) {
    throw 'authoritative Milestone 7 runner must reject crossed thresholds and missing iterations before publishing evidence'
}
$summaryReadIndex = $source.IndexOf('$summary = Get-Content -Raw -LiteralPath $summaryPath | ConvertFrom-Json', $diagnosticThrowIndex, [System.StringComparison]::Ordinal)
if ($summaryReadIndex -le $diagnosticThrowIndex -or
    -not $source.Substring($diagnosticThrowIndex, $summaryReadIndex - $diagnosticThrowIndex).Contains('$null -eq $copyResult -or $copyResult.ExitCode -ne 0')) {
    throw 'authoritative Milestone 7 runner must directly reject a missing or failed docker cp result'
}
foreach ($required in @(
    'grafana/k6:0.57.0',
    "build','--target','payment-stripe-contract'",
    '[System.Security.Cryptography.RandomNumberGenerator]::GetBytes(32)',
    'PAYMENT_STRIPE_CONTRACT_TEST_ONLY=true',
    'PAYMENT_PROVIDER_ACCOUNT_ID=acct_m7_contract',
    'PAYMENT_PROVIDER_API_VERSION=2026-07-29.dahlia',
    'PAYMENT_STRIPE_CONTRACT_PAGE_BARRIER_DELAY=8s',
    'PAYMENT_STRIPE_CONTRACT_ADAPTER_ORIGIN=http://$($contractAlias):8100',
    "'--user','12345:12345'", '/tmp/production-provider-contract-summary.json',
    "'VUS=1'", "'ITERATIONS_PER_VU=1'", "'DURATION=1m'",
    '/readyz','/adapter/balance-transactions','/adapter/payouts','/adapter/error-classification',
    '--summary-export','production-provider-contract.js',
    "'--read-only'", "'--security-opt','no-new-privileges'", "'--cap-drop','ALL'",
    'm7.focused.project=', '[System.IO.Directory]::Delete($evidenceDirectory, $true)',
    '[System.Diagnostics.ProcessStartInfo]::new()', '@(Get-Command docker -CommandType Application -ErrorAction Stop)[0]',
    'WaitForExit($TimeoutSeconds * 1000)', 'Kill($true)',
    "'image','ls','--quiet','--no-trunc','--filter'", 'reference=$imageName', 'image_cleanup_passed',
    '$builtImageID', '{{.Id}}|{{index .Config.Labels "m7.focused.project"}}',
    'field_assertions', 'k6_summary_copied', 'k6_container_removed',
    'try { [System.IO.Directory]::Delete($evidenceDirectory, $true) }', '$k6ContainerName'
)) {
    if (-not $focusedSource.Contains($required)) { throw "focused provider-contract runner omits bounded topology token: $required" }
}
if ($focusedSource -match '(?s)\$finalResult\.status\s*=\s*''passed''\s*\r?\n\s*\$finalResult\.classification\s*=\s*''evidence_runner_diagnostic_failure''') {
    throw 'focused provider-contract runner cannot report a diagnostic failure classification on success'
}
if ($focusedSource.Contains('run-milestone-7-dr-evidence.ps1') -or
    $focusedSource -match "(?m)'(?:-p|--publish)'" -or
    $focusedSource -match "(?m)'volume','create'" -or
    $focusedSource -match '(?i)\b(control-postgres|booking-shard-[01]-postgres|redis|payment-worker|payment-reconciler|settlement-worker|pgbackrest|failover|failback)\b') {
    throw 'focused provider-contract runner includes a forbidden full-DR service or resource shortcut'
}
$lastProbeIndex = $focusedSource.IndexOf('$finalResult.direct_probes_passed = $true', [System.StringComparison]::Ordinal)
$k6Index = $focusedSource.IndexOf('$k6 = Invoke-M7FocusedNative', [System.StringComparison]::Ordinal)
if ($lastProbeIndex -lt 0 -or $k6Index -le $lastProbeIndex) {
    throw 'focused provider-contract runner must complete direct probes before k6'
}
if (-not $focusedSource.Contains("-not `$diagnostic.Contains('summary_inspection_complete')") -or
    -not $focusedSource.Contains('$finalResult.summary_inspection_complete = [bool]$diagnostic.summary_inspection_complete') -or
    $focusedSource.Contains('$finalResult.summary_inspection_complete = $true')) {
    throw 'focused provider-contract runner must directly wire a guarded Boolean summary inspection result'
}
$missingInspectionDiagnostic = [ordered]@{ diagnostic_truncated=$false }
$missingInspectionResult = [ordered]@{ status='failed'; summary_inspection_complete=$false }
$missingInspectionRejected = $false
try {
    if (-not $missingInspectionDiagnostic.Contains('summary_inspection_complete') -or
        $missingInspectionDiagnostic.summary_inspection_complete -isnot [bool]) {
        throw [System.InvalidOperationException]::new('missing Boolean summary inspection contract')
    }
    $missingInspectionResult.summary_inspection_complete = [bool]$missingInspectionDiagnostic.summary_inspection_complete
} catch [System.InvalidOperationException] {
    $missingInspectionRejected = $true
}
if (-not $missingInspectionRejected -or [bool]$missingInspectionResult.summary_inspection_complete -or
    [string]$missingInspectionResult.status -ceq 'passed') {
    throw 'focused provider-contract result assembly did not fail closed for a missing completeness property'
}
$pwshCommand = @(Get-Command pwsh -CommandType Application -ErrorAction Stop)[0]
$focusedInvocationOutput = [string[]]@(
    & $pwshCommand.Source -NoLogo -NoProfile -File $focusedRunnerPath 2>&1 |
        ForEach-Object { [string]$_ }
)
$focusedInvocationExitCode = $LASTEXITCODE
$focusedJSONLine = [string]@(
    $focusedInvocationOutput |
        Where-Object { $_.TrimStart().StartsWith('{') -and $_.TrimEnd().EndsWith('}') } |
        Select-Object -Last 1
)
if ($focusedInvocationExitCode -ne 0 -or [string]::IsNullOrWhiteSpace($focusedJSONLine)) {
    throw 'focused provider-contract runner did not emit a successful strict JSON result'
}
try { $focusedJSONResult = $focusedJSONLine | ConvertFrom-Json } catch { throw 'focused provider-contract runner emitted malformed final JSON' }
$inspectionProperty = $focusedJSONResult.PSObject.Properties['summary_inspection_complete']
if ($null -eq $inspectionProperty) { throw 'focused provider-contract final JSON omits summary_inspection_complete' }
if ($inspectionProperty.Value -isnot [bool]) { throw 'focused provider-contract final JSON summary_inspection_complete is not Boolean' }
if (-not [bool]$inspectionProperty.Value -or [bool]$focusedJSONResult.diagnostic_truncated) {
    throw 'focused provider-contract final JSON does not prove complete non-truncated summary inspection'
}
$assignmentQueryContracts = @(
    @{
        Name = 'healthy physical-shard assignment selection'
        Pattern = '(?ms)^\s*\$m7HealthyTrain\s*=\s*Get-M7Scalar\b.*?-SQL\s+@"\r?\n(?<sql>SELECT\b.*?)\r?\n"@'
        Required = @(
            'FROM public.train_run_shard_assignments',
            "WHERE shard_id='physical-shard-1'",
            'train_run_id IN (',
            'ORDER BY train_run_id LIMIT 1'
        )
    },
    @{
        Name = 'post-cutover refund route'
        Pattern = '(?ms)^\s*\$postCutoverRefundRoute\s*=\s*Get-M7Scalar\b.*?-SQL\s+@"\r?\n(?<sql>SELECT\b.*?)\r?\n"@'
        Required = @('FROM public.train_run_shard_assignments', "train_run_id='`$m7Train'::uuid")
    },
    @{
        Name = 'reverse refund assignment evidence'
        Pattern = '(?ms)^\s*\$reverseRefundEvidence\s*=\s*Get-M7Scalar\b.*?-SQL\s+@"\r?\n(?<sql>SELECT\b.*?)\r?\n"@'
        Required = @('FROM public.train_run_shard_assignments AS assignment', "assignment.train_run_id='`$m7Train'::uuid")
    },
    @{
        Name = 'return refund assignment evidence'
        Pattern = '(?m)^\s*\$returnRefundEvidence\s*=\s*Get-M7Scalar\b.*?-SQL\s+"(?<sql>SELECT\b[^"\r\n]+)"\s*$'
        Required = @('FROM public.train_run_shard_assignments', "train_run_id='`$m7Train'::uuid")
    }
)
foreach ($contract in $assignmentQueryContracts) {
    $queryMatches = [regex]::Matches($source, [string]$contract.Pattern)
    if ($queryMatches.Count -ne 1) {
        throw "Milestone 7 runner must contain exactly one $($contract.Name) query; found $($queryMatches.Count)"
    }
    $assignmentSQL = $queryMatches[0].Groups['sql'].Value
    foreach ($requiredToken in $contract.Required) {
        if (-not $assignmentSQL.Contains($requiredToken)) {
            throw "$($contract.Name) query omits required token: $requiredToken"
        }
    }
    if ($assignmentSQL -match '(?i)\bis_current\b') {
        throw "$($contract.Name) query assumes nonexistent train_run_shard_assignments.is_current"
    }
}
foreach ($replicationBootstrapGuard in @(
    '/etc/railway/configure-replication.sh',
    'pg_hba_file_rules',
    'for ($attempt = 0; $attempt -lt 20; $attempt++)',
    'managed replication HBA diagnostics',
    "WHERE file_name LIKE '%pg_hba.replication.conf' OR error IS NOT NULL",
    "database @> ARRAY['replication']::text[]",
    "auth_method='scram-sha-256'",
    'replication-hba-rules.json',
    "Add-M7Phase 'replication-hba-rules-loaded'"
)) {
    if (-not $source.Contains($replicationBootstrapGuard)) {
        throw "DR runner omits live replication HBA proof: $replicationBootstrapGuard"
    }
}
foreach ($topologyPreflightGuard in @(
    'topology-preflight.json',
    "Add-M7Phase 'topology-preflight-synchronized'",
    "'--reason','region_failure','--dry-run','--timeout','2m'",
    'typed failover dry-run preflight did not validate the synchronized topology',
    'sanitized failover-plan PostgreSQL diagnostics'
)) {
    if (-not $source.Contains($topologyPreflightGuard)) { throw "Milestone 7 runner is missing topology preflight guard: $topologyPreflightGuard" }
}
foreach ($replayFreshnessGuard in @(
    'replayFreshnessMarkerEpoch',
    'replayFreshnessLSN',
    'WITH updated AS (UPDATE public.dr_evidence_markers',
    'Wait-M7Replay -Service $database.Standby',
    'PGOPTIONS=-c role=pg_read_all_stats',
    'primary_tls_streaming_rechecked',
    'receive_covers_marker',
    'standby replay diagnostic',
    'replay_freshness_marker_lsn',
    'replay_freshness_marker_epoch'
)) {
    if (-not $source.Contains($replayFreshnessGuard)) { throw "Milestone 7 runner is missing replay freshness proof: $replayFreshnessGuard" }
}
foreach ($disconnectHealthGuard in @(
    "'stop','--timeout','10','control-postgres'",
    'standby health was not ready before the disconnect test',
    'caught-up standby health did not fail after the receiver disconnected',
    'standby health did not recover after streaming reconnected',
    "Add-M7Phase 'standby-disconnect-health-recovered'"
)) {
    if (-not $source.Contains($disconnectHealthGuard)) { throw "Milestone 7 runner is missing disconnect-sensitive standby health proof: $disconnectHealthGuard" }
}
$requiredGuardrails = @(
    'ConfirmDestructiveDrill',
    'EvidenceDirectory must be outside the source repository',
    'ProjectName already owns Docker resources',
    'down','-v','--remove-orphans',
    'label=com.docker.compose.project=',
    'pg_stat_replication',
    'pg_is_in_recovery()',
    'stanza-create',
    'pg_stat_archiver',
    'populated_v5_fixture.sql', 'seed_hot_train_v6_fixture.sql',
    'seed_read_model_v7_fixture.sql', 'seed_milestone4_v7_fixture.sql',
    'seed_milestone7_v10_fixture.sql',
    "'up-to','7'", "'up-to','10'", "'up-to','1'",
    'restore-validation',
    "set_config('railway.deployment_region'",
    "set_config('railway.deployment_role','recovery'",
    "set_config('railway.region_epoch'",
    "set_config('railway.regional_writes_enabled','false'",
    'region-a-externally-fenced',
    'region-b-externally-fenced-for-failback',
    'region-a-reseeded-from-region-b',
    'source_state_sha256',
    'rendered_compose_config_sha256',
    'evidence-index.json',
    'Assert-M7EvidenceSecretSafe',
    'ExpectedDatabaseTuples', 'required_tuples', 'tuple_coverage',
    'durable_lease_survived_crash', 'lease_token IS NOT NULL', 'lease_released',
    'webhookPreviousB64', 'webhookCurrentB64', 'M7_WEBHOOK_KEYRING',
    'M7_STRIPE_WEBHOOK_KEYRING', 'Stripe-Signature',
    'payment_webhook_key_rotation_audit', 'payment_webhook_key_version_archive',
    'stripe-webhook-durable-grace-lifecycle-proven',
    "EXPECTED_WEBHOOK_STATUS='500'",
    'passive-webhook-keys-verified-and-writes-rejected',
    'Ensure-M7PromotedPrimary', 'Ensure-M7ServicesStopped', 'controller_reobservation_probe', 'resumed_by_reobservation',
    'runtime-database-roles-bounded', 'region-b-complete-authority-set.json',
    'region-a-complete-authority-set.json', 'control_last=$true', 'external_action_reobserved=$true',
    'prematureRegionBServices', 'prematureRegionAServices',
    'worker or proxy started before complete database activation',
    'worker or proxy started before complete failback database activation',
    'backup_expiration_mutation=$false',
    'completed_at_epoch',
    'PITR marker window was not ordered',
    'PITR sentinel WAL was not archived',
    'excluded_sentinel_marker_count',
    'WITH inserted AS (INSERT INTO public.dr_evidence_markers(marker) VALUES (5) RETURNING created_at) SELECT created_at::text FROM inserted',
    'migrations/testdata/assert_booking_shard_v3_financial_evidence.sql',
    "@('bootstrap-booking-shard-0','bootstrap-booking-shard-1')",
    'physical-shards-bootstrapped',
    "@('ps','--all',`$Service)",
    '[System.Security.Cryptography.RandomNumberGenerator]::GetBytes(32)',
    '$script:m7FixtureClientSequence = 0',
    'for ($groupStart=0; $groupStart -lt $Count; $groupStart+=3)',
    'Tokens=[string[]]$tokens',
    '$m7Customer.Tokens[$reservationIndex]',
    'Write-Host "::add-mask::$token"',
    'function Wait-M7IssuedOrder',
    "'missing')",
    'payment completed without converging to one issued order and two active tickets',
    "@('logs','--no-color','--tail','80','payment-worker-1','payment-worker-2')",
    "@('payment-worker-1','payment-worker-2')",
    "'saga='||saga.state||':'||saga.current_step",
    'ON intent.payment_intent_id=saga.payment_intent_id',
    'source_marker=2', 'source_marker=3', 'target_marker_count=$targetMarkerCount',
    'maximum_missing_markers_per_database=1', 'maximum_missing_wal_bytes_per_database=536870912'
)
foreach ($guardrail in $requiredGuardrails) {
    if (-not $source.Contains($guardrail)) { throw "Milestone 7 DR runner omits guardrail: $guardrail" }
}
$randomWebhookKeyCount = [regex]::Matches($source, '\[System\.Security\.Cryptography\.RandomNumberGenerator\]::GetBytes\(32\)').Count
if ($randomWebhookKeyCount -ne 2) { throw "Milestone 7 runner must generate exactly two independent 32-byte webhook keys; found $randomWebhookKeyCount" }
if ($source.Contains('[System.Text.Encoding]::UTF8.GetBytes("m7-prev-')) { throw 'Milestone 7 runner must not derive webhook keys from variable-length prefixed text' }
if ($source.Contains('Write-Output "::add-mask::$token"')) { throw 'M7 fixture creation must not pollute its object return pipeline with masking directives' }
$forwardCutoverIndex = $source.IndexOf('Move-Milestone5Migration -Context $driverContext -MigrationID $m7Migration -Target rollback_window', [System.StringComparison]::Ordinal)
$fixtureCreateIndex = $source.IndexOf('$m7Customer = New-M7CustomerFixtures', [System.StringComparison]::Ordinal)
$reverseBarrierIndex = $source.IndexOf('Move-Milestone5Migration -Context $driverContext -MigrationID $m7ReverseMigration -Target validating_online', [System.StringComparison]::Ordinal)
$partialRefundIndex = $source.IndexOf("Invoke-M7K6 -Script 'partial-ticket-refund.js'", [System.StringComparison]::Ordinal)
$reverseCutoverIndex = $source.IndexOf('Move-Milestone5Migration -Context $driverContext -MigrationID $m7ReverseMigration -Target rollback_window', [System.StringComparison]::Ordinal)
if ($forwardCutoverIndex -lt 0 -or $fixtureCreateIndex -le $forwardCutoverIndex -or
    $reverseBarrierIndex -le $fixtureCreateIndex -or $partialRefundIndex -le $reverseBarrierIndex -or
    $reverseCutoverIndex -le $partialRefundIndex) {
    throw 'partial refund evidence must use a physical assignment while reverse migration remains in validating_online'
}
$settlementDisabledIndex = $source.IndexOf("`$env:REGION_A_SETTLEMENT_ENABLED = 'false'", [System.StringComparison]::Ordinal)
$initialServicesIndex = $source.IndexOf("`$regionAInitialAppServices = @(`$regionAAppServices | Where-Object { `$_ -ne 'settlement-worker-region-a' })", [System.StringComparison]::Ordinal)
$initialAppUpIndex = $source.IndexOf('$activeAppUp += $regionAInitialAppServices', [System.StringComparison]::Ordinal)
$settlementEnabledIndex = $source.IndexOf("`$env:REGION_A_SETTLEMENT_ENABLED = 'true'", [System.StringComparison]::Ordinal)
$settlementUpIndex = $source.IndexOf("`$settlementUp += 'settlement-worker-region-a'", [System.StringComparison]::Ordinal)
if ($settlementDisabledIndex -lt 0 -or $initialServicesIndex -le $settlementDisabledIndex -or
    $initialAppUpIndex -le $initialServicesIndex -or $settlementEnabledIndex -le $initialAppUpIndex -or
    $settlementUpIndex -le $settlementEnabledIndex) {
    throw 'region A settlement worker must remain stopped until settlement processing is explicitly enabled'
}
$failoverMarkerIndex = $source.IndexOf("INSERT INTO public.dr_evidence_markers(marker) VALUES (2)", [System.StringComparison]::Ordinal)
$failoverStopIndex = $source.IndexOf("Ensure-M7ServicesStopped -Services @(`$databases.Primary)", $failoverMarkerIndex, [System.StringComparison]::Ordinal)
$failbackMarkerIndex = $source.IndexOf("INSERT INTO public.dr_evidence_markers(marker) VALUES (3)", [System.StringComparison]::Ordinal)
$failbackStopIndex = $source.IndexOf("Ensure-M7ServicesStopped -Services @(`$databases.Standby)", $failbackMarkerIndex, [System.StringComparison]::Ordinal)
if ($failoverMarkerIndex -lt 0 -or $failoverStopIndex -le $failoverMarkerIndex -or
    $source.Substring($failoverMarkerIndex, $failoverStopIndex-$failoverMarkerIndex).Contains('Wait-M7Replay')) {
    throw 'failover RPO marker is awaited before the source fence'
}
if ($failbackMarkerIndex -lt 0 -or $failbackStopIndex -le $failbackMarkerIndex -or
    $source.Substring($failbackMarkerIndex, $failbackStopIndex-$failbackMarkerIndex).Contains('Wait-M7Replay')) {
    throw 'failback RPO marker is awaited before the source fence'
}
if ($source.Contains('passive-webhook-overlap-generation-verified') -or
    $source -match "api-region-b-[12].*ExpectedStatus @\(200\)") {
    throw 'Milestone 7 DR runner grants webhook acknowledgement authority to a passive region'
}
$dockerfile = Get-Content -Raw -LiteralPath $dockerfilePath
foreach ($required in @(
    'PGBACKREST_VERSION=2.59.0',
    'PGBACKREST_SHA256=faaf8faa14a6392279654ee216a493fcd07b0c513af4b55fe34faec062cb8875',
    'pgbackrest-${PGBACKREST_VERSION}.tar.gz',
    'sha256sum -c -',
    'COPY --from=pgbackrest-build'
)) {
    if (-not $dockerfile.Contains($required)) { throw "Dockerfile omits reproducible pgBackRest contract: $required" }
}
if ($dockerfile -match '(?m)^FROM postgres:16\.14-alpine AS postgres-dr\r?\nRUN apk add[^\r\n]*\bpgbackrest\b') {
    throw 'postgres-dr must not install a drifting repository pgBackRest package'
}
if ($source -match '(?im)^\s*(docker\s+(system\s+prune|volume\s+prune)|Remove-Item\s+.+-Recurse)') {
    throw 'Milestone 7 DR runner contains unscoped destructive cleanup'
}

$compose = Get-Content -Raw -LiteralPath $composePath
$restoreTargetBindingCount = [regex]::Matches($compose, '(?m)^\s+PGBACKREST_PITR_TARGET:\s+').Count
if ($restoreTargetBindingCount -ne 4) { throw "Milestone 7 Compose must bind the PITR target in the restore anchor and all three overriding service environments; found $restoreTargetBindingCount" }
foreach ($required in @(
    'control-postgres-region-b', 'booking-shard-0-postgres-region-b',
    'booking-shard-1-postgres-region-b', 'synchronous_commit=local',
    'start-standby.sh', 'pgbackrest-control-repository', 'pgbackrest-shard-0-repository', 'pgbackrest-shard-1-repository', 'PGBACKREST_CIPHER_FILE', 'archive-push',
    'dr-replication', 'dr-control', 'region-a-app', 'region-a-data', 'region-b-app', 'region-b-data'
)) {
    if (-not $compose.Contains($required)) { throw "DR Compose omits required topology token: $required" }
}
if ($compose -match '(?m)^include:\s*$') { throw 'DR Compose must use ordered -f overlays rather than conflicting imported resources' }
foreach ($required in @(
    'docker-compose.physical-shards.yml', 'deploy/compose/payment.override.yml',
    'docker-compose.dr.yml', 'deploy/compose/dr-app.override.yml'
)) {
    if (-not $source.Contains($required)) { throw "DR runner omits required Compose layer: $required" }
}

$appComposePath = Join-Path $root 'deploy/compose/dr-app.override.yml'
$appCompose = Get-Content -Raw -LiteralPath $appComposePath
foreach ($required in @(
    'api-1:', 'api-2:', 'api-3:',
    'payment-worker-1:', 'payment-worker-2:', 'payment-reconciler:',
    'settlement-worker-region-a:', 'redis:', 'proxy-region-a:',
    'api-region-b-1:', 'api-region-b-2:', 'api-region-b-3:',
    'payment-worker-region-b-1:', 'payment-worker-region-b-2:', 'payment-reconciler-region-b:',
    'settlement-worker-region-b:', 'redis-region-b:', 'proxy-region-b:',
    'admission-worker-region-b-1:', 'admission-worker-region-b-2:',
    'read-model-worker-region-b-1:', 'read-model-worker-region-b-2:',
    'hold-expirer-region-b:', 'outbox-worker-region-b:', 'booking-command-reconciler-region-b:',
    'payment-stripe-contract:', 'stripe-webhook-api-region-a:', 'stripe-webhook-api-region-b:',
    'global-test-ingress:', 'k6-milestone-7:'
)) {
    if (-not $appCompose.Contains($required)) { throw "DR app Compose omits required topology token: $required" }
}
if (-not $appCompose.Contains('SETTLEMENT_WORKER_TIMEOUT: 30s')) {
    throw 'DR app Compose omits the bounded settlement lease/runtime timeout contract'
}
foreach ($required in @(
    'PAYMENT_PROVIDER_API_KEY: ${PAYMENT_CONTRACT_API_KEY}',
    'PAYMENT_PROVIDER_TYPE: stripe', 'PAYMENT_PROVIDER_WEBHOOK_KEYRING: ${M7_STRIPE_WEBHOOK_KEYRING}',
    'REGION_A_CONTROL_DATABASE_URL', 'REGION_A_DEPLOYMENT_ROLE', 'REGION_A_EPOCH',
    'REGION_A_WRITES_ENABLED', 'control-postgres-region-a-reseed'
)) {
    if (-not $appCompose.Contains($required) -and -not $source.Contains($required)) {
        throw "DR app/failback contract omits required token: $required"
    }
}
if (-not ((Get-Content -Raw -LiteralPath (Join-Path $root 'deploy/compose/payment.override.yml')).Contains('PAYMENT_SANDBOX_WEBHOOK_KEYRING: ${M7_WEBHOOK_KEYRING:-current=${PAYMENT_WEBHOOK_KEY_B64:-'))) {
    throw 'payment sandbox signer and API verifier must consume the same M7 webhook keyring'
}
if ($appCompose -match 'PAYMENT_CONTRACT_API_KEY:-') {
    throw 'DR app Compose must not contain a committed provider API key fallback'
}
foreach ($required in @(
    'networks: !override [region-a-app, region-a-data]', 'networks: !override [region-b-app, region-b-data]',
    'postgresql://railway_runtime:runtime-local-only@control-postgres',
    'postgresql://railway_runtime:runtime-local-only@control-postgres-region-b'
)) {
    if (-not $appCompose.Contains($required)) { throw "DR app Compose omits regional isolation/runtime identity: $required" }
}
foreach ($proxyContract in @(
    'networks: [region-a-app, dr-test-ingress]',
    'networks: [region-b-app, dr-test-ingress]'
)) {
    if (-not $appCompose.Contains($proxyContract)) { throw "DR app Compose proxy bypasses the app-only regional network: $proxyContract" }
}
if ($appCompose.Contains('networks: !override [region-a]') -or $appCompose.Contains('networks: !override [region-b]')) {
    throw 'DR app Compose retains a shared app/data regional network'
}
$physicalCompose = Get-Content -Raw -LiteralPath (Join-Path $root 'docker-compose.physical-shards.yml')
foreach ($required in @(
    'runtime-control-role:', 'runtime-booking-shard-0-role:', 'runtime-booking-shard-1-role:',
    'CREATE ROLE railway_runtime LOGIN PASSWORD', 'NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT',
    'REVOKE ALL ON public.regional_write_authority', 'public.backup_expiration_operations FROM railway_runtime',
    'GRANT EXECUTE ON FUNCTION public.lock_regional_write_authority() TO railway_runtime',
    'postgresql://railway_runtime:runtime-local-only@'
)) {
    if (-not $physicalCompose.Contains($required)) { throw "physical Compose omits bounded runtime role contract: $required" }
}

$requiredK6 = @(
    'production-provider-contract.js', 'settlement-import.js', 'partial-ticket-refund.js',
    'partial-refund-idempotency.js', 'webhook-ack-failure.js', 'webhook-key-rotation.js',
    'regional-failover.js', 'payment-during-failover.js', 'refund-during-failover.js',
    'regional-failback.js'
)
foreach ($name in $requiredK6) {
    if (-not (Test-Path -LiteralPath (Join-Path $root "loadtest/k6/$name"))) {
        throw "Milestone 7 k6 module is missing: $name"
    }
    if ([regex]::Matches($source, [regex]::Escape("-Script '$name'")).Count -ne 1) {
        throw "Milestone 7 runner must invoke exact k6 module once: $name"
    }
}
if ([regex]::Matches($source, "Invoke-M7K6 -Script '[^']+\.js'").Count -ne 10) {
    throw 'Milestone 7 runner must invoke exactly the ten required k6 modules and no auxiliary module'
}
$providerContractSource = Get-Content -Raw -LiteralPath (Join-Path $root 'loadtest/k6/production-provider-contract.js')
foreach ($required in @(
    '/adapter/balance-transactions','/adapter/payouts','/adapter/error-classification',
    'provider_contract_operation_duration','provider_auth_rejection','provider_unavailable',
    "value.json('retryable') === true","value.json('uncertain') === false",'p(99)<10000'
)) {
    if (-not $providerContractSource.Contains($required)) { throw "provider contract k6 omits bounded behavioral evidence: $required" }
}
$settlementLoadSource = Get-Content -Raw -LiteralPath (Join-Path $root 'loadtest/k6/settlement-import.js')
foreach ($required in @(
    'settlement_import_total', 'settlement_lag_seconds_sum', 'settlement_lag_seconds_count',
    'SETTLEMENT_OBSERVATION_SECONDS', 'settlementImportRate.add', 'settlementImportLag.add',
    "'settlement import work is visible'", "'settlement lag is measured'"
)) {
    if (-not $settlementLoadSource.Contains($required)) { throw "settlement import k6 omits bounded work/rate/lag evidence: $required" }
}
$expectedStaticCrashPoints = @(
    'capture_provider_committed','ticket_issue_shard_committed','refund_provider_committed',
    'refund_compensation_shard_committed','partial_refund_provider_committed','partial_refund_shard_committed'
)
foreach ($point in $expectedStaticCrashPoints) {
    if ([regex]::Matches($source, [regex]::Escape("-Point '$point'")).Count -ne 1) { throw "Milestone 7 runner must invoke application crash point exactly once: $point" }
}

if (-not [string]::IsNullOrWhiteSpace($EvidenceDirectory)) {
    $EvidenceDirectory = [System.IO.Path]::GetFullPath($EvidenceDirectory)
    foreach ($name in @('run-manifest.json','milestone-7-dr-evidence-summary.json','evidence-index.json')) {
        $path = Join-Path $EvidenceDirectory $name
        if (-not (Test-Path -LiteralPath $path)) { throw "DR evidence is missing $name" }
        $value = Get-Content -Raw -LiteralPath $path | ConvertFrom-Json
        if ([string]$value.status -notin @('passed','complete')) { throw "DR evidence $name is not terminal and passed" }
    }
    $manifest = Get-Content -Raw -LiteralPath (Join-Path $EvidenceDirectory 'run-manifest.json') | ConvertFrom-Json
    $summary = Get-Content -Raw -LiteralPath (Join-Path $EvidenceDirectory 'milestone-7-dr-evidence-summary.json') | ConvertFrom-Json
    $index = Get-Content -Raw -LiteralPath (Join-Path $EvidenceDirectory 'evidence-index.json') | ConvertFrom-Json
    $bindingPath = Join-Path $EvidenceDirectory 'compose-binding.json'
    if (-not (Test-Path -LiteralPath $bindingPath)) { throw 'DR evidence is missing compose-binding.json' }
    $binding = Get-Content -Raw -LiteralPath $bindingPath | ConvertFrom-Json

    $exactExclusions = @('docs/benchmark-report-milestone-7.md','docs/milestone-7-load-testing.md')
    if (@($summary.source_state_exclusions).Count -ne 2 -or @($manifest.source_state_exclusions).Count -ne 2 -or
        (Compare-Object $exactExclusions @($summary.source_state_exclusions) -CaseSensitive) -or
        (Compare-Object $exactExclusions @($manifest.source_state_exclusions) -CaseSensitive)) {
        throw 'DR evidence source-state exclusions are not the exact publication-document fixed point'
    }
    $sourceEntries = [System.Collections.Generic.List[string]]::new()
    $sourcePaths = @(& git -C $root ls-files --cached --others --exclude-standard)
    if ($LASTEXITCODE -ne 0 -or $sourcePaths.Count -eq 0) { throw 'DR evidence source-state recomputation failed' }
    foreach ($relative in @($sourcePaths | Sort-Object -Unique)) {
        $normalized = ([string]$relative).Replace('\','/')
        if ($normalized -in $exactExclusions) { continue }
        $full = Join-Path $root ([string]$relative)
        if (-not [System.IO.File]::Exists($full)) { $sourceEntries.Add("$normalized|missing"); continue }
        $file = [System.IO.FileInfo]::new($full)
        $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $full).Hash.ToLowerInvariant()
        $sourceEntries.Add("$normalized|$($file.Length)|$hash")
    }
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $sourceBytes = [System.Text.Encoding]::UTF8.GetBytes(($sourceEntries -join "`n"))
        $sourceDigest = (($sha.ComputeHash($sourceBytes) | ForEach-Object { $_.ToString('x2') }) -join '')
    } finally { $sha.Dispose() }
    if ($sourceDigest -cne [string]$summary.source_state_sha256 -or $sourceDigest -cne [string]$manifest.source_state_sha256 -or
        $sourceEntries.Count -ne [int]$summary.source_file_count -or $sourceEntries.Count -ne [int]$manifest.source_file_count) {
        throw 'DR evidence source-state binding does not match the current non-publication source tree'
    }

    if ([string]$binding.rendered_compose_config_sha256 -cne [string]$summary.rendered_compose_config_sha256 -or
        [string]$binding.rendered_compose_config_sha256 -cne [string]$manifest.rendered_compose_config_sha256 -or
        [int]$binding.source_count -ne @($binding.sources).Count) { throw 'DR rendered Compose binding metadata is inconsistent' }
    foreach ($entry in @($binding.sources)) {
        $path = Join-Path $root ([string]$entry.path)
        if (-not [System.IO.File]::Exists($path) -or (Get-FileHash -Algorithm SHA256 -LiteralPath $path).Hash.ToLowerInvariant() -cne [string]$entry.sha256) {
            throw "DR Compose binding source changed: $($entry.path)"
        }
    }

    $indexedFiles = @(Get-ChildItem -LiteralPath $EvidenceDirectory -File | Where-Object { $_.Name -ne 'evidence-index.json' } | Sort-Object Name)
    if ($indexedFiles.Count -ne [int]$index.file_count -or @($index.files).Count -ne $indexedFiles.Count) { throw 'DR evidence index file count is inconsistent' }
    $canonical = [System.Collections.Generic.List[string]]::new()
    foreach ($file in $indexedFiles) {
        $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $file.FullName).Hash.ToLowerInvariant()
        $entry = @($index.files | Where-Object { [string]$_.path -ceq $file.Name })
        if ($entry.Count -ne 1 -or [int64]$entry[0].bytes -ne $file.Length -or [string]$entry[0].sha256 -cne $hash) { throw "DR evidence index mismatch for $($file.Name)" }
        $canonical.Add("$($file.Name)|$($file.Length)|$hash")
    }
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try { $bundleDigest = (($sha.ComputeHash([System.Text.Encoding]::UTF8.GetBytes(($canonical -join "`n"))) | ForEach-Object { $_.ToString('x2') }) -join '') }
    finally { $sha.Dispose() }
    if ($bundleDigest -cne [string]$index.bundle_sha256) { throw 'DR evidence index bundle digest is inconsistent' }

    $expectedReplicationTuples = @(
        'region-b|control|none',
        'region-b|booking_shard|shard-0',
        'region-b|booking_shard|shard-1'
    )
    $expectedReplicationCoverage = [System.Collections.Generic.List[string]]::new()
    foreach ($family in @('regional_replication_lag_bytes','regional_replication_lag_seconds','regional_last_replay_timestamp_seconds')) {
        foreach ($tuple in $expectedReplicationTuples) { $expectedReplicationCoverage.Add("$family|$tuple") }
    }
    $invalidPassiveMetrics = 0
    foreach ($entry in @($summary.durable_metrics | Where-Object { $_.phase -in @('region-b-passive-before-failover','region-b-final-passive-after-failback') })) {
        if (@($entry.present_families).Count -ne 3 -or
            'regional_last_replay_timestamp_seconds' -notin @($entry.nonzero_families) -or
            [bool]$entry.truncated -or
            (Compare-Object $expectedReplicationTuples @($entry.required_tuples) -CaseSensitive) -or
            (Compare-Object @($expectedReplicationCoverage) @($entry.tuple_coverage) -CaseSensitive)) {
            $invalidPassiveMetrics++
        }
    }

    $expectedJournalCrashStages = @(
        'external_fencing_verified','control_promoted','shard_0_promoted','epoch_allocated',
        'control_recovery_installed','ingress_switched','customer_writes_configured'
    )
    $invalidJournalCrashStages = 0
    foreach ($kind in @('failover','failback')) {
        $actual = @($summary.journal_process_crashes | Where-Object { $_.operation_kind -ceq $kind } | ForEach-Object { [string]$_.stage } | Sort-Object -Unique)
        if ($actual.Count -ne 7 -or (Compare-Object $expectedJournalCrashStages $actual -CaseSensitive)) { $invalidJournalCrashStages++ }
    }
    $expectedApplicationCrashPoints = @(
        'capture_provider_committed','ticket_issue_shard_committed','refund_provider_committed',
        'refund_compensation_shard_committed','partial_refund_provider_committed','partial_refund_shard_committed'
    )
    $actualApplicationCrashPoints = @($summary.application_transaction_crashes | ForEach-Object { [string]$_.point } | Sort-Object -Unique)
    $requiredSwitchPhases = @(
        'global-ingress-switched-to-region-b-after-complete-authority',
        'global-ingress-switched-to-region-a-after-complete-authority',
        'stripe-webhook-durable-grace-lifecycle-proven'
    )
    $actualPhases = @($summary.phases | Where-Object { $_.status -ceq 'passed' } | ForEach-Object { [string]$_.name })
    $expectedFailoverRTOSeconds = [Math]::Round(([double]$summary.rto_conditions.failover.duration_ms / 1000), 3)
    $expectedFailbackRTOSeconds = [Math]::Round(([double]$summary.rto_conditions.failback.duration_ms / 1000), 3)
    $settlementLoad = @($summary.settlement | Where-Object { [string]$_.phase -ceq 'region-a-load-measurement' })

    if (-not [bool]$manifest.source_state_verified -or [string]$manifest.evidence_secret_scan -ne 'passed' -or [string]$manifest.teardown -ne 'passed' -or
        [string]$summary.source_commit -notmatch '^[0-9a-f]{40}$' -or [int]$summary.exact_k6_modules -ne 10 -or
        $settlementLoad.Count -ne 1 -or [int64]$settlementLoad[0].imported_records -lt 1 -or
        [double]$settlementLoad[0].import_rate_records_per_second -le 0 -or [double]$settlementLoad[0].average_lag_seconds -lt 0 -or
        [bool]$settlementLoad[0].truncated -or
        [int]$summary.concurrent_refund_retries -ne 100 -or -not [bool]$summary.conflicting_refund_selection_rejected -or
        @($summary.recovery_journals).Count -ne 2 -or @($summary.recovery_journals | Where-Object { $_.terminal_stage -ne 'source_retained_fenced' -or [int]$_.checkpoint_version -ne 21 }).Count -ne 0 -or
        @($summary.journal_process_crashes).Count -ne 14 -or
        @($summary.journal_process_crashes | Where-Object { -not [bool]$_.process_exit_nonzero -or -not [bool]$_.resumed -or [string]$_.before -cne [string]$_.after }).Count -ne 0 -or
        @($summary.journal_process_crashes | Group-Object operation_kind | Where-Object { $_.Count -ne 7 }).Count -ne 0 -or
        $invalidJournalCrashStages -ne 0 -or
        @($summary.application_transaction_crashes).Count -ne 6 -or
        @($summary.application_transaction_crashes | Where-Object { [int]$_.process_exit -ne 86 -or -not [bool]$_.external_effect_committed -or -not [bool]$_.control_finalize_not_run -or -not [bool]$_.resumed }).Count -ne 0 -or
        $actualApplicationCrashPoints.Count -ne 6 -or (Compare-Object $expectedApplicationCrashPoints $actualApplicationCrashPoints -CaseSensitive) -or
        @($requiredSwitchPhases | Where-Object { $_ -notin $actualPhases }).Count -ne 0 -or
        -not [bool]$summary.interrupted_payment_recovery.recovered_after_failover -or [int]$summary.interrupted_payment_recovery.provider_effect.terminal_effect_count -ne 1 -or
        [int]$summary.interrupted_payment_recovery.shard_issuance_receipts -ne 2 -or [int]$summary.interrupted_payment_recovery.issued_tickets -ne 4 -or
        -not [bool]$summary.interrupted_full_refund_recovery.provider_pending.recovered_after_failover -or
        -not [bool]$summary.interrupted_full_refund_recovery.shard_committed.recovered_after_failover -or
        [int]$summary.interrupted_full_refund_recovery.provider_pending.provider_effect.terminal_effect_count -ne 1 -or
        [int]$summary.interrupted_full_refund_recovery.shard_committed.provider_effect.terminal_effect_count -ne 1 -or
        [int]$summary.interrupted_full_refund_recovery.terminal_compensation_receipts -ne 2 -or
        -not [bool]$summary.interrupted_refund_recovery.provider_pending.recovered_after_failover -or
        -not [bool]$summary.interrupted_refund_recovery.shard_committed.recovered_after_failover -or
        [int]$summary.interrupted_refund_recovery.provider_pending.provider_effect.terminal_effect_count -ne 1 -or
        [int]$summary.interrupted_refund_recovery.shard_committed.provider_effect.terminal_effect_count -ne 1 -or
        [int]$summary.interrupted_refund_recovery.terminal_receipts.compensation -ne 2 -or
        [int]$summary.interrupted_refund_recovery.terminal_receipts.selected -ne 2 -or
        [int]$summary.interrupted_refund_recovery.terminal_receipts.refunded_tickets -ne 2 -or
        [int]$summary.interrupted_refund_recovery.terminal_receipts.unselected_active_tickets -ne 2 -or
        -not [bool]$summary.interrupted_refund_recovery.terminal_receipts.ledger_balanced -or
        @($summary.refunds).Count -ne 3 -or @($summary.refunds | Where-Object { [int]$_.ledger_transactions -ne 1 -or [int]$_.ledger_postings -ne 2 -or -not [bool]$_.ledger_balanced -or [int]$_.selected_receipts -ne 1 }).Count -ne 0 -or
        -not [bool]$summary.physical_migration_interaction.reverse_completed -or -not [bool]$summary.physical_migration_interaction.return_completed -or
        [int]$summary.physical_migration_interaction.refunds_after_return -ne 2 -or [string]$summary.physical_migration_interaction.final_assignment -cne 'physical-shard-0|4' -or
        [string]$summary.physical_migration_interaction.reverse_migration_id_sha256 -notmatch '^[0-9a-f]{64}$' -or
        [int]$summary.rpo_acceptance_bounds.maximum_missing_markers_per_database -ne 1 -or [int64]$summary.rpo_acceptance_bounds.maximum_missing_wal_bytes_per_database -ne 536870912 -or
        @($summary.observed_rpo.databases).Count -ne 3 -or @($summary.failback_rpo.databases).Count -ne 3 -or
        @($summary.observed_rpo.databases | Where-Object { [int64]$_.missing_wal_bytes -lt 0 -or [int64]$_.missing_wal_bytes -gt [int64]$summary.rpo_acceptance_bounds.maximum_missing_wal_bytes_per_database -or [int]$_.missing_records -lt 0 -or [int]$_.missing_records -gt [int]$summary.rpo_acceptance_bounds.maximum_missing_markers_per_database -or [int]$_.source_marker -ne 2 -or [int]$_.target_marker_count -ne (1-[int]$_.missing_records) -or -not $_.source_end_lsn -or -not $_.standby_replay_lsn }).Count -ne 0 -or
        @($summary.failback_rpo.databases | Where-Object { [int64]$_.missing_wal_bytes -lt 0 -or [int64]$_.missing_wal_bytes -gt [int64]$summary.rpo_acceptance_bounds.maximum_missing_wal_bytes_per_database -or [int]$_.missing_records -lt 0 -or [int]$_.missing_records -gt [int]$summary.rpo_acceptance_bounds.maximum_missing_markers_per_database -or [int]$_.source_marker -ne 3 -or [int]$_.target_marker_count -ne (1-[int]$_.missing_records) -or -not $_.source_end_lsn -or -not $_.standby_replay_lsn }).Count -ne 0 -or
        [int]$summary.webhook_durability.failover_retry_events -ne 1 -or [int]$summary.webhook_durability.retired_key_events -ne 0 -or -not [bool]$summary.webhook_durability.previous_key_retired -or
        [string]$summary.webhook_durability.stripe_rotation.provider -cne 'stripe' -or
        -not [bool]$summary.webhook_durability.stripe_rotation.staged_without_grace -or
        -not [bool]$summary.webhook_durability.stripe_rotation.grace_started_on_demotion -or
        -not [bool]$summary.webhook_durability.stripe_rotation.previous_accepted_during_grace -or
        -not [bool]$summary.webhook_durability.stripe_rotation.previous_rejected_after_grace -or
        -not [bool]$summary.webhook_durability.stripe_rotation.current_accepted_after_retirement -or
        -not [bool]$summary.webhook_durability.stripe_rotation.passive_current_verified_then_fenced -or
        -not [bool]$summary.webhook_durability.stripe_rotation.immutable_transition_audit -or
        [int]$summary.webhook_durability.stripe_rotation.durable_event_count -ne 4 -or
        [int64]$summary.webhook_durability.outage_ms -lt 1 -or -not [bool]$summary.redis_loss_boundary.region_a_volume_destroyed -or
        -not [bool]$summary.redis_loss_boundary.region_b_fresh_ready -or [int]$summary.redis_loss_boundary.keys_before -ne 0 -or -not [bool]$summary.redis_loss_boundary.old_durable_ticket_read -or
        -not [bool]$summary.redis_loss_boundary.active_outage_admission_rejected -or [int]$summary.redis_loss_boundary.booking_bypass_rows -ne 0 -or
        -not [bool]$summary.redis_loss_boundary.new_admission_reservation_durable -or [int]$summary.redis_loss_boundary.keys_after -lt 1 -or
        -not [bool]$summary.promoted_single_shard_outage.affected_route_rejected -or -not [bool]$summary.promoted_single_shard_outage.fallback_forbidden -or
        -not [bool]$summary.promoted_single_shard_outage.healthy_shard_completed -or -not [bool]$summary.promoted_single_shard_outage.healthy_shard_new_operation -or -not [bool]$summary.promoted_single_shard_outage.restored_and_reconciled -or
        [int64]$summary.rto_conditions.failover.duration_ms -lt 1 -or -not [bool]$summary.rto_conditions.failover.authority_active -or -not [bool]$summary.rto_conditions.failover.customer_readiness -or -not [bool]$summary.rto_conditions.failover.ingress_switched_after_resume -or
        [int64]$summary.rto_conditions.failback.duration_ms -lt 1 -or -not [bool]$summary.rto_conditions.failback.authority_active -or -not [bool]$summary.rto_conditions.failback.customer_readiness -or -not [bool]$summary.rto_conditions.failback.ingress_switched_after_resume -or
        [Math]::Abs(([double]$summary.observed_rto_seconds - $expectedFailoverRTOSeconds)) -gt 0.0005 -or
        [Math]::Abs(([double]$summary.failback_seconds - $expectedFailbackRTOSeconds)) -gt 0.0005 -or
        [int]$summary.typed_reconciliation.failover.routed_reservations -ne 8 -or [int]$summary.typed_reconciliation.failover.payment_intents -ne 8 -or
        [int]$summary.typed_reconciliation.failback.routed_reservations -ne 8 -or [int]$summary.typed_reconciliation.failback.payment_intents -ne 9 -or
        @($summary.durable_metrics | Where-Object { $_.phase -in @('region-b-passive-before-failover','region-b-final-passive-after-failback') }).Count -ne 2 -or
        $invalidPassiveMetrics -ne 0 -or
        -not [bool]$summary.final_reconciliation.passed -or [bool]$summary.final_reconciliation.truncated -or
        [int]$summary.settlement_reconciliation.missing_provider -lt 1 -or [int]$summary.settlement_reconciliation.missing_local -lt 1 -or
        [int]$summary.settlement_reconciliation.amount -lt 1 -or [int]$summary.settlement_reconciliation.currency -lt 1 -or
        [int]$summary.settlement_reconciliation.fee -lt 1 -or [int]$summary.settlement_reconciliation.payout -lt 1 -or
        -not [bool]$summary.settlement_reconciliation.mismatch_immutable -or -not [bool]$summary.settlement_reconciliation.manual_review_append_only -or
        [string]$summary.software.pgbackrest -cne 'pgBackRest 2.59.0' -or
        @($summary.software.databases).Count -ne 3 -or @($summary.software.databases | Where-Object { [bool]$_.schema_dirty -or [int]$_.schema_version -notin @(3,11) }).Count -ne 0) {
        throw 'DR evidence summary omits or contradicts a mandatory application/financial/recovery invariant'
    }
}

[ordered]@{
    status = 'passed'
    scoped_teardown = $true
    secret_scan = $true
    pinned_k6_capability_proof = $containerCapabilityProof
    stopped_container_summary_copied = $negativeContainerSummaryCopied
    stopped_container_removed = $negativeContainerRemoved
    wide_traversal_fail_closed = ([bool]$wideDiagnostic.diagnostic_truncated -and [bool]$wideExitZeroDiagnostic.diagnostic_truncated)
    negative_fixture_exit_code = 99
    negative_fixture_failed_checks = [string[]]@('stripe_adapter_payouts returned 200')
    publication_evidence_verified = (-not [string]::IsNullOrWhiteSpace($EvidenceDirectory))
} | ConvertTo-Json -Compress
