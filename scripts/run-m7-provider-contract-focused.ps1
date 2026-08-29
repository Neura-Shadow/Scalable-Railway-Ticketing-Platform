[CmdletBinding()]
param(
    [string]$ProjectName = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$root = [System.IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$rootPrefix = $root.TrimEnd([System.IO.Path]::DirectorySeparatorChar) + [System.IO.Path]::DirectorySeparatorChar
. (Join-Path $PSScriptRoot 'milestone-7/k6-diagnostics.ps1')

$suffix = [guid]::NewGuid().ToString('N').Substring(0, 12)
if ([string]::IsNullOrWhiteSpace($ProjectName)) { $ProjectName = "railway-m7-provider-$suffix" }
if ($ProjectName -notmatch '^[a-z0-9][a-z0-9-]{2,54}$') { throw 'ProjectName is invalid' }

$networkName = "$ProjectName-network"
$contractName = "$ProjectName-contract"
$contractAlias = 'payment-stripe-contract.test'
$imageName = "scalable-railway-ticketing-payment-stripe-contract:$ProjectName"
$k6Image = 'grafana/k6:0.57.0'
$evidenceDirectory = [System.IO.Path]::GetFullPath((Join-Path ([System.IO.Path]::GetTempPath()) "$ProjectName-evidence"))
if ($evidenceDirectory.StartsWith($rootPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw 'focused evidence directory must be outside the source repository'
}
if ([System.IO.Directory]::Exists($evidenceDirectory)) { throw 'focused evidence directory already exists' }

$apiKeyBytes = [System.Security.Cryptography.RandomNumberGenerator]::GetBytes(32)
$apiKey = 'rk_test_' + [Convert]::ToHexString($apiKeyBytes).ToLowerInvariant()
[Array]::Clear($apiKeyBytes, 0, $apiKeyBytes.Length)
$sensitiveValues = [string[]]@($apiKey)
$probeSequence = 0
$networkCreated = $false
$imageCreated = $false
$imageWasAbsent = $false
$builtImageID = ''
$evidenceCreated = $false
$directProbeResults = [System.Collections.Generic.List[object]]::new()

$finalResult = [ordered]@{
    status = 'failed'
    classification = 'unknown'
    direct_probes_passed = $false
    direct_probes = [object[]]@()
    k6_executed = $false
    k6_summary_copied = $false
    k6_container_removed = $false
    k6_exit_code = $null
    check_passes = 0
    check_failures = 0
    failed_checks = [string[]]@()
    failed_thresholds = [string[]]@()
    diagnostic_truncated = $false
    summary_inspection_complete = $false
    iterations = 0
    runtime_error = 'none'
    runner_error = 'none'
    container_cleanup_passed = $false
    network_cleanup_passed = $false
    volume_cleanup_passed = $false
    image_cleanup_passed = $false
    evidence_cleanup_passed = $false
    secret_scan_passed = $false
}

function Invoke-M7FocusedNative {
    param(
        [string[]]$Arguments,
        [ValidateRange(1,900)][int]$TimeoutSeconds = 300
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
        if (-not $process.Start()) { throw 'docker process did not start' }
        $stdoutTask = $process.StandardOutput.ReadToEndAsync()
        $stderrTask = $process.StandardError.ReadToEndAsync()
        $timedOut = -not $process.WaitForExit($TimeoutSeconds * 1000)
        if ($timedOut) {
            $process.Kill($true)
            if (-not $process.WaitForExit(5000)) {
                return [pscustomobject]@{ ExitCode=124; Output=[string[]]@('docker process exceeded its termination bound'); TimedOut=$true }
            }
        }
        $stdout = $stdoutTask.GetAwaiter().GetResult()
        $stderr = $stderrTask.GetAwaiter().GetResult()
        $output = [string[]]@(
            [regex]::Split("$stdout`n$stderr", '\r?\n') |
                Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
                ForEach-Object { if ($_.Length -gt 4096) { $_.Substring(0,4096) } else { $_ } } |
                Select-Object -Last 200
        )
        $exitCode = if ($timedOut) { 124 } else { [int]$process.ExitCode }
        return [pscustomobject]@{ ExitCode=[int]$exitCode; Output=$output; TimedOut=$timedOut }
    } catch {
        $safeMessage = Protect-M7K6DiagnosticText -Text ([string]$_.Exception.Message) -SensitiveValues $sensitiveValues -MaximumLength 256
        return [pscustomobject]@{ ExitCode=127; Output=[string[]]@($safeMessage); TimedOut=$false }
    } finally {
        $process.Dispose()
    }
}

function ConvertTo-M7FocusedNormalizedValue {
    param([object]$Value)
    if ($Value -is [bool]) { return ([string]$Value).ToLowerInvariant() }
    if ($Value -is [System.IFormattable]) {
        return $Value.ToString($null, [System.Globalization.CultureInfo]::InvariantCulture)
    }
    return [string]$Value
}

function Invoke-M7FocusedProbe {
    param(
        [Parameter(Mandatory=$true)][string]$Operation,
        [Parameter(Mandatory=$true)][string]$Path,
        [Parameter(Mandatory=$true)][int]$ExpectedStatus,
        [hashtable]$ExpectedFields = @{},
        [string]$Credential = ''
    )

    $script:probeSequence++
    $probeName = "$ProjectName-probe-$($script:probeSequence)"
    $arguments = @(
        'run','--rm','--name',$probeName,
        '--label',"m7.focused.project=$ProjectName",
        '--network',$networkName,
        '--read-only','--tmpfs','/tmp:rw,noexec,nosuid,size=16m',
        '--security-opt','no-new-privileges','--cap-drop','ALL',
        '--entrypoint','wget',$imageName,
        '-S','-T','3','-O','-'
    )
    if (-not [string]::IsNullOrEmpty($Credential)) {
        $arguments += @(
            "--header=Authorization: Bearer $Credential",
            '--header=Stripe-Account: acct_m7_contract',
            '--header=Stripe-Version: 2026-07-29.dahlia'
        )
    }
    $arguments += "http://$($contractName):8100$Path"
    $native = Invoke-M7FocusedNative -Arguments $arguments
    $text = [string]($native.Output -join [Environment]::NewLine)
    $statusMatches = [regex]::Matches($text, '(?im)^\s*HTTP/\d(?:\.\d)?\s+(?<status>\d{3})\b')
    $status = if ($statusMatches.Count -gt 0) { [int]$statusMatches[$statusMatches.Count - 1].Groups['status'].Value } else { 0 }
    $body = $null
    foreach ($line in @($native.Output | Select-Object -Last 20)) {
        $candidate = ([string]$line).Trim()
        if ($candidate.StartsWith('{') -and $candidate.EndsWith('}')) {
            try { $body = $candidate | ConvertFrom-Json } catch { $body = $null }
        }
    }
    $mismatches = [System.Collections.Generic.List[string]]::new()
    $fieldAssertions = [System.Collections.Generic.List[object]]::new()
    if ($status -ne $ExpectedStatus) {
        $mismatches.Add("status expected=$ExpectedStatus observed=$status")
    }
    foreach ($field in @($ExpectedFields.Keys | Sort-Object)) {
        $property = if ($null -ne $body) { $body.PSObject.Properties[[string]$field] } else { $null }
        $observed = if ($null -ne $property) { ConvertTo-M7FocusedNormalizedValue -Value $property.Value } else { 'missing' }
        $expected = ConvertTo-M7FocusedNormalizedValue -Value $ExpectedFields[$field]
        $safeExpected = Protect-M7K6DiagnosticText -Text $expected -SensitiveValues $sensitiveValues -MaximumLength 80
        $safeObserved = Protect-M7K6DiagnosticText -Text $observed -SensitiveValues $sensitiveValues -MaximumLength 80
        $fieldAssertions.Add([ordered]@{
            field = Protect-M7K6DiagnosticText -Text ([string]$field) -SensitiveValues $sensitiveValues -MaximumLength 64
            expected = $safeExpected
            observed = $safeObserved
            passed = ($observed -ceq $expected)
        })
        if ($observed -cne $expected) {
            $mismatches.Add("$field expected=$safeExpected observed=$safeObserved")
        }
    }
    return [ordered]@{
        operation = $Operation
        status = $status
        passed = ($mismatches.Count -eq 0)
        field_assertions = [object[]]$fieldAssertions
        mismatches = [string[]]@($mismatches | Select-Object -First 20)
    }
}

function Get-M7FocusedProbeClassification {
    param([string]$Operation)
    switch ($Operation) {
        'provider_contract_readiness' { return 'contract_service_not_ready' }
        'stripe_adapter_balance_transactions' { return 'adapter_balance_mapping_mismatch' }
        'stripe_adapter_payouts' { return 'adapter_payout_mapping_mismatch' }
        'stripe_adapter_error_classification' { return 'adapter_error_classification_mismatch' }
        'provider_auth_rejection' { return 'contract_authentication_mismatch' }
        default { return 'unknown' }
    }
}

try {
    [System.IO.Directory]::CreateDirectory($evidenceDirectory) | Out-Null
    $evidenceCreated = $true

    foreach ($resourceKind in @('container','network','volume')) {
        $arguments = switch ($resourceKind) {
            'container' { @('ps','-aq','--filter',"label=m7.focused.project=$ProjectName") }
            'network' { @('network','ls','-q','--filter',"label=m7.focused.project=$ProjectName") }
            'volume' { @('volume','ls','-q','--filter',"label=m7.focused.project=$ProjectName") }
        }
        $inventory = Invoke-M7FocusedNative -Arguments $arguments
        if ($inventory.ExitCode -ne 0 -or @($inventory.Output | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count -ne 0) {
            throw 'focused project name already owns Docker resources'
        }
    }

    $existingImage = Invoke-M7FocusedNative -Arguments @('image','ls','--quiet','--no-trunc','--filter',"reference=$imageName")
    if ($existingImage.ExitCode -ne 0) { throw 'focused image inventory failed' }
    if (@($existingImage.Output | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count -ne 0) { throw 'focused image tag already exists' }
    $imageWasAbsent = $true

    $build = Invoke-M7FocusedNative -Arguments @(
        'build','--target','payment-stripe-contract',
        '--label',"m7.focused.project=$ProjectName",
        '-t',$imageName,$root
    )
    if ($build.ExitCode -ne 0) { throw 'focused payment-stripe-contract image build failed' }
    $imageCreated = $true
    $builtImage = Invoke-M7FocusedNative -Arguments @(
        'image','inspect','--format','{{.Id}}|{{index .Config.Labels "m7.focused.project"}}',$imageName
    )
    $builtImageMetadata = @($builtImage.Output | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($builtImage.ExitCode -ne 0 -or $builtImageMetadata.Count -ne 1) { throw 'focused image ownership was not observable' }
    $builtImageParts = [string]$builtImageMetadata[0] -split '\|', 2
    if ($builtImageParts.Count -ne 2 -or $builtImageParts[0] -notmatch '^sha256:[0-9a-f]{64}$' -or $builtImageParts[1] -cne $ProjectName) {
        throw 'focused image ownership was invalid'
    }
    $builtImageID = $builtImageParts[0]

    $network = Invoke-M7FocusedNative -Arguments @(
        'network','create','--internal','--label',"m7.focused.project=$ProjectName",$networkName
    )
    if ($network.ExitCode -ne 0) { throw 'focused internal Docker network creation failed' }
    $networkCreated = $true

    $contract = Invoke-M7FocusedNative -Arguments @(
        'run','-d','--name',$contractName,
        '--label',"m7.focused.project=$ProjectName",
        '--network',$networkName,'--network-alias',$contractAlias,
        '--read-only','--tmpfs','/tmp:rw,noexec,nosuid,size=16m',
        '--security-opt','no-new-privileges','--cap-drop','ALL',
        '-e','PAYMENT_STRIPE_CONTRACT_TEST_ONLY=true',
        '-e','PAYMENT_STRIPE_CONTRACT_ADDRESS=:8100',
        '-e','PAYMENT_PROVIDER_ACCOUNT_ID=acct_m7_contract',
        '-e',"PAYMENT_PROVIDER_API_KEY=$apiKey",
        '-e','PAYMENT_PROVIDER_API_VERSION=2026-07-29.dahlia',
        '-e',"PAYMENT_STRIPE_CONTRACT_ADAPTER_ORIGIN=http://$($contractAlias):8100",
        '-e','PAYMENT_STRIPE_CONTRACT_PAGE_BARRIER_DELAY=8s',
        $imageName
    )
    if ($contract.ExitCode -ne 0) {
        $finalResult.classification = 'contract_configuration_invalid'
        throw 'focused payment-stripe-contract container failed to start'
    }

    $ready = $false
    foreach ($attempt in 1..60) {
        $readiness = Invoke-M7FocusedNative -Arguments @(
            'exec',$contractName,'wget','-q','-T','2','-O','/dev/null','http://127.0.0.1:8100/readyz'
        )
        if ($readiness.ExitCode -eq 0) { $ready = $true; break }
        Start-Sleep -Milliseconds 250
    }
    if (-not $ready) {
        $finalResult.classification = 'contract_service_not_ready'
        throw 'focused payment-stripe-contract readiness deadline exceeded'
    }

    $probeSpecs = @(
        @{ Operation='provider_contract_readiness'; Path='/readyz'; Status=200; Credential=''; Fields=@{ provider='stripe'; mode='deterministic_test_contract'; mutations_enabled=$false } },
        @{ Operation='stripe_adapter_balance_transactions'; Path='/adapter/balance-transactions'; Status=200; Credential=$apiKey; Fields=@{ provider_record_id='txn_m7_capture'; gross_minor=1000; fee_minor=30; net_minor=970; currency='TWD' } },
        @{ Operation='stripe_adapter_payouts'; Path='/adapter/payouts'; Status=200; Credential=$apiKey; Fields=@{ provider_record_id='po_m7_settlement'; amount_minor=670; currency='TWD'; status='paid' } },
        @{ Operation='stripe_adapter_error_classification'; Path='/adapter/error-classification'; Status=200; Credential=$apiKey; Fields=@{ category='provider_unavailable'; retryable=$true; uncertain=$false } },
        @{ Operation='provider_auth_rejection'; Path='/adapter/balance-transactions'; Status=401; Credential='rk_test_invalid_contract_key'; Fields=@{} }
    )
    foreach ($spec in $probeSpecs) {
        $probe = Invoke-M7FocusedProbe -Operation $spec.Operation -Path $spec.Path -ExpectedStatus $spec.Status -ExpectedFields $spec.Fields -Credential $spec.Credential
        $directProbeResults.Add($probe)
        if (-not [bool]$probe.passed) {
            $finalResult.classification = Get-M7FocusedProbeClassification -Operation $spec.Operation
            throw "focused direct probe failed: $($spec.Operation)"
        }
    }
    $finalResult.direct_probes_passed = $true

    $summaryPath = Join-Path $evidenceDirectory 'production-provider-contract-summary.json'
    $scriptMount = "type=bind,src=$(Join-Path $root 'loadtest/k6'),dst=/scripts,readonly"
    $k6ContainerName = "$ProjectName-k6-$suffix"
    if ($k6ContainerName.Length -gt 120 -or $k6ContainerName -notmatch '^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,119}$') {
        throw 'focused k6 one-off container name is invalid'
    }
    $existingK6Container = Invoke-M7FocusedNative -Arguments @('ps','-aq','--filter',"name=^/$k6ContainerName$")
    if ($existingK6Container.ExitCode -ne 0 -or @($existingK6Container.Output | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count -ne 0) {
        throw 'focused k6 one-off container name is not available'
    }
    $k6 = $null
    try {
        $k6 = Invoke-M7FocusedNative -Arguments @(
            'run','--name',$k6ContainerName,
            '--label',"m7.focused.project=$ProjectName",
            '--network',$networkName,'--user','12345:12345',
            '--security-opt','no-new-privileges','--cap-drop','ALL',
            '--mount',$scriptMount,'--entrypoint','sh',
            '-e','VUS=1','-e','ITERATIONS_PER_VU=1','-e','DURATION=1m',
            '-e',"PROVIDER_CONTRACT_URL=http://$($contractName):8100",
            '-e',"PROVIDER_CONTRACT_API_KEY=$apiKey",
            '-e','PROVIDER_CONTRACT_ACCOUNT_ID=acct_m7_contract',
            '-e','PROVIDER_CONTRACT_API_VERSION=2026-07-29.dahlia',
            $k6Image,'-c','umask 022; exec k6 "$@"','sh','run','--quiet','--summary-export',
            '/tmp/production-provider-contract-summary.json',
            '/scripts/production-provider-contract.js'
        )
        $finalResult.k6_executed = $true
        $copy = Invoke-M7FocusedNative -Arguments @('cp',"${k6ContainerName}:/tmp/production-provider-contract-summary.json",$summaryPath)
        $finalResult.k6_summary_copied = ($copy.ExitCode -eq 0 -and [System.IO.File]::Exists($summaryPath))
    } finally {
        [void](Invoke-M7FocusedNative -Arguments @('rm','-f',$k6ContainerName))
        $remainingK6Container = Invoke-M7FocusedNative -Arguments @('ps','-aq','--filter',"name=^/$k6ContainerName$")
        $finalResult.k6_container_removed = ($remainingK6Container.ExitCode -eq 0 -and @($remainingK6Container.Output | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count -eq 0)
    }
    if ($null -eq $k6) { throw 'focused k6 one-off container did not return an execution result' }
    $diagnostic = Get-M7K6Diagnostic -Script 'production-provider-contract.js' -ExitCode $k6.ExitCode -SummaryPath $summaryPath -LogLines $k6.Output -SensitiveValues $sensitiveValues
    [System.IO.File]::WriteAllLines(
        (Join-Path $evidenceDirectory 'production-provider-contract.log'),
        [string[]]$diagnostic.log_tail,
        [System.Text.UTF8Encoding]::new($false)
    )
    $finalResult.k6_exit_code = [int]$k6.ExitCode
    $finalResult.check_passes = [int64]$diagnostic.check_passes
    $finalResult.check_failures = [int64]$diagnostic.check_failures
    $finalResult.failed_checks = [string[]]$diagnostic.failed_checks
    $finalResult.failed_thresholds = [string[]]$diagnostic.failed_thresholds
    $finalResult.diagnostic_truncated = [bool]$diagnostic.diagnostic_truncated
    if (-not $diagnostic.Contains('summary_inspection_complete') -or $diagnostic.summary_inspection_complete -isnot [bool]) {
        $finalResult.classification = 'evidence_runner_diagnostic_failure'
        throw 'focused k6 diagnostic omitted its Boolean summary inspection contract'
    }
    $finalResult.summary_inspection_complete = [bool]$diagnostic.summary_inspection_complete
    $finalResult.iterations = [int64]$diagnostic.iterations
    $finalResult.runtime_error = [string]$diagnostic.runtime_error
    if ($k6.ExitCode -ne 0 -or -not [bool]$finalResult.k6_summary_copied -or -not [bool]$finalResult.k6_container_removed -or
        -not [bool]$diagnostic.summary_present -or -not [bool]$diagnostic.summary_valid -or
        [bool]$diagnostic.diagnostic_truncated -or -not [bool]$diagnostic.summary_inspection_complete -or
        [int64]$diagnostic.check_failures -ne 0 -or [int64]$diagnostic.check_passes -lt 1 -or
        @($diagnostic.failed_thresholds).Count -ne 0 -or [int64]$diagnostic.iterations -ne 1) {
        $finalResult.classification = [string]$diagnostic.classification
        throw 'focused production-provider-contract k6 scenario failed'
    }

    $secretScanPassed = $true
    foreach ($file in @(Get-ChildItem -LiteralPath $evidenceDirectory -File)) {
        $text = Get-Content -Raw -LiteralPath $file.FullName
        if ($text.Contains($apiKey) -or
            $text -match '(?i)\bBearer\s+(?!\[redacted\])[^\s",;]+' -or
            $text -match '(?i)\bpostgres(?:ql)?://[^\s/@:]+:[^\s/@]+@') {
            $secretScanPassed = $false
            break
        }
    }
    if (-not $secretScanPassed) {
        $finalResult.classification = 'evidence_runner_diagnostic_failure'
        throw 'focused evidence secret scan failed'
    }
    $finalResult.secret_scan_passed = $true
    $finalResult.status = 'passed'
    $finalResult.classification = 'unknown'
} catch {
    $safeMessage = Protect-M7K6DiagnosticText -Text ([string]$_.Exception.Message) -SensitiveValues $sensitiveValues -MaximumLength 256
    $finalResult.runner_error = $safeMessage
    if ($finalResult.classification -eq 'unknown') {
        if ($safeMessage -match '(?i)name resolution|no such host') {
            $finalResult.classification = 'docker_network_resolution_failure'
        }
    }
} finally {
    $finalResult.direct_probes = [object[]]$directProbeResults

    $containers = Invoke-M7FocusedNative -Arguments @('ps','-aq','--filter',"label=m7.focused.project=$ProjectName")
    foreach ($containerID in @($containers.Output | Where-Object { $_ -match '^[0-9a-f]+$' })) {
        [void](Invoke-M7FocusedNative -Arguments @('rm','-f',[string]$containerID))
    }
    $remainingContainers = Invoke-M7FocusedNative -Arguments @('ps','-aq','--filter',"label=m7.focused.project=$ProjectName")
    $finalResult.container_cleanup_passed = ($remainingContainers.ExitCode -eq 0 -and @($remainingContainers.Output | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count -eq 0)

    if ($networkCreated) { [void](Invoke-M7FocusedNative -Arguments @('network','rm',$networkName)) }
    $remainingNetworks = Invoke-M7FocusedNative -Arguments @('network','ls','-q','--filter',"label=m7.focused.project=$ProjectName")
    $finalResult.network_cleanup_passed = ($remainingNetworks.ExitCode -eq 0 -and @($remainingNetworks.Output | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count -eq 0)

    $remainingVolumes = Invoke-M7FocusedNative -Arguments @('volume','ls','-q','--filter',"label=m7.focused.project=$ProjectName")
    $finalResult.volume_cleanup_passed = ($remainingVolumes.ExitCode -eq 0 -and @($remainingVolumes.Output | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count -eq 0)

    $imageRemovalPassed = $true
    if ($imageCreated) {
        $currentImage = Invoke-M7FocusedNative -Arguments @(
            'image','inspect','--format','{{.Id}}|{{index .Config.Labels "m7.focused.project"}}',$imageName
        )
        $currentImageMetadata = @($currentImage.Output | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        if ($currentImage.ExitCode -eq 0 -and $currentImageMetadata.Count -eq 1) {
            $currentImageParts = [string]$currentImageMetadata[0] -split '\|', 2
            if ($currentImageParts.Count -eq 2 -and -not [string]::IsNullOrWhiteSpace($builtImageID) -and
                $currentImageParts[0] -ceq $builtImageID -and $currentImageParts[1] -ceq $ProjectName) {
                $imageRemoval = Invoke-M7FocusedNative -Arguments @('image','rm',$imageName)
                $imageRemovalPassed = ($imageRemoval.ExitCode -eq 0)
            } else {
                $imageRemovalPassed = $false
            }
        }
    }
    if ($imageWasAbsent) {
        $remainingImage = Invoke-M7FocusedNative -Arguments @('image','ls','--quiet','--no-trunc','--filter',"reference=$imageName")
        $finalResult.image_cleanup_passed = ($imageRemovalPassed -and $remainingImage.ExitCode -eq 0 -and @($remainingImage.Output | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count -eq 0)
    } else {
        $finalResult.image_cleanup_passed = -not $imageCreated
    }
    if ($evidenceCreated -and [System.IO.Directory]::Exists($evidenceDirectory)) {
        $tempPrefix = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath()).TrimEnd([System.IO.Path]::DirectorySeparatorChar) + [System.IO.Path]::DirectorySeparatorChar
        if ($evidenceDirectory.StartsWith($tempPrefix, [System.StringComparison]::OrdinalIgnoreCase) -and
            [System.IO.Path]::GetFileName($evidenceDirectory) -ceq "$ProjectName-evidence") {
            try { [System.IO.Directory]::Delete($evidenceDirectory, $true) } catch { }
        }
    }
    $finalResult.evidence_cleanup_passed = -not [System.IO.Directory]::Exists($evidenceDirectory)
    $apiKey = $null
    $sensitiveValues = [string[]]@()

    if (-not [bool]$finalResult.container_cleanup_passed -or -not [bool]$finalResult.network_cleanup_passed -or
        -not [bool]$finalResult.volume_cleanup_passed -or -not [bool]$finalResult.image_cleanup_passed -or
        -not [bool]$finalResult.evidence_cleanup_passed) {
        $finalResult.status = 'failed'
        $finalResult.classification = 'evidence_runner_diagnostic_failure'
    }
}

$finalResult | ConvertTo-Json -Depth 10 -Compress
if ($finalResult.status -cne 'passed') { exit 1 }
