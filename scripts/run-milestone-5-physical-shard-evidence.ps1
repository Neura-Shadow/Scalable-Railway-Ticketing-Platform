[CmdletBinding()]
param(
    # The trusted, repository-local driver owns fixture creation and the timed
    # outage/cutover/reverse transitions. It must define these five functions:
    # Initialize-Milestone5Evidence, Start-Milestone5Scenario,
    # Stop-Milestone5Scenario, Get-Milestone5DatabaseEvidence, and
    # Get-Milestone5MigrationEvidence. The initializer returns
    # EnvironmentByScenario plus an in-memory SecretValues collection. Driver
    # output must never contain credentials; only RawDirectory may be written.
    [Parameter(Mandatory = $true)]
    [string]$DriverScript,

    [string]$ProjectName = '',

    [string]$EvidenceDirectory = '',

    [ValidatePattern('^[1-9][0-9]*(s|m)$')]
    [string]$ScenarioDuration = '45s'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

. (Join-Path $PSScriptRoot 'milestone-5-evidence-guardrails.ps1')

$root = Split-Path -Parent $PSScriptRoot
$repositoryPath = [System.IO.Path]::GetFullPath($root)
$composeFile = Join-Path $root 'docker-compose.physical-shards.yml'
$driverPath = [System.IO.Path]::GetFullPath($DriverScript)
if (-not (Test-Milestone5SameOrDescendantPath -Candidate $driverPath -Parent $repositoryPath) -or
    -not (Test-Path -LiteralPath $driverPath -PathType Leaf)) {
    throw 'DriverScript must be an existing regular file inside the repository'
}
Assert-Milestone5NoReparsePoints -Path $driverPath

$suffix = [guid]::NewGuid().ToString('N').Substring(0, 10)
if ([string]::IsNullOrWhiteSpace($ProjectName)) {
    $ProjectName = "railway-m5-evidence-$suffix"
}
if ($ProjectName -notmatch '^[a-z0-9][a-z0-9_-]{2,62}$') {
    throw 'ProjectName must be a bounded lowercase Compose project name'
}
if ([string]::IsNullOrWhiteSpace($EvidenceDirectory)) {
    $EvidenceDirectory = Join-Path ([System.IO.Path]::GetTempPath()) "railway-m5-evidence-$suffix"
}
$EvidenceDirectory = New-Milestone5EvidenceDirectory `
    -EvidenceDirectory $EvidenceDirectory -RepositoryPath $repositoryPath
$rawDirectory = Join-Path $EvidenceDirectory 'raw'
$canonicalDirectory = Join-Path $EvidenceDirectory 'canonical'
$canonicalSummary = Join-Path $canonicalDirectory 'milestone-5-summary.json'
$candidateSummary = Join-Path $canonicalDirectory 'milestone-5-summary.candidate.json'
$compose = @('compose', '-p', $ProjectName, '-f', $composeFile)
$scenarioResults = [System.Collections.Generic.List[object]]::new()
$secretValues = [System.Collections.Generic.List[string]]::new()
$started = $false
$teardownCompleted = $false
$sanitizationCompleted = $false
$databaseInvariantsPassed = $false
$migrationEvidencePassed = $false
$failureCategory = 'not_started'
$evidenceCommit = ''
$configHash = ''
$databaseEvidence = $null
$migrationEvidence = $null
$driverContext = $null
$driverState = $null
$failure = $null

function Invoke-Milestone5Native {
    param(
        [Parameter(Mandatory = $true)][scriptblock]$Command,
        [switch]$AllowFailure
    )

    $previous = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = @(& $Command 2>&1 | ForEach-Object { [string]$_ })
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    if ($exitCode -ne 0 -and -not $AllowFailure) {
        throw "native command failed with exit code $exitCode"
    }
    return [pscustomobject]@{ Output = @($output); ExitCode = $exitCode }
}

function Invoke-Milestone5Compose {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [string]$CapturePath = '',
        [switch]$AllowFailure
    )

    $result = Invoke-Milestone5Native -AllowFailure -Command { & docker @script:compose @Arguments }
    if (-not [string]::IsNullOrWhiteSpace($CapturePath)) {
        $result.Output | Out-File -LiteralPath $CapturePath -Encoding utf8
    }
    if ($result.ExitCode -ne 0 -and -not $AllowFailure) {
        throw "docker compose failed with exit code $($result.ExitCode)"
    }
    return $result
}

function Assert-Milestone5ComposeProjectUnused {
    $label = "label=com.docker.compose.project=$script:ProjectName"
    foreach ($query in @(
        @('ps', '-a', '-q', '--filter', $label),
        @('volume', 'ls', '-q', '--filter', $label),
        @('network', 'ls', '-q', '--filter', $label)
    )) {
        $result = Invoke-Milestone5Native -AllowFailure -Command { & docker @query }
        if ($result.ExitCode -ne 0) { throw 'could not verify Compose project ownership' }
        if (@($result.Output | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count -ne 0) {
            throw 'ProjectName already owns Docker resources; refusing destructive reuse'
        }
    }
}

function Write-Milestone5Status {
    param(
        [Parameter(Mandatory = $true)][string]$Status,
        [Parameter(Mandatory = $true)][string]$Reason
    )

    Assert-Milestone5Status -Status $Status
    Write-Milestone5JsonAtomic -Path (Join-Path $script:rawDirectory 'evidence-status.json') -Value ([ordered]@{
        milestone = 5
        status = $Status
        reason = $Reason
        project = $script:ProjectName
        generated_at = [DateTimeOffset]::UtcNow.ToString('o')
    })
}

function Get-Milestone5ToolVersion {
    param([Parameter(Mandatory = $true)][scriptblock]$Command)

    $result = Invoke-Milestone5Native -AllowFailure -Command $Command
    if ($result.ExitCode -ne 0) { throw 'required tool version command failed' }
    return ((@($result.Output) -join ' ').Trim() -replace '\s+', ' ')
}

function Assert-Milestone5DriverContract {
    foreach ($name in @(
        'Initialize-Milestone5Evidence',
        'Start-Milestone5Scenario',
        'Stop-Milestone5Scenario',
        'Get-Milestone5DatabaseEvidence',
        'Get-Milestone5MigrationEvidence'
    )) {
        if ($null -eq (Get-Command $name -CommandType Function -ErrorAction SilentlyContinue)) {
            throw "DriverScript omitted required function $name"
        }
    }
}

function Get-Milestone5ScenarioEnvironment {
    param(
        [Parameter(Mandatory = $true)][object]$State,
        [Parameter(Mandatory = $true)][string]$Scenario
    )

    $all = Get-Milestone5OptionalValue -Object $State -Name 'EnvironmentByScenario'
    $environment = Get-Milestone5OptionalValue -Object $all -Name $Scenario
    if ($null -eq $environment) { throw "$Scenario driver environment is missing" }
    $validated = [ordered]@{}
    if ($environment -is [System.Collections.IDictionary]) {
        $properties = @($environment.GetEnumerator() | ForEach-Object {
            [pscustomobject]@{ Name = [string]$_.Key; Value = $_.Value }
        })
    } else {
        $properties = @($environment.PSObject.Properties)
    }
    foreach ($property in $properties) {
        $name = [string]$property.Name
        $value = [string]$property.Value
        if ($name -notmatch '^[A-Z][A-Z0-9_]{0,63}$' -or
            [string]::IsNullOrWhiteSpace($value) -or $value.Length -gt 4096 -or
            $value.Contains([char]0)) {
            throw "$Scenario driver environment contained an invalid entry"
        }
        $validated[$name] = $value
    }
    $validated['DURATION'] = $script:ScenarioDuration
    return $validated
}

function Invoke-Milestone5K6 {
    param(
        [Parameter(Mandatory = $true)][string]$Scenario,
        [Parameter(Mandatory = $true)][System.Collections.IDictionary]$Environment
    )

    $summaryPath = Join-Path $script:rawDirectory "$Scenario-summary.json"
    [System.IO.File]::WriteAllText($summaryPath, '{}', [System.Text.UTF8Encoding]::new($false))
    $arguments = @('run', '--rm', '--network', "$($script:ProjectName)_frontend")
    foreach ($entry in $Environment.GetEnumerator() | Sort-Object Key) {
        $arguments += @('--env', "$($entry.Key)=$($entry.Value)")
    }
    $arguments += @(
        '--volume', "$($script:root.Replace('\', '/'))/loadtest/k6:/scripts:ro",
        '--volume', "$($script:EvidenceDirectory.Replace('\', '/')):/evidence",
        'grafana/k6:0.54.0', 'run', '--summary-export', "/evidence/raw/$Scenario-summary.json",
        "/scripts/$Scenario.js"
    )
    $result = Invoke-Milestone5Native -AllowFailure -Command { & docker @arguments }
    $result.Output | Out-File -LiteralPath (Join-Path $script:rawDirectory "$Scenario.log") -Encoding utf8
    if ($result.ExitCode -ne 0) { throw "$Scenario k6 workload failed" }
    $rawSummary = Get-Content -Raw -LiteralPath $summaryPath | ConvertFrom-Json
    $strict = ConvertFrom-Milestone5K6Summary -Summary $rawSummary -Scenario $Scenario
    Write-Milestone5JsonAtomic `
        -Path (Join-Path $script:canonicalDirectory "$Scenario.json") -Value $strict
    return $strict
}

Write-Milestone5Status -Status 'not_run' -Reason 'preflight_not_started'

try {
    Push-Location $root
    $failureCategory = 'source_provenance'
    $dirty = @(& git status --porcelain=v1 --untracked-files=all 2>$null)
    if ($LASTEXITCODE -ne 0 -or $dirty.Count -ne 0) {
        throw 'Milestone 5 evidence requires a clean committed working tree'
    }
    $evidenceCommit = (& git rev-parse HEAD 2>$null | Out-String).Trim()
    if ($LASTEXITCODE -ne 0 -or $evidenceCommit -notmatch '^[0-9a-f]{40}$') {
        throw 'could not resolve the committed source revision'
    }
    $configHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $composeFile).Hash.ToLowerInvariant()

    $failureCategory = 'docker_unavailable'
    if ($null -eq (Get-Command docker -CommandType Application -ErrorAction SilentlyContinue) -or
        (Invoke-Milestone5Native -AllowFailure -Command { & docker version }).ExitCode -ne 0) {
        throw 'Docker Engine is unavailable'
    }
    $failureCategory = 'compose_unavailable'
    if ((Invoke-Milestone5Native -AllowFailure -Command { & docker compose version }).ExitCode -ne 0) {
        throw 'Docker Compose v2 is unavailable'
    }

    $failureCategory = 'driver_unavailable'
    . $driverPath
    Assert-Milestone5DriverContract
    $failureCategory = 'compose_project_preflight'
    Assert-Milestone5ComposeProjectUnused

    Write-Milestone5Status -Status 'not_run' -Reason 'topology_starting'
    $started = $true
    $failureCategory = 'compose_startup'
    Invoke-Milestone5Compose -Arguments @('up', '-d', '--build', '--wait') `
        -CapturePath (Join-Path $rawDirectory 'compose-up.log') | Out-Null

    $driverContext = [pscustomobject]@{
        RepositoryPath = $repositoryPath
        EvidenceDirectory = $EvidenceDirectory
        RawDirectory = $rawDirectory
        ProjectName = $ProjectName
        ComposeFile = $composeFile
        ComposeArguments = [string[]]$compose.Clone()
    }
    $failureCategory = 'driver_initialization'
    $driverState = Initialize-Milestone5Evidence -Context $driverContext
    if ($null -eq $driverState) { throw 'driver initialization returned no state' }
    foreach ($secret in @(Get-Milestone5OptionalValue -Object $driverState -Name 'SecretValues' -Default @())) {
        if (-not [string]::IsNullOrWhiteSpace([string]$secret)) { $secretValues.Add([string]$secret) }
    }

    foreach ($scenario in Get-Milestone5ScenarioNames) {
        $failureCategory = "scenario_$($scenario.Replace('-', '_'))"
        $environment = Get-Milestone5ScenarioEnvironment -State $driverState -Scenario $scenario
        $scenarioStarted = $false
        try {
            Start-Milestone5Scenario -Context $driverContext -State $driverState `
                -Scenario $scenario -Environment $environment
            $scenarioStarted = $true
            $scenarioResults.Add((Invoke-Milestone5K6 -Scenario $scenario -Environment $environment))
        } finally {
            Stop-Milestone5Scenario -Context $driverContext -State $driverState `
                -Scenario $scenario -Started $scenarioStarted
        }
    }

    $failureCategory = 'database_invariants'
    $databaseEvidence = Assert-Milestone5DatabaseInvariants -Evidence (
        Get-Milestone5DatabaseEvidence -Context $driverContext -State $driverState
    )
    $databaseInvariantsPassed = $true
    Write-Milestone5JsonAtomic -Path (Join-Path $canonicalDirectory 'database-invariants.json') `
        -Value $databaseEvidence

    $failureCategory = 'migration_measurements'
    $migrationEvidence = Assert-Milestone5MeasuredMigrationEvidence -Evidence (
        Get-Milestone5MigrationEvidence -Context $driverContext -State $driverState
    )
    $migrationEvidencePassed = $true
    Write-Milestone5JsonAtomic -Path (Join-Path $canonicalDirectory 'migration-measurements.json') `
        -Value $migrationEvidence

    $failureCategory = 'summary_preparation'
    $toolVersions = [ordered]@{
        docker = Get-Milestone5ToolVersion -Command { & docker --version }
        compose = Get-Milestone5ToolVersion -Command { & docker compose version }
        k6_image = 'grafana/k6:0.54.0'
        postgres_image = 'postgres:16-alpine'
        redis_image = 'redis:7-alpine'
    }
    [ordered]@{
        milestone = 5
        status = 'passed'
        commit = $evidenceCommit
        compose_config_sha256 = $configHash
        topology = [ordered]@{ postgres_instances = 3; redis_instances = 1; api_replicas = 3 }
        tool_versions = $toolVersions
        scenarios = @($scenarioResults)
        database_invariants = $databaseEvidence
        migration_measurements = $migrationEvidence
        cleanup = [ordered]@{ command = 'docker compose down -v --remove-orphans'; completed = $true }
        sanitization = [ordered]@{ completed = $true; dsn_or_secret_values_published = $false }
        generated_at = [DateTimeOffset]::UtcNow.ToString('o')
    } | ConvertTo-Json -Depth 20 | Out-File -LiteralPath $candidateSummary -Encoding utf8
} catch {
    $failure = $_
} finally {
    if ($started) {
        $teardown = Invoke-Milestone5Compose -AllowFailure `
            -Arguments @('down', '-v', '--remove-orphans') `
            -CapturePath (Join-Path $rawDirectory 'compose-down.log')
        $teardownCompleted = $teardown.ExitCode -eq 0
        if (-not $teardownCompleted -and $null -eq $failure) {
            $failureCategory = 'compose_teardown_failed'
            $failure = [System.Management.Automation.ErrorRecord]::new(
                [System.Exception]::new('Docker Compose teardown failed'),
                'compose_teardown_failed',
                [System.Management.Automation.ErrorCategory]::OperationStopped,
                $null
            )
        }
    }
    try {
        Assert-Milestone5ArtifactsSanitized `
            -EvidenceDirectory $EvidenceDirectory -SecretValues @($secretValues)
        $sanitizationCompleted = $true
    } catch {
        $sanitizationCompleted = $false
        if ($null -eq $failure) {
            $failureCategory = 'artifact_sanitization_failed'
            $failure = $_
        }
    }
    Pop-Location
}

$ready = Test-Milestone5CanonicalSummaryReady `
    -Scenarios @($scenarioResults) `
    -DatabaseInvariantsPassed $databaseInvariantsPassed `
    -MigrationEvidencePassed $migrationEvidencePassed `
    -TeardownCompleted $teardownCompleted `
    -SanitizationCompleted $sanitizationCompleted
if ($null -eq $failure -and $ready -and (Test-Path -LiteralPath $candidateSummary)) {
    Move-Item -LiteralPath $candidateSummary -Destination $canonicalSummary -ErrorAction Stop
    Write-Output "Milestone 5 evidence passed: $EvidenceDirectory"
    exit 0
}

foreach ($path in @($candidateSummary, $canonicalSummary)) {
    if (Test-Path -LiteralPath $path) { Remove-Item -LiteralPath $path -Force }
}
$status = Get-Milestone5EvidenceFailureStatus -Category $failureCategory
Write-Milestone5Status -Status $status -Reason $failureCategory
if ($null -ne $failure) {
    throw "Milestone 5 evidence did not pass (category=$failureCategory): $($failure.Exception.Message)"
}
throw "Milestone 5 evidence did not pass (category=$failureCategory)"
