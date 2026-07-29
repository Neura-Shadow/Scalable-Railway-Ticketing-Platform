Set-StrictMode -Version Latest

$script:Milestone5ScenarioNames = @(
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

function Get-Milestone5ScenarioNames {
    return [string[]]$script:Milestone5ScenarioNames.Clone()
}

function Get-Milestone5OptionalValue {
    param(
        [object]$Object,
        [Parameter(Mandatory = $true)][string]$Name,
        [object]$Default = $null
    )

    if ($null -eq $Object) { return $Default }
    if ($Object -is [System.Collections.IDictionary]) {
        foreach ($key in $Object.Keys) {
            if ([string]$key -ieq $Name) { return $Object[$key] }
        }
        return $Default
    }
    $property = $Object.PSObject.Properties |
        Where-Object { $_.Name -ieq $Name } | Select-Object -First 1
    if ($null -eq $property) { return $Default }
    return $property.Value
}

function Get-Milestone5NormalizedPath {
    param([Parameter(Mandatory = $true)][string]$Path)

    $full = [System.IO.Path]::GetFullPath($Path)
    $root = [System.IO.Path]::GetPathRoot($full)
    if ($full.Equals($root, [StringComparison]::OrdinalIgnoreCase)) { return $root }
    return $full.TrimEnd(
        [System.IO.Path]::DirectorySeparatorChar,
        [System.IO.Path]::AltDirectorySeparatorChar
    )
}

function Test-Milestone5SameOrDescendantPath {
    param(
        [Parameter(Mandatory = $true)][string]$Candidate,
        [Parameter(Mandatory = $true)][string]$Parent
    )

    $comparison = if ([System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
        [System.Runtime.InteropServices.OSPlatform]::Windows
    )) { [StringComparison]::OrdinalIgnoreCase } else { [StringComparison]::Ordinal }
    if ($Candidate.Equals($Parent, $comparison)) { return $true }
    $prefix = $Parent.TrimEnd('/', '\') + [System.IO.Path]::DirectorySeparatorChar
    return $Candidate.StartsWith($prefix, $comparison)
}

function Assert-Milestone5NoReparsePoints {
    param([Parameter(Mandatory = $true)][string]$Path)

    $cursor = $Path
    while (-not [string]::IsNullOrWhiteSpace($cursor)) {
        if (Test-Path -LiteralPath $cursor) {
            $item = Get-Item -Force -LiteralPath $cursor
            $linkType = $item.PSObject.Properties['LinkType']
            if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0 -or
                ($null -ne $linkType -and -not [string]::IsNullOrWhiteSpace([string]$linkType.Value))) {
                throw 'EvidenceDirectory must not traverse a reparse point or symbolic link'
            }
        }
        $parent = [System.IO.Directory]::GetParent($cursor)
        if ($null -eq $parent) { break }
        $cursor = $parent.FullName
    }
}

function New-Milestone5EvidenceDirectory {
    param(
        [Parameter(Mandatory = $true)][string]$EvidenceDirectory,
        [Parameter(Mandatory = $true)][string]$RepositoryPath
    )

    $evidence = Get-Milestone5NormalizedPath -Path $EvidenceDirectory
    $repository = Get-Milestone5NormalizedPath -Path $RepositoryPath
    Assert-Milestone5NoReparsePoints -Path $repository
    if ((Test-Milestone5SameOrDescendantPath -Candidate $evidence -Parent $repository) -or
        (Test-Milestone5SameOrDescendantPath -Candidate $repository -Parent $evidence)) {
        throw 'EvidenceDirectory must not overlap the repository or an ancestor of it'
    }
    if (Test-Path -LiteralPath $evidence) {
        throw 'EvidenceDirectory must not already exist'
    }
    Assert-Milestone5NoReparsePoints -Path $evidence
    New-Item -ItemType Directory -Path $evidence -ErrorAction Stop | Out-Null
    foreach ($name in @('raw', 'canonical')) {
        New-Item -ItemType Directory -Path (Join-Path $evidence $name) -ErrorAction Stop | Out-Null
    }
    Assert-Milestone5NoReparsePoints -Path $evidence
    return $evidence
}

function Get-Milestone5EvidenceFailureStatus {
    param([Parameter(Mandatory = $true)][string]$Category)

    if ($Category -in @(
        'source_provenance', 'docker_unavailable', 'compose_unavailable',
        'compose_project_preflight', 'driver_unavailable', 'driver_blocked'
    )) { return 'blocked' }
    if ($Category -eq 'not_started') { return 'not_run' }
    return 'failed'
}

function Assert-Milestone5Status {
    param([Parameter(Mandatory = $true)][string]$Status)

    if ($Status -notin @('passed', 'failed', 'blocked', 'not_run')) {
        throw 'evidence status must be passed, failed, blocked, or not_run'
    }
}

function ConvertFrom-Milestone5K6Summary {
    param(
        [Parameter(Mandatory = $true)][object]$Summary,
        [Parameter(Mandatory = $true)][string]$Scenario
    )

    if ($Scenario -notin (Get-Milestone5ScenarioNames)) { throw 'unknown Milestone 5 scenario' }
    $metrics = Get-Milestone5OptionalValue -Object $Summary -Name 'metrics'
    if ($null -eq $metrics) { throw "$Scenario k6 summary omitted metrics" }
    $checks = Get-Milestone5OptionalValue -Object $metrics -Name 'checks'
    $iterations = Get-Milestone5OptionalValue -Object $metrics -Name 'iterations'
    $httpRequests = Get-Milestone5OptionalValue -Object $metrics -Name 'http_reqs'
    foreach ($metricName in @('checks', 'iterations', 'http_reqs')) {
        if ($null -eq (Get-Milestone5OptionalValue -Object $metrics -Name $metricName)) {
            throw "$Scenario k6 summary omitted $metricName"
        }
    }
    $checkValues = Get-Milestone5OptionalValue -Object $checks -Name 'values' -Default $checks
    $iterationValues = Get-Milestone5OptionalValue -Object $iterations -Name 'values' -Default $iterations
    $requestValues = Get-Milestone5OptionalValue -Object $httpRequests -Name 'values' -Default $httpRequests
    $passes = [int64](Get-Milestone5OptionalValue -Object $checkValues -Name 'passes' -Default -1)
    $failures = [int64](Get-Milestone5OptionalValue -Object $checkValues -Name 'fails' -Default -1)
    $rate = [double](Get-Milestone5OptionalValue -Object $checkValues -Name 'value' -Default -1)
    $iterationCount = [int64](Get-Milestone5OptionalValue -Object $iterationValues -Name 'count' -Default -1)
    $requestCount = [int64](Get-Milestone5OptionalValue -Object $requestValues -Name 'count' -Default -1)
    if ($passes -le 0 -or $failures -ne 0 -or $rate -ne 1.0 -or
        $iterationCount -le 0 -or $requestCount -le 0) {
        throw "$Scenario k6 summary was vacuous or contained failed checks"
    }
    foreach ($metric in $metrics.PSObject.Properties) {
        $thresholds = Get-Milestone5OptionalValue -Object $metric.Value -Name 'thresholds'
        if ($null -eq $thresholds) { continue }
        foreach ($threshold in $thresholds.PSObject.Properties) {
            if (-not [bool](Get-Milestone5OptionalValue -Object $threshold.Value -Name 'ok' -Default $false)) {
                throw "$Scenario k6 threshold failed"
            }
        }
    }
    return [ordered]@{
        scenario = $Scenario
        status = 'passed'
        raw_summary = "raw/$Scenario-summary.json"
        checks_passed = $passes
        checks_failed = 0
        iterations = $iterationCount
        http_requests = $requestCount
    }
}

function Assert-Milestone5DatabaseInvariants {
    param([Parameter(Mandatory = $true)][object]$Evidence)

    $exactZero = @(
        'dual_writer_violations', 'assignment_ledger_mismatches', 'directory_mismatches',
        'quota_violations', 'journal_gaps', 'apply_receipt_conflicts',
        'command_receipt_conflicts', 'unreconciled_commands'
    )
    foreach ($name in $exactZero) {
        $value = Get-Milestone5OptionalValue -Object $Evidence -Name $name
        $parsed = 0L
        if ($null -eq $value -or -not [int64]::TryParse([string]$value, [ref]$parsed) -or $parsed -ne 0) {
            throw "database invariant $name must be exactly zero"
        }
    }
    $writers = Get-Milestone5OptionalValue -Object $Evidence -Name 'enabled_writer_fences'
    $writerCount = 0L
    if ($null -eq $writers -or -not [int64]::TryParse([string]$writers, [ref]$writerCount) -or
        $writerCount -lt 1 -or $writerCount -gt 2) {
        throw 'enabled_writer_fences must prove the bounded one-writer topology'
    }
    return [ordered]@{
        status = 'passed'
        enabled_writer_fences = $writerCount
        dual_writer_violations = 0
        assignment_ledger_mismatches = 0
        directory_mismatches = 0
        quota_violations = 0
        journal_gaps = 0
        apply_receipt_conflicts = 0
        command_receipt_conflicts = 0
        unreconciled_commands = 0
    }
}

function Assert-Milestone5MeasuredMigrationEvidence {
    param([Parameter(Mandatory = $true)][object]$Evidence)

    $pause = Get-Milestone5OptionalValue -Object $Evidence -Name 'final_write_pause_ms'
    $limit = Get-Milestone5OptionalValue -Object $Evidence -Name 'maximum_final_write_pause_ms'
    $pauseValue = 0.0
    $limitValue = 0.0
    if ($null -eq $pause -or $null -eq $limit -or
        -not [double]::TryParse([string]$pause, [ref]$pauseValue) -or
        -not [double]::TryParse([string]$limit, [ref]$limitValue) -or
        $pauseValue -le 0 -or $limitValue -le 0 -or $pauseValue -gt $limitValue) {
        throw 'final write pause evidence was missing, unbounded, or over budget'
    }
    foreach ($name in @('target_write_observed_before_reverse', 'target_write_preserved_after_reverse')) {
        $value = Get-Milestone5OptionalValue -Object $Evidence -Name $name
        if ($value -isnot [bool] -or -not $value) { throw "$name must be true" }
    }
    $targetGeneration = [int64](Get-Milestone5OptionalValue -Object $Evidence -Name 'target_generation' -Default 0)
    $reverseGeneration = [int64](Get-Milestone5OptionalValue -Object $Evidence -Name 'reverse_generation' -Default 0)
    if ($targetGeneration -le 0 -or $reverseGeneration -le $targetGeneration) {
        throw 'reverse generation must be newer than the target-write generation'
    }
    return [ordered]@{
        status = 'passed'
        final_write_pause_ms = [Math]::Round($pauseValue, 3)
        maximum_final_write_pause_ms = [Math]::Round($limitValue, 3)
        target_write_observed_before_reverse = $true
        target_write_preserved_after_reverse = $true
        target_generation = $targetGeneration
        reverse_generation = $reverseGeneration
    }
}

function Test-Milestone5CanonicalSummaryReady {
    param(
        [Parameter(Mandatory = $true)][object[]]$Scenarios,
        [Parameter(Mandatory = $true)][bool]$DatabaseInvariantsPassed,
        [Parameter(Mandatory = $true)][bool]$MigrationEvidencePassed,
        [Parameter(Mandatory = $true)][bool]$TeardownCompleted,
        [Parameter(Mandatory = $true)][bool]$SanitizationCompleted
    )

    $seen = @{}
    foreach ($scenario in $Scenarios) {
        $name = [string](Get-Milestone5OptionalValue -Object $scenario -Name 'scenario')
        $status = [string](Get-Milestone5OptionalValue -Object $scenario -Name 'status')
        if ($name -in (Get-Milestone5ScenarioNames) -and $status -eq 'passed') { $seen[$name] = $true }
    }
    return $seen.Count -eq (Get-Milestone5ScenarioNames).Count -and
        $DatabaseInvariantsPassed -and $MigrationEvidencePassed -and
        $TeardownCompleted -and $SanitizationCompleted
}

function Assert-Milestone5ArtifactsSanitized {
    param(
        [Parameter(Mandatory = $true)][string]$EvidenceDirectory,
        [string[]]$SecretValues = @()
    )

    foreach ($file in Get-ChildItem -LiteralPath $EvidenceDirectory -File -Recurse) {
        $content = Get-Content -Raw -LiteralPath $file.FullName -ErrorAction SilentlyContinue
        if ([string]::IsNullOrEmpty($content)) { continue }
        if ($content -match '(?i)postgres(?:ql)?://[^\s"'']+' -or
            $content -match '(?i)\b(?:password|passwd|jwt_secret|authorization)\s*[:=]\s*[^\s,}]+' -or
            $content -match '(?i)Bearer\s+[A-Za-z0-9._~-]+') {
            throw 'evidence artifact contained a credential or DSN shaped value'
        }
        foreach ($secret in $SecretValues) {
            if (-not [string]::IsNullOrWhiteSpace($secret) -and $content.Contains($secret)) {
                throw 'evidence artifact contained an in-memory synthetic secret'
            }
        }
    }
}

function Write-Milestone5JsonAtomic {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][object]$Value
    )

    $candidate = "$Path.candidate-$([guid]::NewGuid().ToString('N'))"
    try {
        [System.IO.File]::WriteAllText(
            $candidate,
            ($Value | ConvertTo-Json -Depth 20),
            [System.Text.UTF8Encoding]::new($false)
        )
        Move-Item -LiteralPath $candidate -Destination $Path -ErrorAction Stop
    } finally {
        if (Test-Path -LiteralPath $candidate) { Remove-Item -LiteralPath $candidate -Force }
    }
}
