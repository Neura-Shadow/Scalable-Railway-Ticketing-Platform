Set-StrictMode -Version Latest

function Get-Milestone4NormalizedPath {
    param([Parameter(Mandatory = $true)][string]$Path)

    $fullPath = [System.IO.Path]::GetFullPath($Path)
    $rootPath = [System.IO.Path]::GetPathRoot($fullPath)
    if ($fullPath.Equals($rootPath, [StringComparison]::OrdinalIgnoreCase)) {
        return $rootPath
    }
    return $fullPath.TrimEnd(
        [System.IO.Path]::DirectorySeparatorChar,
        [System.IO.Path]::AltDirectorySeparatorChar
    )
}

function Test-Milestone4SameOrDescendantPath {
    param(
        [Parameter(Mandatory = $true)][string]$Candidate,
        [Parameter(Mandatory = $true)][string]$Parent,
        [Parameter(Mandatory = $true)][StringComparison]$Comparison
    )

    if ($Candidate.Equals($Parent, $Comparison)) { return $true }
    $prefix = $Parent
    if (-not $prefix.EndsWith([string][System.IO.Path]::DirectorySeparatorChar, $Comparison) -and
        -not $prefix.EndsWith([string][System.IO.Path]::AltDirectorySeparatorChar, $Comparison)) {
        $prefix += [System.IO.Path]::DirectorySeparatorChar
    }
    return $Candidate.StartsWith($prefix, $Comparison)
}

function Test-Milestone4ReparsePoint {
    param([Parameter(Mandatory = $true)][System.IO.FileSystemInfo]$Item)

    if (($Item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
        return $true
    }
    $linkType = $Item.PSObject.Properties['LinkType']
    return $null -ne $linkType -and -not [string]::IsNullOrWhiteSpace([string]$linkType.Value)
}

function Assert-Milestone4PathHasNoReparsePoints {
    param([Parameter(Mandatory = $true)][string]$Path)

    $cursor = $Path
    while (-not [string]::IsNullOrWhiteSpace($cursor)) {
        if (Test-Path -LiteralPath $cursor) {
            $item = Get-Item -Force -LiteralPath $cursor
            if (Test-Milestone4ReparsePoint -Item $item) {
                throw 'EvidenceDirectory must not use a reparse point or symbolic-link alias'
            }
        }
        $parent = [System.IO.Directory]::GetParent($cursor)
        if ($null -eq $parent) { break }
        $cursor = $parent.FullName
    }
}

function New-Milestone4EvidenceDirectory {
    param(
        [Parameter(Mandatory = $true)][string]$EvidenceDirectory,
        [Parameter(Mandatory = $true)][string]$RepositoryPath
    )

    $normalizedEvidence = Get-Milestone4NormalizedPath -Path $EvidenceDirectory
    $normalizedRepository = Get-Milestone4NormalizedPath -Path $RepositoryPath
    $isWindowsHost = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
        [System.Runtime.InteropServices.OSPlatform]::Windows
    )
    $comparison = if ($isWindowsHost) {
        [StringComparison]::OrdinalIgnoreCase
    } else {
        [StringComparison]::Ordinal
    }
    Assert-Milestone4PathHasNoReparsePoints -Path $normalizedRepository
    if ((Test-Milestone4SameOrDescendantPath -Candidate $normalizedEvidence `
            -Parent $normalizedRepository -Comparison $comparison) -or
        (Test-Milestone4SameOrDescendantPath -Candidate $normalizedRepository `
            -Parent $normalizedEvidence -Comparison $comparison)) {
        throw 'EvidenceDirectory must not overlap the repository or any repository ancestor'
    }
    if (Test-Path -LiteralPath $normalizedEvidence) {
        throw 'EvidenceDirectory must not already exist; this run must create it'
    }

    Assert-Milestone4PathHasNoReparsePoints -Path $normalizedEvidence
    New-Item -ItemType Directory -Path $normalizedEvidence -ErrorAction Stop | Out-Null
    Assert-Milestone4PathHasNoReparsePoints -Path $normalizedEvidence
    if (@(Get-ChildItem -Force -LiteralPath $normalizedEvidence).Count -ne 0) {
        throw 'EvidenceDirectory was not empty immediately after creation'
    }
    return $normalizedEvidence
}

function Initialize-Milestone4K6SummaryFile {
    param([Parameter(Mandatory = $true)][string]$Path)

    [System.IO.File]::WriteAllText(
        $Path,
        '{}',
        [System.Text.UTF8Encoding]::new($false)
    )
    if ([System.IO.Path]::DirectorySeparatorChar -eq '\') {
        return
    }

    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        & chmod '0666' '--' $Path 2>&1 | Out-Null
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if ($exitCode -ne 0) {
        throw 'k6 summary permission initialization failed'
    }
}

function Assert-Milestone4ComposeProjectUnused {
    param(
        [Parameter(Mandatory = $true)][string]$ProjectName,
        [Parameter(Mandatory = $true)][scriptblock]$DockerInvoker
    )

    if ($ProjectName -notmatch '^[a-z0-9][a-z0-9_-]{2,62}$') {
        throw 'ProjectName must be a bounded lowercase Compose project name'
    }
    $labelFilter = "label=com.docker.compose.project=$ProjectName"
    $queries = @(
        [pscustomobject]@{ Kind = 'container'; Arguments = @('ps', '-a', '-q', '--filter', $labelFilter) },
        [pscustomobject]@{ Kind = 'volume'; Arguments = @('volume', 'ls', '-q', '--filter', $labelFilter) },
        [pscustomobject]@{ Kind = 'network'; Arguments = @('network', 'ls', '-q', '--filter', $labelFilter) }
    )
    foreach ($query in $queries) {
        $result = & $DockerInvoker -DockerArguments ([string[]]$query.Arguments)
        if ($null -eq $result -or $null -eq $result.PSObject.Properties['ExitCode'] -or
            [int]$result.ExitCode -ne 0) {
            throw "could not verify existing Compose $($query.Kind) labels"
        }
        $matches = @($result.Output | Where-Object {
            -not [string]::IsNullOrWhiteSpace([string]$_)
        })
        if ($matches.Count -ne 0) {
            throw 'ProjectName already labels Docker resources; refusing to reuse or tear down that Compose project'
        }
    }
}

function Get-Milestone4AdminResult {
    param([Parameter(Mandatory = $true)][object]$Envelope)

    $resultProperty = $Envelope.PSObject.Properties |
        Where-Object { $_.Name -ieq 'result' } | Select-Object -First 1
    $result = if ($null -ne $resultProperty) { $resultProperty.Value } else { $null }
    foreach ($nestedName in @('record', 'migration')) {
        if ($null -eq $result) { break }
        $nestedProperty = $result.PSObject.Properties |
            Where-Object { $_.Name -ieq $nestedName } | Select-Object -First 1
        if ($null -ne $nestedProperty -and $null -ne $nestedProperty.Value) {
            $result = $nestedProperty.Value
            break
        }
    }
    return $result
}

function Get-Milestone4MigrationState {
    param([Parameter(Mandatory = $true)][object]$Envelope)

    $result = Get-Milestone4AdminResult -Envelope $Envelope
    $stateProperty = if ($null -ne $result) {
        $result.PSObject.Properties |
            Where-Object { $_.Name -ieq 'state' } | Select-Object -First 1
    } else { $null }
    if ($null -eq $stateProperty -or
        [string]::IsNullOrWhiteSpace([string]$stateProperty.Value)) {
        throw 'shard-admin result omitted migration state'
    }
    return ([string]$stateProperty.Value).ToLowerInvariant()
}

function Get-Milestone4K6MetricValues {
    param([object]$Metric)

    if ($null -eq $Metric) { return $null }
    $valuesProperty = $Metric.PSObject.Properties |
        Where-Object { $_.Name -ieq 'values' } | Select-Object -First 1
    if ($null -ne $valuesProperty -and $null -ne $valuesProperty.Value) {
        return $valuesProperty.Value
    }
    return $Metric
}

function Get-Milestone4OptionalPropertyValue {
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

function Get-Milestone4MaximumPostgresConnections {
    param([Parameter(Mandatory = $true)][object[]]$Samples)

    if ($Samples.Count -eq 0) {
        throw 'PostgreSQL connection evidence was empty'
    }
    $values = foreach ($sample in $Samples) {
        $raw = Get-Milestone4OptionalPropertyValue -Object $sample -Name 'connections'
        $parsed = 0L
        if ($null -eq $raw -or
            -not [int64]::TryParse([string]$raw, [ref]$parsed) -or $parsed -lt 0) {
            throw 'PostgreSQL connection evidence contained an invalid sample'
        }
        $parsed
    }
    return [int64](($values | Measure-Object -Maximum).Maximum)
}

function Get-Milestone4EvidenceFailureStatus {
    param([Parameter(Mandatory = $true)][string]$Category)

    if ($Category -in @(
        'source_provenance', 'docker_unavailable', 'compose_unavailable',
        'compose_project_preflight', 'operator_cli_unrun', 'reconcile_cli_unrun'
    )) {
        return 'blocked'
    }
    return 'failed'
}

function Test-Milestone4CanonicalSummaryReady {
    param(
        [Parameter(Mandatory = $true)][bool]$RunSucceeded,
        [Parameter(Mandatory = $true)][bool]$SummaryPrepared,
        [Parameter(Mandatory = $true)][bool]$SanitizationCompleted,
        [Parameter(Mandatory = $true)][bool]$TeardownRequired,
        [Parameter(Mandatory = $true)][bool]$TeardownCompleted
    )
    return $RunSucceeded -and $SummaryPrepared -and $SanitizationCompleted -and
        (-not $TeardownRequired -or $TeardownCompleted)
}

function ConvertFrom-Milestone4K6CoreSummary {
    param(
        [Parameter(Mandatory = $true)][object]$Summary,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $metrics = Get-Milestone4OptionalPropertyValue -Object $Summary -Name 'metrics'
    if ($null -eq $metrics) { throw "$Name k6 summary omitted metrics" }
    $durationValues = Get-Milestone4K6MetricValues -Metric (
        Get-Milestone4OptionalPropertyValue -Object $metrics -Name 'shard_request_duration'
    )
    $requestValues = Get-Milestone4K6MetricValues -Metric (
        Get-Milestone4OptionalPropertyValue -Object $metrics -Name 'http_reqs'
    )
    $checkValues = Get-Milestone4K6MetricValues -Metric (
        Get-Milestone4OptionalPropertyValue -Object $metrics -Name 'checks'
    )
    $iterationValues = Get-Milestone4K6MetricValues -Metric (
        Get-Milestone4OptionalPropertyValue -Object $metrics -Name 'iterations'
    )
    $p50 = Get-Milestone4OptionalPropertyValue -Object $durationValues -Name 'med'
    $p95 = Get-Milestone4OptionalPropertyValue -Object $durationValues -Name 'p(95)'
    $p99 = Get-Milestone4OptionalPropertyValue -Object $durationValues -Name 'p(99)'
    $durationSampleCount = Get-Milestone4OptionalPropertyValue -Object $durationValues -Name 'count'
    $requestCount = Get-Milestone4OptionalPropertyValue -Object $requestValues -Name 'count'
    $requestRate = Get-Milestone4OptionalPropertyValue -Object $requestValues -Name 'rate'
    $checkPasses = Get-Milestone4OptionalPropertyValue -Object $checkValues -Name 'passes'
    $checkFailures = Get-Milestone4OptionalPropertyValue -Object $checkValues -Name 'fails'
    $checkRate = Get-Milestone4OptionalPropertyValue -Object $checkValues -Name 'value'
    $iterationCount = Get-Milestone4OptionalPropertyValue -Object $iterationValues -Name 'count'
    if ($null -eq $p50 -or $null -eq $p95 -or $null -eq $p99) {
        throw "$Name k6 summary omitted required p50, p95, or p99 latency evidence"
    }
    if ($null -eq $requestCount -or $null -eq $requestRate -or
        [double]$requestCount -le 0 -or [double]$requestRate -le 0) {
        throw "$Name k6 summary omitted non-vacuous request count or achieved-rate evidence"
    }
    if ($null -eq $durationSampleCount -or [int64]$durationSampleCount -le 0 -or
        $null -eq $checkPasses -or $null -eq $checkFailures -or $null -eq $checkRate -or
        [int64]$checkPasses -le 0 -or [int64]$checkFailures -ne 0 -or [double]$checkRate -ne 1.0 -or
        $null -eq $iterationCount -or [int64]$iterationCount -le 0) {
        throw "$Name k6 summary omitted strict check, iteration, or latency sample evidence"
    }
    return [ordered]@{
        artifact = "$Name-summary.json"
        request_count = [int64]$requestCount
        achieved_rate_per_second = [Math]::Round([double]$requestRate, 6)
        measurement_duration_seconds = [Math]::Round([double]$requestCount / [double]$requestRate, 6)
        iterations = [int64]$iterationCount
        repetitions = 1
        latency_sample_count = [int64]$durationSampleCount
        percentile_interpretation = if ([int64]$iterationCount -eq 1) { 'not_distribution' } else { 'bounded_distribution' }
        check_passes = [int64]$checkPasses
        check_failures = [int64]$checkFailures
        check_pass_rate = [double]$checkRate
        p50_ms = $p50
        p95_ms = $p95
        p99_ms = $p99
    }
}

function Assert-Milestone4OperatorHealth {
    param(
        [Parameter(Mandatory = $true)][object]$Invocation,
        [Parameter(Mandatory = $true)][bool]$ExpectedReady
    )

    $envelope = Get-Milestone4OptionalPropertyValue -Object $Invocation -Name 'Envelope'
    $result = Get-Milestone4OptionalPropertyValue -Object $envelope -Name 'result'
    $expectedExit = if ($ExpectedReady) { 0 } else { 1 }
    $expectedStatus = if ($ExpectedReady) { 'completed' } else { 'failed' }
    $expectedWritable = if ($ExpectedReady) { 3 } else { 2 }
    $expectedDegraded = if ($ExpectedReady) { 0 } else { 1 }
    if ($null -eq $envelope -or $null -eq $result -or
        [int](Get-Milestone4OptionalPropertyValue -Object $Invocation -Name 'ExitCode' -Default -1) -ne $expectedExit -or
        [string](Get-Milestone4OptionalPropertyValue -Object $envelope -Name 'command') -ne 'inspect-health' -or
        [string](Get-Milestone4OptionalPropertyValue -Object $envelope -Name 'status') -ne $expectedStatus -or
        -not [bool](Get-Milestone4OptionalPropertyValue -Object $envelope -Name 'read_only' -Default $false) -or
        [bool](Get-Milestone4OptionalPropertyValue -Object $result -Name 'ready' -Default (-not $ExpectedReady)) -ne $ExpectedReady -or
        [int64](Get-Milestone4OptionalPropertyValue -Object $result -Name 'schema_version' -Default -1) -ne 11 -or
        [bool](Get-Milestone4OptionalPropertyValue -Object $result -Name 'schema_dirty' -Default $true) -or
        [int](Get-Milestone4OptionalPropertyValue -Object $result -Name 'shard_catalog_entries' -Default -1) -ne 3 -or
        [int](Get-Milestone4OptionalPropertyValue -Object $result -Name 'writable_active_shards' -Default -1) -ne $expectedWritable -or
        [int](Get-Milestone4OptionalPropertyValue -Object $result -Name 'degraded_shards' -Default -1) -ne $expectedDegraded -or
        [int64](Get-Milestone4OptionalPropertyValue -Object $result -Name 'active_migrations_observed' -Default -1) -lt 0 -or
        [bool](Get-Milestone4OptionalPropertyValue -Object $result -Name 'active_migrations_truncated' -Default $true)) {
        throw 'shard-admin health envelope did not match the exact bounded topology state'
    }
    return [ordered]@{
        ready = $ExpectedReady
        schema_version = 11
        shard_catalog_entries = 3
        writable_active_shards = $expectedWritable
        degraded_shards = $expectedDegraded
        active_migrations_observed = [int64](
            Get-Milestone4OptionalPropertyValue -Object $result -Name 'active_migrations_observed' -Default -1
        )
    }
}

function Test-Milestone4MetricLabels {
    param(
        [Parameter(Mandatory = $true)][string]$Labels,
        [string]$Required = ''
    )

    $requiredParts = @($Required.Split(',') | ForEach-Object { $_.Trim() } | Where-Object {
        -not [string]::IsNullOrWhiteSpace($_)
    })
    foreach ($requiredPart in $requiredParts) {
        $boundedPattern = "(?:^|,)$([regex]::Escape($requiredPart))(?:,|$)"
        if ($Labels -notmatch $boundedPattern) {
            return $false
        }
    }
    return $true
}

function Assert-BoundedShardReport {
    param(
        [Parameter(Mandatory = $true)][object]$Invocation,
        [Parameter(Mandatory = $true)][ValidateSet('complete', 'partial', 'unavailable')][string]$Expected,
        [string]$ExpectedUnavailableShardID = '',
        [string]$ExpectedUnavailableFailure = ''
    )
    $envelope = Get-Milestone4OptionalPropertyValue -Object $Invocation -Name 'Envelope'
    $report = Get-Milestone4OptionalPropertyValue -Object $envelope -Name 'result'
    if ($null -eq $envelope -or $null -eq $report) {
        throw 'bounded reconciliation omitted its report'
    }
    $scope = [string](Get-Milestone4OptionalPropertyValue -Object $report -Name 'scope')
    $command = [string](Get-Milestone4OptionalPropertyValue -Object $envelope -Name 'command')
    if ([string]::IsNullOrWhiteSpace($scope) -or $command -ne $scope) {
        throw 'bounded reconciliation envelope command did not match its report scope'
    }
    $completeness = [string](Get-Milestone4OptionalPropertyValue -Object $report -Name 'completeness')
    $expectedExit = if ($Expected -eq 'complete') { 0 } else { 1 }
    $expectedStatus = if ($Expected -eq 'complete') { 'healthy' } else { $Expected }
    $exitCode = [int](Get-Milestone4OptionalPropertyValue -Object $Invocation -Name 'ExitCode' -Default -1)
    if ($exitCode -ne $expectedExit -or $completeness -ne $Expected) {
        throw "bounded reconciliation expected $Expected but returned $completeness/exit $exitCode"
    }
    $violations = [int64](Get-Milestone4OptionalPropertyValue -Object $report -Name 'violations' -Default -1)
    $truncated = [bool](Get-Milestone4OptionalPropertyValue -Object $report -Name 'truncated' -Default $true)
    $rowsExamined = [int64](Get-Milestone4OptionalPropertyValue -Object $report -Name 'rows_examined' -Default -1)
    $pages = [int](Get-Milestone4OptionalPropertyValue -Object $report -Name 'pages' -Default -1)
    if ([string](Get-Milestone4OptionalPropertyValue -Object $envelope -Name 'status') -ne $expectedStatus -or
        -not [bool](Get-Milestone4OptionalPropertyValue -Object $envelope -Name 'read_only' -Default $false) -or
        $violations -lt 0) {
        throw "bounded reconciliation returned an invalid $Expected envelope"
    }
    if ($Expected -eq 'complete' -and (
        $violations -ne 0 -or $truncated -or $rowsExamined -le 0 -or $pages -le 0
    )) {
        throw 'complete reconciliation was vacuous, truncated, unhealthy, or inconsistent'
    }
    $shards = @(Get-Milestone4OptionalPropertyValue -Object $report -Name 'shards' -Default @())
    $expectedShardIDs = @('legacy', 'shard-0', 'shard-1')
    $shardEvidence = @()
    foreach ($shard in $shards) {
        $shardID = [string](Get-Milestone4OptionalPropertyValue -Object $shard -Name 'shard_id')
        $status = [string](Get-Milestone4OptionalPropertyValue -Object $shard -Name 'status')
        if ($shardID -notin $expectedShardIDs -or $status -notin @('healthy', 'unavailable')) {
            throw 'bounded reconciliation returned an unknown shard identity or status'
        }
        $shardEvidence += [ordered]@{
            shard_id = $shardID
            status = $status
            failure = [string](Get-Milestone4OptionalPropertyValue -Object $shard -Name 'failure' -Default '')
            pages = [int](Get-Milestone4OptionalPropertyValue -Object $shard -Name 'pages' -Default 0)
            rows_examined = [int64](Get-Milestone4OptionalPropertyValue -Object $shard -Name 'rows_examined' -Default 0)
        }
    }
    $actualShardIDs = @($shardEvidence | ForEach-Object { [string]$_['shard_id'] })
    if ($shardEvidence.Count -ne 3 -or @($actualShardIDs | Select-Object -Unique).Count -ne 3 -or
        @($expectedShardIDs | Where-Object { $_ -notin $actualShardIDs }).Count -ne 0) {
        throw 'bounded reconciliation omitted or duplicated a fixed shard identity'
    }
    $healthyShardCount = @($shardEvidence | Where-Object { [string]$_['status'] -eq 'healthy' }).Count
    $unavailableShardCount = @($shardEvidence | Where-Object { [string]$_['status'] -eq 'unavailable' }).Count
    if ($Expected -eq 'complete' -and ($healthyShardCount -ne 3 -or $unavailableShardCount -ne 0)) {
        throw 'complete reconciliation did not preserve three healthy fixed shards'
    }
    if ($Expected -eq 'partial' -and (
        $truncated -or $rowsExamined -le 0 -or $pages -le 0 -or
        $healthyShardCount -ne 2 -or $unavailableShardCount -ne 1 -or
        [string]::IsNullOrWhiteSpace($ExpectedUnavailableShardID) -or
        [string]::IsNullOrWhiteSpace($ExpectedUnavailableFailure)
    )) {
        throw 'partial reconciliation omitted bounded healthy and unavailable shard evidence'
    }
    if ($Expected -eq 'partial') {
        $unavailable = @($shardEvidence | Where-Object { [string]$_['status'] -eq 'unavailable' })[0]
        if ([string]$unavailable['shard_id'] -ne $ExpectedUnavailableShardID -or
            [string]$unavailable['failure'] -ne $ExpectedUnavailableFailure) {
            throw 'partial reconciliation attributed the failure to the wrong shard or category'
        }
    }
    if ($Expected -eq 'unavailable' -and (
        $truncated -or $healthyShardCount -ne 0 -or $unavailableShardCount -ne 3
    )) {
        throw 'unavailable reconciliation returned contradictory shard evidence'
    }
    return [ordered]@{
        completeness = $completeness
        scope = $scope
        duration_ms = [double](Get-Milestone4OptionalPropertyValue -Object $Invocation -Name 'DurationMilliseconds' -Default 0)
        pages = $pages
        rows_examined = $rowsExamined
        violations = $violations
        truncated = $truncated
        healthy_shards = $healthyShardCount
        unavailable_shards = $unavailableShardCount
        shards = $shardEvidence
        deferred_checks = @(Get-Milestone4OptionalPropertyValue -Object $report -Name 'deferred_checks' -Default @())
    }
}
