[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot 'milestone-4-evidence-guardrails.ps1')

function Assert-Throws {
    param(
        [Parameter(Mandatory = $true)][scriptblock]$Action,
        [Parameter(Mandatory = $true)][string]$Label
    )
    try {
        & $Action
    } catch {
        return
    }
    throw "expected guardrail rejection: $Label"
}

$sandbox = Join-Path ([System.IO.Path]::GetTempPath()) `
    "railway-m4-guardrail-test-$([guid]::NewGuid().ToString('N'))"
$sandbox = [System.IO.Path]::GetFullPath($sandbox)
$tempRoot = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
if (-not $sandbox.StartsWith($tempRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'guardrail test sandbox escaped the temporary directory'
}

New-Item -ItemType Directory -Path $sandbox | Out-Null
$aliasPath = Join-Path $sandbox 'alias'
try {
    $repository = Join-Path $sandbox 'repository'
    New-Item -ItemType Directory -Path $repository | Out-Null

    $freshEvidence = Join-Path $sandbox 'fresh-evidence'
    $created = New-Milestone4EvidenceDirectory `
        -EvidenceDirectory $freshEvidence -RepositoryPath $repository
    if ($created -ne [System.IO.Path]::GetFullPath($freshEvidence) -or
        -not (Test-Path -LiteralPath $created) -or
        @(Get-ChildItem -Force -LiteralPath $created).Count -ne 0) {
        throw 'fresh evidence directory was not created empty'
    }

    $summaryPath = Join-Path $freshEvidence 'permission-probe-summary.json'
    Initialize-Milestone4K6SummaryFile -Path $summaryPath
    if ([System.IO.File]::ReadAllText($summaryPath) -ne '{}') {
        throw 'k6 summary initializer did not create the exact empty JSON object'
    }
    $summaryRunsOnWindows = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
        [System.Runtime.InteropServices.OSPlatform]::Windows
    )
    if (-not $summaryRunsOnWindows) {
        $mode = [int](& stat '-c' '%a' '--' $summaryPath)
        if ($LASTEXITCODE -ne 0 -or $mode -ne 666) {
            throw "k6 summary initializer mode = $mode, want 666"
        }
    }

    $existingEvidence = Join-Path $sandbox 'existing-evidence'
    New-Item -ItemType Directory -Path $existingEvidence | Out-Null
    Assert-Throws -Label 'pre-existing target' -Action {
        New-Milestone4EvidenceDirectory `
            -EvidenceDirectory $existingEvidence -RepositoryPath $repository | Out-Null
    }
    Assert-Throws -Label 'repository descendant' -Action {
        New-Milestone4EvidenceDirectory `
            -EvidenceDirectory (Join-Path $repository 'evidence') -RepositoryPath $repository | Out-Null
    }
    Assert-Throws -Label 'repository ancestor' -Action {
        New-Milestone4EvidenceDirectory `
            -EvidenceDirectory $sandbox -RepositoryPath $repository | Out-Null
    }

    $aliasTarget = Join-Path $sandbox 'alias-target'
    New-Item -ItemType Directory -Path $aliasTarget | Out-Null
    $isWindowsHost = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
        [System.Runtime.InteropServices.OSPlatform]::Windows
    )
    $itemType = if ($isWindowsHost) { 'Junction' } else { 'SymbolicLink' }
    New-Item -ItemType $itemType -Path $aliasPath -Target $aliasTarget | Out-Null
    Assert-Throws -Label 'reparse-point alias' -Action {
        New-Milestone4EvidenceDirectory `
            -EvidenceDirectory (Join-Path $aliasPath 'evidence') -RepositoryPath $repository | Out-Null
    }

    $emptyProbe = {
        param([string[]]$DockerArguments)
        [pscustomobject]@{ ExitCode = 0; Output = @() }
    }
    Assert-Milestone4ComposeProjectUnused -ProjectName 'railway-m4-test' -DockerInvoker $emptyProbe
    Assert-Throws -Label 'invalid Compose project label filter' -Action {
        Assert-Milestone4ComposeProjectUnused `
            -ProjectName 'invalid;project' -DockerInvoker $emptyProbe
    }

    foreach ($busyKind in @('ps', 'volume', 'network')) {
        $busyProbe = {
            param([string[]]$DockerArguments)
            $output = if ($DockerArguments[0] -eq $busyKind) { @('existing-resource') } else { @() }
            [pscustomobject]@{ ExitCode = 0; Output = $output }
        }.GetNewClosure()
        Assert-Throws -Label "existing $busyKind label" -Action {
            Assert-Milestone4ComposeProjectUnused `
                -ProjectName 'railway-m4-test' -DockerInvoker $busyProbe
        }
    }

    $failedProbe = {
        param([string[]]$DockerArguments)
        [pscustomobject]@{ ExitCode = 1; Output = @() }
    }
    Assert-Throws -Label 'metadata query failure' -Action {
        Assert-Milestone4ComposeProjectUnused `
            -ProjectName 'railway-m4-test' -DockerInvoker $failedProbe
    }

    $migrationStates = @(
        @{ Label = 'direct result'; JSON = '{"result":{"state":"COPYING"}}'; Expected = 'copying' },
        @{ Label = 'record envelope'; JSON = '{"result":{"record":{"state":"VALIDATING"}}}'; Expected = 'validating' },
        @{ Label = 'migration envelope'; JSON = '{"result":{"migration":{"state":"CUTOVER_READY"}}}'; Expected = 'cutover_ready' }
    )
    foreach ($migrationState in $migrationStates) {
        $actual = Get-Milestone4MigrationState -Envelope (
            $migrationState.JSON | ConvertFrom-Json
        )
        if ($actual -ne $migrationState.Expected) {
            throw "migration-state parser rejected $($migrationState.Label)"
        }
    }
    Assert-Throws -Label 'missing migration state' -Action {
        Get-Milestone4MigrationState -Envelope (
            '{"result":{"migration":{"migration_id":"41000000-0000-4000-8000-000000000401"}}}' |
                ConvertFrom-Json
        ) | Out-Null
    }

    $directMetric = '{"count":4,"med":2.5,"p(95)":8.5,"p(99)":12.5}' | ConvertFrom-Json
    $directValues = Get-Milestone4K6MetricValues -Metric $directMetric
    if ($directValues.count -ne 4 -or $directValues.'p(99)' -ne 12.5) {
        throw 'direct k6 metric values were not preserved'
    }
    $wrappedMetric = '{"values":{"count":7,"med":3.5,"p(95)":9.5,"p(99)":13.5}}' |
        ConvertFrom-Json
    $wrappedValues = Get-Milestone4K6MetricValues -Metric $wrappedMetric
    if ($wrappedValues.count -ne 7 -or $wrappedValues.'p(95)' -ne 9.5) {
        throw 'wrapped k6 metric values were not unwrapped'
    }
    $requestMetric = '{"values":{"count":23,"rate":4.25}}' | ConvertFrom-Json
    $requestValues = Get-Milestone4K6MetricValues -Metric $requestMetric
    if ($requestValues.count -ne 23 -or $requestValues.rate -ne 4.25) {
        throw 'wrapped k6 request count and achieved rate were not preserved'
    }
    $syntheticSummary = @'
{"metrics":{"shard_request_duration":{"count":4,"med":2.5,"p(95)":8.5,"p(99)":12.5},"http_reqs":{"count":4,"rate":2},"checks":{"passes":6,"fails":0,"value":1},"iterations":{"count":1,"rate":0.5}}}
'@ | ConvertFrom-Json
    $coreSummary = ConvertFrom-Milestone4K6CoreSummary -Summary $syntheticSummary -Name 'fixture'
    if ($coreSummary.request_count -ne 4 -or $coreSummary.measurement_duration_seconds -ne 2 -or
        $coreSummary.latency_sample_count -ne 4 -or $coreSummary.check_failures -ne 0 -or
        $coreSummary.percentile_interpretation -ne 'not_distribution') {
        throw 'strict k6 core summary did not preserve execution and correctness context'
    }
    $syntheticSummary.metrics.checks.fails = 1
    $syntheticSummary.metrics.checks.value = 0.857
    Assert-Throws -Label 'k6 summary with failed checks' -Action {
        ConvertFrom-Milestone4K6CoreSummary -Summary $syntheticSummary -Name 'failed-checks' | Out-Null
    }
    $syntheticSummary.metrics.checks.fails = 0
    $syntheticSummary.metrics.checks.value = 1
    $syntheticSummary.metrics.shard_request_duration.count = 0
    Assert-Throws -Label 'k6 summary with no latency samples' -Action {
        ConvertFrom-Milestone4K6CoreSummary -Summary $syntheticSummary -Name 'empty-latency' | Out-Null
    }

    $connectionSamples = @(
        [pscustomobject][ordered]@{ label = 'ready'; connections = 7 },
        [ordered]@{ label = 'after-cutover'; connections = 11 }
    )
    if ((Get-Milestone4MaximumPostgresConnections -Samples $connectionSamples) -ne 11) {
        throw 'PostgreSQL connection maximum did not handle structured evidence samples'
    }
    Assert-Throws -Label 'missing PostgreSQL connection value' -Action {
        Get-Milestone4MaximumPostgresConnections -Samples @([ordered]@{ label = 'bad' }) | Out-Null
    }
    foreach ($blockedCategory in @(
        'source_provenance', 'docker_unavailable', 'compose_unavailable',
        'compose_project_preflight', 'operator_cli_unrun', 'reconcile_cli_unrun'
    )) {
        if ((Get-Milestone4EvidenceFailureStatus -Category $blockedCategory) -ne 'blocked') {
            throw "blocked evidence category was classified as failed: $blockedCategory"
        }
    }
    if ((Get-Milestone4EvidenceFailureStatus -Category 'integrity_validation') -ne 'failed') {
        throw 'an executed evidence failure was incorrectly classified as blocked'
    }
    $summaryReadyCases = @(
        @{ Label = 'all final gates'; Run = $true; Prepared = $true; Sanitized = $true; Required = $true; Down = $true; Expected = $true },
        @{ Label = 'teardown failure'; Run = $true; Prepared = $true; Sanitized = $true; Required = $true; Down = $false; Expected = $false },
        @{ Label = 'sanitization failure'; Run = $true; Prepared = $true; Sanitized = $false; Required = $true; Down = $true; Expected = $false },
        @{ Label = 'missing summary'; Run = $true; Prepared = $false; Sanitized = $true; Required = $true; Down = $true; Expected = $false },
        @{ Label = 'workload failure'; Run = $false; Prepared = $true; Sanitized = $true; Required = $true; Down = $true; Expected = $false },
        @{ Label = 'intentional keep environment'; Run = $true; Prepared = $true; Sanitized = $true; Required = $false; Down = $false; Expected = $true }
    )
    foreach ($case in $summaryReadyCases) {
        $actual = Test-Milestone4CanonicalSummaryReady `
            -RunSucceeded $case.Run -SummaryPrepared $case.Prepared `
            -SanitizationCompleted $case.Sanitized -TeardownRequired $case.Required `
            -TeardownCompleted $case.Down
        if ($actual -ne $case.Expected) {
            throw "canonical summary readiness rejected $($case.Label)"
        }
    }

    $requiredRouteLabels = 'operation="write",result="success",reason="none",shard_id="shard-0"'
    $prometheusSortedLabels = 'operation="write",reason="none",result="success",shard_id="shard-0"'
    if (-not (Test-Milestone4MetricLabels -Labels $prometheusSortedLabels -Required $requiredRouteLabels)) {
        throw 'metric label matching incorrectly depends on Prometheus label ordering'
    }
    if (Test-Milestone4MetricLabels `
        -Labels 'other_operation="write",reason="none",result="success",shard_id="shard-0"' `
        -Required $requiredRouteLabels) {
        throw 'metric label matching accepted a non-exact label name'
    }

    $partialReport = [pscustomobject]@{
        scope = 'shard-assignments'
        completeness = 'partial'
        pages = 2
        rows_examined = 7
        violations = 1
        truncated = $false
        shards = @(
            [pscustomobject]@{ shard_id = 'legacy'; status = 'healthy'; pages = 1; rows_examined = 1 },
            [pscustomobject]@{ shard_id = 'shard-0'; status = 'unavailable'; failure = 'catalog_disabled'; pages = 0; rows_examined = 0 },
            [pscustomobject]@{ shard_id = 'shard-1'; status = 'healthy'; pages = 1; rows_examined = 1 }
        )
    }
    $partialInvocation = [pscustomobject]@{
        ExitCode = 1
        DurationMilliseconds = 5.5
        Envelope = [pscustomobject]@{
            command = 'shard-assignments'
            status = 'partial'
            read_only = $true
            result = $partialReport
        }
    }
    $acceptedPartial = Assert-BoundedShardReport -Invocation $partialInvocation -Expected 'partial' `
        -ExpectedUnavailableShardID 'shard-0' -ExpectedUnavailableFailure 'catalog_disabled'
    if ($acceptedPartial.healthy_shards -ne 2 -or $acceptedPartial.unavailable_shards -ne 1 -or
        $acceptedPartial.violations -ne 1 -or $acceptedPartial.deferred_checks.Count -ne 0 -or
        @($acceptedPartial.shards).Count -ne 3) {
        throw 'bounded partial reconciliation did not preserve exact shard evidence'
    }
    $partialInvocation.Envelope.status = 'healthy'
    Assert-Throws -Label 'partial report with contradictory envelope status' -Action {
        Assert-BoundedShardReport -Invocation $partialInvocation -Expected 'partial' `
            -ExpectedUnavailableShardID 'shard-0' -ExpectedUnavailableFailure 'catalog_disabled' | Out-Null
    }
    $partialInvocation.Envelope.status = 'partial'
    $partialReport.shards[1].failure = 'query_failed'
    Assert-Throws -Label 'partial report attributed to wrong failure' -Action {
        Assert-BoundedShardReport -Invocation $partialInvocation -Expected 'partial' `
            -ExpectedUnavailableShardID 'shard-0' -ExpectedUnavailableFailure 'catalog_disabled' | Out-Null
    }
    $partialReport.shards[1].failure = 'catalog_disabled'
    $partialReport.truncated = $true
    Assert-Throws -Label 'truncated partial report' -Action {
        Assert-BoundedShardReport -Invocation $partialInvocation -Expected 'partial' `
            -ExpectedUnavailableShardID 'shard-0' -ExpectedUnavailableFailure 'catalog_disabled' | Out-Null
    }
    $partialReport.truncated = $false

    $readyHealth = [pscustomobject]@{
        ExitCode = 0
        Envelope = [pscustomobject]@{
            command = 'inspect-health'; status = 'completed'; read_only = $true
            result = [pscustomobject]@{
                ready = $true; schema_version = 9; schema_dirty = $false
                shard_catalog_entries = 3; writable_active_shards = 3; degraded_shards = 0
                active_migrations_observed = 0; active_migrations_truncated = $false
            }
        }
    }
    if (-not (Assert-Milestone4OperatorHealth -Invocation $readyHealth -ExpectedReady $true).ready) {
        throw 'ready shard-admin health envelope was not accepted'
    }
    $degradedHealth = [pscustomobject]@{
        ExitCode = 1
        Envelope = [pscustomobject]@{
            command = 'inspect-health'; status = 'failed'; read_only = $true
            result = [pscustomobject]@{
                ready = $false; schema_version = 9; schema_dirty = $false
                shard_catalog_entries = 3; writable_active_shards = 2; degraded_shards = 1
                active_migrations_observed = 2; active_migrations_truncated = $false
            }
        }
    }
    if ((Assert-Milestone4OperatorHealth -Invocation $degradedHealth -ExpectedReady $false).degraded_shards -ne 1) {
        throw 'degraded shard-admin health envelope was not accepted'
    }
    $readyHealth.Envelope.status = 'healthy'
    Assert-Throws -Label 'invented shard-admin healthy status' -Action {
        Assert-Milestone4OperatorHealth -Invocation $readyHealth -ExpectedReady $true | Out-Null
    }

    $runner = Get-Content -Raw -LiteralPath `
        (Join-Path $PSScriptRoot 'run-milestone-4-multi-replica-evidence.ps1')
    $guardrailSource = Get-Content -Raw -LiteralPath `
        (Join-Path $PSScriptRoot 'milestone-4-evidence-guardrails.ps1')
    $evidenceContractSource = "$runner`n$guardrailSource"
    $collisionCheck = $runner.IndexOf(
        'Assert-Milestone4ComposeProjectUnused -ProjectName $ProjectName',
        [StringComparison]::Ordinal
    )
    $startedFlag = $runner.IndexOf('$started = $true', [StringComparison]::Ordinal)
    $composeUp = $runner.IndexOf("Invoke-Compose -Arguments @('up'", [StringComparison]::Ordinal)
    if ($collisionCheck -lt 0 -or $collisionCheck -gt $startedFlag -or
        $collisionCheck -gt $composeUp) {
        throw 'runner does not reject Compose project collisions before startup and teardown ownership'
    }

    $dockerLookup = $runner.IndexOf(
        'Get-Command docker -CommandType Application -ErrorAction SilentlyContinue',
        [StringComparison]::Ordinal
    )
    $dockerInvocation = $runner.IndexOf(
        'Invoke-Native -AllowFailure -Command { & docker version }',
        [StringComparison]::Ordinal
    )
    if ($dockerLookup -lt 0 -or $dockerInvocation -le $dockerLookup -or
        -not $runner.Contains("`$failureCategory = 'docker_unavailable'")) {
        throw 'runner does not classify a missing Docker executable as blocked before invocation'
    }

    $invokeK6Start = $runner.IndexOf('function Invoke-K6 {', [StringComparison]::Ordinal)
    $startK6Start = $runner.IndexOf('function Start-K6 {', [StringComparison]::Ordinal)
    $waitK6Start = $runner.IndexOf('function Wait-K6 {', [StringComparison]::Ordinal)
    if ($invokeK6Start -lt 0 -or $startK6Start -le $invokeK6Start -or $waitK6Start -le $startK6Start) {
        throw 'runner k6 function boundaries are missing or reordered'
    }
    foreach ($region in @(
        $runner.Substring($invokeK6Start, $startK6Start - $invokeK6Start),
        $runner.Substring($startK6Start, $waitK6Start - $startK6Start)
    )) {
        $summaryInitialization = $region.IndexOf(
            'Initialize-Milestone4K6SummaryFile -Path $summaryPath',
            [StringComparison]::Ordinal
        )
        $composeInvocation = $region.IndexOf('Invoke-Compose -Arguments', [StringComparison]::Ordinal)
        if ($summaryInitialization -lt 0 -or $composeInvocation -le $summaryInitialization) {
            throw 'runner did not initialize a container-writable k6 summary before Compose execution'
        }
    }

    $cleanTreeCheck = $runner.IndexOf(
        'git status --porcelain=v1 --untracked-files=all',
        [StringComparison]::Ordinal
    )
    $evidenceCommit = $runner.IndexOf('$evidenceCommit =', [StringComparison]::Ordinal)
    if ($cleanTreeCheck -lt 0 -or $evidenceCommit -lt $cleanTreeCheck -or
        $evidenceCommit -gt $composeUp -or
        -not $runner.Contains('commit_sha = $evidenceCommit')) {
        throw 'runner does not bind clean committed source provenance before its Docker build'
    }
    foreach ($requiredRunnerContract in @(
        '[ValidateRange(20, 30)]',
        '[int]$CustomerCount = 20',
        "'api-1', 'api-2', 'api-3'",
        "'admission-worker-1', 'admission-worker-2'",
        "'read-model-worker-1', 'read-model-worker-2', 'hold-expirer', 'outbox-worker'",
        '&service_date=$($serviceDate.Trim())&seat_class=$seatClass&page=1&limit=100&sort=departure_at',
        "GREATEST(reservation.created_at + interval '1 second', clock_timestamp() + interval '1 second')",
        "`$fixtureExpiryArmCount.Trim() -ne '2'",
        "`$postCutoverExpiryArmCount.Trim() -ne '1'",
        "`$failureCategory = 'stale_probe_cache_reset'",
        "Invoke-Compose -Arguments @('restart', 'api-1', 'api-2', 'api-3')",
        'foreach ($api in $apis) { Wait-Ready -Service $api -Port 8080 }',
        '$prewarmCustomer = $overlapCustomers[$index]',
        "Save-Metrics -Label 'stale-probe-baseline'",
        "stale_probe_cache_setup = 'api_processes_restarted_then_prewarms_completed_before_cutover'",
        'operation="read",shard_id="legacy"',
        'operation="refresh",result="success",shard_id="shard-0"',
        'operation="write",result="success",reason="none",shard_id="legacy"',
        'operation="write",result="success",reason="none",shard_id="shard-0"',
        '$stalePreflightDelta -ne 1',
        '$refreshDelta -ne 1',
        '$sourceWriteDelta -ne 0',
        '$targetWriteDelta -ne 1',
        'stale_preflight_rejection_delta = $stalePreflightDelta',
        'source_write_success_delta = $sourceWriteDelta',
        "-Artifact 'admin-fanout-partial.log' -AllowFailure",
        "-Artifact 'admin-fanout-complete-after.log'",
        "'reconcile-shard-assignments.log'",
        "'reconcile-shard-locators.log'",
        "'shard-post-cutover-lifecycle'",
        'source_copy_count_mismatches',
        'missing_copied_target_rows',
        'successful_target_write_generations',
        'missing_reservation_outbox_events',
        'request_count = [int64]$requestCount',
        'achieved_rate_per_second = [Math]::Round([double]$requestRate, 6)',
        'check_failures = [int64]$checkFailures',
        'Get-Milestone4MaximumPostgresConnections',
        'route_cache = [ordered]@{',
        'admin_fanout_evidence = $adminFanoutEvidence',
        'postgres_connections = [ordered]@{',
        'redis_latency = $redisLatencyEvidence'
        '$teardownCompleted = $teardown.ExitCode -eq 0'
        "`$failureCategory = 'compose_teardown_failed'"
        'scanned_after_teardown = $teardownCompleted'
    )) {
        if (-not $evidenceContractSource.Contains($requiredRunnerContract)) {
            throw "runner omitted required fail-closed evidence contract: $requiredRunnerContract"
        }
    }
    foreach ($forbiddenRunnerContract in @(
        '$staleAfter', '$staleBefore', "-Name 'cross-shard-healthy'", "-Name 'cross-shard-partial'",
        '$prewarmCustomer = $staleCustomers[$index]',
        '$staleWriteDelta', 'stale_write_rejection_delta',
        "SET expires_at = clock_timestamp() - interval '1 minute'",
        "SET expires_at=clock_timestamp() - interval '1 minute'"
    )) {
        if ($runner.Contains($forbiddenRunnerContract)) {
            throw "runner retained a stale or misleading evidence contract: $forbiddenRunnerContract"
        }
    }
    foreach ($shardIndex in @(0, 1)) {
        foreach ($copiedTable in @(
            'seat_inventory', 'reservations', 'reservation_seats',
            'ticket_orders', 'tickets', 'idempotency_records'
        )) {
            $antiJoin = "LEFT JOIN booking_shard_$shardIndex.$copiedTable AS target"
            if (-not $runner.Contains($antiJoin)) {
                throw "runner omitted final source-to-target identity anti-join: shard-$shardIndex/$copiedTable"
            }
        }
    }

    $staleCacheReset = $runner.IndexOf(
        "Invoke-Compose -Arguments @('restart', 'api-1', 'api-2', 'api-3')",
        [StringComparison]::Ordinal
    )
    $stalePrewarm = $runner.IndexOf(
        '$prewarmCustomer = $overlapCustomers[$index]',
        [StringComparison]::Ordinal
    )
    $trainAMigration = $runner.IndexOf(
        'Invoke-MigrationToCutoverReady -TrainRunID $fixtureTrainA',
        [StringComparison]::Ordinal
    )
    if ($staleCacheReset -lt 0 -or $stalePrewarm -le $staleCacheReset -or
        $trainAMigration -le $stalePrewarm) {
        throw 'stale-route probe does not reset, prewarm, and then cut over in order'
    }

    foreach ($requiredReceiptContract in @(
        'function Wait-TrainRunReadModelCaughtUp {',
        "receipt.consumer_name='railway-read-model'",
        'function Get-CutoverOutboxEventID {',
        "event.payload->>'reason'='shard_cutover'",
        'function Wait-ReadModelReceipt {',
        "consumer_name='railway-read-model' AND event_id='`$literal'::uuid",
        'exact_cutover_event_receipted = $true',
        "availability_rotation = 'prior_events_receipted_then_exact_shard_cutover_event_receipted'"
    )) {
        if (-not $runner.Contains($requiredReceiptContract)) {
            throw "runner omitted exact cutover read-model attribution: $requiredReceiptContract"
        }
    }
    foreach ($receiptOrder in @(
        @(
            'Invoke-K6 -Script ''shard-route-prewarm.js'' -Name "prewarm-$api"',
            '$trainAPreCutoverReceiptCount = Wait-TrainRunReadModelCaughtUp -TrainRunID $fixtureTrainA',
            '$availabilityVersionBefore = Get-AvailabilityCacheVersion -TrainRunID $fixtureTrainA',
            'Invoke-Cutover -MigrationID $migrationA -Prefix ''train-a''',
            '$trainACutoverEventID = Get-CutoverOutboxEventID -TrainRunID $fixtureTrainA',
            'Wait-ReadModelReceipt -EventID $trainACutoverEventID',
            '$availabilityVersionAfter = Wait-AvailabilityVersionRotated -TrainRunID $fixtureTrainA'
        ),
        @(
            'Invoke-K6 -Script ''shard-route-prewarm.js'' -Name ''prewarm-train-b''',
            '$trainBPreCutoverReceiptCount = Wait-TrainRunReadModelCaughtUp -TrainRunID $fixtureTrainB',
            '$trainBAvailabilityVersionBefore = Get-AvailabilityCacheVersion -TrainRunID $fixtureTrainB',
            'Invoke-Cutover -MigrationID $migrationB -Prefix ''train-b''',
            '$trainBCutoverEventID = Get-CutoverOutboxEventID -TrainRunID $fixtureTrainB',
            'Wait-ReadModelReceipt -EventID $trainBCutoverEventID',
            '$trainBAvailabilityVersionAfter = Wait-AvailabilityVersionRotated -TrainRunID $fixtureTrainB'
        )
    )) {
        $previousIndex = -1
        foreach ($step in $receiptOrder) {
            $currentIndex = $runner.IndexOf($step, [StringComparison]::Ordinal)
            if ($currentIndex -le $previousIndex) {
                throw "runner cutover receipt barrier is missing or reordered: $step"
            }
            $previousIndex = $currentIndex
        }
    }

    $summaryBuild = $runner.IndexOf('$milestoneSummary = [ordered]@{', [StringComparison]::Ordinal)
    $summaryGate = $runner.IndexOf(
        '$summaryReady = Test-Milestone4CanonicalSummaryReady',
        [StringComparison]::Ordinal
    )
    $summaryCandidateWrite = $runner.IndexOf(
        'Out-File -LiteralPath $summaryCandidatePath -Encoding utf8',
        [StringComparison]::Ordinal
    )
    $summaryFinalScan = $runner.IndexOf(
        'Assert-ArtifactsSanitized',
        $summaryCandidateWrite,
        [StringComparison]::Ordinal
    )
    $summaryPublish = $runner.IndexOf(
        'Move-Item -LiteralPath $summaryCandidatePath -Destination $canonicalSummaryPath',
        [StringComparison]::Ordinal
    )
    $finalPassedStatus = $runner.LastIndexOf(
        "Write-EvidenceStatus -Status 'passed' -Reason 'bounded_evidence_completed'",
        [StringComparison]::Ordinal
    )
    if ($summaryBuild -lt 0 -or $summaryGate -le $summaryBuild -or
        $summaryCandidateWrite -le $summaryGate -or $summaryFinalScan -le $summaryCandidateWrite -or
        $finalPassedStatus -le $summaryFinalScan -or $summaryPublish -le $finalPassedStatus -or
        -not $runner.Contains("`$failureCategory = 'summary_finalization_failed'") -or
        -not $runner.Contains('Remove-Item -Force -LiteralPath $path') -or
        -not $runner.Contains('$canonicalSummaryRemains = Test-Path -LiteralPath $canonicalSummaryPath')) {
        throw 'canonical passed summary is not gated after teardown and sanitization'
    }
    if ($runner.Contains(
        "Out-File -LiteralPath (Join-Path `$EvidenceDirectory 'milestone-4-summary.json')"
    )) {
        throw 'runner retained an early canonical passed-summary write'
    }

    $tokens = $null
    $parseErrors = $null
    [System.Management.Automation.Language.Parser]::ParseFile(
        (Join-Path $PSScriptRoot 'run-milestone-4-multi-replica-evidence.ps1'),
        [ref]$tokens,
        [ref]$parseErrors
    ) | Out-Null
    if ($parseErrors.Count -ne 0) {
        throw 'runner must remain valid PowerShell after evidence-contract changes'
    }

    $crossShardProbe = Get-Content -Raw -LiteralPath (
        Join-Path (Split-Path -Parent $PSScriptRoot) 'loadtest/k6/cross-shard-admin.js'
    )
    foreach ($requiredPartialContract in @(
        "responses[0].status === 503 && publicErrorCode(responses[0]) === 'unavailable'",
        "responses[1].status === 503 && publicErrorCode(responses[1]) === 'unavailable'",
        'if (exactPartial) partialShardResults.add(1)',
        "checks: ['rate==1']"
    )) {
        if (-not $crossShardProbe.Contains($requiredPartialContract)) {
            throw "cross-route customer probe omitted exact partial contract: $requiredPartialContract"
        }
    }
    if ($crossShardProbe.Contains('firstHealthy !== secondHealthy')) {
        throw 'cross-route customer probe accepts arbitrary asymmetric HTTP failures as shard outage evidence'
    }
    $outageProbe = Get-Content -Raw -LiteralPath (
        Join-Path (Split-Path -Parent $PSScriptRoot) 'loadtest/k6/shard-outage-isolation.js'
    )
    if (-not $outageProbe.Contains("checks: ['rate==1']")) {
        throw 'logical shard outage probe does not require every correctness check to pass'
    }

    Write-Output 'Milestone 4 evidence guardrail regression tests passed'
} finally {
    if (Test-Path -LiteralPath $aliasPath) {
        Remove-Item -Force -LiteralPath $aliasPath
    }
    if (Test-Path -LiteralPath $sandbox) {
        Remove-Item -Recurse -Force -LiteralPath $sandbox
    }
}
