[CmdletBinding()]
param(
    [string]$EvidenceDirectory = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$root = [System.IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$runnerPath = Join-Path $PSScriptRoot 'run-milestone-7-dr-evidence.ps1'
$composePath = Join-Path $root 'docker-compose.dr.yml'
$replicationPath = Join-Path $root 'deploy/postgres/dr/10-replication.sh'
$standbyPath = Join-Path $root 'deploy/postgres/dr/start-standby.sh'
$workflowPath = Join-Path $root '.github/workflows/milestone-7-dr.yml'
$dockerfilePath = Join-Path $root 'Dockerfile'
foreach ($required in @($runnerPath, $composePath, $replicationPath, $standbyPath, $workflowPath)) {
    if (-not (Test-Path -LiteralPath $required)) { throw "Milestone 7 DR artifact is missing: $(Split-Path -Leaf $required)" }
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

$source = Get-Content -Raw -LiteralPath $runnerPath
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
    'source_marker=2', 'source_marker=3', 'target_marker_count=$targetMarkerCount',
    'maximum_missing_markers_per_database=1', 'maximum_missing_wal_bytes_per_database=536870912'
)
foreach ($guardrail in $requiredGuardrails) {
    if (-not $source.Contains($guardrail)) { throw "Milestone 7 DR runner omits guardrail: $guardrail" }
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
    publication_evidence_verified = (-not [string]::IsNullOrWhiteSpace($EvidenceDirectory))
} | ConvertTo-Json -Compress
