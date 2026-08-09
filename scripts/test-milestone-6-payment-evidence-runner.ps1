[CmdletBinding()]
param(
    [string]$EvidenceDirectory = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$root = [System.IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$runnerPath = Join-Path $PSScriptRoot 'run-milestone-6-payment-evidence.ps1'
$ciRunnerPath = Join-Path $PSScriptRoot 'run-milestone-6-payment-ci-evidence.ps1'
if (-not (Test-Path -LiteralPath $runnerPath)) { throw 'Milestone 6 evidence runner is missing' }
if (-not (Test-Path -LiteralPath $ciRunnerPath)) { throw 'Milestone 6 CI evidence runner is missing' }

$tokens = $null
$parseErrors = $null
[System.Management.Automation.Language.Parser]::ParseFile(
    $runnerPath, [ref]$tokens, [ref]$parseErrors
) | Out-Null
if ($parseErrors.Count -ne 0) {
    throw "Milestone 6 evidence runner has $($parseErrors.Count) PowerShell parse errors"
}

$source = Get-Content -Raw -LiteralPath $runnerPath
$requiredScripts = @(
    'payment-intent-create.js',
    'payment-idempotency.js',
    'payment-webhook-burst.js',
    'payment-capture-recovery.js',
    'ticket-issuance.js',
    'payment-refund.js',
    'payment-provider-outage.js',
    'payment-shard-outage.js',
    'payment-during-migration.js',
    'multi-replica-payment.js'
)
foreach ($script in $requiredScripts) {
    if (-not $source.Contains("'$script'")) { throw "evidence runner omits $script" }
    if (-not (Test-Path -LiteralPath (Join-Path $root "loadtest/k6/$script"))) { throw "k6 script $script is missing" }
}

$requiredGuardrails = @(
    "ProjectName already owns Docker resources",
    "docker @composeArguments down -v --remove-orphans",
    "label=com.docker.compose.project=",
    "Assert-M6FinalInvariants",
    "final-reconciler-control-role-violation-count.log",
    "public.users','SELECT",
    "Assert-M6EvidenceIsSecretSafe",
    "payment-reconciler','--once'",
    "PAYMENT_PROCESSING_GRACE_SECONDS=1",
    "rows_examined -lt 1",
    "shard_rows_found -lt 1",
    "issued_orders -lt 1",
    "reconciliationResult.truncated",
    "clean non-empty pass",
    "booking-shard-1-postgres",
    "Wait-Job -Job `$migrationJob -Timeout 180",
    "`$attempt -le 5",
    "Docker evidence user identity is malformed",
    "@('--user'",
    "--summary-export",
    "checks.fails",
    "-pool-metrics.prom",
    "-payment-metrics.prom",
    "Docker host capacity evidence is malformed",
    "source_state_sha256",
    "Get-M6SourceState",
    "prebuilt-image-digests",
    "--no-build",
    "EvidenceDirectory must be outside the source repository",
    "source state changed during the evidence run"
    "source_digest_exclusions"
    "ending_source_state_sha256"
    "source_state_verified"
    "completedAt.Subtract(`$start)"
    "rendered_compose_config_sha256"
    "rendered Compose config is empty"
    "final-reconciliation-control-candidates.log"
    'final-reconciliation-$service-snapshots.log'
    'final-reconciliation-$service-role-snapshots.log'
)
foreach ($guardrail in $requiredGuardrails) {
    if (-not $source.Contains($guardrail)) { throw "evidence runner omits guardrail: $guardrail" }
}

if ($source -match '(?im)^\s*(docker\s+(system\s+prune|volume\s+prune)|Remove-Item\s+.+-Recurse)') {
    throw 'evidence runner contains an unscoped destructive cleanup'
}
if ($source -match '(?im)^\s*\$(token|password|secret)\s*\|\s*(Set-Content|Out-File)') {
    throw 'evidence runner appears to persist a secret-bearing value'
}

$docs = @(
    (Join-Path $root 'docs/milestone-6-load-testing.md')
    (Join-Path $root 'docs/benchmark-report-milestone-6.md')
)

function Get-M6TestHash {
    param([Parameter(Mandatory=$true)][string]$Text)
    $sha256 = [System.Security.Cryptography.SHA256]::Create()
    try { $bytes = $sha256.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($Text)) } finally { $sha256.Dispose() }
    return (($bytes | ForEach-Object { $_.ToString('x2') }) -join '')
}

function Get-M6TestSourceState {
    param([string[]]$Exclusions)
    $paths = @(& git -C $root ls-files --cached --others --exclude-standard)
    if ($LASTEXITCODE -ne 0 -or $paths.Count -eq 0) { throw 'publication source-state inventory failed' }
    $entries = [System.Collections.Generic.List[string]]::new()
    foreach ($relative in @($paths | Sort-Object -Unique)) {
        $normalized = ([string]$relative).Replace('\','/')
        if ($Exclusions -contains $normalized) { continue }
        $full = Join-Path $root ([string]$relative)
        if (-not [System.IO.File]::Exists($full)) { $entries.Add("$normalized|missing"); continue }
        $file = [System.IO.FileInfo]::new($full)
        $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $full).Hash.ToLowerInvariant()
        $entries.Add("$normalized|$($file.Length)|$hash")
    }
    [pscustomobject]@{ FileCount=$entries.Count; SHA256=(Get-M6TestHash -Text ($entries -join "`n")) }
}

if (-not [string]::IsNullOrWhiteSpace($EvidenceDirectory)) {
    $EvidenceDirectory = [System.IO.Path]::GetFullPath($EvidenceDirectory)
    $indexPath = Join-Path $EvidenceDirectory 'evidence-index.json'
    $manifestPath = Join-Path $EvidenceDirectory 'run-manifest.json'
    $summaryPath = Join-Path $EvidenceDirectory 'milestone-6-evidence-summary.json'
    foreach ($required in @($indexPath,$manifestPath,$summaryPath)) {
        if (-not (Test-Path -LiteralPath $required)) { throw "publication evidence is missing $(Split-Path -Leaf $required)" }
    }
    $index = Get-Content -Raw -LiteralPath $indexPath | ConvertFrom-Json
    $manifest = Get-Content -Raw -LiteralPath $manifestPath | ConvertFrom-Json
    $summary = Get-Content -Raw -LiteralPath $summaryPath | ConvertFrom-Json
    if ([string]$index.status -ne 'complete' -or [string]$manifest.status -ne 'passed' -or [string]$summary.status -ne 'passed') {
        throw 'publication evidence is not terminal and passed'
    }
    $expectedExclusions = @('docs/benchmark-report-milestone-6.md','docs/milestone-6-load-testing.md')
    if ((@($manifest.source_digest_exclusions) -join '|') -ne ($expectedExclusions -join '|') -or
        (@($summary.source_digest_exclusions) -join '|') -ne ($expectedExclusions -join '|')) {
        throw 'publication evidence source-digest exclusions are not exact'
    }
    if (-not [bool]$manifest.source_state_verified -or
        [string]$manifest.source_state_sha256 -ne [string]$manifest.ending_source_state_sha256 -or
        [int]$manifest.source_file_count -ne [int]$manifest.ending_source_file_count -or
        [string]$manifest.source_state_sha256 -ne [string]$summary.source_state_sha256 -or
        [int]$manifest.source_file_count -ne [int]$summary.source_file_count -or
        [string]$manifest.rendered_compose_config_sha256 -ne [string]$summary.rendered_compose_config_sha256) {
        throw 'publication evidence source/config identity is inconsistent'
    }
    $currentSource = Get-M6TestSourceState -Exclusions $expectedExclusions
    if ($currentSource.SHA256 -ne [string]$manifest.source_state_sha256 -or
        $currentSource.FileCount -ne [int]$manifest.source_file_count) {
        throw 'publication evidence does not bind the current scoped source tree'
    }
    $actualFiles = @(Get-ChildItem -LiteralPath $EvidenceDirectory -Recurse -File | Where-Object { $_.Name -ne 'evidence-index.json' })
    if ([int]$index.file_count -ne @($index.files).Count -or $actualFiles.Count -ne @($index.files).Count) {
        throw 'publication evidence index file count is incomplete'
    }
    $canonicalEntries = [System.Collections.Generic.List[string]]::new()
    foreach ($entry in @($index.files)) {
        $relative = [string]$entry.path
        $full = Join-Path $EvidenceDirectory ($relative.Replace('/',[System.IO.Path]::DirectorySeparatorChar))
        if (-not (Test-Path -LiteralPath $full)) { throw "publication evidence index entry is missing: $relative" }
        $file = [System.IO.FileInfo]::new($full)
        $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $full).Hash.ToLowerInvariant()
        if ($file.Length -ne [int64]$entry.bytes -or $hash -ne [string]$entry.sha256) {
            throw "publication evidence index entry is corrupt: $relative"
        }
        $canonicalEntries.Add("$relative|$($file.Length)|$hash")
    }
    $bundleHash = Get-M6TestHash -Text ($canonicalEntries -join "`n")
    if ($bundleHash -ne [string]$index.bundle_sha256) { throw 'publication evidence bundle digest is invalid' }
    $bundleName = Split-Path -Leaf $EvidenceDirectory
    $reconciliation = $summary.final_reconciliation
    if ([int]$reconciliation.rows_examined -lt 1 -or [int]$reconciliation.shard_rows_found -lt 1 -or
        [int]$reconciliation.issued_orders -lt 1 -or [int]$reconciliation.mismatch_count -ne 0 -or
        [int]$reconciliation.manual_reviews -ne 0 -or [bool]$reconciliation.truncated) {
        throw 'publication evidence reconciliation is not clean and non-empty'
    }
    $publicationTokens = @(
        $bundleName,
        $bundleHash,
        [string]$manifest.source_state_sha256,
        [string]$manifest.rendered_compose_config_sha256,
        "rows_examined=$([int]$reconciliation.rows_examined)",
        "shard_rows_found=$([int]$reconciliation.shard_rows_found)",
        "issued_orders=$([int]$reconciliation.issued_orders)",
        "mismatch_count=$([int]$reconciliation.mismatch_count)",
        "manual_reviews=$([int]$reconciliation.manual_reviews)",
        "truncated=$(([bool]$reconciliation.truncated).ToString().ToLowerInvariant())"
    )
    foreach ($document in $docs) {
        $content = Get-Content -Raw -LiteralPath $document
        foreach ($token in $publicationTokens) {
            if (-not $content.Contains($token)) { throw "$(Split-Path -Leaf $document) omits publication token: $token" }
        }
    }
}

[ordered]@{
    status = 'passed'
    required_scenarios = $requiredScripts.Count
    scoped_teardown = $true
    secret_scan = $true
    docs_publish_status = 'passed'
    publication_evidence_verified = (-not [string]::IsNullOrWhiteSpace($EvidenceDirectory))
} | ConvertTo-Json -Compress
