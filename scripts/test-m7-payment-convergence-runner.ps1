Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$runner = Join-Path $PSScriptRoot 'run-m7-payment-convergence.ps1'
$source = Get-Content -LiteralPath $runner -Raw
foreach ($required in @(
    "'completed|completed|complete'",
    'runtime-privilege-preflight',
    'payment-reconciliation',
    'reconcileResult.mismatch_count',
    'TestM7PaymentWorkerRunOnceV11Lanes',
    'm7-payment-integration.Dockerfile',
    'integration-probe-image-remove',
    'integration-probe-runtime-user',
    '{{.Config.User}}',
    'test "$(id -u)" -ne 0',
    'go env GOPATH',
    'go env GOMODCACHE',
    'go env GOCACHE',
    "'railway_runtime'",
    'ticket.ticket_code',
    'timeout_enforced',
    'Kill($true)',
    "@('logs',`$container)",
    'diagnostic_unavailable',
    'compose_down_failed',
    'financial_ledger_postings',
    'financial_ledger_reversals',
    'reservation_shard_locators',
    'ticket_order_shard_locators',
    'ticket_code_directory',
    "@('api-1')",
    "services += 'payment-worker-1'"
)) {
    if (-not $source.Contains($required)) { throw "focused runner omitted required contract: $required" }
}
foreach ($forbidden in @('postgres-standby', 'pgbackrest', 'failover-admin', 'settlement-worker')) {
    if ($source.Contains($forbidden)) { throw "focused runner included forbidden topology token: $forbidden" }
}
if ($source.Contains("'completed|completed|done'")) { throw 'focused runner retained the invalid saga step expectation' }
if ($source.Contains('payment_reconciliation_mismatches')) { throw 'focused runner referenced a nonexistent payment mismatch table' }
if ($source.Contains("@('logs','--tail','40'") -or $source.Contains("@('logs','--since'")) { throw 'focused runner retained a lossy worker log window' }
if ($source.Contains("'run','--rm','--no-deps','-e','PAYMENT_PROCESSING_GRACE_SECONDS=1','payment-reconciler'")) {
    throw 'focused runner bypassed the reconciler least-privilege role initializers'
}

$probeDockerfile = Get-Content -LiteralPath (Join-Path $PSScriptRoot '..\deploy\docker\m7-payment-integration.Dockerfile') -Raw
$probeUsers = [regex]::Matches($probeDockerfile, '(?im)^\s*USER\s+([^\s#]+)\s*(?:#.*)?$')
$probeCacheIndex = $probeDockerfile.IndexOf('ENV GOCACHE=/home/probe/.cache/go-build', [System.StringComparison]::Ordinal)
$probeUserIndex = $probeDockerfile.LastIndexOf('USER probe', [System.StringComparison]::Ordinal)
$probeDownloadIndex = $probeDockerfile.IndexOf('RUN go mod download', [System.StringComparison]::Ordinal)
if ($probeUsers.Count -eq 0 -or $probeUsers[$probeUsers.Count-1].Groups[1].Value -cne 'probe' -or
    $probeCacheIndex -lt 0 -or $probeCacheIndex -ge $probeUserIndex -or
    $probeUserIndex -lt 0 -or $probeDownloadIndex -le $probeUserIndex) {
    throw 'focused integration probe image must provision its private cache and keep probe as the final user before module download'
}

'm7-payment-convergence-runner-contract:passed'
