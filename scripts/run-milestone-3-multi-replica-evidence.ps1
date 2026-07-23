[CmdletBinding()]
param(
    [string]$ProjectName = '',
    [string]$EvidenceDirectory = '',
    [switch]$KeepEnvironment
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $root 'docker-compose.multi-replica.yml'
$fixtureFile = Join-Path $root 'loadtest/fixtures/milestone-2-multi-replica.sql'
$suffix = [guid]::NewGuid().ToString('N').Substring(0, 10)
if ([string]::IsNullOrWhiteSpace($ProjectName)) {
    $ProjectName = "railway-m3-evidence-$suffix"
}
if ([string]::IsNullOrWhiteSpace($EvidenceDirectory)) {
    $EvidenceDirectory = Join-Path ([System.IO.Path]::GetTempPath()) "railway-m3-evidence-$suffix"
}
$EvidenceDirectory = [System.IO.Path]::GetFullPath($EvidenceDirectory)
New-Item -ItemType Directory -Force -Path $EvidenceDirectory | Out-Null

$compose = @('compose', '-p', $ProjectName, '-f', $composeFile)
$started = $false
$succeeded = $false
$previousClaimIdle = $env:READ_MODEL_CLAIM_MIN_IDLE_SECONDS
$previousWorkerPassTimeout = $env:READ_MODEL_WORKER_PASS_TIMEOUT

function Invoke-Compose {
    param([string[]]$Arguments, [switch]$AllowFailure)
    # Windows PowerShell wraps ordinary native stderr progress as ErrorRecords.
    # Decide native success exclusively from the process exit code.
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = & docker @compose @Arguments 2>&1
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if ($code -ne 0 -and -not $AllowFailure) {
        throw "docker compose failed with exit code $code"
    }
    return @($output)
}

function Invoke-NativeProbe {
    param([scriptblock]$Command)
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $null = & $Command 2>&1
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    return $code
}

function Wait-ServiceHTTP {
    param([string]$Service, [string]$Path = '/readyz', [int]$TimeoutSeconds = 90)
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $apiCode = Invoke-NativeProbe -Command {
            & docker @compose exec -T $Service wget -q -T 2 -O /dev/null "http://127.0.0.1:8080$Path"
        }
        if ($apiCode -eq 0) { return }
        $workerCode = Invoke-NativeProbe -Command {
            & docker @compose exec -T $Service wget -q -T 2 -O /dev/null "http://127.0.0.1:9090$Path"
        }
        if ($workerCode -eq 0) { return }
        Start-Sleep -Seconds 1
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "$Service did not satisfy $Path within ${TimeoutSeconds}s"
}

function Get-RedisValue {
    param([string]$Key)
    $value = (& docker @compose exec -T redis redis-cli --raw GET $Key 2>$null | Out-String).Trim()
    if ($LASTEXITCODE -ne 0) { throw "failed to read exact Redis key $Key" }
    return $value
}

function Wait-VersionChange {
    param([string]$Previous, [int]$TimeoutSeconds = 30)
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $current = Get-RedisValue -Key 'cache:train-search:version'
        if ($current -match '^[A-Za-z0-9_-]{24}$' -and $current -ne $Previous) { return $current }
        Start-Sleep -Seconds 1
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw 'search cache generation did not rotate within the bounded window'
}

function Invoke-K6Read {
    param([string]$BaseURL, [string]$Script, [string]$Name, [string]$ServiceDate, [string]$Duration = '3s')
    $arguments = @(
        'run', '--rm', '--no-deps',
        '-v', "${EvidenceDirectory}:/evidence",
        '-e', "BASE_URL=$BaseURL",
        '-e', 'ORIGIN_CODE=M2A',
        '-e', 'DESTINATION_CODE=M2B',
        '-e', "SERVICE_DATE=$ServiceDate",
        '-e', 'SEAT_CLASS=standard',
        '-e', 'TRAIN_RUN_ID=21000000-0000-4000-8000-000000000401',
        '-e', 'VUS=3',
        '-e', "DURATION=$Duration",
        'k6', 'run', '--summary-export', "/evidence/$Name-summary.json", "/scripts/$Script"
    )
    $output = Invoke-Compose -Arguments $arguments
    $output | Out-File -LiteralPath (Join-Path $EvidenceDirectory "$Name.log") -Encoding utf8
}

function Wait-ReadModelReceipt {
    param([string]$EventID, [int]$TimeoutSeconds = 30)
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $count = (& docker @compose exec -T postgres psql -U railway -d railway -At -v ON_ERROR_STOP=1 `
            -c "SELECT count(*) FROM read_model_event_receipts WHERE consumer_name='railway-read-model' AND event_id='$EventID'" 2>$null | Out-String).Trim()
        if ($LASTEXITCODE -eq 0 -and $count -eq '1') { return }
        Start-Sleep -Milliseconds 500
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw 'read-model receipt did not converge within the bounded window'
}

function Get-APIMetricTotal {
    param([string]$Family, [string]$RequiredLabel)
    $total = 0.0
    foreach ($service in @('api-1', 'api-2', 'api-3')) {
        $metrics = (& docker @compose exec -T $service wget -q -O - http://127.0.0.1:8080/metrics 2>$null | Out-String)
        if ($LASTEXITCODE -ne 0) { throw "failed to collect metrics from $service" }
        foreach ($line in ($metrics -split "`n")) {
            if ($line -match "^$([regex]::Escape($Family))\{(?<labels>[^}]*)\}\s+(?<value>[0-9eE+.-]+)\s*$" -and
                $matches.labels -like "*$RequiredLabel*") {
                $total += [double]::Parse($matches.value, [System.Globalization.CultureInfo]::InvariantCulture)
            }
        }
    }
    return $total
}

try {
    Push-Location $root
    $env:READ_MODEL_CLAIM_MIN_IDLE_SECONDS = '3'
    $env:READ_MODEL_WORKER_PASS_TIMEOUT = '2s'
    & docker version | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Docker Engine is unavailable' }
    & docker compose version | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Docker Compose v2 is unavailable' }

    Write-Host "Starting isolated Milestone 3 project $ProjectName"
    $started = $true
    Invoke-Compose -Arguments @('up', '-d', '--build') |
        Out-File -LiteralPath (Join-Path $EvidenceDirectory 'compose-up.log') -Encoding utf8
    foreach ($service in @('api-1', 'api-2', 'api-3', 'read-model-worker-1', 'read-model-worker-2')) {
        Wait-ServiceHTTP -Service $service
    }

    $fixture = Get-Content -Raw -LiteralPath $fixtureFile
    $fixtureOutput = $fixture | & docker @compose exec -T postgres psql -U railway -d railway -v ON_ERROR_STOP=1 2>&1
    if ($LASTEXITCODE -ne 0) { throw 'synthetic fixture load failed' }
    $fixtureOutput | Out-File -LiteralPath (Join-Path $EvidenceDirectory 'fixture.log') -Encoding utf8

    $rebuildCursor = ''
    $rebuildPage = 0
    do {
        $rebuildPage++
        $rebuildArguments = @('rebuild-all', '--batch-size', '100', '--apply')
        if (-not [string]::IsNullOrWhiteSpace($rebuildCursor)) {
            $rebuildArguments += @('--after', $rebuildCursor)
        }
        $rebuild = & docker @compose exec -T read-model-worker-1 /usr/local/bin/read-model-admin `
            @rebuildArguments 2>&1
        if ($LASTEXITCODE -ne 0) { throw "projection rebuild page $rebuildPage failed" }
        $rebuild | Out-File -LiteralPath (Join-Path $EvidenceDirectory "rebuild-all-$rebuildPage.json") -Encoding utf8
        $rebuildEnvelope = ($rebuild | Select-Object -Last 1 | ConvertFrom-Json)
        if ($null -eq $rebuildEnvelope.result) { throw "projection rebuild page $rebuildPage omitted its result" }
        $rebuildCursor = [string]$rebuildEnvelope.result.NextCursor
        $rebuildHasMore = [bool]$rebuildEnvelope.result.HasMore
        if ($rebuildHasMore -and [string]::IsNullOrWhiteSpace($rebuildCursor)) {
            throw "projection rebuild page $rebuildPage omitted its continuation cursor"
        }
    } while ($rebuildHasMore)

    $projectionReady = (& docker @compose exec -T postgres psql -U railway -d railway -At -v ON_ERROR_STOP=1 `
        -c "SELECT ready::text FROM read_model_projection_state WHERE projection_name='journey_search'" 2>$null | Out-String).Trim()
    if ($LASTEXITCODE -ne 0 -or $projectionReady -ne 'true') {
        throw 'journey-search projection did not become ready after bounded rebuild'
    }

    $serviceDate = (& docker @compose exec -T postgres psql -U railway -d railway -At -v ON_ERROR_STOP=1 `
        -c "SELECT service_date FROM train_runs WHERE id='21000000-0000-4000-8000-000000000401'" 2>$null | Out-String).Trim()
    if ($LASTEXITCODE -ne 0 -or $serviceDate -notmatch '^\d{4}-\d{2}-\d{2}$') {
        throw 'synthetic service date was unavailable'
    }

    # Exact-key reset gives api-1 one cold fill. api-2 must then report a hit
    # from the shared Redis cache; no keyspace enumeration is used.
    & docker @compose exec -T redis redis-cli DEL cache:train-search:version | Out-Null
    Invoke-K6Read -BaseURL 'http://api-1:8080' -Script 'train-search-cold-cache.js' `
        -Name 'api-1-cold-search' -ServiceDate $serviceDate
    $initialVersion = Get-RedisValue -Key 'cache:train-search:version'
    if ($initialVersion -notmatch '^[A-Za-z0-9_-]{24}$') { throw 'initial search generation is invalid' }
    Invoke-K6Read -BaseURL 'http://api-2:8080' -Script 'train-search-warm-cache.js' `
        -Name 'api-2-warm-search' -ServiceDate $serviceDate
    $apiTwoMetrics = (& docker @compose exec -T api-2 wget -q -O - http://127.0.0.1:8080/metrics 2>$null | Out-String)
    if ($LASTEXITCODE -ne 0 -or $apiTwoMetrics -notmatch 'cache_hit_total\{cache_type="train_search"\} [1-9]') {
        throw 'api-2 did not observe the cache entry filled by api-1'
    }

    # A minimized synthetic station event exercises worker-driven projection
    # rebuild and shared generation rotation.
    $eventID = [guid]::NewGuid().ToString()
    & docker @compose exec -T redis redis-cli XADD railway:outbox:v1 '*' `
        event_id $eventID event_type station.updated aggregate_type station `
        aggregate_id 21000000-0000-4000-8000-000000000001 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'failed to append synthetic station event' }
    $rotatedVersion = Wait-VersionChange -Previous $initialVersion
    Invoke-K6Read -BaseURL 'http://load-balancer:8080' -Script 'multi-replica-search-cache.js' `
        -Name 'multi-replica-after-rotation' -ServiceDate $serviceDate -Duration '5s'

    $publishedEndpoint = Invoke-Compose -Arguments @('port', 'load-balancer', '8080') |
        Where-Object { ([string]$_).Trim() -match ':[0-9]+$' } | Select-Object -First 1
    if ([string]::IsNullOrWhiteSpace([string]$publishedEndpoint)) { throw 'load balancer published port unavailable' }
    $loadBalancerPort = (([string]$publishedEndpoint).Trim() -split ':')[-1]
    $upstreams = [System.Collections.Generic.HashSet[string]]::new()
    $probePath = "/api/v1/train-runs/search?origin_station_code=M2A&destination_station_code=M2B&service_date=$serviceDate&seat_class=standard&page=1&limit=100&sort=departure_at"
    for ($probe = 0; $probe -lt 30; $probe++) {
        $response = Invoke-WebRequest -UseBasicParsing -TimeoutSec 5 -Uri "http://127.0.0.1:$loadBalancerPort$probePath"
        if ($response.StatusCode -ne 200) { throw 'load balancer distribution probe failed' }
        $upstream = [string]$response.Headers['X-Upstream-Addr']
        if (-not [string]::IsNullOrWhiteSpace($upstream)) { [void]$upstreams.Add($upstream) }
    }
    if ($upstreams.Count -lt 2) { throw 'load balancer did not prove at least two distinct API upstreams' }
    [ordered]@{ distinct_upstreams = $upstreams.Count; probes = 30 } | ConvertTo-Json -Compress |
        Out-File -LiteralPath (Join-Path $EvidenceDirectory 'upstream-distribution.json') -Encoding utf8

    Invoke-Compose -Arguments @('stop', 'read-model-worker-1', 'read-model-worker-2') | Out-Null
    $eventID = [guid]::NewGuid().ToString()
    $pendingMessageID = (& docker @compose exec -T redis redis-cli --raw XADD railway:outbox:v1 '*' `
        event_id $eventID event_type trainrun.updated aggregate_type train_run `
        aggregate_id 21000000-0000-4000-8000-000000000401 | Out-String).Trim()
    if ($LASTEXITCODE -ne 0 -or $pendingMessageID -notmatch '^\d+-\d+$') { throw 'failed to append worker-failover event' }
    & docker @compose exec -T redis redis-cli --raw XREADGROUP GROUP railway-read-model read-model-multi-1 `
        COUNT 100 STREAMS railway:outbox:v1 '>' | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'failed to create worker-1 pending entry' }
    $pendingEvidence = (& docker @compose exec -T redis redis-cli --raw XPENDING railway:outbox:v1 railway-read-model `
        $pendingMessageID $pendingMessageID 1 | Out-String)
    if ($LASTEXITCODE -ne 0 -or $pendingEvidence -notmatch 'read-model-multi-1') {
        throw 'worker-1 pending ownership was not established'
    }
    Invoke-Compose -Arguments @('start', 'read-model-worker-2') | Out-Null
    Wait-ServiceHTTP -Service 'read-model-worker-2'
    Wait-ReadModelReceipt -EventID $eventID
    $pendingAfterClaim = (& docker @compose exec -T redis redis-cli --raw XPENDING railway:outbox:v1 railway-read-model `
        $pendingMessageID $pendingMessageID 1 | Out-String).Trim()
    if ($LASTEXITCODE -ne 0 -or $pendingAfterClaim -ne '') { throw 'claimed pending entry was not acknowledged' }
    $afterFailover = Wait-VersionChange -Previous $rotatedVersion
    Invoke-Compose -Arguments @('start', 'read-model-worker-1') | Out-Null
    Wait-ServiceHTTP -Service 'read-model-worker-1'

    Invoke-Compose -Arguments @('stop', 'api-1') | Out-Null
    Invoke-K6Read -BaseURL 'http://load-balancer:8080' -Script 'multi-replica-search-cache.js' `
        -Name 'two-api-search' -ServiceDate $serviceDate -Duration '3s'
    Invoke-Compose -Arguments @('start', 'api-1') | Out-Null
    Wait-ServiceHTTP -Service 'api-1'

    $fallbackBefore = Get-APIMetricTotal -Family 'read_model_fallback_total' -RequiredLabel 'reason="redis"'
    $cacheFailureBefore = Get-APIMetricTotal -Family 'cache_failure_total' -RequiredLabel 'reason="redis"'
    Invoke-Compose -Arguments @('stop', 'redis') | Out-Null
    Invoke-K6Read -BaseURL 'http://load-balancer:8080' -Script 'redis-cache-outage.js' `
        -Name 'redis-outage-read-fallback' -ServiceDate $serviceDate -Duration '3s'
    Invoke-Compose -Arguments @('start', 'redis') | Out-Null
    $redisDeadline = [DateTimeOffset]::UtcNow.AddSeconds(60)
    do {
        $redisCode = Invoke-NativeProbe -Command { & docker @compose exec -T redis redis-cli ping }
        if ($redisCode -eq 0) { break }
        Start-Sleep -Seconds 1
    } while ([DateTimeOffset]::UtcNow -lt $redisDeadline)
    if ($redisCode -ne 0) { throw 'Redis did not recover within 60s' }
    foreach ($service in @('read-model-worker-1', 'read-model-worker-2')) { Wait-ServiceHTTP -Service $service }
    $fallbackAfter = Get-APIMetricTotal -Family 'read_model_fallback_total' -RequiredLabel 'reason="redis"'
    $cacheFailureAfter = Get-APIMetricTotal -Family 'cache_failure_total' -RequiredLabel 'reason="redis"'
    if ($fallbackAfter -le $fallbackBefore -or $cacheFailureAfter -le $cacheFailureBefore) {
        throw "Redis outage did not produce bounded fallback and cache-failure metric deltas (fallback $fallbackBefore -> $fallbackAfter, cache failures $cacheFailureBefore -> $cacheFailureAfter)"
    }

    & docker @compose exec -T redis redis-cli DEL cache:train-search:version | Out-Null
    Invoke-K6Read -BaseURL 'http://api-3:8080' -Script 'train-search-cold-cache.js' `
        -Name 'post-redis-recovery-cold-search' -ServiceDate $serviceDate
    $recoveredVersion = Get-RedisValue -Key 'cache:train-search:version'
    if ($recoveredVersion -notmatch '^[A-Za-z0-9_-]{24}$' -or $recoveredVersion -eq $afterFailover) {
        throw 'Redis recovery did not establish a fresh valid search generation'
    }

    foreach ($runID in @(
        '21000000-0000-4000-8000-000000000401',
        '21000000-0000-4000-8000-000000000402'
    )) {
        $reconcile = & docker @compose exec -T read-model-worker-1 /usr/local/bin/read-model-admin `
            reconcile --train-run-id $runID 2>&1
        if ($LASTEXITCODE -ne 0) { throw "read-model reconciliation failed for $runID" }
        $reconcile | Out-File -LiteralPath (Join-Path $EvidenceDirectory "reconcile-$($runID.Substring(35)).json") -Encoding utf8
    }

    [ordered]@{
        status = 'passed'
        topology = [ordered]@{ api_replicas = 3; admission_workers = 2; read_model_workers = 2 }
        cache = [ordered]@{
            cross_replica_warm_hit = $true
            worker_driven_rotation = $true
            redis_recovery_fresh_namespace = $true
        }
        recovery = [ordered]@{
            read_model_worker_failover = $true
            pending_entry_xautoclaim = $true
            api_restart = $true
            redis_read_fallback = $true
        }
        reconciliation = 'passed'
        limitation = 'Bounded local functional evidence; not production capacity or multi-region evidence.'
    } | ConvertTo-Json -Depth 6 | Out-File -LiteralPath (Join-Path $EvidenceDirectory 'evidence-summary.json') -Encoding utf8
    $succeeded = $true
    Write-Host "Milestone 3 multi-replica evidence passed: $EvidenceDirectory"
} finally {
    Pop-Location
    if ($started -and (-not $KeepEnvironment -or -not $succeeded)) {
        try { Invoke-Compose -Arguments @('down', '-v', '--remove-orphans') -AllowFailure | Out-Null } catch { }
    }
    if ($KeepEnvironment -and $started) {
        Write-Host "Cleanup: docker compose -p $ProjectName -f `"$composeFile`" down -v --remove-orphans"
    }
    $env:READ_MODEL_CLAIM_MIN_IDLE_SECONDS = $previousClaimIdle
    $env:READ_MODEL_WORKER_PASS_TIMEOUT = $previousWorkerPassTimeout
}
