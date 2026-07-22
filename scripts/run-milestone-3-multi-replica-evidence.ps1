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
        '-e', "BASE_URL=$BaseURL",
        '-e', 'ORIGIN_CODE=M2A',
        '-e', 'DESTINATION_CODE=M2B',
        '-e', "SERVICE_DATE=$ServiceDate",
        '-e', 'SEAT_CLASS=standard',
        '-e', 'TRAIN_RUN_ID=21000000-0000-4000-8000-000000000401',
        '-e', 'VUS=3',
        '-e', "DURATION=$Duration",
        'k6', 'run', "/scripts/$Script"
    )
    $output = Invoke-Compose -Arguments $arguments
    $output | Out-File -LiteralPath (Join-Path $EvidenceDirectory "$Name.log") -Encoding utf8
}

try {
    Push-Location $root
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

    foreach ($runID in @(
        '21000000-0000-4000-8000-000000000401',
        '21000000-0000-4000-8000-000000000402'
    )) {
        $rebuild = & docker @compose exec -T read-model-worker-1 /usr/local/bin/read-model-admin `
            rebuild-train-run --train-run-id $runID --apply 2>&1
        if ($LASTEXITCODE -ne 0) { throw "projection rebuild failed for $runID" }
        $rebuild | Out-File -LiteralPath (Join-Path $EvidenceDirectory "rebuild-$($runID.Substring(35)).json") -Encoding utf8
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

    Invoke-Compose -Arguments @('stop', 'read-model-worker-1') | Out-Null
    $eventID = [guid]::NewGuid().ToString()
    & docker @compose exec -T redis redis-cli XADD railway:outbox:v1 '*' `
        event_id $eventID event_type trainrun.updated aggregate_type train_run `
        aggregate_id 21000000-0000-4000-8000-000000000401 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'failed to append worker-failover event' }
    $afterFailover = Wait-VersionChange -Previous $rotatedVersion
    Invoke-Compose -Arguments @('start', 'read-model-worker-1') | Out-Null
    Wait-ServiceHTTP -Service 'read-model-worker-1'

    Invoke-Compose -Arguments @('stop', 'api-1') | Out-Null
    Invoke-K6Read -BaseURL 'http://load-balancer:8080' -Script 'multi-replica-search-cache.js' `
        -Name 'two-api-search' -ServiceDate $serviceDate -Duration '3s'
    Invoke-Compose -Arguments @('start', 'api-1') | Out-Null
    Wait-ServiceHTTP -Service 'api-1'

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
}
