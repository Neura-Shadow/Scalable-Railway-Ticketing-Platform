[CmdletBinding()]
param(
    [ValidateRange(10, 50)]
    [int]$CustomerCount = 30,

    [ValidatePattern('^[1-9][0-9]*(s|m)$')]
    [string]$SteadyStateDuration = '30s',

    [string]$ProjectName,

    [string]$EvidenceDirectory,

    [switch]$KeepEnvironment
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
Add-Type -AssemblyName System.Net.Http

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repositoryRoot 'docker-compose.multi-replica.yml'
$fixtureFile = Join-Path $repositoryRoot 'loadtest/fixtures/milestone-2-multi-replica.sql'
$runSuffix = ([Guid]::NewGuid().ToString('N')).Substring(0, 12)
if ([string]::IsNullOrWhiteSpace($ProjectName)) {
    $ProjectName = "railway-m2-evidence-$runSuffix"
}
if ([string]::IsNullOrWhiteSpace($EvidenceDirectory)) {
    $EvidenceDirectory = Join-Path ([System.IO.Path]::GetTempPath()) "railway-m2-evidence-$runSuffix"
}
$EvidenceDirectory = [System.IO.Path]::GetFullPath($EvidenceDirectory)
$directorySeparators = [char[]]@(
    [System.IO.Path]::DirectorySeparatorChar,
    [System.IO.Path]::AltDirectorySeparatorChar
)
$repositoryPath = [System.IO.Path]::GetFullPath($repositoryRoot).TrimEnd($directorySeparators)
$repositoryPrefix = $repositoryPath + [System.IO.Path]::DirectorySeparatorChar
$pathComparison = if ([System.IO.Path]::DirectorySeparatorChar -eq '\') {
    [StringComparison]::OrdinalIgnoreCase
} else {
    [StringComparison]::Ordinal
}
if (
    $EvidenceDirectory.Equals($repositoryPath, $pathComparison) -or
    $EvidenceDirectory.StartsWith($repositoryPrefix, $pathComparison)
) {
    throw 'EvidenceDirectory must be outside the repository'
}
New-Item -ItemType Directory -Path $EvidenceDirectory -Force | Out-Null

$hotTrainRunID = '21000000-0000-4000-8000-000000000401'
$nonHotTrainRunID = '21000000-0000-4000-8000-000000000402'
$originCode = 'M2A'
$destinationCode = 'M2B'
$seatClass = 'standard'
$configuredAdmissionRate = 5
$configuredInflightLimit = 5
$baseURL = ''
$useDockerBridgeTransport = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
    [System.Runtime.InteropServices.OSPlatform]::Linux
)
$orchestrationTransport = if ($useDockerBridgeTransport) {
    'docker_bridge'
} else {
    'published_loopback'
}
$composeArguments = @('compose', '-p', $ProjectName, '-f', $composeFile)
$environmentNames = @(
    'BASE_URL',
    'CUSTOMER_TOKEN',
    'CUSTOMER_TOKENS',
    'ENTRY_IDS',
    'TRAIN_RUN_ID',
    'HOT_TRAIN_RUN_ID',
    'NON_HOT_TRAIN_RUN_ID',
    'ORIGIN_CODE',
    'DESTINATION_CODE',
    'SEAT_CLASS',
    'PASSENGER_COUNT',
    'NON_HOT_PASSENGER_IDS',
    'IDEMPOTENCY_KEY_PREFIX',
    'CONFIRM_REDIS_IS_DOWN',
    'VUS',
    'ITERATIONS',
    'DURATION',
    'MAX_DURATION',
    'GRACEFUL_STOP'
)
$savedEnvironment = @{}
foreach ($name in $environmentNames) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

function Invoke-DockerCompose {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments,

        [string]$CapturePath
    )

    # Windows PowerShell surfaces ordinary native stderr (for example Compose
    # pull progress) as ErrorRecord objects. Keep strict mode for the harness,
    # but decide native success from the process exit code in every host.
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = & docker @script:composeArguments @Arguments 2>&1
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if (-not [string]::IsNullOrWhiteSpace($CapturePath)) {
        $output | Out-File -FilePath $CapturePath -Encoding utf8
    }
    if ($exitCode -ne 0) {
        throw "docker compose command failed with exit code $exitCode"
    }
    return $output
}

function Invoke-NativeProbe {
    param(
        [Parameter(Mandatory = $true)]
        [scriptblock]$Command
    )

    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = & $Command 2>&1
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    return [pscustomobject]@{
        Output   = @($output)
        ExitCode = $exitCode
    }
}

function Remove-K6SetupData {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        return
    }
    $summary = Get-Content -Raw -LiteralPath $Path | ConvertFrom-Json
    if ($summary.PSObject.Properties.Match('setup_data').Count -gt 0) {
        $summary.PSObject.Properties.Remove('setup_data')
    }
    $json = $summary | ConvertTo-Json -Compress -Depth 100
    [System.IO.File]::WriteAllText(
        $Path,
        $json,
        [System.Text.UTF8Encoding]::new($false)
    )
}

function Initialize-K6SummaryFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

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
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if ($exitCode -ne 0) {
        throw 'k6 summary permission initialization failed'
    }
}

function Invoke-API {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Method,

        [Parameter(Mandatory = $true)]
        [string]$Path,

        [string]$AccessToken,

        [hashtable]$Headers,

        [object]$Body,

        [string]$BodyJSON,

        [string]$RequestBaseURL = $script:baseURL
    )

    $requestHeaders = @{ Accept = 'application/json' }
    if ($null -ne $Headers) {
        foreach ($key in $Headers.Keys) {
            $requestHeaders[$key] = $Headers[$key]
        }
    }
    if (-not [string]::IsNullOrWhiteSpace($AccessToken)) {
        $requestHeaders.Authorization = "Bearer $AccessToken"
    }

    $parameters = @{
        Uri              = "$($RequestBaseURL.TrimEnd('/'))$Path"
        Method           = $Method
        Headers          = $requestHeaders
        UseBasicParsing  = $true
        TimeoutSec       = 15
        DisableKeepAlive = $true
    }
    if (-not [string]::IsNullOrWhiteSpace($BodyJSON)) {
        $parameters.Body = $BodyJSON
        $parameters.ContentType = 'application/json'
    } elseif ($null -ne $Body) {
        $parameters.Body = $Body | ConvertTo-Json -Compress -Depth 8
        $parameters.ContentType = 'application/json'
    }
    return Invoke-WebRequest @parameters
}

function Get-PublishedServiceBaseURL {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Service
    )

    $probe = Invoke-NativeProbe -Command {
        & docker @script:composeArguments port $Service 8080
    }
    if ($probe.ExitCode -ne 0) {
        throw "could not resolve the ephemeral loopback port for $Service"
    }
    $published = $probe.Output
    $endpoint = @($published | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) }) |
        Select-Object -First 1
    if ([string]::IsNullOrWhiteSpace([string]$endpoint)) {
        throw "$Service did not publish its HTTP endpoint"
    }
    $match = [regex]::Match(([string]$endpoint).Trim(), ':(?<port>[0-9]+)$')
    if (-not $match.Success) {
        throw "could not parse the published HTTP endpoint for $Service"
    }
    return "http://127.0.0.1:$($match.Groups['port'].Value)"
}

function Get-DockerBridgeServiceBaseURL {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Service
    )

    $containerProbe = Invoke-NativeProbe -Command {
        & docker @script:composeArguments ps --status running -q $Service
    }
    if ($containerProbe.ExitCode -ne 0) {
        throw "could not resolve the running container for $Service"
    }
    $containerID = @(
        $containerProbe.Output |
            ForEach-Object { ([string]$_).Trim() } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    ) | Select-Object -First 1
    if ([string]::IsNullOrWhiteSpace([string]$containerID)) {
        throw "$Service does not have a running container"
    }

    $addressProbe = Invoke-NativeProbe -Command {
        & docker inspect --format `
            '{{range .NetworkSettings.Networks}}{{println .IPAddress}}{{end}}' `
            $containerID
    }
    if ($addressProbe.ExitCode -ne 0) {
        throw "could not inspect the Docker bridge address for $Service"
    }
    $bridgeAddresses = @(
        foreach ($line in $addressProbe.Output) {
            $candidate = ([string]$line).Trim()
            $parsedAddress = $null
            if (
                -not [string]::IsNullOrWhiteSpace($candidate) -and
                [System.Net.IPAddress]::TryParse($candidate, [ref]$parsedAddress) -and
                $parsedAddress.AddressFamily -eq [System.Net.Sockets.AddressFamily]::InterNetwork
            ) {
                $candidate
            }
        }
    )
    if ($bridgeAddresses.Count -ne 1) {
        throw "$Service must have exactly one IPv4 Docker bridge address"
    }
    return "http://$($bridgeAddresses[0]):8080"
}

function Get-ServiceBaseURL {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Service
    )

    if ($script:useDockerBridgeTransport) {
        return Get-DockerBridgeServiceBaseURL -Service $Service
    }
    return Get-PublishedServiceBaseURL -Service $Service
}

function Invoke-PostgresScalar {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Query
    )

    $output = & docker @script:composeArguments exec -T postgres `
        psql -U railway -d railway -v ON_ERROR_STOP=1 -Atc $Query 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw 'PostgreSQL evidence query failed'
    }
    return ([string]($output | Select-Object -Last 1)).Trim()
}

function Convert-ResponseBody {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Response
    )
    return $Response.Content | ConvertFrom-Json
}

function Add-Upstream {
    param(
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [System.Collections.Generic.HashSet[string]]$Set,

        [Parameter(Mandatory = $true)]
        [object]$Response
    )

    $value = Get-Upstream -Response $Response
    if (-not [string]::IsNullOrWhiteSpace($value)) {
        [void]$Set.Add($value)
    }
}

function Get-Upstream {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Response
    )

    $value = [string]$Response.Headers['X-Upstream-Addr']
    return Get-FinalUpstreamAddress -Value $value
}

function Get-FinalUpstreamAddress {
    param(
        [AllowEmptyString()]
        [string]$Value
    )

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return ''
    }
    $addresses = @(
        $Value.Split(',') |
            ForEach-Object { $_.Trim() } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )
    if ($addresses.Count -eq 0) {
        return ''
    }
    return [string]$addresses[-1]
}

function Wait-APIReady {
    param([int]$Attempts = 120)

    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        try {
            $response = Invoke-API -Method GET -Path '/readyz'
            if ($response.StatusCode -eq 200) {
                return
            }
        } catch {
            # The bounded retry is expected while Compose health checks settle.
        }
        Start-Sleep -Seconds 1
    }
    throw 'load balancer did not become ready within the bounded startup window'
}

function Wait-APIServiceReady {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Service,

        [int]$Attempts = 30
    )

    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $probe = Invoke-NativeProbe -Command {
            & docker @script:composeArguments exec -T $Service `
                wget -q -T 2 -O /dev/null http://127.0.0.1:8080/readyz
        }
        if ($probe.ExitCode -eq 0) {
            return
        }
        Start-Sleep -Seconds 1
    }
    throw "$Service did not remain ready during the bounded API termination probe"
}

function Wait-WorkerReady {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Service,

        [int]$Attempts = 60
    )

    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $probe = Invoke-NativeProbe -Command {
            & docker @script:composeArguments exec -T $Service `
                wget -q -T 2 -O /dev/null http://127.0.0.1:9090/readyz
        }
        if ($probe.ExitCode -eq 0) {
            return
        }
        Start-Sleep -Seconds 1
    }
    throw "$Service did not become ready within the bounded startup window"
}

function Wait-RedisReady {
    param([int]$Attempts = 60)

    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $probe = Invoke-NativeProbe -Command {
            & docker @script:composeArguments exec -T redis redis-cli ping
        }
        $response = @($probe.Output | Where-Object {
                -not [string]::IsNullOrWhiteSpace([string]$_)
            }) | Select-Object -Last 1
        if ($probe.ExitCode -eq 0 -and ([string]$response).Trim() -eq 'PONG') {
            return
        }
        Start-Sleep -Seconds 1
    }
    throw 'Redis did not become ready within the bounded recovery window'
}

function Wait-PolicyInitialized {
    param([int]$Attempts = 60)

    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $probe = Invoke-NativeProbe -Command {
            & docker @script:composeArguments exec -T postgres `
                psql -U railway -d railway -Atc `
                "SELECT coalesce(redis_initialized_version,0) FROM hot_train_policies WHERE id='21000000-0000-4000-8000-000000000500';"
        }
        $value = @($probe.Output | Where-Object {
                -not [string]::IsNullOrWhiteSpace([string]$_)
            }) | Select-Object -Last 1
        if ($probe.ExitCode -eq 0 -and ([string]$value).Trim() -eq '1') {
            return
        }
        Start-Sleep -Milliseconds 250
    }
    throw 'admission workers did not initialize the synthetic policy generation'
}

function Save-OperationalSnapshot {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Label
    )

    Invoke-DockerCompose -Arguments @('ps', '--format', 'json') `
        -CapturePath (Join-Path $script:EvidenceDirectory "$Label-compose-ps.json") | Out-Null
    foreach ($worker in @('admission-worker-1', 'admission-worker-2')) {
        Invoke-DockerCompose -Arguments @(
            'exec', '-T', $worker,
            'wget', '-q', '-T', '3', '-O', '-', 'http://127.0.0.1:9090/metrics'
        ) -CapturePath (Join-Path $script:EvidenceDirectory "$Label-$worker-metrics.prom") | Out-Null
    }
    foreach ($api in @('api-1', 'api-2', 'api-3')) {
        Invoke-DockerCompose -Arguments @(
            'exec', '-T', $api,
            'wget', '-q', '-T', '3', '-O', '-', 'http://127.0.0.1:8080/metrics'
        ) -CapturePath (Join-Path $script:EvidenceDirectory "$Label-$api-metrics.prom") | Out-Null
    }
    Invoke-DockerCompose -Arguments @(
        'exec', '-T', 'redis', 'redis-cli', 'INFO', 'persistence'
    ) -CapturePath (Join-Path $script:EvidenceDirectory "$Label-redis-persistence.txt") | Out-Null
    Invoke-DockerCompose -Arguments @(
        'exec', '-T', 'redis', 'redis-cli', 'INFO', 'stats'
    ) -CapturePath (Join-Path $script:EvidenceDirectory "$Label-redis-stats.txt") | Out-Null
    Invoke-DockerCompose -Arguments @(
        'exec', '-T', 'redis', 'redis-cli', 'INFO', 'commandstats'
    ) -CapturePath (Join-Path $script:EvidenceDirectory "$Label-redis-commandstats.txt") | Out-Null
    Invoke-DockerCompose -Arguments @(
        'exec', '-T', 'postgres', 'psql', '-U', 'railway', '-d', 'railway',
        '-Atc',
        "SELECT json_build_object('connections',numbackends,'commits',xact_commit,'rollbacks',xact_rollback,'deadlocks',deadlocks,'lock_waiters',(SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock'))::text FROM pg_stat_database WHERE datname=current_database(); SELECT json_build_object('reservations',(SELECT count(*) FROM reservations),'outbox_pending',(SELECT count(*) FROM outbox_events WHERE status='pending'),'outbox_processing',(SELECT count(*) FROM outbox_events WHERE status='processing'),'outbox_dead_letter',(SELECT count(*) FROM outbox_events WHERE status='dead_letter'))::text;"
    ) -CapturePath (Join-Path $script:EvidenceDirectory "$Label-postgres.jsonl") | Out-Null
}

function Invoke-K6SteadyState {
    $env:BASE_URL = 'http://load-balancer:8080'
    $env:CUSTOMER_TOKENS = ($script:customers | ForEach-Object { $_.AccessToken }) -join ','
    $env:TRAIN_RUN_ID = $script:hotTrainRunID
    $env:ORIGIN_CODE = $script:originCode
    $env:DESTINATION_CODE = $script:destinationCode
    $env:SEAT_CLASS = $script:seatClass
    $env:PASSENGER_COUNT = '1'
    $env:VUS = [string]$script:CustomerCount
    $env:DURATION = $script:SteadyStateDuration
    $env:GRACEFUL_STOP = '10s'

    $summaryMount = "${EvidenceDirectory}:/evidence"
    $summaryPath = Join-Path $script:EvidenceDirectory 'multi-replica-summary.json'
    Initialize-K6SummaryFile -Path $summaryPath
    try {
        Invoke-DockerCompose -Arguments @(
            'run', '--rm', '-T', '--no-deps',
            '-v', $summaryMount,
            '-e', 'BASE_URL',
            '-e', 'CUSTOMER_TOKENS',
            '-e', 'TRAIN_RUN_ID',
            '-e', 'ORIGIN_CODE',
            '-e', 'DESTINATION_CODE',
            '-e', 'SEAT_CLASS',
            '-e', 'PASSENGER_COUNT',
            '-e', 'VUS',
            '-e', 'DURATION',
            '-e', 'GRACEFUL_STOP',
            'k6',
            'run',
            '--summary-export', '/evidence/multi-replica-summary.json',
            '/scripts/multi-replica-hot-train.js'
        ) -CapturePath (Join-Path $script:EvidenceDirectory 'multi-replica-k6.log') | Out-Null
    }
    finally {
        Remove-K6SetupData -Path $summaryPath
    }
}

function Invoke-K6StatusSteadyState {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Customer
    )

    $env:BASE_URL = 'http://load-balancer:8080'
    $env:CUSTOMER_TOKEN = $Customer.AccessToken
    $env:ENTRY_IDS = $Customer.EntryID
    $env:VUS = '10'
    $env:DURATION = '15s'

    $summaryMount = "${EvidenceDirectory}:/evidence"
    $summaryPath = Join-Path $script:EvidenceDirectory 'waiting-room-status-summary.json'
    Initialize-K6SummaryFile -Path $summaryPath
    try {
        Invoke-DockerCompose -Arguments @(
            'run', '--rm', '-T', '--no-deps',
            '-v', $summaryMount,
            '-e', 'BASE_URL',
            '-e', 'CUSTOMER_TOKEN',
            '-e', 'ENTRY_IDS',
            '-e', 'VUS',
            '-e', 'DURATION',
            'k6',
            'run',
            '--summary-export', '/evidence/waiting-room-status-summary.json',
            '/scripts/waiting-room-status.js'
        ) -CapturePath (Join-Path $script:EvidenceDirectory 'waiting-room-status-k6.log') | Out-Null
    }
    finally {
        Remove-K6SetupData -Path $summaryPath
    }
}

function Invoke-K6RedisOutage {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Customer
    )

    $env:BASE_URL = 'http://load-balancer:8080'
    $env:CUSTOMER_TOKEN = $Customer.AccessToken
    $env:HOT_TRAIN_RUN_ID = $script:hotTrainRunID
    $env:NON_HOT_TRAIN_RUN_ID = $script:nonHotTrainRunID
    $env:ORIGIN_CODE = $script:originCode
    $env:DESTINATION_CODE = $script:destinationCode
    $env:SEAT_CLASS = $script:seatClass
    $env:NON_HOT_PASSENGER_IDS = $Customer.PassengerID
    $env:IDEMPOTENCY_KEY_PREFIX = "m2-outage-$script:runSuffix"
    $env:CONFIRM_REDIS_IS_DOWN = 'yes'
    $env:VUS = '1'
    $env:ITERATIONS = '1'
    $env:MAX_DURATION = '30s'

    $summaryMount = "${EvidenceDirectory}:/evidence"
    $summaryPath = Join-Path $script:EvidenceDirectory 'redis-outage-summary.json'
    Initialize-K6SummaryFile -Path $summaryPath
    try {
        Invoke-DockerCompose -Arguments @(
            'run', '--rm', '-T', '--no-deps',
            '-v', $summaryMount,
            '-e', 'BASE_URL',
            '-e', 'CUSTOMER_TOKEN',
            '-e', 'HOT_TRAIN_RUN_ID',
            '-e', 'NON_HOT_TRAIN_RUN_ID',
            '-e', 'ORIGIN_CODE',
            '-e', 'DESTINATION_CODE',
            '-e', 'SEAT_CLASS',
            '-e', 'NON_HOT_PASSENGER_IDS',
            '-e', 'IDEMPOTENCY_KEY_PREFIX',
            '-e', 'CONFIRM_REDIS_IS_DOWN',
            '-e', 'VUS',
            '-e', 'ITERATIONS',
            '-e', 'MAX_DURATION',
            'k6',
            'run',
            '--summary-export', '/evidence/redis-outage-summary.json',
            '/scripts/redis-outage.js'
        ) -CapturePath (Join-Path $script:EvidenceDirectory 'redis-outage-k6.log') | Out-Null
    }
    finally {
        Remove-K6SetupData -Path $summaryPath
    }
}

function Submit-Reservation {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Customer,

        [Parameter(Mandatory = $true)]
        [string]$AdmissionToken,

        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [System.Collections.Generic.HashSet[string]]$Upstreams,

        [string]$IdempotencyKey,

        [string]$RequestBaseURL = $script:baseURL,

        [string]$BodyJSON
    )

    if ([string]::IsNullOrWhiteSpace($IdempotencyKey)) {
        $IdempotencyKey = [Guid]::NewGuid().ToString()
    }
    $headers = @{
        'Idempotency-Key'   = $IdempotencyKey
        'X-Admission-Token' = $AdmissionToken
    }
    if ([string]::IsNullOrWhiteSpace($BodyJSON)) {
        $BodyJSON = [ordered]@{
            train_run_id             = $script:hotTrainRunID
            origin_station_code      = $script:originCode
            destination_station_code = $script:destinationCode
            seat_class               = $script:seatClass
            passenger_ids            = @($Customer.PassengerID)
        } | ConvertTo-Json -Compress -Depth 8
    }
    $response = Invoke-API -Method POST -Path '/api/v1/reservations' `
        -AccessToken $Customer.AccessToken `
        -Headers $headers `
        -BodyJSON $BodyJSON `
        -RequestBaseURL $RequestBaseURL
    Add-Upstream -Set $Upstreams -Response $response
    $Customer.ReservationUpstream = Get-Upstream -Response $response
    if ($response.StatusCode -ne 201) {
        throw "reservation returned unexpected HTTP status $($response.StatusCode)"
    }
    $body = Convert-ResponseBody -Response $response
    if ([string]::IsNullOrWhiteSpace([string]$body.id)) {
        throw 'reservation response omitted its durable identifier'
    }
    return [string]$body.id
}

function Start-ReservationHttpAttempt {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Customer,

        [Parameter(Mandatory = $true)]
        [string]$AdmissionToken,

        [Parameter(Mandatory = $true)]
        [string]$IdempotencyKey,

        [Parameter(Mandatory = $true)]
        [string]$BodyJSON,

        [Parameter(Mandatory = $true)]
        [string]$RequestBaseURL
    )

    $client = [System.Net.Http.HttpClient]::new()
    $client.Timeout = [TimeSpan]::FromSeconds(15)
    $request = [System.Net.Http.HttpRequestMessage]::new(
        [System.Net.Http.HttpMethod]::Post,
        "$($RequestBaseURL.TrimEnd('/'))/api/v1/reservations"
    )
    $request.Headers.Authorization = [System.Net.Http.Headers.AuthenticationHeaderValue]::new(
        'Bearer',
        $Customer.AccessToken
    )
    [void]$request.Headers.TryAddWithoutValidation('Accept', 'application/json')
    [void]$request.Headers.TryAddWithoutValidation('Idempotency-Key', $IdempotencyKey)
    [void]$request.Headers.TryAddWithoutValidation('X-Admission-Token', $AdmissionToken)
    [void]$request.Headers.TryAddWithoutValidation('Connection', 'close')
    $request.Content = [System.Net.Http.StringContent]::new(
        $BodyJSON,
        [System.Text.Encoding]::UTF8,
        'application/json'
    )
    return [pscustomobject]@{
        Client  = $client
        Request = $request
        Task    = $client.SendAsync($request)
    }
}

function Complete-ReservationHttpAttempt {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Attempt,

        [int]$TimeoutMilliseconds = 15000
    )

    try {
        if (-not $Attempt.Task.Wait($TimeoutMilliseconds)) {
            throw 'reservation HTTP attempt exceeded its bounded completion window'
        }
        $response = $Attempt.Task.GetAwaiter().GetResult()
        $body = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
        $upstreamValues = $null
        $upstream = ''
        if ($response.Headers.TryGetValues('X-Upstream-Addr', [ref]$upstreamValues)) {
            $upstream = Get-FinalUpstreamAddress -Value ([string]::Join(',', @($upstreamValues)))
        }
        return [pscustomobject]@{
            StatusCode = [int]$response.StatusCode
            Body       = [string]$body
            Upstream   = $upstream
            Response   = $response
        }
    } finally {
        $Attempt.Request.Dispose()
        $Attempt.Client.Dispose()
    }
}

function Confirm-TerminatedReservationAttempt {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Attempt
    )

    $completed = $false
    try {
        $completed = $Attempt.Task.Wait(15000)
    } catch [AggregateException] {
        $completed = $true
    } finally {
        if ($Attempt.Task.Status -eq [System.Threading.Tasks.TaskStatus]::RanToCompletion) {
            $response = $Attempt.Task.GetAwaiter().GetResult()
            try {
                if ([int]$response.StatusCode -eq 201) {
                    throw 'the terminated API returned a committed reservation'
                }
            } finally {
                $response.Dispose()
            }
        }
        $Attempt.Request.Dispose()
        $Attempt.Client.Dispose()
    }
    if (-not $completed) {
        throw 'the terminated API request did not end within the bounded window'
    }
}

function Start-BookingBlocker {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ApplicationName
    )

    $query = "BEGIN; SELECT id FROM train_runs WHERE id='$script:hotTrainRunID' FOR UPDATE; SELECT pg_sleep(30); COMMIT;"
    & docker @script:composeArguments exec -T -d `
        -e "PGAPPNAME=$ApplicationName" `
        postgres psql -U railway -d railway -v ON_ERROR_STOP=1 -c $query 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw 'could not start the bounded PostgreSQL booking blocker'
    }
    $readyQuery = @'
DO $evidence$
DECLARE
    deadline timestamptz := clock_timestamp() + interval '2 seconds';
BEGIN
    LOOP
        IF EXISTS (
            SELECT 1
            FROM pg_stat_activity
            WHERE application_name = '__BLOCKER_APPLICATION__'
              AND state = 'active'
              AND wait_event = 'PgSleep'
        ) THEN
            RETURN;
        END IF;
        IF clock_timestamp() >= deadline THEN
            RAISE EXCEPTION 'synthetic booking blocker did not acquire its row lock';
        END IF;
        PERFORM pg_sleep(0.025);
    END LOOP;
END
$evidence$;
'@.Replace('__BLOCKER_APPLICATION__', $ApplicationName)
    [void](Invoke-PostgresScalar -Query $readyQuery)
}

function Assert-BookingTransactionBlocked {
    param(
        [Parameter(Mandatory = $true)]
        [string]$BlockerApplicationName
    )

    $query = @'
DO $evidence$
DECLARE
    deadline timestamptz := clock_timestamp() + interval '2 seconds';
BEGIN
    LOOP
        IF EXISTS (
            SELECT 1
            FROM pg_stat_activity AS waiting
            CROSS JOIN LATERAL unnest(pg_blocking_pids(waiting.pid)) AS blocked(blocker_pid)
            JOIN pg_stat_activity AS blocker ON blocker.pid = blocked.blocker_pid
            WHERE blocker.application_name = '__BLOCKER_APPLICATION__'
              AND waiting.query ILIKE '%FROM train_runs%'
        ) THEN
            RETURN;
        END IF;
        IF clock_timestamp() >= deadline THEN
            RAISE EXCEPTION 'booking request did not block on the synthetic train-run lock';
        END IF;
        PERFORM pg_sleep(0.025);
    END LOOP;
END
$evidence$;
SELECT waiting.pid
FROM pg_stat_activity AS waiting
CROSS JOIN LATERAL unnest(pg_blocking_pids(waiting.pid)) AS blocked(blocker_pid)
JOIN pg_stat_activity AS blocker ON blocker.pid = blocked.blocker_pid
WHERE blocker.application_name = '__BLOCKER_APPLICATION__'
  AND waiting.query ILIKE '%FROM train_runs%'
ORDER BY waiting.pid
LIMIT 1;
'@.Replace('__BLOCKER_APPLICATION__', $BlockerApplicationName)
    $backendPID = Invoke-PostgresScalar -Query $query
    if ($backendPID -notmatch '^[1-9][0-9]*$') {
        throw 'could not identify the blocked hot-reservation database backend'
    }
    return [int]$backendPID
}

function Stop-BookingBlocker {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ApplicationName
    )

    $terminated = Invoke-PostgresScalar -Query (
        "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity " +
        "WHERE application_name='$ApplicationName' AND pid <> pg_backend_pid();"
    )
    if ($terminated -notmatch '^[0-9]+$') {
        throw 'PostgreSQL blocker cleanup returned an invalid result'
    }
}

function Wait-PostgresBackendEnded {
    param(
        [Parameter(Mandatory = $true)]
        [int]$BackendPID
    )

    $query = @"
DO `$evidence`$
DECLARE
    deadline timestamptz := clock_timestamp() + interval '5 seconds';
BEGIN
    LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $BackendPID) THEN
            RETURN;
        END IF;
        IF clock_timestamp() >= deadline THEN
            RAISE EXCEPTION 'terminated API database backend remained active';
        END IF;
        PERFORM pg_sleep(0.025);
    END LOOP;
END
`$evidence`$;
"@
    [void](Invoke-PostgresScalar -Query $query)
}

function Get-BookingResidue {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PassengerID
    )

    $parsedPassengerID = [Guid]::Empty
    if (-not [Guid]::TryParse($PassengerID, [ref]$parsedPassengerID)) {
        throw 'synthetic passenger identifier is invalid'
    }
    $query = @"
WITH target_owner AS (
    SELECT user_id
    FROM passengers
    WHERE id = '$PassengerID'
),
target_reservations AS (
    SELECT id
    FROM reservations
    WHERE user_id = (SELECT user_id FROM target_owner)
      AND train_run_id = '$script:hotTrainRunID'
)
SELECT concat_ws(
    '|',
    (SELECT count(*) FROM target_reservations),
    (SELECT count(*) FROM reservation_seats WHERE passenger_id = '$PassengerID'),
    (
        SELECT count(*)
        FROM idempotency_records
        WHERE user_id = (SELECT user_id FROM target_owner)
          AND operation = 'reservation.create'
    ),
    (
        SELECT count(*)
        FROM outbox_events
        WHERE aggregate_type = 'reservation'
          AND aggregate_id IN (SELECT id FROM target_reservations)
    ),
    (
        SELECT coalesce(sum(bit_count(occupied_segments)), 0)
        FROM seat_inventory
        WHERE train_run_id = '$script:hotTrainRunID'
    )
);
"@
    $values = (Invoke-PostgresScalar -Query $query).Split('|')
    if ($values.Count -ne 5) {
        throw 'PostgreSQL residue query returned an invalid result'
    }
    return [pscustomobject]@{
        Reservations      = [int64]$values[0]
        ReservationSeats  = [int64]$values[1]
        Idempotency       = [int64]$values[2]
        Outbox            = [int64]$values[3]
        OccupiedSegments  = [int64]$values[4]
    }
}

function Get-PassengerTrainRunReservationCount {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PassengerID,

        [Parameter(Mandatory = $true)]
        [string]$TrainRunID
    )

    $parsedPassengerID = [Guid]::Empty
    $parsedTrainRunID = [Guid]::Empty
    if (
        -not [Guid]::TryParse($PassengerID, [ref]$parsedPassengerID) -or
        -not [Guid]::TryParse($TrainRunID, [ref]$parsedTrainRunID)
    ) {
        throw 'synthetic reservation evidence identifier is invalid'
    }
    $value = Invoke-PostgresScalar -Query @"
SELECT count(DISTINCT reservations.id)
FROM reservations
JOIN reservation_seats
  ON reservation_seats.reservation_id = reservations.id
WHERE reservations.train_run_id = '$TrainRunID'
  AND reservation_seats.passenger_id = '$PassengerID';
"@
    if ($value -notmatch '^[0-9]+$') {
        throw 'PostgreSQL reservation evidence query returned an invalid result'
    }
    return [int64]$value
}

function Assert-BookingResidue {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Actual,

        [Parameter(Mandatory = $true)]
        [int64]$Expected
    )

    if (
        $Actual.Reservations -ne $Expected -or
        $Actual.ReservationSeats -ne $Expected -or
        $Actual.Idempotency -ne $Expected -or
        $Actual.Outbox -ne $Expected -or
        $Actual.OccupiedSegments -ne $Expected
    ) {
        throw 'booking termination left an unexpected PostgreSQL reservation, seat, idempotency, outbox, or inventory result'
    }
}

function Get-MaximumSlidingAdmissions {
    param(
        [Parameter(Mandatory = $true)]
        [DateTimeOffset[]]$Times
    )

    $ordered = @($Times | Sort-Object)
    $maximum = 0
    for ($left = 0; $left -lt $ordered.Count; $left++) {
        $right = $left
        while (
            $right -lt $ordered.Count -and
            ($ordered[$right] - $ordered[$left]).TotalMilliseconds -lt 1000
        ) {
            $right++
        }
        $count = $right - $left
        if ($count -gt $maximum) {
            $maximum = $count
        }
    }
    return $maximum
}

$started = $false
$succeeded = $false
$customers = @()
$upstreams = New-Object 'System.Collections.Generic.HashSet[string]'
$initialUpstreams = New-Object 'System.Collections.Generic.HashSet[string]'
$apiTerminationUpstreams = New-Object 'System.Collections.Generic.HashSet[string]'
$apiTerminationReadyReplicas = New-Object 'System.Collections.Generic.HashSet[string]'
$apiTerminationReadyAfterProbeReplicas = New-Object 'System.Collections.Generic.HashSet[string]'
$apiRecoveryUpstreams = New-Object 'System.Collections.Generic.HashSet[string]'
$reservationIDs = New-Object 'System.Collections.Generic.HashSet[string]'
$entryIDs = New-Object 'System.Collections.Generic.HashSet[string]'
$admissionTimes = New-Object 'System.Collections.Generic.List[DateTimeOffset]'
$workerOneRestarted = $false
$bookingBlockerApplicationName = ''
$bookingHttpAttempt = $null
$bookingTerminationRetryAttempts = 0
$bookingTerminationRecoveryMilliseconds = 0
$bookingTerminationReplayVerified = $false
$redisStopped = $false
$redisOutageHotFailClosed = $false
$redisOutageNonHotReservationsCreated = 0
$redisOutageRedisRestored = $false

try {
    Push-Location $repositoryRoot
    & docker version | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw 'Docker Engine is unavailable'
    }
    & docker compose version | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw 'Docker Compose v2 is unavailable'
    }

    Write-Host "Starting isolated Compose project $ProjectName"
    $started = $true
    Invoke-DockerCompose -Arguments @('up', '-d', '--build') `
        -CapturePath (Join-Path $EvidenceDirectory 'compose-up.log') | Out-Null
    $baseURL = Get-ServiceBaseURL -Service 'load-balancer'
    Wait-APIReady
    Wait-WorkerReady -Service 'admission-worker-1'
    Wait-WorkerReady -Service 'admission-worker-2'

    Write-Host 'Loading the synthetic, disposable Milestone 2 fixture'
    $fixtureSQL = Get-Content -Raw $fixtureFile
    $fixtureOutput = $fixtureSQL |
        & docker @composeArguments exec -T postgres `
            psql -U railway -d railway -v ON_ERROR_STOP=1 2>&1
    $fixtureExit = $LASTEXITCODE
    $fixtureOutput | Out-File -FilePath (Join-Path $EvidenceDirectory 'fixture-load.log') -Encoding utf8
    if ($fixtureExit -ne 0) {
        throw "fixture load failed with exit code $fixtureExit"
    }

    Write-Host "Provisioning $CustomerCount disposable customer identities"
    $password = "M2Evidence-$runSuffix-Aa1!"
    for ($index = 1; $index -le $CustomerCount; $index++) {
        $forwardedAddress = "198.18.0.$($index + 10)"
        $headers = @{ 'X-Forwarded-For' = $forwardedAddress }
        $email = "m2-evidence-$runSuffix-$index@example.test"
        $register = Invoke-API -Method POST -Path '/api/v1/auth/register' `
            -Headers $headers `
            -Body @{
                email        = $email
                password     = $password
                display_name = "Synthetic Rider $index"
            }
        if ($register.StatusCode -ne 202) {
            throw "registration returned unexpected HTTP status $($register.StatusCode)"
        }
        $login = Invoke-API -Method POST -Path '/api/v1/auth/login' `
            -Headers $headers `
            -Body @{ email = $email; password = $password }
        $loginBody = Convert-ResponseBody -Response $login
        $accessToken = [string]$loginBody.access_token
        if ([string]::IsNullOrWhiteSpace($accessToken)) {
            throw 'login response omitted its access credential'
        }
        $passengerResponse = Invoke-API -Method POST -Path '/api/v1/passengers' `
            -AccessToken $accessToken `
            -Body @{ display_name = "Synthetic Load Rider $index" }
        if ($passengerResponse.StatusCode -ne 201) {
            throw "passenger creation returned unexpected HTTP status $($passengerResponse.StatusCode)"
        }
        $passengerBody = Convert-ResponseBody -Response $passengerResponse
        if ([string]::IsNullOrWhiteSpace([string]$passengerBody.id)) {
            throw 'passenger creation response omitted its synthetic passenger identifier'
        }
        $customers += [pscustomobject]@{
            AccessToken         = $accessToken
            PassengerID         = [string]$passengerBody.id
            EntryID             = ''
            EntryStatus         = ''
            JoinUpstream        = ''
            StatusUpstream      = ''
            ReservationUpstream = ''
            Completed           = $false
        }
        $loginBody = $null
    }
    $password = $null

    Wait-PolicyInitialized
    Save-OperationalSnapshot -Label 'baseline'

    Write-Host 'Pausing admission workers so duplicate/status load cannot claim one-time token delivery'
    Invoke-DockerCompose -Arguments @(
        'stop', 'admission-worker-1', 'admission-worker-2'
    ) -CapturePath (Join-Path $EvidenceDirectory 'preload-workers-stop.log') | Out-Null

    Write-Host "Running the bounded $SteadyStateDuration multi-replica k6 smoke"
    Invoke-K6SteadyState
    Wait-APIReady -Attempts 30

    Write-Host 'Resolving one shared queue entry per customer through the load balancer'
    foreach ($customer in $customers) {
        $join = Invoke-API -Method POST -Path '/api/v1/waiting-room/entries' `
            -AccessToken $customer.AccessToken `
            -Body @{
                train_run_id             = $hotTrainRunID
                origin_station_code      = $originCode
                destination_station_code = $destinationCode
                seat_class               = $seatClass
                passenger_count          = 1
            }
        Add-Upstream -Set $upstreams -Response $join
        Add-Upstream -Set $initialUpstreams -Response $join
        if ($join.StatusCode -ne 201) {
            throw "duplicate join returned unexpected HTTP status $($join.StatusCode)"
        }
        $joinBody = Convert-ResponseBody -Response $join
        $customer.EntryID = [string]$joinBody.entry_id
        $customer.EntryStatus = [string]$joinBody.status
        $customer.JoinUpstream = Get-Upstream -Response $join
        if ([string]::IsNullOrWhiteSpace($customer.EntryID)) {
            throw 'waiting-room response omitted its entry identifier'
        }
        [void]$entryIDs.Add($customer.EntryID)
    }
    if ($entryIDs.Count -ne $CustomerCount) {
        throw "shared queue produced $($entryIDs.Count) unique entries for $CustomerCount customers"
    }
    if ($initialUpstreams.Count -ne 3) {
        throw "initial topology probe observed $($initialUpstreams.Count) final upstreams; want exactly three API replicas"
    }
    $queuedStatusCustomer = $customers |
        Where-Object { $_.EntryStatus -eq 'queued' } |
        Select-Object -First 1
    if ($null -eq $queuedStatusCustomer) {
        throw 'fixture did not retain a queued entry for the status steady-state smoke'
    }
    Write-Host 'Running a bounded 15-second waiting-room status smoke against a queued entry'
    Invoke-K6StatusSteadyState -Customer $queuedStatusCustomer

    Write-Host 'Stopping one API and proving the remaining replicas preserve the shared entry'
    Invoke-DockerCompose -Arguments @('stop', 'api-1') `
        -CapturePath (Join-Path $EvidenceDirectory 'api-1-stop.log') | Out-Null
    Wait-APIReady -Attempts 30
    foreach ($service in @('api-2', 'api-3')) {
        Wait-APIServiceReady -Service $service
        [void]$apiTerminationReadyReplicas.Add($service)
    }
    $expectedEntryID = $customers[0].EntryID
    for ($attempt = 1; $attempt -le 30; $attempt++) {
        $join = Invoke-API -Method POST -Path '/api/v1/waiting-room/entries' `
            -AccessToken $customers[0].AccessToken `
            -Body @{
                train_run_id             = $hotTrainRunID
                origin_station_code      = $originCode
                destination_station_code = $destinationCode
                seat_class               = $seatClass
                passenger_count          = 1
            }
        Add-Upstream -Set $apiTerminationUpstreams -Response $join
        Add-Upstream -Set $upstreams -Response $join
        $joinBody = Convert-ResponseBody -Response $join
        if ($join.StatusCode -ne 201 -or [string]$joinBody.entry_id -ne $expectedEntryID) {
            throw 'API termination changed or lost the shared duplicate entry'
        }
    }
    if ($apiTerminationUpstreams.Count -ne 2) {
        throw "API termination probe observed $($apiTerminationUpstreams.Count) final upstreams; want exactly two surviving replicas"
    }
    foreach ($service in @('api-2', 'api-3')) {
        Wait-APIServiceReady -Service $service
        [void]$apiTerminationReadyAfterProbeReplicas.Add($service)
    }
    Invoke-DockerCompose -Arguments @('start', 'api-1') `
        -CapturePath (Join-Path $EvidenceDirectory 'api-1-start.log') | Out-Null
    for ($attempt = 1; $attempt -le 60; $attempt++) {
        $probe = Invoke-NativeProbe -Command {
            & docker @composeArguments exec -T api-1 `
                wget -q -T 2 -O /dev/null http://127.0.0.1:8080/readyz
        }
        if ($probe.ExitCode -eq 0) {
            break
        }
        if ($attempt -eq 60) {
            throw 'api-1 did not recover within the bounded restart window'
        }
        Start-Sleep -Seconds 1
    }
    $apiRecoveryStartedAt = [DateTimeOffset]::UtcNow
    $apiRecoveryDeadline = $apiRecoveryStartedAt.AddSeconds(30)
    $apiRecoveryAttempts = 0
    while (
        $apiRecoveryUpstreams.Count -lt 3 -and
        [DateTimeOffset]::UtcNow -lt $apiRecoveryDeadline
    ) {
        $apiRecoveryAttempts++
        $join = Invoke-API -Method POST -Path '/api/v1/waiting-room/entries' `
            -AccessToken $customers[0].AccessToken `
            -Body @{
                train_run_id             = $hotTrainRunID
                origin_station_code      = $originCode
                destination_station_code = $destinationCode
                seat_class               = $seatClass
                passenger_count          = 1
            }
        Add-Upstream -Set $apiRecoveryUpstreams -Response $join
        Add-Upstream -Set $upstreams -Response $join
        $joinBody = Convert-ResponseBody -Response $join
        if ($join.StatusCode -ne 201 -or [string]$joinBody.entry_id -ne $expectedEntryID) {
            throw 'API recovery changed or lost the shared duplicate entry'
        }
        if ($apiRecoveryUpstreams.Count -lt 3) {
            Start-Sleep -Seconds 1
        }
    }
    $apiRecoveryMilliseconds = [int][Math]::Ceiling(
        ([DateTimeOffset]::UtcNow - $apiRecoveryStartedAt).TotalMilliseconds
    )
    if ($apiRecoveryUpstreams.Count -ne 3) {
        throw "API recovery probe observed $($apiRecoveryUpstreams.Count) final upstreams after $apiRecoveryAttempts bounded attempts; want exactly three recovered replicas"
    }

    Write-Host 'Starting both admission workers for the global-rate and inflight-cap probe'
    Invoke-DockerCompose -Arguments @(
        'start', 'admission-worker-1', 'admission-worker-2'
    ) -CapturePath (Join-Path $EvidenceDirectory 'preload-workers-start.log') | Out-Null
    Wait-WorkerReady -Service 'admission-worker-1'
    Wait-WorkerReady -Service 'admission-worker-2'

    Write-Host 'Holding the first admission batch to observe the global inflight bound'
    $heldAdmissions = @()
    $initialDeadline = [DateTimeOffset]::UtcNow.AddSeconds(45)
    while ($heldAdmissions.Count -lt $configuredInflightLimit) {
        foreach ($customer in $customers) {
            if ($heldAdmissions.Count -ge $configuredInflightLimit) {
                break
            }
            if ($customer.Completed) {
                continue
            }
            $alreadyHeld = $false
            foreach ($held in $heldAdmissions) {
                if ($held.Customer.EntryID -eq $customer.EntryID) {
                    $alreadyHeld = $true
                    break
                }
            }
            if ($alreadyHeld) {
                continue
            }
            $status = Invoke-API -Method GET `
                -Path "/api/v1/waiting-room/entries/$($customer.EntryID)" `
                -AccessToken $customer.AccessToken
            Add-Upstream -Set $upstreams -Response $status
            $statusBody = Convert-ResponseBody -Response $status
            if ([string]$statusBody.status -eq 'admitted') {
                $rawAdmissionToken = [string]$status.Headers['X-Admission-Token']
                if ([string]::IsNullOrWhiteSpace($rawAdmissionToken)) {
                    throw 'admitted entry did not deliver its one-time admission credential'
                }
                $admittedAt = [DateTimeOffset]::Parse([string]$statusBody.admitted_at)
                $admissionTimes.Add($admittedAt)
                $customer.StatusUpstream = Get-Upstream -Response $status
                $heldAdmissions += [pscustomobject]@{
                    Customer       = $customer
                    AdmissionToken = $rawAdmissionToken
                }
            }
        }
        if ([DateTimeOffset]::UtcNow -ge $initialDeadline) {
            throw 'initial inflight batch was not admitted within the bounded window'
        }
        if ($heldAdmissions.Count -lt $configuredInflightLimit) {
            Start-Sleep -Milliseconds 100
        }
    }

    Start-Sleep -Milliseconds 1500
    foreach ($customer in $customers) {
        $isHeld = $false
        foreach ($held in $heldAdmissions) {
            if ($held.Customer.EntryID -eq $customer.EntryID) {
                $isHeld = $true
                break
            }
        }
        if ($isHeld) {
            continue
        }
        $status = Invoke-API -Method GET `
            -Path "/api/v1/waiting-room/entries/$($customer.EntryID)" `
            -AccessToken $customer.AccessToken
        Add-Upstream -Set $upstreams -Response $status
        $statusBody = Convert-ResponseBody -Response $status
        if ([string]$statusBody.status -eq 'admitted') {
            throw 'global inflight admission bound was exceeded while the first batch was held'
        }
    }

    foreach ($worker in @('admission-worker-1', 'admission-worker-2')) {
        Invoke-DockerCompose -Arguments @(
            'exec', '-T', $worker,
            'wget', '-q', '-T', '3', '-O', '-', 'http://127.0.0.1:9090/metrics'
        ) -CapturePath (Join-Path $EvidenceDirectory "inflight-cap-$worker-metrics.prom") | Out-Null
    }
    $capMetrics = (
        Get-Content -Raw (Join-Path $EvidenceDirectory 'inflight-cap-admission-worker-1-metrics.prom')
    ) + (
        Get-Content -Raw (Join-Path $EvidenceDirectory 'inflight-cap-admission-worker-2-metrics.prom')
    )
    if ($capMetrics -notmatch '(?m)^waiting_room_inflight_admissions\s+5(?:\.0+)?\s*$') {
        throw 'worker metrics did not expose the configured inflight cap'
    }

    Write-Host 'Terminating api-1 during a blocked hot reservation and proving rollback plus exact retry'
    $bookingTarget = $heldAdmissions[0]
    $bookingIdempotencyKey = [Guid]::NewGuid().ToString()
    $bookingBodyJSON = [ordered]@{
        train_run_id             = $hotTrainRunID
        origin_station_code      = $originCode
        destination_station_code = $destinationCode
        seat_class               = $seatClass
        passenger_ids            = @($bookingTarget.Customer.PassengerID)
    } | ConvertTo-Json -Compress -Depth 8
    $apiOneBaseURL = Get-ServiceBaseURL -Service 'api-1'
    $bookingBlockerApplicationName = "m2-booking-blocker-$runSuffix"
    Start-BookingBlocker -ApplicationName $bookingBlockerApplicationName
    $bookingHttpAttempt = Start-ReservationHttpAttempt `
        -Customer $bookingTarget.Customer `
        -AdmissionToken $bookingTarget.AdmissionToken `
        -IdempotencyKey $bookingIdempotencyKey `
        -BodyJSON $bookingBodyJSON `
        -RequestBaseURL $apiOneBaseURL
    $terminatedBookingBackendPID = Assert-BookingTransactionBlocked `
        -BlockerApplicationName $bookingBlockerApplicationName
    Invoke-DockerCompose -Arguments @('stop', '-t', '0', 'api-1') `
        -CapturePath (Join-Path $EvidenceDirectory 'api-1-booking-termination.log') | Out-Null
    Confirm-TerminatedReservationAttempt -Attempt $bookingHttpAttempt
    $bookingHttpAttempt = $null
    Stop-BookingBlocker -ApplicationName $bookingBlockerApplicationName
    $bookingBlockerApplicationName = ''
    Wait-PostgresBackendEnded -BackendPID $terminatedBookingBackendPID
    Assert-BookingResidue `
        -Actual (Get-BookingResidue -PassengerID $bookingTarget.Customer.PassengerID) `
        -Expected 0

    Invoke-DockerCompose -Arguments @('start', 'api-1') `
        -CapturePath (Join-Path $EvidenceDirectory 'api-1-booking-restart.log') | Out-Null
    for ($attempt = 1; $attempt -le 60; $attempt++) {
        $probe = Invoke-NativeProbe -Command {
            & docker @composeArguments exec -T api-1 `
                wget -q -T 2 -O /dev/null http://127.0.0.1:8080/readyz
        }
        if ($probe.ExitCode -eq 0) {
            break
        }
        if ($attempt -eq 60) {
            throw 'api-1 did not become ready after booking-transaction termination'
        }
        Start-Sleep -Seconds 1
    }

    $recoveryStartedAt = [DateTimeOffset]::UtcNow
    Start-Sleep -Milliseconds 5500
    $retryResult = $null
    for ($attempt = 1; $attempt -le 4; $attempt++) {
        $bookingTerminationRetryAttempts = $attempt
        $retryAttempt = Start-ReservationHttpAttempt `
            -Customer $bookingTarget.Customer `
            -AdmissionToken $bookingTarget.AdmissionToken `
            -IdempotencyKey $bookingIdempotencyKey `
            -BodyJSON $bookingBodyJSON `
            -RequestBaseURL $baseURL
        $candidateResult = Complete-ReservationHttpAttempt -Attempt $retryAttempt
        if ($candidateResult.StatusCode -eq 201) {
            $retryResult = $candidateResult
            break
        }
        try {
            if ($candidateResult.StatusCode -notin @(409, 429, 503)) {
                throw "exact booking retry returned unexpected HTTP status $($candidateResult.StatusCode)"
            }
        } finally {
            $candidateResult.Response.Dispose()
        }
        Start-Sleep -Milliseconds 500
    }
    $bookingTerminationRecoveryMilliseconds = [int](
        ([DateTimeOffset]::UtcNow - $recoveryStartedAt).TotalMilliseconds
    )
    if ($null -eq $retryResult) {
        throw 'the exact admission-token/idempotency retry did not recover within the bounded window'
    }
    try {
        $retryBody = $retryResult.Body | ConvertFrom-Json
        $bookingReservationID = [string]$retryBody.id
        if ([string]::IsNullOrWhiteSpace($bookingReservationID)) {
            throw 'the recovered booking retry omitted its durable identifier'
        }
        if ([string]::IsNullOrWhiteSpace($retryResult.Upstream)) {
            throw 'the recovered booking retry did not traverse the shared load balancer'
        }
        [void]$upstreams.Add($retryResult.Upstream)
        $bookingTarget.Customer.ReservationUpstream = $retryResult.Upstream
    } finally {
        $retryResult.Response.Dispose()
    }
    [void]$reservationIDs.Add($bookingReservationID)
    $bookingTarget.Customer.Completed = $true
    Assert-BookingResidue `
        -Actual (Get-BookingResidue -PassengerID $bookingTarget.Customer.PassengerID) `
        -Expected 1

    $replayAttempt = Start-ReservationHttpAttempt `
        -Customer $bookingTarget.Customer `
        -AdmissionToken $bookingTarget.AdmissionToken `
        -IdempotencyKey $bookingIdempotencyKey `
        -BodyJSON $bookingBodyJSON `
        -RequestBaseURL $baseURL
    $replayResult = Complete-ReservationHttpAttempt -Attempt $replayAttempt
    try {
        $replayBody = $replayResult.Body | ConvertFrom-Json
        if ($replayResult.StatusCode -ne 201 -or [string]$replayBody.id -ne $bookingReservationID) {
            throw 'the committed exact retry did not replay the same durable reservation'
        }
    } finally {
        $replayResult.Response.Dispose()
    }
    Assert-BookingResidue `
        -Actual (Get-BookingResidue -PassengerID $bookingTarget.Customer.PassengerID) `
        -Expected 1
    $bookingTerminationReplayVerified = $true
    $bookingTarget.AdmissionToken = $null
    $bookingIdempotencyKey = $null
    $bookingBodyJSON = $null

    Write-Host 'Stopping one admission worker, releasing capacity, and proving bounded continuation'
    Invoke-DockerCompose -Arguments @('stop', 'admission-worker-1') `
        -CapturePath (Join-Path $EvidenceDirectory 'admission-worker-1-stop.log') | Out-Null
    foreach ($held in $heldAdmissions) {
        if ($held.Customer.Completed) {
            $held.AdmissionToken = $null
            continue
        }
        $reservationID = Submit-Reservation `
            -Customer $held.Customer `
            -AdmissionToken $held.AdmissionToken `
            -Upstreams $upstreams
        [void]$reservationIDs.Add($reservationID)
        $held.Customer.Completed = $true
        $held.AdmissionToken = $null
    }

    $completionDeadline = [DateTimeOffset]::UtcNow.AddSeconds(120)
    while (@($customers | Where-Object { -not $_.Completed }).Count -gt 0) {
        $madeProgress = $false
        foreach ($customer in $customers) {
            if ($customer.Completed) {
                continue
            }
            $status = Invoke-API -Method GET `
                -Path "/api/v1/waiting-room/entries/$($customer.EntryID)" `
                -AccessToken $customer.AccessToken
            Add-Upstream -Set $upstreams -Response $status
            $statusBody = Convert-ResponseBody -Response $status
            if ([string]$statusBody.status -ne 'admitted') {
                continue
            }
            $rawAdmissionToken = [string]$status.Headers['X-Admission-Token']
            if ([string]::IsNullOrWhiteSpace($rawAdmissionToken)) {
                throw 'admitted entry did not deliver its one-time admission credential'
            }
            $admissionTimes.Add([DateTimeOffset]::Parse([string]$statusBody.admitted_at))
            $customer.StatusUpstream = Get-Upstream -Response $status
            $reservationID = Submit-Reservation `
                -Customer $customer `
                -AdmissionToken $rawAdmissionToken `
                -Upstreams $upstreams
            $rawAdmissionToken = $null
            [void]$reservationIDs.Add($reservationID)
            $customer.Completed = $true
            $madeProgress = $true

            if (
                -not $workerOneRestarted -and
                $reservationIDs.Count -ge [Math]::Ceiling($CustomerCount / 2)
            ) {
                Invoke-DockerCompose -Arguments @('start', 'admission-worker-1') `
                    -CapturePath (Join-Path $EvidenceDirectory 'admission-worker-1-start.log') | Out-Null
                Wait-WorkerReady -Service 'admission-worker-1'
                $workerOneRestarted = $true
            }
        }
        if ([DateTimeOffset]::UtcNow -ge $completionDeadline) {
            throw 'admission/reservation completion exceeded the bounded evidence window'
        }
        if (-not $madeProgress) {
            Start-Sleep -Milliseconds 100
        }
    }
    if (-not $workerOneRestarted) {
        Invoke-DockerCompose -Arguments @('start', 'admission-worker-1') `
            -CapturePath (Join-Path $EvidenceDirectory 'admission-worker-1-start.log') | Out-Null
        Wait-WorkerReady -Service 'admission-worker-1'
        $workerOneRestarted = $true
    }
    if ($reservationIDs.Count -ne $CustomerCount) {
        throw "expected $CustomerCount durable reservations, observed $($reservationIDs.Count)"
    }
    if ($admissionTimes.Count -ne $CustomerCount) {
        throw "expected $CustomerCount unique admissions, observed $($admissionTimes.Count)"
    }

    $maximumSlidingAdmissions = Get-MaximumSlidingAdmissions -Times $admissionTimes.ToArray()
    if ($maximumSlidingAdmissions -gt $configuredAdmissionRate) {
        throw "observed $maximumSlidingAdmissions admissions in a sliding second; configured bound is $configuredAdmissionRate"
    }
    $crossReplicaTokenFlows = @(
        $customers | Where-Object {
            $replicas = @(
                $_.JoinUpstream,
                $_.StatusUpstream,
                $_.ReservationUpstream
            ) |
                Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
                Select-Object -Unique
            @($replicas).Count -ge 2
        }
    ).Count
    if ($crossReplicaTokenFlows -lt 1) {
        throw 'no admission token completed a join/status/reservation flow across API replicas'
    }

    Write-Host 'Stopping Redis and proving hot fail-closed plus non-hot PostgreSQL authority'
    $outageCustomer = $customers[0]
    $hotReservationsBeforeOutage = Get-PassengerTrainRunReservationCount `
        -PassengerID $outageCustomer.PassengerID `
        -TrainRunID $hotTrainRunID
    $nonHotReservationsBeforeOutage = Get-PassengerTrainRunReservationCount `
        -PassengerID $outageCustomer.PassengerID `
        -TrainRunID $nonHotTrainRunID
    try {
        Invoke-DockerCompose -Arguments @('stop', 'redis') `
            -CapturePath (Join-Path $EvidenceDirectory 'redis-outage-stop.log') | Out-Null
        $redisStopped = $true
        Invoke-K6RedisOutage -Customer $outageCustomer
        $redisOutageHotFailClosed = $true

        $hotReservationsAfterOutage = Get-PassengerTrainRunReservationCount `
            -PassengerID $outageCustomer.PassengerID `
            -TrainRunID $hotTrainRunID
        $nonHotReservationsAfterOutage = Get-PassengerTrainRunReservationCount `
            -PassengerID $outageCustomer.PassengerID `
            -TrainRunID $nonHotTrainRunID
        if ($hotReservationsAfterOutage -ne $hotReservationsBeforeOutage) {
            throw 'hot Redis-outage probe mutated authoritative reservation state'
        }
        $redisOutageNonHotReservationsCreated = (
            $nonHotReservationsAfterOutage - $nonHotReservationsBeforeOutage
        )
        if ($redisOutageNonHotReservationsCreated -ne 1) {
            throw 'non-hot Redis-outage probe did not create exactly one PostgreSQL reservation'
        }
    } finally {
        if ($redisStopped) {
            Invoke-DockerCompose -Arguments @('start', 'redis') `
                -CapturePath (Join-Path $EvidenceDirectory 'redis-outage-start.log') | Out-Null
            Wait-RedisReady
            $redisStopped = $false
            Wait-APIReady -Attempts 30
            Wait-WorkerReady -Service 'admission-worker-1'
            Wait-WorkerReady -Service 'admission-worker-2'
            $redisOutageRedisRestored = $true
        }
    }
    Save-OperationalSnapshot -Label 'post-redis-outage'

    Write-Host 'Running read-only seat, quota, and admission reconciliation'
    Invoke-DockerCompose -Arguments @('build', 'reconcile') `
        -CapturePath (Join-Path $EvidenceDirectory 'reconcile-build.log') | Out-Null
    Invoke-DockerCompose -Arguments @(
        'run', '--rm', '-T', 'reconcile',
        'seat-inventory', '--train-run-id', $hotTrainRunID
    ) -CapturePath (Join-Path $EvidenceDirectory 'reconcile-seat-inventory.json') | Out-Null
    Invoke-DockerCompose -Arguments @(
        'run', '--rm', '-T', 'reconcile',
        'seat-inventory', '--train-run-id', $nonHotTrainRunID
    ) -CapturePath (Join-Path $EvidenceDirectory 'reconcile-seat-inventory-non-hot.json') | Out-Null
    Invoke-DockerCompose -Arguments @(
        'run', '--rm', '-T', 'reconcile', 'reservation-quotas'
    ) -CapturePath (Join-Path $EvidenceDirectory 'reconcile-reservation-quotas.json') | Out-Null
    Invoke-DockerCompose -Arguments @(
        'run', '--rm', '-T', 'reconcile', 'admission-state'
    ) -CapturePath (Join-Path $EvidenceDirectory 'reconcile-admission-state.json') | Out-Null

    Save-OperationalSnapshot -Label 'final'

    $commitSHA = (& git -c core.fsmonitor=false rev-parse HEAD).Trim()
    $summary = [ordered]@{
        status = 'passed'
        commit_sha = $commitSHA
        topology = [ordered]@{
            api_replicas = 3
            admission_worker_replicas = 2
            orchestration_transport = $orchestrationTransport
            api_upstreams_observed = $initialUpstreams.Count
            surviving_upstreams_during_api_termination = $apiTerminationUpstreams.Count
            surviving_api_replicas_ready_before_termination_probe = $apiTerminationReadyReplicas.Count
            surviving_api_replicas_ready_after_termination_probe = $apiTerminationReadyAfterProbeReplicas.Count
            recovered_upstreams_after_api_restart = $apiRecoveryUpstreams.Count
            cross_replica_token_flows = $crossReplicaTokenFlows
        }
        steady_state_smoke = [ordered]@{
            join_duration = $SteadyStateDuration
            join_virtual_users = $CustomerCount
            join_iteration_pause_seconds = 1
            join_k6_summary = 'multi-replica-summary.json'
            status_duration = '15s'
            status_virtual_users = 10
            status_iteration_pause_seconds = 1
            status_k6_summary = 'waiting-room-status-summary.json'
        }
        admission = [ordered]@{
            configured_rate_per_second = $configuredAdmissionRate
            maximum_observed_in_sliding_second = $maximumSlidingAdmissions
            configured_inflight_limit = $configuredInflightLimit
            inflight_cap_observed = $true
            durable_hot_reservations = $reservationIDs.Count
            durable_reservations = (
                $reservationIDs.Count + $redisOutageNonHotReservationsCreated
            )
        }
        failure_recovery = [ordered]@{
            api_termination_preserved_shared_entry = $true
            api_termination_during_booking_rolled_back = $true
            booking_retry_reused_exact_identity = $true
            booking_retry_committed_once = $bookingTerminationReplayVerified
            booking_retry_traversed_shared_topology = $true
            booking_retry_attempts_after_recovery_wait = $bookingTerminationRetryAttempts
            bounded_lease_recovery_wait_milliseconds = $bookingTerminationRecoveryMilliseconds
            bounded_api_recovery_wait_milliseconds = $apiRecoveryMilliseconds
            worker_termination_preserved_progress = $true
            stopped_worker_restarted_ready = $workerOneRestarted
            redis_outage_hot_join_failed_closed = $redisOutageHotFailClosed
            redis_outage_non_hot_postgres_reservations_created = $redisOutageNonHotReservationsCreated
            redis_restored_ready = $redisOutageRedisRestored
            redis_outage_k6_summary = 'redis-outage-summary.json'
        }
        reconciliation = [ordered]@{
            hot_seat_inventory = 'passed'
            non_hot_seat_inventory = 'passed'
            reservation_quotas = 'passed'
            admission_state = 'passed'
        }
        limitations = @(
            'This is a bounded local functional and steady-state smoke, not production capacity evidence.',
            'It does not establish national-scale, global-fairness, or multi-region behavior.'
        )
    }
    $summary | ConvertTo-Json -Depth 8 |
        Out-File -FilePath (Join-Path $EvidenceDirectory 'evidence-summary.json') -Encoding utf8
    $succeeded = $true
    Write-Host "Milestone 2 evidence passed; sanitized artifacts: $EvidenceDirectory"
} catch {
    $failure = $_
    if ($started) {
        try {
            Invoke-DockerCompose -Arguments @('ps', '-a') `
                -CapturePath (Join-Path $EvidenceDirectory 'compose-failure-ps.log') | Out-Null
        } catch {
            Write-Warning 'could not capture bounded Compose failure state'
        }
        try {
            Invoke-DockerCompose -Arguments @(
                'logs', '--no-color', '--tail', '200', 'load-balancer'
            ) -CapturePath (Join-Path $EvidenceDirectory 'load-balancer-failure.log') | Out-Null
        } catch {
            Write-Warning 'could not capture bounded load-balancer failure logs'
        }
    }
    throw $failure
} finally {
    if ($null -ne $bookingHttpAttempt) {
        try {
            Invoke-DockerCompose -Arguments @('stop', '-t', '0', 'api-1') `
                -CapturePath (Join-Path $EvidenceDirectory 'api-1-booking-emergency-stop.log') | Out-Null
        } catch {
            Write-Warning 'could not stop api-1 while cleaning up the booking-termination probe'
        }
    }
    if (-not [string]::IsNullOrWhiteSpace($bookingBlockerApplicationName)) {
        try {
            Stop-BookingBlocker -ApplicationName $bookingBlockerApplicationName
        } catch {
            Write-Warning 'could not terminate the synthetic PostgreSQL booking blocker'
        }
    }
    if ($null -ne $bookingHttpAttempt) {
        try {
            $bookingHttpAttempt.Request.Dispose()
            $bookingHttpAttempt.Client.Dispose()
        } catch {
            Write-Warning 'could not dispose the interrupted booking HTTP attempt'
        }
        $bookingHttpAttempt = $null
    }
    if ($redisStopped -and $started -and $KeepEnvironment) {
        try {
            Invoke-DockerCompose -Arguments @('start', 'redis') `
                -CapturePath (Join-Path $EvidenceDirectory 'redis-outage-emergency-start.log') | Out-Null
            Wait-RedisReady
            $redisStopped = $false
        } catch {
            Write-Warning 'could not restore Redis while preserving the evidence environment'
        }
    }
    foreach ($name in $environmentNames) {
        $previous = $savedEnvironment[$name]
        if ($null -eq $previous) {
            [Environment]::SetEnvironmentVariable($name, $null, 'Process')
        } else {
            [Environment]::SetEnvironmentVariable($name, [string]$previous, 'Process')
        }
    }
    $customers = @()
    if ($started -and -not $KeepEnvironment) {
        try {
            Invoke-DockerCompose -Arguments @('down', '-v', '--remove-orphans') `
                -CapturePath (Join-Path $EvidenceDirectory 'compose-down.log') | Out-Null
        } catch {
            Write-Warning 'isolated Compose cleanup failed; run the exact cleanup command from the evidence plan'
        }
    }
    Pop-Location
}

if (-not $succeeded) {
    exit 1
}
