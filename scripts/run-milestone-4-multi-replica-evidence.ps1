[CmdletBinding()]
param(
    [ValidateRange(20, 30)]
    [int]$CustomerCount = 20,

    [ValidatePattern('^[1-9][0-9]*(s|m)$')]
    [string]$LoadDuration = '15s',

    [string]$ProjectName = '',

    [string]$EvidenceDirectory = '',

    [switch]$KeepEnvironment
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

. (Join-Path $PSScriptRoot 'milestone-4-evidence-guardrails.ps1')

$root = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $root 'docker-compose.multi-replica.yml'
$fixtureFile = Join-Path $root 'loadtest/fixtures/milestone-2-multi-replica.sql'
$suffix = [guid]::NewGuid().ToString('N').Substring(0, 10)
if ([string]::IsNullOrWhiteSpace($ProjectName)) {
    $ProjectName = "railway-m4-evidence-$suffix"
}
if ($ProjectName -notmatch '^[a-z0-9][a-z0-9_-]{2,62}$') {
    throw 'ProjectName must be a bounded lowercase Compose project name'
}
if ([string]::IsNullOrWhiteSpace($EvidenceDirectory)) {
    $EvidenceDirectory = Join-Path ([System.IO.Path]::GetTempPath()) "railway-m4-evidence-$suffix"
}
$repositoryPath = [System.IO.Path]::GetFullPath($root)
$EvidenceDirectory = New-Milestone4EvidenceDirectory `
    -EvidenceDirectory $EvidenceDirectory -RepositoryPath $repositoryPath
$canonicalSummaryPath = Join-Path $EvidenceDirectory 'milestone-4-summary.json'
$summaryCandidatePath = Join-Path $EvidenceDirectory 'milestone-4-summary.candidate.json'

$compose = @('compose', '-p', $ProjectName, '-f', $composeFile)
$fixtureTrainA = '21000000-0000-4000-8000-000000000401'
$fixtureTrainB = '21000000-0000-4000-8000-000000000402'
$migrationA = '41000000-0000-4000-8000-000000000401'
$migrationB = '41000000-0000-4000-8000-000000000402'
$origin = 'M2A'
$destination = 'M2B'
$seatClass = 'standard'
$started = $false
$teardownCompleted = $false
$sanitizationCompleted = $false
$succeeded = $false
$operatorCLI = 'unrun'
$failureCategory = 'not_started'
$customers = @()
$replicaRouteEvidence = [ordered]@{}
$migrationEvidence = [ordered]@{}
$reconciliationEvidence = [ordered]@{}
$adminFanoutEvidence = [ordered]@{}
$operatorHealthEvidence = [ordered]@{}
$availabilityEvidence = [ordered]@{}
$legacySourceImmutabilityEvidence = [ordered]@{}
$legacySourceFingerprints = [ordered]@{}
$postgresConnectionSamples = [System.Collections.Generic.List[object]]::new()
$redisLatencyEvidence = [ordered]@{}
$secretValues = [System.Collections.Generic.List[string]]::new()
$milestoneSummary = $null
$savedEnvironment = @{}
$environmentNames = @(
    'BASE_URL', 'CUSTOMER_TOKEN', 'CUSTOMER_TOKENS', 'PASSENGER_IDS', 'RESERVATION_IDS',
    'TRAIN_RUN_ID', 'TRAIN_RUN_IDS',
    'LEGACY_TRAIN_RUN_ID', 'SCHEMA_TRAIN_RUN_ID', 'API_URLS', 'ORIGIN_CODE',
    'DESTINATION_CODE', 'SEAT_CLASS', 'VUS', 'VUS_PER_SHARD', 'ITERATIONS',
    'DURATION', 'MAX_DURATION', 'ALLOW_REBALANCING_503', 'EXPECT_PARTIAL'
)
foreach ($name in $environmentNames) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

function Write-EvidenceStatus {
    param(
        [Parameter(Mandatory = $true)][string]$Status,
        [Parameter(Mandatory = $true)][string]$Reason
    )
    [ordered]@{
        milestone = 4
        status = $Status
        reason = $Reason
        operator_cli = $script:operatorCLI
        project = $script:ProjectName
        generated_at = [DateTimeOffset]::UtcNow.ToString('o')
    } | ConvertTo-Json -Depth 4 |
        Out-File -LiteralPath (Join-Path $script:EvidenceDirectory 'evidence-status.json') -Encoding utf8
}

function Invoke-Native {
    param(
        [Parameter(Mandatory = $true)][scriptblock]$Command,
        [switch]$AllowFailure
    )
    $previous = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        # Windows PowerShell represents native stderr as ErrorRecord objects.
        # Convert them while the preference is non-terminating so normal Docker
        # progress output cannot be re-thrown when the captured result is read.
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

function Invoke-Compose {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [string]$CapturePath = '',
        [switch]$AllowFailure
    )
    $result = Invoke-Native -AllowFailure -Command { & docker @script:compose @Arguments }
    if (-not [string]::IsNullOrWhiteSpace($CapturePath)) {
        $result.Output | Out-File -LiteralPath $CapturePath -Encoding utf8
    }
    if ($result.ExitCode -ne 0 -and -not $AllowFailure) {
        throw "docker compose command failed with exit code $($result.ExitCode)"
    }
    return $result
}

function Invoke-PSQL {
    param(
        [Parameter(Mandatory = $true)][string]$SQL,
        [Parameter(Mandatory = $true)][string]$Artifact
    )
    return Invoke-Compose -Arguments @(
        'exec', '-T', 'postgres', 'psql', '-U', 'railway', '-d', 'railway',
        '-v', 'ON_ERROR_STOP=1', '-At', '-c', $SQL
    ) -CapturePath (Join-Path $script:EvidenceDirectory $Artifact)
}

function Get-PublishedURL {
    $result = Invoke-Compose -Arguments @('port', 'load-balancer', '8080')
    $endpoint = [string](@($result.Output | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) }) | Select-Object -Last 1)
    $endpoint = $endpoint.Trim()
    if ($endpoint -notmatch '^(127\.0\.0\.1|0\.0\.0\.0|\[::\]|::):(?<port>[1-9][0-9]{1,4})$') {
        throw 'load-balancer published endpoint was not a bounded loopback address'
    }
    return "http://127.0.0.1:$($matches.port)"
}

function Invoke-API {
    param(
        [Parameter(Mandatory = $true)][ValidateSet('GET', 'POST')][string]$Method,
        [Parameter(Mandatory = $true)][string]$Path,
        [hashtable]$Body,
        [string]$Token = '',
        [string]$IdempotencyKey = '',
        [string]$ForwardedFor = ''
    )
    $headers = @{ Accept = 'application/json' }
    if (-not [string]::IsNullOrWhiteSpace($Token)) { $headers['Authorization'] = "Bearer $Token" }
    if (-not [string]::IsNullOrWhiteSpace($IdempotencyKey)) { $headers['Idempotency-Key'] = $IdempotencyKey }
    if (-not [string]::IsNullOrWhiteSpace($ForwardedFor)) { $headers['X-Forwarded-For'] = $ForwardedFor }
    $parameters = @{
        Uri = "$script:baseURL$Path"
        Method = $Method
        Headers = $headers
        UseBasicParsing = $true
        TimeoutSec = 15
    }
    if ($null -ne $Body) {
        $parameters['ContentType'] = 'application/json'
        $parameters['Body'] = ($Body | ConvertTo-Json -Compress -Depth 6)
    }
    $response = Invoke-WebRequest @parameters
    $decoded = $null
    if (-not [string]::IsNullOrWhiteSpace([string]$response.Content)) {
        $decoded = $response.Content | ConvertFrom-Json
    }
    return [pscustomobject]@{ StatusCode = [int]$response.StatusCode; Body = $decoded }
}

function Wait-ReservationStatus {
    param(
        [Parameter(Mandatory = $true)][string]$ReservationID,
        [Parameter(Mandatory = $true)][string]$Token,
        [Parameter(Mandatory = $true)][string]$ExpectedStatus,
        [int]$Attempts = 240
    )
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        try {
            $loaded = Invoke-API -Method GET -Path "/api/v1/reservations/$ReservationID" -Token $Token
            if ($loaded.StatusCode -eq 200 -and [string]$loaded.Body.status -eq $ExpectedStatus) {
                return
            }
        } catch {
            # The bounded status predicate, not a fixed delay, controls completion.
        }
        Start-Sleep -Milliseconds 250
    }
    throw "reservation did not reach $ExpectedStatus inside the bounded worker window"
}

function Wait-Ready {
    param([string]$Service, [int]$Port, [int]$Attempts = 90)
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $probe = Invoke-Compose -AllowFailure -Arguments @(
            'exec', '-T', $Service, 'wget', '-q', '-T', '2', '-O', '/dev/null',
            "http://127.0.0.1:$Port/readyz"
        )
        if ($probe.ExitCode -eq 0) { return }
        Start-Sleep -Seconds 1
    }
    throw "$Service did not become ready in the bounded startup window"
}

function Wait-HotPoliciesInitialized {
    param([int]$Attempts = 80)
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $result = Invoke-Compose -AllowFailure -Arguments @(
            'exec', '-T', 'postgres', 'psql', '-U', 'railway', '-d', 'railway', '-At', '-c',
            "SELECT count(*) FROM public.hot_train_policies WHERE train_run_id IN ('$fixtureTrainA','$fixtureTrainB') AND enabled AND redis_initialized_version=version;"
        )
        $value = [string](@($result.Output | Where-Object {
            ([string]$_).Trim() -match '^[0-9]+$'
        }) | Select-Object -Last 1)
        if ($result.ExitCode -eq 0 -and $value.Trim() -eq '2') { return }
        Start-Sleep -Milliseconds 250
    }
    throw 'two hot-train policies were not initialized by the bounded worker barrier'
}

function Wait-OutboxPublished {
    param(
        [Parameter(Mandatory = $true)][string[]]$TrainRunIDs,
        [ValidateRange(1, 240)][int]$Attempts = 120
    )
    $literals = @()
    foreach ($trainRunID in $TrainRunIDs) {
        $parsed = [guid]::Empty
        if (-not [guid]::TryParse($trainRunID, [ref]$parsed) -or $parsed -eq [guid]::Empty) {
            throw 'outbox drain barrier received an invalid train-run identifier'
        }
        $literals += "'$($parsed.ToString())'::uuid"
    }
    $inList = $literals -join ','
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $probe = Invoke-Compose -AllowFailure -Arguments @(
            'exec', '-T', 'postgres', 'psql', '-U', 'railway', '-d', 'railway', '-At', '-c',
            "SELECT count(*) FROM public.outbox_events WHERE train_run_id IN ($inList) AND status <> 'published';"
        )
        $pending = [string](@($probe.Output | Where-Object {
            ([string]$_).Trim() -match '^[0-9]+$'
        }) | Select-Object -Last 1)
        if ($probe.ExitCode -eq 0 -and $pending.Trim() -eq '0') { return }
        if ($attempt -lt $Attempts) { Start-Sleep -Milliseconds 250 }
    }
    throw 'selected train-run outbox events did not publish within the bounded drain window'
}

function Wait-TrainRunReadModelCaughtUp {
    param(
        [Parameter(Mandatory = $true)][string]$TrainRunID,
        [ValidateRange(1, 240)][int]$Attempts = 240
    )
    $parsedTrainRunID = [guid]::Empty
    if (-not [guid]::TryParse($TrainRunID, [ref]$parsedTrainRunID) -or
        $parsedTrainRunID -eq [guid]::Empty) {
        throw 'read-model catch-up barrier received an invalid train-run identifier'
    }
    $literal = $parsedTrainRunID.ToString()
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $probe = Invoke-Compose -AllowFailure -Arguments @(
            'exec', '-T', 'postgres', 'psql', '-U', 'railway', '-d', 'railway',
            '-v', 'ON_ERROR_STOP=1', '-At', '-F', '|', '-c', @"
SELECT count(*), count(*) FILTER (WHERE receipt.event_id IS NULL)
FROM public.outbox_events AS event
LEFT JOIN public.read_model_event_receipts AS receipt
  ON receipt.consumer_name='railway-read-model'
 AND receipt.event_id=event.id
WHERE event.train_run_id='$literal'::uuid;
"@
        )
        $line = [string](@($probe.Output | Where-Object {
            ([string]$_).Trim() -match '^[0-9]+\|[0-9]+$'
        }) | Select-Object -Last 1)
        if ($probe.ExitCode -eq 0 -and $line.Trim() -match '^(?<total>[0-9]+)\|(?<pending>[0-9]+)$' -and
            [int64]$matches.total -gt 0 -and [int64]$matches.pending -eq 0) {
            return [int64]$matches.total
        }
        if ($attempt -lt $Attempts) { Start-Sleep -Milliseconds 250 }
    }
    throw 'selected train-run read-model events did not receive durable receipts within the bounded catch-up window'
}

function Get-CutoverOutboxEventID {
    param(
        [Parameter(Mandatory = $true)][string]$TrainRunID,
        [Parameter(Mandatory = $true)][ValidateSet('shard-0', 'shard-1')][string]$ShardID,
        [Parameter(Mandatory = $true)][ValidateRange(2, [int64]::MaxValue)][int64]$AssignmentGeneration
    )
    $parsedTrainRunID = [guid]::Empty
    if (-not [guid]::TryParse($TrainRunID, [ref]$parsedTrainRunID) -or
        $parsedTrainRunID -eq [guid]::Empty) {
        throw 'cutover event lookup received an invalid train-run identifier'
    }
    $literal = $parsedTrainRunID.ToString()
    $result = Invoke-Compose -Arguments @(
        'exec', '-T', 'postgres', 'psql', '-U', 'railway', '-d', 'railway',
        '-v', 'ON_ERROR_STOP=1', '-At', '-F', '|', '-c', @"
SELECT count(*), coalesce(max(event.id::text), '')
FROM public.outbox_events AS event
WHERE event.train_run_id='$literal'::uuid
  AND event.aggregate_type='train_run'
  AND event.aggregate_id='$literal'::uuid
  AND event.event_type='trainrun.updated'
  AND event.payload->>'reason'='shard_cutover'
  AND event.shard_id='$ShardID'
  AND event.assignment_generation=$AssignmentGeneration;
"@
    )
    $line = [string](@($result.Output | Where-Object {
        ([string]$_).Trim() -match '^[0-9]+\|.*$'
    }) | Select-Object -Last 1)
    if ($line.Trim() -notmatch '^1\|(?<event>[0-9a-fA-F-]{36})$') {
        throw 'cutover did not produce exactly one attributable shard_cutover outbox event'
    }
    $parsedEventID = [guid]::Empty
    if (-not [guid]::TryParse($matches.event, [ref]$parsedEventID) -or
        $parsedEventID -eq [guid]::Empty) {
        throw 'cutover outbox event lookup returned an invalid event identifier'
    }
    return $parsedEventID.ToString()
}

function Wait-ReadModelReceipt {
    param(
        [Parameter(Mandatory = $true)][string]$EventID,
        [ValidateRange(1, 240)][int]$Attempts = 240
    )
    $parsedEventID = [guid]::Empty
    if (-not [guid]::TryParse($EventID, [ref]$parsedEventID) -or
        $parsedEventID -eq [guid]::Empty) {
        throw 'read-model receipt barrier received an invalid event identifier'
    }
    $literal = $parsedEventID.ToString()
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $probe = Invoke-Compose -AllowFailure -Arguments @(
            'exec', '-T', 'postgres', 'psql', '-U', 'railway', '-d', 'railway',
            '-v', 'ON_ERROR_STOP=1', '-At', '-c',
            "SELECT count(*) FROM public.read_model_event_receipts WHERE consumer_name='railway-read-model' AND event_id='$literal'::uuid;"
        )
        $count = [string](@($probe.Output | Where-Object {
            ([string]$_).Trim() -match '^[0-9]+$'
        }) | Select-Object -Last 1)
        if ($probe.ExitCode -eq 0 -and $count.Trim() -eq '1') { return }
        if ($attempt -lt $Attempts) { Start-Sleep -Milliseconds 250 }
    }
    throw 'exact cutover read-model receipt did not converge within the bounded window'
}

function Get-AvailabilityCacheVersion {
    param([Parameter(Mandatory = $true)][string]$TrainRunID)
    $key = "cache:availability:version:$TrainRunID"
    $result = Invoke-Compose -AllowFailure -Arguments @(
        'exec', '-T', 'redis', 'redis-cli', '--raw', 'GET', $key
    )
    if ($result.ExitCode -ne 0) { return '' }
    $value = [string](@($result.Output | Where-Object {
        -not [string]::IsNullOrWhiteSpace([string]$_)
    }) | Select-Object -Last 1)
    if ($value.Trim() -notmatch '^[A-Za-z0-9_-]{24}$') { return '' }
    return $value.Trim()
}

function Wait-AvailabilityVersionRotated {
    param(
        [Parameter(Mandatory = $true)][string]$TrainRunID,
        [Parameter(Mandatory = $true)][string]$Previous,
        [int]$Attempts = 120
    )
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $current = Get-AvailabilityCacheVersion -TrainRunID $TrainRunID
        if (-not [string]::IsNullOrWhiteSpace($current) -and $current -ne $Previous) {
            return $current
        }
        Start-Sleep -Milliseconds 250
    }
    throw 'availability cache namespace did not rotate after cutover'
}

function Get-AssignmentState {
    param(
        [Parameter(Mandatory = $true)][string]$TrainRunID,
        [Parameter(Mandatory = $true)][string]$Artifact
    )
    $result = Invoke-PSQL -Artifact $Artifact -SQL @"
SELECT json_build_object(
    'shard_id', shard_id,
    'assignment_generation', assignment_generation,
    'availability_generation', availability_generation,
    'assignment_state', assignment_state
)::text
FROM public.train_run_shard_assignments
WHERE train_run_id = '$TrainRunID'::uuid;
"@
    $line = [string](@($result.Output | Where-Object {
        ([string]$_).TrimStart().StartsWith('{')
    }) | Select-Object -Last 1)
    if ([string]::IsNullOrWhiteSpace($line)) {
        throw 'assignment state query omitted its structured result'
    }
    return $line | ConvertFrom-Json
}

function Get-LegacySourceFingerprint {
    param(
        [Parameter(Mandatory = $true)][string]$TrainRunID,
        [Parameter(Mandatory = $true)][string]$Artifact
    )
    $result = Invoke-PSQL -Artifact $Artifact -SQL @"
SELECT json_build_object(
    'seat_inventory', (
        SELECT json_build_object(
            'rows', count(*),
            'fingerprint', md5(coalesce(jsonb_agg(to_jsonb(row_data) ORDER BY row_data.seat_id)::text, '[]'))
        )
        FROM public.seat_inventory AS row_data
        WHERE row_data.train_run_id='$TrainRunID'::uuid
    ),
    'reservations', (
        SELECT json_build_object(
            'rows', count(*),
            'fingerprint', md5(coalesce(jsonb_agg(to_jsonb(row_data) ORDER BY row_data.id)::text, '[]'))
        )
        FROM public.reservations AS row_data
        WHERE row_data.train_run_id='$TrainRunID'::uuid
    ),
    'reservation_seats', (
        SELECT json_build_object(
            'rows', count(*),
            'fingerprint', md5(coalesce(jsonb_agg(to_jsonb(row_data) ORDER BY row_data.id)::text, '[]'))
        )
        FROM public.reservation_seats AS row_data
        WHERE row_data.train_run_id='$TrainRunID'::uuid
    ),
    'ticket_orders', (
        SELECT json_build_object(
            'rows', count(*),
            'fingerprint', md5(coalesce(jsonb_agg(to_jsonb(row_data) ORDER BY row_data.id)::text, '[]'))
        )
        FROM public.ticket_orders AS row_data
        JOIN public.reservations AS scoped_reservation ON scoped_reservation.id=row_data.reservation_id
        WHERE scoped_reservation.train_run_id='$TrainRunID'::uuid
    ),
    'tickets', (
        SELECT json_build_object(
            'rows', count(*),
            'fingerprint', md5(coalesce(jsonb_agg(to_jsonb(row_data) ORDER BY row_data.id)::text, '[]'))
        )
        FROM public.tickets AS row_data
        JOIN public.ticket_orders AS scoped_order ON scoped_order.id=row_data.ticket_order_id
        JOIN public.reservations AS scoped_reservation ON scoped_reservation.id=scoped_order.reservation_id
        WHERE scoped_reservation.train_run_id='$TrainRunID'::uuid
    ),
    'idempotency_records', (
        SELECT json_build_object(
            'rows', count(*),
            'fingerprint', md5(coalesce(jsonb_agg(to_jsonb(row_data) ORDER BY row_data.id)::text, '[]'))
        )
        FROM public.idempotency_records AS row_data
        WHERE row_data.train_run_id='$TrainRunID'::uuid
    )
)::text;
"@
    $line = [string](@($result.Output | Where-Object {
        ([string]$_).TrimStart().StartsWith('{')
    }) | Select-Object -Last 1)
    if ([string]::IsNullOrWhiteSpace($line)) {
        throw 'retained legacy-source fingerprint query omitted its structured result'
    }
    $fingerprint = $line | ConvertFrom-Json
    foreach ($tableName in @(
        'seat_inventory', 'reservations', 'reservation_seats',
        'ticket_orders', 'tickets', 'idempotency_records'
    )) {
        $table = Get-ObjectPropertyValue -Object $fingerprint -Name $tableName
        $hash = Get-ObjectPropertyValue -Object $table -Name 'fingerprint'
        $rows = Get-ObjectPropertyValue -Object $table -Name 'rows'
        if ($null -eq $table -or $null -eq $rows -or [int64]$rows -lt 0 -or
            [string]$hash -notmatch '^[0-9a-f]{32}$') {
            throw 'retained legacy-source fingerprint was incomplete or malformed'
        }
    }
    return $fingerprint
}

function Assert-LegacySourceUnchanged {
    param(
        [Parameter(Mandatory = $true)][object]$Before,
        [Parameter(Mandatory = $true)][object]$After
    )
    $tables = @()
    foreach ($tableName in @(
        'seat_inventory', 'reservations', 'reservation_seats',
        'ticket_orders', 'tickets', 'idempotency_records'
    )) {
        $beforeTable = Get-ObjectPropertyValue -Object $Before -Name $tableName
        $afterTable = Get-ObjectPropertyValue -Object $After -Name $tableName
        $beforeRows = [int64](Get-ObjectPropertyValue -Object $beforeTable -Name 'rows')
        $afterRows = [int64](Get-ObjectPropertyValue -Object $afterTable -Name 'rows')
        $beforeHash = [string](Get-ObjectPropertyValue -Object $beforeTable -Name 'fingerprint')
        $afterHash = [string](Get-ObjectPropertyValue -Object $afterTable -Name 'fingerprint')
        if ($beforeRows -ne $afterRows -or $beforeHash -ne $afterHash) {
            throw "retained legacy source changed after cutover in $tableName"
        }
        $tables += [ordered]@{ table = $tableName; rows = $afterRows; unchanged = $true }
    }
    return [ordered]@{ unchanged = $true; tables = $tables }
}

function Invoke-ShardAdmin {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$Artifact,
        [switch]$AllowFailure
    )
    $result = Invoke-Compose -AllowFailure:$AllowFailure -Arguments (@(
        '--profile', 'tools', 'run', '--rm', '-T', '--no-deps', 'shard-admin'
    ) + $Arguments) -CapturePath (Join-Path $script:EvidenceDirectory $Artifact)
    $jsonLine = @($result.Output | ForEach-Object { [string]$_ } | Where-Object {
        $_.TrimStart().StartsWith('{') -and $_.TrimEnd().EndsWith('}')
    }) | Select-Object -Last 1
    $envelope = $null
    if (-not [string]::IsNullOrWhiteSpace([string]$jsonLine)) {
        try { $envelope = $jsonLine | ConvertFrom-Json } catch { $envelope = $null }
    }
    if (-not $AllowFailure -and ($result.ExitCode -ne 0 -or $null -eq $envelope)) {
        throw 'shard-admin invocation failed or omitted its structured envelope'
    }
    return [pscustomobject]@{ ExitCode = $result.ExitCode; Envelope = $envelope }
}

function Invoke-Reconcile {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$Artifact,
        [switch]$AllowFailure
    )
    $timer = [Diagnostics.Stopwatch]::StartNew()
    $result = Invoke-Compose -AllowFailure:$AllowFailure -Arguments (@(
        '--profile', 'tools', 'run', '--rm', '-T', '--no-deps', 'reconcile'
    ) + $Arguments) -CapturePath (Join-Path $script:EvidenceDirectory $Artifact)
    $timer.Stop()
    $jsonLine = @($result.Output | ForEach-Object { [string]$_ } | Where-Object {
        $_.TrimStart().StartsWith('{') -and $_.TrimEnd().EndsWith('}')
    }) | Select-Object -Last 1
    $envelope = $null
    if (-not [string]::IsNullOrWhiteSpace([string]$jsonLine)) {
        try { $envelope = $jsonLine | ConvertFrom-Json } catch { $envelope = $null }
    }
    if (-not $AllowFailure -and ($result.ExitCode -ne 0 -or $null -eq $envelope)) {
        throw 'bounded reconcile invocation failed or omitted its structured envelope'
    }
    return [pscustomobject]@{
        ExitCode = $result.ExitCode
        Envelope = $envelope
        DurationMilliseconds = [Math]::Round($timer.Elapsed.TotalMilliseconds, 3)
    }
}

function Invoke-HealthyReconcile {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$Artifact,
        [ValidateRange(1, 60)][int]$Attempts = 1
    )
    $lastInvocation = $null
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $lastInvocation = Invoke-Reconcile -Arguments $Arguments -Artifact $Artifact -AllowFailure
        if ($lastInvocation.ExitCode -eq 0 -and
            $null -ne $lastInvocation.Envelope -and
            [string]$lastInvocation.Envelope.command -eq [string]$Arguments[0] -and
            [string]$lastInvocation.Envelope.status -eq 'healthy' -and
            [bool]$lastInvocation.Envelope.read_only -and
            $null -ne $lastInvocation.Envelope.result) {
            return $lastInvocation
        }
        if ($attempt -lt $Attempts) { Start-Sleep -Milliseconds 500 }
    }
    $lastStatus = if ($null -ne $lastInvocation -and $null -ne $lastInvocation.Envelope) {
        [string]$lastInvocation.Envelope.status
    } else { 'missing' }
    throw "$($Arguments[0]) reconciliation did not become healthy within $Attempts bounded attempts (last_status=$lastStatus)"
}

function Add-PostgresConnectionSample {
    param([Parameter(Mandatory = $true)][string]$Label)
    $sample = Invoke-PSQL -Artifact "postgres-connections-$Label.log" -SQL @"
SELECT count(*)::bigint
FROM pg_stat_activity
WHERE datname = current_database();
"@
    $value = [string](@($sample.Output | Where-Object {
        ([string]$_).Trim() -match '^[0-9]+$'
    }) | Select-Object -Last 1)
    if ([string]::IsNullOrWhiteSpace($value)) {
        throw "PostgreSQL connection sample $Label was not numeric"
    }
    $script:postgresConnectionSamples.Add([pscustomobject][ordered]@{
        label = $Label
        connections = [int64]$value.Trim()
    })
}

function Measure-RedisLatency {
    $result = Invoke-Compose -Arguments @(
        'exec', '-T', 'redis', 'redis-benchmark', '-q', '-t', 'ping', '-n', '1000', '-c', '10'
    ) -CapturePath (Join-Path $script:EvidenceDirectory 'redis-ping-latency.log')
    $measurements = @()
    foreach ($line in $result.Output) {
        $text = [string]$line
        if ($text -match '^(?<operation>PING_[A-Z]+):.*p50=(?<p50>[0-9.]+)\s+msec') {
            $measurements += [ordered]@{
                operation = $matches.operation
                p50_ms = [double]::Parse($matches.p50, [Globalization.CultureInfo]::InvariantCulture)
            }
        }
    }
    if ($measurements.Count -eq 0) {
        throw 'bounded Redis PING benchmark omitted latency measurements'
    }
    $script:redisLatencyEvidence = [ordered]@{
        requests = 1000
        clients = 10
        measurements = $measurements
        artifact = 'redis-ping-latency.log'
    }
}

function Set-K6Environment {
    param([hashtable]$Values)
    foreach ($entry in $Values.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable([string]$entry.Key, [string]$entry.Value, 'Process')
    }
}

function Invoke-K6 {
    param(
        [Parameter(Mandatory = $true)][string]$Script,
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][hashtable]$Environment
    )
    Set-K6Environment -Values $Environment
    $environmentArguments = @()
    foreach ($environmentName in ($Environment.Keys | Sort-Object)) {
        $environmentArguments += @('-e', [string]$environmentName)
    }
    $summaryPath = Join-Path $script:EvidenceDirectory "$Name-summary.json"
    Initialize-Milestone4K6SummaryFile -Path $summaryPath
    $arguments = @(
        '--profile', 'tools', 'run', '--rm', '-T', '--no-deps', '-v', "${EvidenceDirectory}:/evidence"
    ) + $environmentArguments + @(
        'k6', 'run', '--summary-export', "/evidence/$Name-summary.json", "/scripts/$Script"
    )
    Invoke-Compose -Arguments $arguments -CapturePath (Join-Path $script:EvidenceDirectory "$Name.log") | Out-Null
}

function Start-K6 {
    param(
        [Parameter(Mandatory = $true)][string]$Script,
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][hashtable]$Environment
    )
    Set-K6Environment -Values $Environment
    $containerName = "$ProjectName-$Name"
    $environmentArguments = @()
    foreach ($variable in ($Environment.Keys | Sort-Object)) { $environmentArguments += @('-e', [string]$variable) }
    $summaryPath = Join-Path $script:EvidenceDirectory "$Name-summary.json"
    Initialize-Milestone4K6SummaryFile -Path $summaryPath
    $arguments = @(
        '--profile', 'tools', 'run', '-d', '--name', $containerName, '--no-deps', '-v', "${EvidenceDirectory}:/evidence"
    ) + $environmentArguments + @(
        'k6', 'run', '--summary-export', "/evidence/$Name-summary.json", "/scripts/$Script"
    )
    $result = Invoke-Compose -Arguments $arguments
    if ($result.ExitCode -ne 0) { throw 'background k6 container failed to start' }
    return $containerName
}

function Wait-K6 {
    param([Parameter(Mandatory = $true)][string]$ContainerName, [Parameter(Mandatory = $true)][string]$Name)
    $wait = Invoke-Native -Command { & docker wait $ContainerName }
    $logs = Invoke-Native -AllowFailure -Command { & docker logs $ContainerName }
    $logs.Output | Out-File -LiteralPath (Join-Path $script:EvidenceDirectory "$Name.log") -Encoding utf8
    Invoke-Native -AllowFailure -Command { & docker rm $ContainerName } | Out-Null
    $exitText = [string](@($wait.Output) | Select-Object -Last 1)
    if ($exitText.Trim() -ne '0') { throw "$Name k6 workload failed its bounded thresholds" }
}

function Stop-K6 {
    param([Parameter(Mandatory = $true)][string]$ContainerName, [Parameter(Mandatory = $true)][string]$Name)
    $inspect = Invoke-Native -AllowFailure -Command { & docker inspect $ContainerName }
    if ($inspect.ExitCode -ne 0) { return }
    Invoke-Native -AllowFailure -Command { & docker stop -t 5 $ContainerName } | Out-Null
    $logs = Invoke-Native -AllowFailure -Command { & docker logs $ContainerName }
    $logs.Output | Out-File -LiteralPath (Join-Path $script:EvidenceDirectory "$Name.log") -Encoding utf8
    Invoke-Native -AllowFailure -Command { & docker rm -f $ContainerName } | Out-Null
}

function Save-Metrics {
    param([Parameter(Mandatory = $true)][string]$Label)
    foreach ($service in @('api-1', 'api-2', 'api-3')) {
        Invoke-Compose -Arguments @(
            'exec', '-T', $service, 'wget', '-q', '-T', '3', '-O', '-', 'http://127.0.0.1:8080/metrics'
        ) -CapturePath (Join-Path $script:EvidenceDirectory "$Label-$service.prom") | Out-Null
    }
}

function Get-APIMetricTotal {
    param(
        [Parameter(Mandatory = $true)][string]$Family,
        [string]$RequiredLabel = '',
        [string[]]$Services = @('api-1', 'api-2', 'api-3')
    )
    $total = 0.0
    foreach ($service in $Services) {
        $result = Invoke-Compose -Arguments @(
            'exec', '-T', $service, 'wget', '-q', '-T', '3', '-O', '-', 'http://127.0.0.1:8080/metrics'
        )
        foreach ($line in $result.Output) {
            if ([string]$line -notmatch "^$([regex]::Escape($Family))\{(?<labels>[^}]*)\}\s+(?<value>[0-9eE+.-]+)$") {
                continue
            }
            $labels = [string]$matches.labels
            $metricValue = [string]$matches.value
            if (Test-Milestone4MetricLabels -Labels $labels -Required $RequiredLabel) {
                $total += [double]::Parse($metricValue, [Globalization.CultureInfo]::InvariantCulture)
            }
        }
    }
    return $total
}

function Wait-WorkloadBarrier {
    param([double]$Before, [string]$ContainerName, [int]$Attempts = 30)
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $inspect = Invoke-Native -AllowFailure -Command { & docker inspect -f '{{.State.Running}}' $ContainerName }
        if ($inspect.ExitCode -ne 0 -or ([string](@($inspect.Output) | Select-Object -Last 1)).Trim() -ne 'true') {
            throw 'cutover workload exited before reaching the explicit route-metric barrier'
        }
        if ((Get-APIMetricTotal -Family 'shard_route_total') -gt $Before) { return }
        Start-Sleep -Milliseconds 500
    }
    throw 'cutover workload did not cross the explicit route-metric barrier'
}

function Invoke-MigrationToCutoverReady {
    param(
        [Parameter(Mandatory = $true)][string]$TrainRunID,
        [Parameter(Mandatory = $true)][string]$TargetShard,
        [Parameter(Mandatory = $true)][string]$MigrationID,
        [Parameter(Mandatory = $true)][string]$Prefix
    )
    Invoke-ShardAdmin -Arguments @(
        'plan-migration', '--train-run-id', $TrainRunID, '--target-shard', $TargetShard,
        '--migration-id', $MigrationID, '--rollback-window', '5m', '--confirm', '--timeout', '30s'
    ) -Artifact "$Prefix-plan.json" | Out-Null
    $copyTimer = [Diagnostics.Stopwatch]::StartNew()
    $state = 'planned'
    $finalEnvelope = $null
    for ($page = 1; $page -le 100 -and $state -notin @('validating', 'cutover_ready'); $page++) {
        $command = if ($page -eq 1) { 'start-migration' } else { 'resume-migration' }
        $copy = Invoke-ShardAdmin -Arguments @(
            $command, '--migration-id', $MigrationID, '--batch-size', '100', '--confirm', '--timeout', '30s'
        ) -Artifact "$Prefix-copy-$page.json"
        $finalEnvelope = $copy.Envelope
        $state = Get-Milestone4MigrationState -Envelope $copy.Envelope
    }
    $copyTimer.Stop()
    if ($state -notin @('validating', 'cutover_ready')) {
        throw 'migration copy did not converge within 100 bounded batches'
    }
    $validationDurationMilliseconds = 0.0
    if ($state -eq 'validating') {
        $validationTimer = [Diagnostics.Stopwatch]::StartNew()
        $validation = Invoke-ShardAdmin -Arguments @(
            'validate-migration', '--migration-id', $MigrationID, '--row-cap', '10000',
            '--confirm', '--timeout', '30s'
        ) -Artifact "$Prefix-validate.json"
        $validationTimer.Stop()
        $validationDurationMilliseconds = $validationTimer.Elapsed.TotalMilliseconds
        $finalEnvelope = $validation.Envelope
        $state = Get-Milestone4MigrationState -Envelope $validation.Envelope
    }
    if ($state -ne 'cutover_ready') { throw 'migration validation did not reach cutover_ready' }
    $record = Get-Milestone4AdminResult -Envelope $finalEnvelope
    $copiedRows = [int64](Get-ObjectPropertyValue -Object $record -Name 'copied_rows')
    $validationResult = Get-ObjectPropertyValue -Object $record -Name 'validation'
    $validationRows = [int64](Get-ObjectPropertyValue -Object $validationResult -Name 'rows_examined')
    if ($copiedRows -lt 0 -or $validationRows -le 0 -or $copyTimer.Elapsed.TotalSeconds -le 0) {
        throw 'migration timing evidence is incomplete'
    }
    $script:migrationEvidence[$Prefix] = [ordered]@{
        copied_rows = $copiedRows
        copy_duration_ms = [Math]::Round($copyTimer.Elapsed.TotalMilliseconds, 3)
        copy_rows_per_second = [Math]::Round($copiedRows / $copyTimer.Elapsed.TotalSeconds, 3)
        validation_duration_ms = [Math]::Round($validationDurationMilliseconds, 3)
        validation_rows_examined = $validationRows
    }
}

function Invoke-Cutover {
    param(
        [Parameter(Mandatory = $true)][string]$MigrationID,
        [Parameter(Mandatory = $true)][string]$Prefix
    )
    $cutoverTimer = [Diagnostics.Stopwatch]::StartNew()
    $cutover = Invoke-ShardAdmin -Arguments @(
        'cutover', '--migration-id', $MigrationID, '--row-cap', '10000',
        '--locator-row-cap', '10000', '--confirm', '--timeout', '30s'
    ) -Artifact "$Prefix-cutover.json"
    $cutoverTimer.Stop()
    if ((Get-Milestone4MigrationState -Envelope $cutover.Envelope) -ne 'rollback_window') {
        throw 'cutover did not reach rollback_window'
    }
    if (-not $script:migrationEvidence.Contains($Prefix)) {
        throw 'cutover timing could not be associated with migration evidence'
    }
    $script:migrationEvidence[$Prefix]['cutover_command_duration_ms'] =
        [Math]::Round($cutoverTimer.Elapsed.TotalMilliseconds, 3)
}

function Get-ObjectPropertyValue {
    param([object]$Object, [Parameter(Mandatory = $true)][string]$Name)
    if ($null -eq $Object) { return $null }
    $property = $Object.PSObject.Properties |
        Where-Object { $_.Name -eq $Name } | Select-Object -First 1
    if ($null -eq $property) { return $null }
    return $property.Value
}

function Get-K6Summary {
    param([Parameter(Mandatory = $true)][string]$Name)
    $path = Join-Path $script:EvidenceDirectory "$Name-summary.json"
    if (-not (Test-Path -LiteralPath $path)) { return $null }
    $summary = Get-Content -Raw -LiteralPath $path | ConvertFrom-Json
    $metrics = Get-ObjectPropertyValue -Object $summary -Name 'metrics'
    if ($null -eq $metrics) { return $null }
    $unexpectedMetric = Get-ObjectPropertyValue -Object $metrics -Name 'unexpected_5xx'
    $rebalancingMetric = Get-ObjectPropertyValue -Object $metrics -Name 'expected_rebalancing_503'
    $outageMetric = Get-ObjectPropertyValue -Object $metrics -Name 'expected_outage_503'
    $unexpectedValues = Get-Milestone4K6MetricValues -Metric $unexpectedMetric
    $rebalancingValues = Get-Milestone4K6MetricValues -Metric $rebalancingMetric
    $outageValues = Get-Milestone4K6MetricValues -Metric $outageMetric
    $result = ConvertFrom-Milestone4K6CoreSummary -Summary $summary -Name $Name
    $result['unexpected_5xx'] = if ($null -ne $unexpectedValues) {
        Get-ObjectPropertyValue -Object $unexpectedValues -Name 'count'
    } else { 0 }
    $result['expected_rebalancing_503'] = if ($null -ne $rebalancingValues) {
        Get-ObjectPropertyValue -Object $rebalancingValues -Name 'count'
    } else { 0 }
    $result['expected_outage_503'] = if ($null -ne $outageValues) {
        Get-ObjectPropertyValue -Object $outageValues -Name 'count'
    } else { 0 }
    foreach ($counterName in @(
        'shard_routing_success', 'shard_rate_limited', 'shard_allocation_conflicts',
        'partial_shard_results', 'stale_refresh_success',
        'post_cutover_lifecycle_success', 'post_cutover_ticket_order_read_success'
    )) {
        $metric = Get-ObjectPropertyValue -Object $metrics -Name $counterName
        $values = Get-Milestone4K6MetricValues -Metric $metric
        $result[$counterName] = if ($null -ne $values) {
            $count = Get-ObjectPropertyValue -Object $values -Name 'count'
            if ($null -eq $count) { 0 } else { $count }
        } else { 0 }
    }
    $trends = [ordered]@{}
    foreach ($trendName in @(
        'booking_success_duration', 'legacy_shard_duration', 'schema_shard_duration',
        'shard_a_duration', 'shard_b_duration', 'cutover_rejection_elapsed_ms'
    )) {
        $metric = Get-ObjectPropertyValue -Object $metrics -Name $trendName
        $values = Get-Milestone4K6MetricValues -Metric $metric
        if ($null -eq $values) { continue }
        $trend = [ordered]@{}
        foreach ($statistic in @('min', 'med', 'p(95)', 'p(99)', 'max')) {
            $value = Get-ObjectPropertyValue -Object $values -Name $statistic
            if ($null -ne $value) { $trend[$statistic] = $value }
        }
        if ($trend.Count -gt 0) { $trends[$trendName] = $trend }
    }
    $result['trends'] = $trends
    return $result
}

function Assert-ArtifactsSanitized {
    foreach ($file in Get-ChildItem -LiteralPath $script:EvidenceDirectory -File) {
        $content = Get-Content -Raw -LiteralPath $file.FullName -ErrorAction SilentlyContinue
        if ([string]::IsNullOrEmpty($content)) { continue }
        if ($content -match 'postgres(?:ql)?://[^\s"'']+' -or $content -match 'Bearer\s+[A-Za-z0-9._-]+') {
            throw 'evidence artifact contained a forbidden credential-shaped value'
        }
        foreach ($secret in $script:secretValues) {
            if (-not [string]::IsNullOrWhiteSpace($secret) -and $content.Contains($secret)) {
                throw 'evidence artifact contained an in-memory synthetic credential'
            }
        }
    }
}

$baseURL = ''
Write-EvidenceStatus -Status 'running' -Reason 'evidence_in_progress'

try {
    Push-Location $root
    $failureCategory = 'source_provenance'
    $workingTree = @(& git status --porcelain=v1 --untracked-files=all 2>$null)
    if ($LASTEXITCODE -ne 0 -or $workingTree.Count -ne 0) {
        throw 'Milestone 4 runtime evidence requires a clean committed working tree'
    }
    $evidenceCommit = (& git rev-parse HEAD 2>$null | Out-String).Trim()
    if ($LASTEXITCODE -ne 0 -or $evidenceCommit -notmatch '^[0-9a-f]{40}$') {
        throw 'Milestone 4 runtime evidence could not resolve a committed source revision'
    }
    $failureCategory = 'docker_preflight'
    if ($null -eq (Get-Command docker -CommandType Application -ErrorAction SilentlyContinue)) {
        $failureCategory = 'docker_unavailable'
        throw 'Docker CLI is unavailable'
    }
    if ((Invoke-Native -AllowFailure -Command { & docker version }).ExitCode -ne 0) {
        $failureCategory = 'docker_unavailable'
        throw 'Docker Engine is unavailable'
    }
    if ((Invoke-Native -AllowFailure -Command { & docker compose version }).ExitCode -ne 0) {
        $failureCategory = 'compose_unavailable'
        throw 'Docker Compose v2 is unavailable'
    }

    $failureCategory = 'compose_project_preflight'
    Assert-Milestone4ComposeProjectUnused -ProjectName $ProjectName -DockerInvoker {
        param([string[]]$DockerArguments)
        Invoke-Native -AllowFailure -Command { & docker @DockerArguments }
    }
    $started = $true
    $failureCategory = 'compose_startup'
    Invoke-Compose -Arguments @('up', '-d', '--build') `
        -CapturePath (Join-Path $EvidenceDirectory 'compose-up.log') | Out-Null
    $failureCategory = 'operator_cli_build'
    $adminBuild = Invoke-Compose -AllowFailure -Arguments @('--profile', 'tools', 'build', 'shard-admin') `
        -CapturePath (Join-Path $EvidenceDirectory 'shard-admin-build.log')
    if ($adminBuild.ExitCode -ne 0) {
        $failureCategory = 'operator_cli_unrun'
        throw 'hardened shard-admin image could not be built'
    }
    $reconcileBuild = Invoke-Compose -AllowFailure -Arguments @('--profile', 'tools', 'build', 'reconcile') `
        -CapturePath (Join-Path $EvidenceDirectory 'reconcile-build.log')
    if ($reconcileBuild.ExitCode -ne 0) {
        $failureCategory = 'reconcile_cli_unrun'
        throw 'bounded reconcile image could not be built'
    }
    $failureCategory = 'service_readiness'
    foreach ($service in @('api-1', 'api-2', 'api-3')) { Wait-Ready -Service $service -Port 8080 }
    foreach ($service in @('admission-worker-1', 'admission-worker-2')) { Wait-Ready -Service $service -Port 9090 }
    foreach ($service in @(
        'read-model-worker-1', 'read-model-worker-2', 'hold-expirer', 'outbox-worker'
    )) { Wait-Ready -Service $service -Port 9090 }
    $baseURL = Get-PublishedURL
    Add-PostgresConnectionSample -Label 'ready'
    Measure-RedisLatency

    $failureCategory = 'fixture_setup'
    $fixtureSQL = Get-Content -Raw -LiteralPath $fixtureFile
    $fixtureLoad = Invoke-Native -Command {
        $fixtureSQL | & docker @script:compose exec -T postgres psql -U railway -d railway -v ON_ERROR_STOP=1
    }
    $fixtureLoad.Output | Out-File -LiteralPath (Join-Path $EvidenceDirectory 'fixture-load.log') -Encoding utf8
    Invoke-PSQL -Artifact 'fixture-m4-adjustments.log' -SQL @"
UPDATE public.hot_train_policies
SET enabled=false, updated_at=clock_timestamp()
WHERE train_run_id IN ('$fixtureTrainA'::uuid, '$fixtureTrainB'::uuid);
INSERT INTO public.hot_train_policies (
    id, train_run_id, seat_class, enabled, version, max_queue_size,
    admission_rate_per_second, max_inflight_admissions,
    admission_token_ttl_seconds, processing_lease_seconds, queue_entry_ttl_seconds
) VALUES (
    '21000000-0000-4000-8000-000000000501', '$fixtureTrainB', 'standard', false,
    1, 1000, 5, 5, 240, 5, 300
)
ON CONFLICT (train_run_id, seat_class) DO UPDATE SET enabled=false, updated_at=clock_timestamp();
"@ | Out-Null

    $failureCategory = 'global_read_cache_prewarm'
    $serviceDateResult = Invoke-PSQL -Artifact 'fixture-service-date.log' -SQL @"
SELECT service_date::text FROM public.train_runs WHERE id='$fixtureTrainA'::uuid;
"@
    $serviceDate = [string](@($serviceDateResult.Output | Where-Object {
        ([string]$_).Trim() -match '^\d{4}-\d{2}-\d{2}$'
    }) | Select-Object -Last 1)
    if ($serviceDate.Trim() -notmatch '^\d{4}-\d{2}-\d{2}$') {
        throw 'fixture service date could not be resolved for cache prewarm'
    }
    $stationPrewarm = Invoke-API -Method GET -Path '/api/v1/stations?page=1&limit=100&sort=code'
    $searchPrewarm = Invoke-API -Method GET -Path (
        "/api/v1/train-runs/search?origin_station_code=$origin&destination_station_code=$destination" +
        "&service_date=$($serviceDate.Trim())&page=1&limit=100&sort=departure_at"
    )
    if ($stationPrewarm.StatusCode -ne 200 -or $searchPrewarm.StatusCode -ne 200) {
        throw 'global station or train-search cache prewarm failed'
    }

    $failureCategory = 'operator_health_baseline'
    $health = Invoke-ShardAdmin -Arguments @('inspect-health', '--timeout', '30s') -Artifact 'operator-health-baseline.json' -AllowFailure
    if ($health.ExitCode -ne 0 -or $null -eq $health.Envelope) {
        $failureCategory = 'operator_cli_unrun'
        throw 'hardened shard-admin service is unavailable'
    }
    $operatorCLI = 'run'
    $operatorHealthEvidence['baseline'] = Assert-Milestone4OperatorHealth `
        -Invocation $health -ExpectedReady $true
    $failureCategory = 'admin_fanout_baseline'
    $adminBaseline = Invoke-Reconcile -Arguments @(
        'shard-assignments', '--page-size', '100', '--max-pages', '1000',
        '--max-rows', '100000', '--timeout', '30s'
    ) -Artifact 'admin-fanout-complete-before.json'
    $adminFanoutEvidence['complete_before'] = Assert-BoundedShardReport `
        -Invocation $adminBaseline -Expected 'complete'

    $failureCategory = 'synthetic_customer_setup'
    $password = "M4-$suffix-Aa1!Evidence"
    $secretValues.Add($password)
    for ($index = 1; $index -le $CustomerCount; $index++) {
        $email = "m4-$suffix-$index@example.test"
        $forwardedFor = "198.19.0.$($index + 10)"
        $register = Invoke-API -Method POST -Path '/api/v1/auth/register' -Body @{
            email = $email; password = $password; display_name = "M4 Synthetic Rider $index"
        } -ForwardedFor $forwardedFor
        if ($register.StatusCode -ne 202) { throw 'synthetic registration failed' }
        $login = Invoke-API -Method POST -Path '/api/v1/auth/login' `
            -Body @{ email = $email; password = $password } -ForwardedFor $forwardedFor
        $token = [string]$login.Body.access_token
        if ([string]::IsNullOrWhiteSpace($token)) { throw 'synthetic login omitted access token' }
        $secretValues.Add($token)
        $passenger = Invoke-API -Method POST -Path '/api/v1/passengers' -Token $token -Body @{
            display_name = "M4 Load Passenger $index"
        }
        if ($passenger.StatusCode -ne 201 -or [string]::IsNullOrWhiteSpace([string]$passenger.Body.id)) {
            throw 'synthetic passenger creation failed'
        }
        $customers += [pscustomobject]@{ Token = $token; PassengerID = [string]$passenger.Body.id }
    }
    $password = $null

    $fixtureCustomers = @($customers[0..3])
    $routingCustomers = @($customers[4..6])
    $cacheCustomers = @($customers[7..9])
    $staleCustomers = @($customers[10..12])
    $lifecycleCustomers = @($customers[13..14])
    $overlapCustomers = @($customers[15..19])

    $failureCategory = 'seed_reservations'
    $seedReservations = @()
    $expiredReservations = @()
    foreach ($trainRunID in @($fixtureTrainA, $fixtureTrainB)) {
        $createdByState = [ordered]@{}
        foreach ($stateSeed in @(
            @{ State = 'held'; Customer = $fixtureCustomers[0] },
            @{ State = 'confirmed'; Customer = $fixtureCustomers[1] },
            @{ State = 'cancelled'; Customer = $fixtureCustomers[2] },
            @{ State = 'expired'; Customer = $fixtureCustomers[3] }
        )) {
            $state = [string]$stateSeed.State
            $customer = $stateSeed.Customer
            $createKey = "m4-seed-$suffix-$($trainRunID.Substring(35))-$state"
            $created = Invoke-API -Method POST -Path '/api/v1/reservations' `
                -Token $customer.Token -IdempotencyKey $createKey -Body @{
                    train_run_id = $trainRunID
                    origin_station_code = $origin
                    destination_station_code = $destination
                    seat_class = $seatClass
                    passenger_ids = @($customer.PassengerID)
            }
            $reservationID = [string]$created.Body.id
            $parsedReservationID = [guid]::Empty
            if ($created.StatusCode -ne 201 -or
                -not [guid]::TryParse($reservationID, [ref]$parsedReservationID)) {
                throw "deterministic $state migration fixture creation failed"
            }
            $createdByState[$state] = [ordered]@{ ID = $reservationID; Customer = $customer }
        }
        $seedReservations += [string]$createdByState.held.ID

        foreach ($action in @('confirm', 'cancel')) {
            $seedState = if ($action -eq 'confirm') { 'confirmed' } else { 'cancelled' }
            $record = $createdByState[$seedState]
            $mutated = Invoke-API -Method POST -Path "/api/v1/reservations/$($record.ID)/$action" `
                -Token $record.Customer.Token `
                -IdempotencyKey "m4-seed-$suffix-$($trainRunID.Substring(35))-$action"
            if ($mutated.StatusCode -ne 200 -or [string]$mutated.Body.status -ne $seedState) {
                throw "deterministic $seedState migration fixture transition failed"
            }
        }
        $expiredReservations += [pscustomobject]@{
            ID = [string]$createdByState.expired.ID
            Token = [string]$createdByState.expired.Customer.Token
        }
    }
    $expiryIDs = ($expiredReservations.ID | ForEach-Object { "'$($_)'::uuid" }) -join ','
    Invoke-PSQL -Artifact 'migration-fixture-expiry-arm.log' -SQL @"
UPDATE public.reservations
SET expires_at = clock_timestamp() - interval '1 minute'
WHERE id IN ($expiryIDs);
"@ | Out-Null
    foreach ($expired in $expiredReservations) {
        Wait-ReservationStatus -ReservationID $expired.ID -Token $expired.Token -ExpectedStatus 'expired'
    }
    $fixtureLifecycleEvidence = [ordered]@{
        pre_copy = [ordered]@{
            train_runs = 2
            held = 2
            confirmed = 2
            cancelled = 2
            expired = 2
            ticket_orders = 2
            tickets = 2
            idempotency_and_outbox = 'validated_by_copy_and_reconciliation'
        }
    }

    $commonK6 = @{
        BASE_URL = 'http://load-balancer:8080'
        CUSTOMER_TOKEN = $customers[0].Token
        CUSTOMER_TOKENS = ($routingCustomers.Token -join ',')
        PASSENGER_IDS = ($routingCustomers.PassengerID -join ',')
        RESERVATION_IDS = ($seedReservations -join ',')
        TRAIN_RUN_IDS = "$fixtureTrainA,$fixtureTrainB"
        ORIGIN_CODE = $origin
        DESTINATION_CODE = $destination
        SEAT_CLASS = $seatClass
    }

    $failureCategory = 'baseline_metrics'
    Save-Metrics -Label 'baseline'
    $failureCategory = 'shard_routing_workload'
    Invoke-K6 -Script 'shard-routing.js' -Name 'shard-routing' -Environment ($commonK6 + @{
        VUS = '3'
        ITERATIONS = '48'; MAX_DURATION = '2m'; ALLOW_REBALANCING_503 = 'no'
    })
    $failureCategory = 'route_cache_workload'
    $routeCacheHitsBefore = Get-APIMetricTotal -Family 'shard_route_cache_total' -RequiredLabel 'result="hit"'
    $routeCacheMissesBefore = Get-APIMetricTotal -Family 'shard_route_cache_total' -RequiredLabel 'result="miss"'
    $cacheEnvironment = $commonK6.Clone()
    $cacheEnvironment['CUSTOMER_TOKENS'] = ($cacheCustomers.Token -join ',')
    $cacheEnvironment['PASSENGER_IDS'] = ($cacheCustomers.PassengerID -join ',')
    $cacheEnvironment['TRAIN_RUN_ID'] = $fixtureTrainA
    $cacheEnvironment['VUS'] = '3'
    $cacheEnvironment['DURATION'] = $LoadDuration
    Invoke-K6 -Script 'shard-route-cache.js' -Name 'shard-route-cache' -Environment $cacheEnvironment
    $failureCategory = 'route_cache_metric'
    $routeCacheHits = Get-APIMetricTotal -Family 'shard_route_cache_total' -RequiredLabel 'result="hit"'
    $routeCacheMisses = Get-APIMetricTotal -Family 'shard_route_cache_total' -RequiredLabel 'result="miss"'
    $routeCacheHitDelta = $routeCacheHits - $routeCacheHitsBefore
    $routeCacheMissDelta = $routeCacheMisses - $routeCacheMissesBefore
    $routeCacheLookups = $routeCacheHitDelta + $routeCacheMissDelta
    if ($routeCacheHitDelta -le 0 -or $routeCacheLookups -le 0) {
        throw 'route-cache workload produced no bounded cache-hit ratio evidence'
    }
    $routeCacheHitRatio = [Math]::Round($routeCacheHitDelta / $routeCacheLookups, 6)

    $failureCategory = 'route_cache_prewarm'
    $apis = @('api-1', 'api-2', 'api-3')
    for ($index = 0; $index -lt $apis.Count; $index++) {
        $api = $apis[$index]
        $prewarmCustomer = $staleCustomers[$index]
        $prewarmEnvironment = $commonK6.Clone()
        $prewarmEnvironment['BASE_URL'] = "http://${api}:8080"
        $prewarmEnvironment['CUSTOMER_TOKENS'] = $prewarmCustomer.Token
        $prewarmEnvironment['PASSENGER_IDS'] = $prewarmCustomer.PassengerID
        $prewarmEnvironment['TRAIN_RUN_ID'] = $fixtureTrainA
        Invoke-K6 -Script 'shard-route-prewarm.js' -Name "prewarm-$api" -Environment $prewarmEnvironment
    }
    $failureCategory = 'train_a_read_model_catchup'
    $trainAPreCutoverReceiptCount = Wait-TrainRunReadModelCaughtUp -TrainRunID $fixtureTrainA
    $assignmentBeforeCutover = Get-AssignmentState -TrainRunID $fixtureTrainA `
        -Artifact 'train-a-assignment-before-cutover.json'
    $availabilityVersionBefore = Get-AvailabilityCacheVersion -TrainRunID $fixtureTrainA
    if ([string]::IsNullOrWhiteSpace($availabilityVersionBefore)) {
        throw 'train A prewarm did not establish an availability cache namespace'
    }
    $replicaMetricBaselines = [ordered]@{}
    foreach ($api in $apis) {
        $replicaMetricBaselines[$api] = [ordered]@{
            stale_write = Get-APIMetricTotal -Family 'shard_assignment_stale_total' `
                -RequiredLabel 'operation="write",shard_id="legacy"' -Services @($api)
            refresh_success = Get-APIMetricTotal -Family 'shard_route_refresh_total' `
                -RequiredLabel 'operation="refresh",result="success",shard_id="shard-0"' -Services @($api)
            target_write = Get-APIMetricTotal -Family 'shard_route_total' `
                -RequiredLabel 'operation="write",result="success",reason="none",shard_id="shard-0"' -Services @($api)
        }
    }
    $refreshDurationSumBefore = Get-APIMetricTotal -Family 'shard_request_duration_seconds_sum' `
        -RequiredLabel 'operation="refresh",result="success",shard_id="shard-0"'
    $refreshDurationCountBefore = Get-APIMetricTotal -Family 'shard_request_duration_seconds_count' `
        -RequiredLabel 'operation="refresh",result="success",shard_id="shard-0"'
    $failureCategory = 'train_a_migration'
    Invoke-MigrationToCutoverReady -TrainRunID $fixtureTrainA -TargetShard 'shard-0' `
        -MigrationID $migrationA -Prefix 'train-a'

    $failureCategory = 'train_a_cutover'
    Invoke-Cutover -MigrationID $migrationA -Prefix 'train-a'
    $assignmentAfterCutover = Get-AssignmentState -TrainRunID $fixtureTrainA `
        -Artifact 'train-a-assignment-after-cutover.json'
    if ([int64]$assignmentAfterCutover.assignment_generation -le [int64]$assignmentBeforeCutover.assignment_generation -or
        [int64]$assignmentAfterCutover.availability_generation -le [int64]$assignmentBeforeCutover.availability_generation -or
        [string]$assignmentAfterCutover.shard_id -ne 'shard-0') {
        throw 'train A cutover did not rotate assignment and availability generations onto shard-0'
    }
    $trainACutoverEventID = Get-CutoverOutboxEventID -TrainRunID $fixtureTrainA `
        -ShardID 'shard-0' -AssignmentGeneration ([int64]$assignmentAfterCutover.assignment_generation)
    Wait-ReadModelReceipt -EventID $trainACutoverEventID
    $availabilityVersionAfter = Wait-AvailabilityVersionRotated -TrainRunID $fixtureTrainA `
        -Previous $availabilityVersionBefore
    $availabilityEvidence['train-a'] = [ordered]@{
        assignment_generation_before = [int64]$assignmentBeforeCutover.assignment_generation
        assignment_generation_after = [int64]$assignmentAfterCutover.assignment_generation
        availability_generation_before = [int64]$assignmentBeforeCutover.availability_generation
        availability_generation_after = [int64]$assignmentAfterCutover.availability_generation
        pre_cutover_receipted_event_count = $trainAPreCutoverReceiptCount
        exact_cutover_event_receipted = $true
        redis_namespace_rotated = ($availabilityVersionAfter -ne $availabilityVersionBefore)
    }
    $legacySourceFingerprints['train-a'] = Get-LegacySourceFingerprint `
        -TrainRunID $fixtureTrainA -Artifact 'train-a-legacy-source-after-cutover.json'

    $failureCategory = 'stale_router_refresh_workload'
    $refreshEnvironment = $commonK6.Clone()
    $refreshEnvironment['CUSTOMER_TOKENS'] = ($staleCustomers.Token -join ',')
    $refreshEnvironment['PASSENGER_IDS'] = ($staleCustomers.PassengerID -join ',')
    $refreshEnvironment['TRAIN_RUN_ID'] = $fixtureTrainA
    $refreshEnvironment['API_URLS'] = 'http://api-1:8080,http://api-2:8080,http://api-3:8080'
    Invoke-K6 -Script 'stale-router-refresh.js' -Name 'stale-router-refresh' -Environment $refreshEnvironment
    foreach ($api in $apis) {
        $replicaStaleCount = Get-APIMetricTotal -Family 'shard_assignment_stale_total' -Services @($api)
        $replicaStaleWrites = Get-APIMetricTotal -Family 'shard_assignment_stale_total' `
            -RequiredLabel 'operation="write",shard_id="legacy"' -Services @($api)
        $replicaRefreshes = Get-APIMetricTotal -Family 'shard_route_refresh_total' `
            -RequiredLabel 'operation="refresh",result="success",shard_id="shard-0"' -Services @($api)
        $replicaTargetWrites = Get-APIMetricTotal -Family 'shard_route_total' `
            -RequiredLabel 'operation="write",result="success",reason="none",shard_id="shard-0"' -Services @($api)
        $staleWriteDelta = $replicaStaleWrites - [double]$replicaMetricBaselines[$api].stale_write
        $refreshDelta = $replicaRefreshes - [double]$replicaMetricBaselines[$api].refresh_success
        $targetWriteDelta = $replicaTargetWrites - [double]$replicaMetricBaselines[$api].target_write
        if ($staleWriteDelta -lt 1 -or $refreshDelta -lt 1 -or $targetWriteDelta -lt 1) {
            throw "$api did not prove stale write rejection, authoritative refresh, and target write success"
        }
        $replicaRouteEvidence[$api] = [ordered]@{
            stale_assignment_rejections_total = $replicaStaleCount
            stale_write_rejection_delta = $staleWriteDelta
            refresh_success_delta = $refreshDelta
            target_write_success_delta = $targetWriteDelta
        }
    }
    $refreshDurationSumAfter = Get-APIMetricTotal -Family 'shard_request_duration_seconds_sum' `
        -RequiredLabel 'operation="refresh",result="success",shard_id="shard-0"'
    $refreshDurationCountAfter = Get-APIMetricTotal -Family 'shard_request_duration_seconds_count' `
        -RequiredLabel 'operation="refresh",result="success",shard_id="shard-0"'
    $refreshDurationCountDelta = $refreshDurationCountAfter - $refreshDurationCountBefore
    $refreshDurationSumDelta = $refreshDurationSumAfter - $refreshDurationSumBefore
    if ($refreshDurationCountDelta -lt 3 -or $refreshDurationSumDelta -lt 0) {
        throw 'refresh latency histogram did not record all three replica refreshes'
    }
    $refreshLatencyMeanMilliseconds = [Math]::Round(
        1000.0 * $refreshDurationSumDelta / $refreshDurationCountDelta, 3
    )

    $failureCategory = 'copied_train_a_post_cutover_transition'
    $copiedTrainAConfirmation = Invoke-API -Method POST `
        -Path "/api/v1/reservations/$($seedReservations[0])/confirm" `
        -Token $fixtureCustomers[0].Token `
        -IdempotencyKey "m4-copied-held-confirm-$suffix"
    if ($copiedTrainAConfirmation.StatusCode -ne 200 -or
        [string]$copiedTrainAConfirmation.Body.id -ne [string]$seedReservations[0] -or
        [string]$copiedTrainAConfirmation.Body.status -ne 'confirmed') {
        throw 'copied train A hold did not transition on the target after cutover'
    }

    $failureCategory = 'post_cutover_lifecycle_workload'
    $lifecycleEnvironment = $commonK6.Clone()
    $lifecycleEnvironment['CUSTOMER_TOKENS'] = ($lifecycleCustomers.Token -join ',')
    $lifecycleEnvironment['PASSENGER_IDS'] = ($lifecycleCustomers.PassengerID -join ',')
    $lifecycleEnvironment['TRAIN_RUN_ID'] = $fixtureTrainA
    Invoke-K6 -Script 'shard-post-cutover-lifecycle.js' -Name 'shard-post-cutover-lifecycle' `
        -Environment $lifecycleEnvironment
    $postCutoverExpiryCustomer = $fixtureCustomers[3]
    $postCutoverExpiry = Invoke-API -Method POST -Path '/api/v1/reservations' `
        -Token $postCutoverExpiryCustomer.Token -IdempotencyKey "m4-post-cutover-expire-$suffix" -Body @{
            train_run_id = $fixtureTrainA
            origin_station_code = $origin
            destination_station_code = $destination
            seat_class = $seatClass
            passenger_ids = @($postCutoverExpiryCustomer.PassengerID)
        }
    $postCutoverExpiryID = [string]$postCutoverExpiry.Body.id
    $parsedPostCutoverExpiryID = [guid]::Empty
    if ($postCutoverExpiry.StatusCode -ne 201 -or
        -not [guid]::TryParse($postCutoverExpiryID, [ref]$parsedPostCutoverExpiryID)) {
        throw 'post-cutover expiration fixture creation failed'
    }
    Invoke-PSQL -Artifact 'post-cutover-expiry-arm.log' -SQL @"
UPDATE booking_shard_0.reservations
SET expires_at=clock_timestamp() - interval '1 minute'
WHERE id='$postCutoverExpiryID'::uuid AND train_run_id='$fixtureTrainA'::uuid AND status='held';
"@ | Out-Null
    Wait-ReservationStatus -ReservationID $postCutoverExpiryID `
        -Token $postCutoverExpiryCustomer.Token -ExpectedStatus 'expired'
    $fixtureLifecycleEvidence['post_cutover'] = [ordered]@{
        create_and_replay = 2
        get = 2
        confirmed = 1
        cancelled = 1
        expired = 1
        copied_pre_cutover_held_confirmed = 1
        ticket_order_read = 1
        strict_status_contract = $true
    }
    Add-PostgresConnectionSample -Label 'after-train-a-cutover'

    $failureCategory = 'legacy_schema_comparison_workload'
    Invoke-K6 -Script 'legacy-vs-schema-shard.js' -Name 'legacy-vs-schema-shard' -Environment ($commonK6 + @{
        LEGACY_TRAIN_RUN_ID = $fixtureTrainB; SCHEMA_TRAIN_RUN_ID = $fixtureTrainA
        VUS_PER_SHARD = '4'; DURATION = $LoadDuration
    })

    $failureCategory = 'train_b_route_cache_prewarm'
    $trainBPrewarmEnvironment = $commonK6.Clone()
    $trainBPrewarmEnvironment['CUSTOMER_TOKENS'] = $cacheCustomers[0].Token
    $trainBPrewarmEnvironment['PASSENGER_IDS'] = $cacheCustomers[0].PassengerID
    $trainBPrewarmEnvironment['TRAIN_RUN_ID'] = $fixtureTrainB
    Invoke-K6 -Script 'shard-route-prewarm.js' -Name 'prewarm-train-b' `
        -Environment $trainBPrewarmEnvironment
    $failureCategory = 'train_b_read_model_catchup'
    $trainBPreCutoverReceiptCount = Wait-TrainRunReadModelCaughtUp -TrainRunID $fixtureTrainB
    $trainBAssignmentBeforeCutover = Get-AssignmentState -TrainRunID $fixtureTrainB `
        -Artifact 'train-b-assignment-before-cutover.json'
    $trainBAvailabilityVersionBefore = Get-AvailabilityCacheVersion -TrainRunID $fixtureTrainB
    if ([string]::IsNullOrWhiteSpace($trainBAvailabilityVersionBefore)) {
        throw 'train B prewarm did not establish an availability cache namespace'
    }

    $failureCategory = 'train_b_migration'
    Invoke-MigrationToCutoverReady -TrainRunID $fixtureTrainB -TargetShard 'shard-1' `
        -MigrationID $migrationB -Prefix 'train-b'
    $failureCategory = 'train_b_cutover_overlap'
    $metricBarrier = Get-APIMetricTotal -Family 'shard_route_total'
    $cutoverEnvironment = $commonK6.Clone()
    $cutoverEnvironment['CUSTOMER_TOKENS'] = ($overlapCustomers.Token -join ',')
    $cutoverEnvironment['PASSENGER_IDS'] = ($overlapCustomers.PassengerID -join ',')
    $cutoverEnvironment['TRAIN_RUN_ID'] = $fixtureTrainB
    $cutoverEnvironment['VUS'] = '5'
    $cutoverEnvironment['DURATION'] = '10s'
    $cutoverContainer = ''
    try {
        $cutoverContainer = Start-K6 -Script 'shard-cutover.js' -Name 'shard-cutover' `
            -Environment $cutoverEnvironment
        Wait-WorkloadBarrier -Before $metricBarrier -ContainerName $cutoverContainer
        Invoke-Cutover -MigrationID $migrationB -Prefix 'train-b'
        Wait-K6 -ContainerName $cutoverContainer -Name 'shard-cutover'
        $cutoverContainer = ''
    } finally {
        if (-not [string]::IsNullOrWhiteSpace($cutoverContainer)) {
            Stop-K6 -ContainerName $cutoverContainer -Name 'shard-cutover'
        }
    }
    $trainBAssignmentAfterCutover = Get-AssignmentState -TrainRunID $fixtureTrainB `
        -Artifact 'train-b-assignment-after-cutover.json'
    if ([int64]$trainBAssignmentAfterCutover.assignment_generation -le
            [int64]$trainBAssignmentBeforeCutover.assignment_generation -or
        [int64]$trainBAssignmentAfterCutover.availability_generation -le
            [int64]$trainBAssignmentBeforeCutover.availability_generation -or
        [string]$trainBAssignmentAfterCutover.shard_id -ne 'shard-1') {
        throw 'train B cutover did not rotate assignment and availability generations onto shard-1'
    }
    $trainBCutoverEventID = Get-CutoverOutboxEventID -TrainRunID $fixtureTrainB `
        -ShardID 'shard-1' -AssignmentGeneration ([int64]$trainBAssignmentAfterCutover.assignment_generation)
    Wait-ReadModelReceipt -EventID $trainBCutoverEventID
    $trainBAvailabilityVersionAfter = Wait-AvailabilityVersionRotated -TrainRunID $fixtureTrainB `
        -Previous $trainBAvailabilityVersionBefore
    $availabilityEvidence['train-b'] = [ordered]@{
        assignment_generation_before = [int64]$trainBAssignmentBeforeCutover.assignment_generation
        assignment_generation_after = [int64]$trainBAssignmentAfterCutover.assignment_generation
        availability_generation_before = [int64]$trainBAssignmentBeforeCutover.availability_generation
        availability_generation_after = [int64]$trainBAssignmentAfterCutover.availability_generation
        pre_cutover_receipted_event_count = $trainBPreCutoverReceiptCount
        exact_cutover_event_receipted = $true
        redis_namespace_rotated = ($trainBAvailabilityVersionAfter -ne $trainBAvailabilityVersionBefore)
    }
    $legacySourceFingerprints['train-b'] = Get-LegacySourceFingerprint `
        -TrainRunID $fixtureTrainB -Artifact 'train-b-legacy-source-after-cutover.json'
    $copiedTrainBCancellation = Invoke-API -Method POST `
        -Path "/api/v1/reservations/$($seedReservations[1])/cancel" `
        -Token $fixtureCustomers[0].Token `
        -IdempotencyKey "m4-copied-held-cancel-$suffix"
    if ($copiedTrainBCancellation.StatusCode -ne 200 -or
        [string]$copiedTrainBCancellation.Body.id -ne [string]$seedReservations[1] -or
        [string]$copiedTrainBCancellation.Body.status -ne 'cancelled') {
        throw 'copied train B hold did not transition on the target after cutover'
    }
    $fixtureLifecycleEvidence['post_cutover']['copied_pre_cutover_held_cancelled'] = 1
    Add-PostgresConnectionSample -Label 'after-train-b-cutover'

    $failureCategory = 'hot_policy_enablement'
    Invoke-PSQL -Artifact 'two-hot-policies-enable.log' -SQL @"
UPDATE public.hot_train_policies
SET enabled=true, updated_at=clock_timestamp()
WHERE train_run_id IN ('$fixtureTrainA'::uuid, '$fixtureTrainB'::uuid)
  AND seat_class='standard';
"@ | Out-Null
    Wait-HotPoliciesInitialized
    $failureCategory = 'two_hot_shards_workload'
    Invoke-K6 -Script 'two-hot-train-shards.js' -Name 'two-hot-train-shards' -Environment ($commonK6 + @{
        VUS_PER_SHARD = '5'; DURATION = $LoadDuration
    })
    $failureCategory = 'cross_shard_customer_healthy_workload'
    Invoke-K6 -Script 'cross-shard-admin.js' -Name 'cross-shard-customer-healthy' -Environment ($commonK6 + @{
        EXPECT_PARTIAL = 'no'; DURATION = '10s'
    })

    $failureCategory = 'shard_outage_injection'
    Invoke-PSQL -Artifact 'shard-outage-inject.log' -SQL @"
UPDATE public.booking_shards
SET enabled=false, write_enabled=false, state='disabled', updated_at=clock_timestamp()
WHERE shard_id='shard-0';
"@ | Out-Null
    try {
        $failureCategory = 'admin_fanout_partial'
        $partialHealth = Invoke-ShardAdmin -Arguments @('inspect-health', '--timeout', '30s') `
            -Artifact 'operator-health-partial.json' -AllowFailure
        $operatorHealthEvidence['catalog_disabled'] = Assert-Milestone4OperatorHealth `
            -Invocation $partialHealth -ExpectedReady $false
        $adminPartial = Invoke-Reconcile -Arguments @(
            'shard-assignments', '--page-size', '100', '--max-pages', '1000',
            '--max-rows', '100000', '--timeout', '30s'
        ) -Artifact 'admin-fanout-partial.json' -AllowFailure
        $partialReport = Assert-BoundedShardReport -Invocation $adminPartial -Expected 'partial' `
            -ExpectedUnavailableShardID 'shard-0' -ExpectedUnavailableFailure 'catalog_disabled'
        if ([int]$partialReport.healthy_shards -ne 2 -or
            [int]$partialReport.unavailable_shards -ne 1 -or
            [int64]$partialReport.violations -ne 1 -or
            [int64]$partialReport.rows_examined -le 0 -or [int]$partialReport.pages -le 0) {
            throw 'bounded admin fanout did not report the exact catalog-disable partial result'
        }
        $adminFanoutEvidence['partial_during_shard_0_disablement'] = $partialReport

        $failureCategory = 'shard_outage_workloads'
        Invoke-K6 -Script 'shard-outage-isolation.js' -Name 'shard-outage-isolation' -Environment ($commonK6 + @{
            DURATION = $LoadDuration
        })
        Invoke-K6 -Script 'cross-shard-admin.js' -Name 'cross-shard-customer-partial' -Environment ($commonK6 + @{
            EXPECT_PARTIAL = 'yes'; DURATION = '10s'
        })
    } finally {
        Invoke-PSQL -Artifact 'shard-outage-restore.log' -SQL @"
UPDATE public.booking_shards
SET enabled=true, write_enabled=true, state='active', updated_at=clock_timestamp()
WHERE shard_id='shard-0';
"@ | Out-Null
    }

    $failureCategory = 'admin_fanout_recovery'
    $adminRecovery = Invoke-Reconcile -Arguments @(
        'shard-assignments', '--page-size', '100', '--max-pages', '1000',
        '--max-rows', '100000', '--timeout', '30s'
    ) -Artifact 'admin-fanout-complete-after.json'
    $adminFanoutEvidence['complete_after_restore'] = Assert-BoundedShardReport `
        -Invocation $adminRecovery -Expected 'complete'
    $recoveredHealth = Invoke-ShardAdmin -Arguments @('inspect-health', '--timeout', '30s') `
        -Artifact 'operator-health-recovered.json' -AllowFailure
    $operatorHealthEvidence['recovered'] = Assert-Milestone4OperatorHealth `
        -Invocation $recoveredHealth -ExpectedReady $true
    $failureCategory = 'outbox_drain'
    Wait-OutboxPublished -TrainRunIDs @($fixtureTrainA, $fixtureTrainB)
    $failureCategory = 'final_reconciliation'
    foreach ($pair in @(
        @{ Run = $fixtureTrainA; Name = 'train-a' },
        @{ Run = $fixtureTrainB; Name = 'train-b' }
    )) {
        $reconciliation = Invoke-ShardAdmin -Arguments @(
            'reconcile', '--train-run-id', $pair.Run, '--row-cap', '10000', '--timeout', '30s'
        ) -Artifact "$($pair.Name)-reconcile.json"
        $reconciliationResult = Get-Milestone4AdminResult -Envelope $reconciliation.Envelope
        $completeness = [string](Get-ObjectPropertyValue -Object $reconciliationResult -Name 'completeness')
        $rowsExamined = [int64](Get-ObjectPropertyValue -Object $reconciliationResult -Name 'rows_examined')
        $violations = [int64](Get-ObjectPropertyValue -Object $reconciliationResult -Name 'violations')
        $truncated = [bool](Get-ObjectPropertyValue -Object $reconciliationResult -Name 'truncated')
        if ($completeness -ne 'complete' -or $rowsExamined -le 0 -or $violations -ne 0 -or $truncated) {
            throw "$($pair.Name) reconciliation was incomplete, truncated, or inconsistent"
        }
        $reconciliationEvidence["$($pair.Name)-locator-operator"] = [ordered]@{
            completeness = $completeness
            rows_examined = $rowsExamined
            violations = $violations
            truncated = $truncated
        }
    }

    $assignmentReconcile = Invoke-HealthyReconcile -Arguments @(
        'shard-assignments', '--page-size', '100', '--max-pages', '1000',
        '--max-rows', '100000', '--timeout', '30s'
    ) -Artifact 'reconcile-shard-assignments.json'
    $reconciliationEvidence['shard-assignments'] = Assert-BoundedShardReport `
        -Invocation $assignmentReconcile -Expected 'complete'

    $locatorReconcile = Invoke-HealthyReconcile -Arguments @(
        'shard-locators', '--page-size', '100', '--max-pages', '1000',
        '--max-rows', '100000', '--timeout', '30s'
    ) -Artifact 'reconcile-shard-locators.json'
    $reconciliationEvidence['shard-locators'] = Assert-BoundedShardReport `
        -Invocation $locatorReconcile -Expected 'complete'

    foreach ($migration in @(
        @{ ID = $migrationA; Name = 'train-a' },
        @{ ID = $migrationB; Name = 'train-b' }
    )) {
        $migrationReconcile = Invoke-HealthyReconcile -Arguments @(
            'shard-migration', '--migration-id', $migration.ID, '--page-size', '100',
            '--max-pages', '1000', '--max-rows', '100000', '--timeout', '30s'
        ) -Artifact "reconcile-$($migration.Name)-migration.json"
        $boundedMigration = Assert-BoundedShardReport -Invocation $migrationReconcile -Expected 'complete'
        $migrationSummary = $migrationReconcile.Envelope.result.migration
        if ($null -eq $migrationSummary -or
            [int64]$migrationSummary.generation_write_rows -lt 1 -or
            [int64]$migrationSummary.outbox_events -lt 1 -or
            [string]$migrationSummary.validation_status -ne 'passed') {
            throw "$($migration.Name) migration reconciliation omitted non-vacuous validation, write, or outbox evidence"
        }
        $boundedMigration['migration_state'] = [string]$migrationSummary.state
        $boundedMigration['generation_write_rows'] = [int64]$migrationSummary.generation_write_rows
        $boundedMigration['outbox_events'] = [int64]$migrationSummary.outbox_events
        $boundedMigration['source_counts'] = $migrationSummary.source_counts
        $boundedMigration['target_counts'] = $migrationSummary.target_counts
        $boundedMigration['cutover_source_counts'] = $migrationSummary.cutover_source_counts
        $boundedMigration['cutover_target_counts'] = $migrationSummary.cutover_target_counts
        $reconciliationEvidence["$($migration.Name)-migration"] = $boundedMigration
    }

    $admissionReconcile = Invoke-HealthyReconcile -Arguments @(
        'admission-state', '--page-size', '100', '--max-pages', '1000', '--timeout', '30s'
    ) -Artifact 'reconcile-admission-state.json' -Attempts 10
    $admissionResult = $admissionReconcile.Envelope.result
    $admissionViolationTotal = [int64]$admissionResult.duplicate_active_users +
        [int64]$admissionResult.inflight_token_mismatches +
        [int64]$admissionResult.expired_inflight_tokens +
        [int64]$admissionResult.expired_processing_leases +
        [int64]$admissionResult.token_entry_owner_mismatches +
        [int64]$admissionResult.uninitialized_policy_generations +
        [int64]$admissionResult.missing_current_policy_generations +
        [int64]$admissionResult.invalid_current_policy_generations
    if ([int64]$admissionResult.policies -lt 2 -or
        [int64]$admissionResult.redis_pages -lt 1 -or $admissionViolationTotal -ne 0) {
        throw 'admission-state reconciliation was vacuous or inconsistent'
    }
    $reconciliationEvidence['admission-state'] = [ordered]@{
        status = 'healthy'
        duration_ms = $admissionReconcile.DurationMilliseconds
        policies = [int64]$admissionResult.policies
        redis_pages = [int64]$admissionResult.redis_pages
        violations = $admissionViolationTotal
    }

    foreach ($pair in @(
        @{ Run = $fixtureTrainA; Name = 'train-a' },
        @{ Run = $fixtureTrainB; Name = 'train-b' }
    )) {
        $readModelReconcile = Invoke-HealthyReconcile -Arguments @(
            'read-model', '--train-run-id', $pair.Run, '--timeout', '30s'
        ) -Artifact "reconcile-$($pair.Name)-read-model.json" -Attempts 30
        $readModelResult = $readModelReconcile.Envelope.result
        if (-not [bool]$readModelResult.Consistent -or
            [int]$readModelResult.ExpectedRows -le 0 -or
            [int]$readModelResult.ActualRows -le 0) {
            throw "$($pair.Name) read-model reconciliation was vacuous or inconsistent"
        }
        $reconciliationEvidence["$($pair.Name)-read-model"] = [ordered]@{
            status = 'healthy'
            duration_ms = $readModelReconcile.DurationMilliseconds
            expected_rows = [int]$readModelResult.ExpectedRows
            actual_rows = [int]$readModelResult.ActualRows
            consistent = [bool]$readModelResult.Consistent
        }

        $cacheReconcile = Invoke-HealthyReconcile -Arguments @(
            'cache-versions', '--train-run-id', $pair.Run, '--timeout', '30s'
        ) -Artifact "reconcile-$($pair.Name)-cache-versions.json" -Attempts 10
        $cacheResult = $cacheReconcile.Envelope.result
        if ([int]$cacheResult.checked -ne 3 -or
            [int]$cacheResult.missing -ne 0 -or [int]$cacheResult.invalid -ne 0) {
            throw "$($pair.Name) cache-version reconciliation was incomplete or inconsistent"
        }
        $reconciliationEvidence["$($pair.Name)-cache-versions"] = [ordered]@{
            status = 'healthy'
            duration_ms = $cacheReconcile.DurationMilliseconds
            checked = [int]$cacheResult.checked
            missing = [int]$cacheResult.missing
            invalid = [int]$cacheResult.invalid
        }
    }

    $failureCategory = 'retained_source_immutability'
    foreach ($sourceCheck in @(
        @{ Name = 'train-a'; Run = $fixtureTrainA },
        @{ Name = 'train-b'; Run = $fixtureTrainB }
    )) {
        $afterArtifact = "$($sourceCheck.Name)-legacy-source-final.json"
        $finalFingerprint = Get-LegacySourceFingerprint `
            -TrainRunID $sourceCheck.Run -Artifact $afterArtifact
        $sourceEvidence = Assert-LegacySourceUnchanged `
            -Before $legacySourceFingerprints[$sourceCheck.Name] -After $finalFingerprint
        $sourceEvidence['after_cutover_artifact'] = "$($sourceCheck.Name)-legacy-source-after-cutover.json"
        $sourceEvidence['final_artifact'] = $afterArtifact
        $sourceEvidence['copied_reservation_transitioned_on_target'] = $true
        $legacySourceImmutabilityEvidence[$sourceCheck.Name] = $sourceEvidence
    }

    $failureCategory = 'integrity_validation'
    $integrityResult = Invoke-PSQL -Artifact 'integrity-evidence.json' -SQL @"
WITH selected_assignments AS (
    SELECT train_run_id, shard_id, assignment_generation
    FROM public.train_run_shard_assignments
    WHERE train_run_id IN ('$fixtureTrainA'::uuid, '$fixtureTrainB'::uuid)
), authoritative_reservations AS (
    SELECT reservation.id, reservation.train_run_id, reservation.user_id, reservation.status,
           assignment.shard_id, assignment.assignment_generation
    FROM public.reservations AS reservation
    JOIN selected_assignments AS assignment
      ON assignment.train_run_id=reservation.train_run_id AND assignment.shard_id='legacy'
    UNION ALL
    SELECT reservation.id, reservation.train_run_id, reservation.user_id, reservation.status,
           assignment.shard_id, assignment.assignment_generation
    FROM booking_shard_0.reservations AS reservation
    JOIN selected_assignments AS assignment
      ON assignment.train_run_id=reservation.train_run_id AND assignment.shard_id='shard-0'
    UNION ALL
    SELECT reservation.id, reservation.train_run_id, reservation.user_id, reservation.status,
           assignment.shard_id, assignment.assignment_generation
    FROM booking_shard_1.reservations AS reservation
    JOIN selected_assignments AS assignment
      ON assignment.train_run_id=reservation.train_run_id AND assignment.shard_id='shard-1'
), authoritative_seats AS (
    SELECT seat.id, seat.reservation_id, seat.train_run_id, seat.seat_id, seat.segment_mask
    FROM public.reservation_seats AS seat
    JOIN selected_assignments AS assignment
      ON assignment.train_run_id=seat.train_run_id AND assignment.shard_id='legacy'
    UNION ALL
    SELECT seat.id, seat.reservation_id, seat.train_run_id, seat.seat_id, seat.segment_mask
    FROM booking_shard_0.reservation_seats AS seat
    JOIN selected_assignments AS assignment
      ON assignment.train_run_id=seat.train_run_id AND assignment.shard_id='shard-0'
    UNION ALL
    SELECT seat.id, seat.reservation_id, seat.train_run_id, seat.seat_id, seat.segment_mask
    FROM booking_shard_1.reservation_seats AS seat
    JOIN selected_assignments AS assignment
      ON assignment.train_run_id=seat.train_run_id AND assignment.shard_id='shard-1'
), authoritative_orders AS (
    SELECT ticket_order.id, ticket_order.reservation_id, ticket_order.status,
           reservation.train_run_id, assignment.shard_id, assignment.assignment_generation
    FROM public.ticket_orders AS ticket_order
    JOIN public.reservations AS reservation ON reservation.id=ticket_order.reservation_id
    JOIN selected_assignments AS assignment
      ON assignment.train_run_id=reservation.train_run_id AND assignment.shard_id='legacy'
    UNION ALL
    SELECT ticket_order.id, ticket_order.reservation_id, ticket_order.status,
           reservation.train_run_id, assignment.shard_id, assignment.assignment_generation
    FROM booking_shard_0.ticket_orders AS ticket_order
    JOIN booking_shard_0.reservations AS reservation ON reservation.id=ticket_order.reservation_id
    JOIN selected_assignments AS assignment
      ON assignment.train_run_id=reservation.train_run_id AND assignment.shard_id='shard-0'
    UNION ALL
    SELECT ticket_order.id, ticket_order.reservation_id, ticket_order.status,
           reservation.train_run_id, assignment.shard_id, assignment.assignment_generation
    FROM booking_shard_1.ticket_orders AS ticket_order
    JOIN booking_shard_1.reservations AS reservation ON reservation.id=ticket_order.reservation_id
    JOIN selected_assignments AS assignment
      ON assignment.train_run_id=reservation.train_run_id AND assignment.shard_id='shard-1'
), authoritative_tickets AS (
    SELECT ticket.id, ticket.status, ticket_order.train_run_id,
           ticket_order.shard_id, ticket_order.assignment_generation
    FROM public.tickets AS ticket
    JOIN authoritative_orders AS ticket_order
      ON ticket_order.id=ticket.ticket_order_id AND ticket_order.shard_id='legacy'
    UNION ALL
    SELECT ticket.id, ticket.status, ticket_order.train_run_id,
           ticket_order.shard_id, ticket_order.assignment_generation
    FROM booking_shard_0.tickets AS ticket
    JOIN authoritative_orders AS ticket_order
      ON ticket_order.id=ticket.ticket_order_id AND ticket_order.shard_id='shard-0'
    UNION ALL
    SELECT ticket.id, ticket.status, ticket_order.train_run_id,
           ticket_order.shard_id, ticket_order.assignment_generation
    FROM booking_shard_1.tickets AS ticket
    JOIN authoritative_orders AS ticket_order
      ON ticket_order.id=ticket.ticket_order_id AND ticket_order.shard_id='shard-1'
), authoritative_idempotency AS (
    SELECT idempotency.id
    FROM public.idempotency_records AS idempotency
    JOIN selected_assignments AS assignment
      ON assignment.train_run_id=idempotency.train_run_id AND assignment.shard_id='legacy'
    UNION ALL
    SELECT idempotency.id
    FROM booking_shard_0.idempotency_records AS idempotency
    JOIN selected_assignments AS assignment
      ON assignment.train_run_id=idempotency.train_run_id AND assignment.shard_id='shard-0'
    UNION ALL
    SELECT idempotency.id
    FROM booking_shard_1.idempotency_records AS idempotency
    JOIN selected_assignments AS assignment
      ON assignment.train_run_id=idempotency.train_run_id AND assignment.shard_id='shard-1'
), all_fences AS (
    SELECT train_run_id, assignment_generation, write_enabled, 'legacy'::text AS shard_id
    FROM public.train_run_write_fences
    UNION ALL
    SELECT train_run_id, assignment_generation, write_enabled, 'shard-0'::text AS shard_id
    FROM booking_shard_0.train_run_write_fences
    UNION ALL
    SELECT train_run_id, assignment_generation, write_enabled, 'shard-1'::text AS shard_id
    FROM booking_shard_1.train_run_write_fences
), migration_copy_checks AS (
    SELECT migration.id,
           (
               ((SELECT count(*) FROM public.seat_inventory WHERE train_run_id=migration.train_run_id) <> migration.inventory_rows_copied)::integer +
               ((SELECT count(*) FROM public.reservations WHERE train_run_id=migration.train_run_id) <> migration.reservation_rows_copied)::integer +
               ((SELECT count(*) FROM public.reservation_seats WHERE train_run_id=migration.train_run_id) <> migration.reservation_seat_rows_copied)::integer +
               ((SELECT count(*) FROM public.ticket_orders AS ticket_order JOIN public.reservations AS reservation ON reservation.id=ticket_order.reservation_id WHERE reservation.train_run_id=migration.train_run_id) <> migration.ticket_order_rows_copied)::integer +
               ((SELECT count(*) FROM public.tickets AS ticket JOIN public.ticket_orders AS ticket_order ON ticket_order.id=ticket.ticket_order_id JOIN public.reservations AS reservation ON reservation.id=ticket_order.reservation_id WHERE reservation.train_run_id=migration.train_run_id) <> migration.ticket_rows_copied)::integer +
               ((SELECT count(*) FROM public.idempotency_records WHERE train_run_id=migration.train_run_id) <> migration.idempotency_rows_copied)::integer
           ) AS source_count_mismatches,
           CASE migration.target_shard_id
               WHEN 'shard-0' THEN (
                   ((SELECT count(*) FROM booking_shard_0.seat_inventory WHERE train_run_id=migration.train_run_id) < migration.inventory_rows_copied)::integer +
                   ((SELECT count(*) FROM booking_shard_0.reservations WHERE train_run_id=migration.train_run_id) < migration.reservation_rows_copied)::integer +
                   ((SELECT count(*) FROM booking_shard_0.reservation_seats WHERE train_run_id=migration.train_run_id) < migration.reservation_seat_rows_copied)::integer +
                   ((SELECT count(*) FROM booking_shard_0.ticket_orders AS ticket_order JOIN booking_shard_0.reservations AS reservation ON reservation.id=ticket_order.reservation_id WHERE reservation.train_run_id=migration.train_run_id) < migration.ticket_order_rows_copied)::integer +
                   ((SELECT count(*) FROM booking_shard_0.tickets AS ticket JOIN booking_shard_0.ticket_orders AS ticket_order ON ticket_order.id=ticket.ticket_order_id JOIN booking_shard_0.reservations AS reservation ON reservation.id=ticket_order.reservation_id WHERE reservation.train_run_id=migration.train_run_id) < migration.ticket_rows_copied)::integer +
                   ((SELECT count(*) FROM booking_shard_0.idempotency_records WHERE train_run_id=migration.train_run_id) < migration.idempotency_rows_copied)::integer
               )
               WHEN 'shard-1' THEN (
                   ((SELECT count(*) FROM booking_shard_1.seat_inventory WHERE train_run_id=migration.train_run_id) < migration.inventory_rows_copied)::integer +
                   ((SELECT count(*) FROM booking_shard_1.reservations WHERE train_run_id=migration.train_run_id) < migration.reservation_rows_copied)::integer +
                   ((SELECT count(*) FROM booking_shard_1.reservation_seats WHERE train_run_id=migration.train_run_id) < migration.reservation_seat_rows_copied)::integer +
                   ((SELECT count(*) FROM booking_shard_1.ticket_orders AS ticket_order JOIN booking_shard_1.reservations AS reservation ON reservation.id=ticket_order.reservation_id WHERE reservation.train_run_id=migration.train_run_id) < migration.ticket_order_rows_copied)::integer +
                   ((SELECT count(*) FROM booking_shard_1.tickets AS ticket JOIN booking_shard_1.ticket_orders AS ticket_order ON ticket_order.id=ticket.ticket_order_id JOIN booking_shard_1.reservations AS reservation ON reservation.id=ticket_order.reservation_id WHERE reservation.train_run_id=migration.train_run_id) < migration.ticket_rows_copied)::integer +
                   ((SELECT count(*) FROM booking_shard_1.idempotency_records WHERE train_run_id=migration.train_run_id) < migration.idempotency_rows_copied)::integer
               )
               ELSE 6
           END AS target_count_regressions,
           CASE migration.target_shard_id
               WHEN 'shard-0' THEN (
                   (SELECT count(*) FROM public.seat_inventory AS source LEFT JOIN booking_shard_0.seat_inventory AS target ON target.train_run_id=source.train_run_id AND target.seat_id=source.seat_id WHERE source.train_run_id=migration.train_run_id AND target.seat_id IS NULL) +
                   (SELECT count(*) FROM public.reservations AS source LEFT JOIN booking_shard_0.reservations AS target ON target.id=source.id AND target.train_run_id=source.train_run_id WHERE source.train_run_id=migration.train_run_id AND target.id IS NULL) +
                   (SELECT count(*) FROM public.reservation_seats AS source LEFT JOIN booking_shard_0.reservation_seats AS target ON target.id=source.id AND target.train_run_id=source.train_run_id WHERE source.train_run_id=migration.train_run_id AND target.id IS NULL) +
                   (SELECT count(*) FROM public.ticket_orders AS source JOIN public.reservations AS source_reservation ON source_reservation.id=source.reservation_id LEFT JOIN booking_shard_0.ticket_orders AS target ON target.id=source.id AND target.reservation_id=source.reservation_id WHERE source_reservation.train_run_id=migration.train_run_id AND target.id IS NULL) +
                   (SELECT count(*) FROM public.tickets AS source JOIN public.ticket_orders AS source_order ON source_order.id=source.ticket_order_id JOIN public.reservations AS source_reservation ON source_reservation.id=source_order.reservation_id LEFT JOIN booking_shard_0.tickets AS target ON target.id=source.id AND target.ticket_order_id=source.ticket_order_id WHERE source_reservation.train_run_id=migration.train_run_id AND target.id IS NULL) +
                   (SELECT count(*) FROM public.idempotency_records AS source LEFT JOIN booking_shard_0.idempotency_records AS target ON target.id=source.id AND target.train_run_id=source.train_run_id WHERE source.train_run_id=migration.train_run_id AND target.id IS NULL)
               )
               WHEN 'shard-1' THEN (
                   (SELECT count(*) FROM public.seat_inventory AS source LEFT JOIN booking_shard_1.seat_inventory AS target ON target.train_run_id=source.train_run_id AND target.seat_id=source.seat_id WHERE source.train_run_id=migration.train_run_id AND target.seat_id IS NULL) +
                   (SELECT count(*) FROM public.reservations AS source LEFT JOIN booking_shard_1.reservations AS target ON target.id=source.id AND target.train_run_id=source.train_run_id WHERE source.train_run_id=migration.train_run_id AND target.id IS NULL) +
                   (SELECT count(*) FROM public.reservation_seats AS source LEFT JOIN booking_shard_1.reservation_seats AS target ON target.id=source.id AND target.train_run_id=source.train_run_id WHERE source.train_run_id=migration.train_run_id AND target.id IS NULL) +
                   (SELECT count(*) FROM public.ticket_orders AS source JOIN public.reservations AS source_reservation ON source_reservation.id=source.reservation_id LEFT JOIN booking_shard_1.ticket_orders AS target ON target.id=source.id AND target.reservation_id=source.reservation_id WHERE source_reservation.train_run_id=migration.train_run_id AND target.id IS NULL) +
                   (SELECT count(*) FROM public.tickets AS source JOIN public.ticket_orders AS source_order ON source_order.id=source.ticket_order_id JOIN public.reservations AS source_reservation ON source_reservation.id=source_order.reservation_id LEFT JOIN booking_shard_1.tickets AS target ON target.id=source.id AND target.ticket_order_id=source.ticket_order_id WHERE source_reservation.train_run_id=migration.train_run_id AND target.id IS NULL) +
                   (SELECT count(*) FROM public.idempotency_records AS source LEFT JOIN booking_shard_1.idempotency_records AS target ON target.id=source.id AND target.train_run_id=source.train_run_id WHERE source.train_run_id=migration.train_run_id AND target.id IS NULL)
               )
               ELSE 1
           END AS missing_copied_target_rows,
           CASE migration.target_shard_id
               WHEN 'shard-0' THEN (SELECT count(*) FROM booking_shard_0.reservations WHERE train_run_id=migration.train_run_id) > migration.reservation_rows_copied
               WHEN 'shard-1' THEN (SELECT count(*) FROM booking_shard_1.reservations WHERE train_run_id=migration.train_run_id) > migration.reservation_rows_copied
               ELSE false
           END AS target_grew_after_cutover
    FROM public.train_run_shard_migrations AS migration
    WHERE migration.id IN ('$migrationA'::uuid, '$migrationB'::uuid)
), overlap_violations AS (
    SELECT left_seat.id
    FROM authoritative_seats AS left_seat
    JOIN authoritative_seats AS right_seat
      ON right_seat.train_run_id=left_seat.train_run_id
     AND right_seat.seat_id=left_seat.seat_id
     AND right_seat.id>left_seat.id
    JOIN authoritative_reservations AS left_reservation
      ON left_reservation.id=left_seat.reservation_id AND left_reservation.status IN ('held','confirmed')
    JOIN authoritative_reservations AS right_reservation
      ON right_reservation.id=right_seat.reservation_id AND right_reservation.status IN ('held','confirmed')
    WHERE bit_count(left_seat.segment_mask & right_seat.segment_mask)>0
)
SELECT json_build_object(
    'assignment_count', (SELECT count(*) FROM selected_assignments),
    'assignment_target_count', (SELECT count(*) FROM selected_assignments WHERE (train_run_id='$fixtureTrainA'::uuid AND shard_id='shard-0') OR (train_run_id='$fixtureTrainB'::uuid AND shard_id='shard-1')),
    'enabled_fence_count', (SELECT count(*) FROM all_fences WHERE train_run_id IN ('$fixtureTrainA'::uuid, '$fixtureTrainB'::uuid) AND write_enabled),
    'mismatched_enabled_fences', (SELECT count(*) FROM all_fences AS fence JOIN selected_assignments AS assignment USING (train_run_id) WHERE fence.write_enabled AND (fence.shard_id<>assignment.shard_id OR fence.assignment_generation<>assignment.assignment_generation)),
    'legacy_enabled_fences', (SELECT count(*) FROM all_fences WHERE train_run_id IN ('$fixtureTrainA'::uuid, '$fixtureTrainB'::uuid) AND shard_id='legacy' AND write_enabled),
    'authoritative_reservation_count', (SELECT count(*) FROM authoritative_reservations),
    'reservation_status_counts', (SELECT json_build_object('held', count(*) FILTER (WHERE status='held'), 'confirmed', count(*) FILTER (WHERE status='confirmed'), 'cancelled', count(*) FILTER (WHERE status='cancelled'), 'expired', count(*) FILTER (WHERE status='expired')) FROM authoritative_reservations),
    'authoritative_ticket_order_count', (SELECT count(*) FROM authoritative_orders),
    'authoritative_ticket_count', (SELECT count(*) FROM authoritative_tickets),
    'authoritative_idempotency_count', (SELECT count(*) FROM authoritative_idempotency),
    'selected_outbox_count', (SELECT count(*) FROM public.outbox_events WHERE train_run_id IN ('$fixtureTrainA'::uuid, '$fixtureTrainB'::uuid)),
    'unpublished_outbox_count', (SELECT count(*) FROM public.outbox_events WHERE train_run_id IN ('$fixtureTrainA'::uuid, '$fixtureTrainB'::uuid) AND status<>'published'),
    'missing_reservation_outbox_events', (SELECT count(*) FROM authoritative_reservations AS reservation WHERE NOT EXISTS (SELECT 1 FROM public.outbox_events AS event WHERE event.train_run_id=reservation.train_run_id AND event.aggregate_type='reservation' AND event.aggregate_id=reservation.id AND event.event_type='reservation.held') OR (reservation.status<>'held' AND NOT EXISTS (SELECT 1 FROM public.outbox_events AS event WHERE event.train_run_id=reservation.train_run_id AND event.aggregate_type='reservation' AND event.aggregate_id=reservation.id AND event.event_type='reservation.' || reservation.status))),
    'missing_ticket_outbox_events', (SELECT count(*) FROM authoritative_tickets AS ticket WHERE NOT EXISTS (SELECT 1 FROM public.outbox_events AS event WHERE event.train_run_id=ticket.train_run_id AND event.aggregate_type='ticket' AND event.aggregate_id=ticket.id AND event.event_type='ticket.created')),
    'duplicate_authoritative_reservation_ids', (SELECT count(*) FROM (SELECT id FROM authoritative_reservations GROUP BY id HAVING count(*)>1) AS duplicate),
    'overlap_violations', (SELECT count(*) FROM overlap_violations),
    'missing_or_stale_reservation_locators', (SELECT count(*) FROM authoritative_reservations AS reservation LEFT JOIN public.reservation_shard_locators AS locator ON locator.reservation_id=reservation.id AND locator.train_run_id=reservation.train_run_id AND locator.shard_id=reservation.shard_id AND locator.assignment_generation=reservation.assignment_generation AND locator.owner_user_id=reservation.user_id WHERE locator.reservation_id IS NULL),
    'missing_or_stale_ticket_order_locators', (SELECT count(*) FROM authoritative_orders AS ticket_order LEFT JOIN public.ticket_order_shard_locators AS locator ON locator.ticket_order_id=ticket_order.id AND locator.reservation_id=ticket_order.reservation_id AND locator.train_run_id=ticket_order.train_run_id AND locator.shard_id=ticket_order.shard_id AND locator.assignment_generation=ticket_order.assignment_generation WHERE locator.ticket_order_id IS NULL),
    'missing_or_stale_ticket_locators', (SELECT count(*) FROM authoritative_tickets AS ticket LEFT JOIN public.ticket_shard_locators AS locator ON locator.ticket_id=ticket.id AND locator.train_run_id=ticket.train_run_id AND locator.shard_id=ticket.shard_id AND locator.assignment_generation=ticket.assignment_generation WHERE locator.ticket_id IS NULL),
    'target_write_generations', (SELECT count(*) FROM public.train_run_generation_writes WHERE train_run_id IN ('$fixtureTrainA'::uuid, '$fixtureTrainB'::uuid)),
    'successful_target_write_generations', (SELECT count(*) FROM public.train_run_generation_writes WHERE train_run_id IN ('$fixtureTrainA'::uuid, '$fixtureTrainB'::uuid) AND successful_write_count>0),
    'successful_target_write_count', (SELECT coalesce(sum(successful_write_count), 0) FROM public.train_run_generation_writes WHERE train_run_id IN ('$fixtureTrainA'::uuid, '$fixtureTrainB'::uuid)),
    'retained_source_copied_holds_unchanged', (SELECT count(*) FROM public.reservations WHERE id IN ('$($seedReservations[0])'::uuid, '$($seedReservations[1])'::uuid) AND status='held'),
    'target_copied_reservation_transitions', (
        (SELECT count(*) FROM booking_shard_0.reservations WHERE id='$($seedReservations[0])'::uuid AND train_run_id='$fixtureTrainA'::uuid AND status='confirmed') +
        (SELECT count(*) FROM booking_shard_1.reservations WHERE id='$($seedReservations[1])'::uuid AND train_run_id='$fixtureTrainB'::uuid AND status='cancelled')
    ),
    'migration_validation_failures', (SELECT count(*) FROM public.train_run_shard_migrations WHERE id IN ('$migrationA'::uuid, '$migrationB'::uuid) AND (validation_status<>'passed' OR last_validation IS NULL OR NOT coalesce((last_validation->>'Passed')::boolean, false) OR coalesce((last_validation#>>'{Snapshot,Truncated}')::boolean, true) OR coalesce((last_validation#>>'{Snapshot,RowsExamined}')::bigint, 0)<=0 OR coalesce(jsonb_array_length(last_validation#>'{Snapshot,Source,Tables}'), 0)<>6 OR coalesce(jsonb_array_length(last_validation#>'{Snapshot,Target,Tables}'), 0)<>6)),
    'source_copy_count_mismatches', (SELECT coalesce(sum(source_count_mismatches), 0) FROM migration_copy_checks),
    'target_copy_count_regressions', (SELECT coalesce(sum(target_count_regressions), 0) FROM migration_copy_checks),
    'missing_copied_target_rows', (SELECT coalesce(sum(missing_copied_target_rows), 0) FROM migration_copy_checks),
    'targets_with_post_cutover_growth', (SELECT count(*) FROM migration_copy_checks WHERE target_grew_after_cutover)
)::text;
"@
    $integrityLine = [string](@($integrityResult.Output | Where-Object {
        ([string]$_).TrimStart().StartsWith('{')
    }) | Select-Object -Last 1)
    if ([string]::IsNullOrWhiteSpace($integrityLine)) { throw 'integrity evidence omitted its JSON result' }
    $integrity = $integrityLine | ConvertFrom-Json
    if ([int64]$integrity.assignment_count -ne 2 -or
        [int64]$integrity.assignment_target_count -ne 2 -or
        [int64]$integrity.enabled_fence_count -ne 2 -or
        [int64]$integrity.mismatched_enabled_fences -ne 0 -or
        [int64]$integrity.legacy_enabled_fences -ne 0 -or
        [int64]$integrity.authoritative_reservation_count -le 0 -or
        [int64]$integrity.authoritative_ticket_order_count -le 0 -or
        [int64]$integrity.authoritative_ticket_count -le 0 -or
        [int64]$integrity.authoritative_idempotency_count -le 0 -or
        [int64]$integrity.selected_outbox_count -le 0 -or
        [int64]$integrity.unpublished_outbox_count -ne 0 -or
        [int64]$integrity.missing_reservation_outbox_events -ne 0 -or
        [int64]$integrity.missing_ticket_outbox_events -ne 0 -or
        [int64]$integrity.duplicate_authoritative_reservation_ids -ne 0 -or
        [int64]$integrity.overlap_violations -ne 0 -or
        [int64]$integrity.missing_or_stale_reservation_locators -ne 0 -or
        [int64]$integrity.missing_or_stale_ticket_order_locators -ne 0 -or
        [int64]$integrity.missing_or_stale_ticket_locators -ne 0 -or
        [int64]$integrity.target_write_generations -ne 2 -or
        [int64]$integrity.successful_target_write_generations -ne 2 -or
        [int64]$integrity.successful_target_write_count -le 0 -or
        [int64]$integrity.retained_source_copied_holds_unchanged -ne 2 -or
        [int64]$integrity.target_copied_reservation_transitions -ne 2 -or
        [int64]$integrity.migration_validation_failures -ne 0 -or
        [int64]$integrity.source_copy_count_mismatches -ne 0 -or
        [int64]$integrity.target_copy_count_regressions -ne 0 -or
        [int64]$integrity.missing_copied_target_rows -ne 0 -or
        [int64]$integrity.targets_with_post_cutover_growth -ne 2 -or
        [int64]$integrity.reservation_status_counts.held -le 0 -or
        [int64]$integrity.reservation_status_counts.confirmed -le 0 -or
        [int64]$integrity.reservation_status_counts.cancelled -le 0 -or
        [int64]$integrity.reservation_status_counts.expired -le 0) {
        throw 'authoritative lifecycle, outbox, fencing, locator, migration-copy, or target-write evidence check failed'
    }
    $failureCategory = 'final_metrics'
    Save-Metrics -Label 'final'

    $failureCategory = 'summary_generation'
    $names = @(
        'shard-routing', 'shard-route-cache', 'prewarm-api-1', 'prewarm-api-2',
        'prewarm-api-3', 'prewarm-train-b', 'shard-cutover', 'stale-router-refresh',
        'shard-post-cutover-lifecycle', 'legacy-vs-schema-shard', 'two-hot-train-shards',
        'cross-shard-customer-healthy', 'shard-outage-isolation',
        'cross-shard-customer-partial'
    )
    $workloads = [ordered]@{}
    foreach ($name in $names) {
        $workload = Get-K6Summary -Name $name
        if ($null -eq $workload) { throw "$name k6 summary artifact is missing or invalid" }
        $workloads[$name] = $workload
    }
    $staleRouterRefreshCountDelta = 0.0
    foreach ($replicaEvidence in $replicaRouteEvidence.Values) {
        $staleRouterRefreshCountDelta += [double]$replicaEvidence.refresh_success_delta
    }
    if ($staleRouterRefreshCountDelta -lt 3) {
        throw 'summary generation did not retain all three replica refresh deltas'
    }
    $cutoverWorkload = $workloads['shard-cutover']
    $cutoverRejectionCount = [int64]$cutoverWorkload.expected_rebalancing_503
    $cutoverRejectionEvidence = [ordered]@{
        observed = $false
        count = $cutoverRejectionCount
        window_ms = 0.0
    }
    if ($cutoverRejectionCount -gt 0) {
        if (-not $cutoverWorkload.trends.Contains('cutover_rejection_elapsed_ms')) {
            throw 'cutover rejection count was nonzero but its elapsed-window trend was omitted'
        }
        $cutoverTrend = $cutoverWorkload.trends['cutover_rejection_elapsed_ms']
        $cutoverRejectionEvidence['observed'] = $true
        $cutoverRejectionEvidence['window_ms'] = [Math]::Round(
            [double]$cutoverTrend.max - [double]$cutoverTrend.min, 3
        )
    }
    $maximumPostgresConnections = Get-Milestone4MaximumPostgresConnections `
        -Samples @($postgresConnectionSamples)
    $milestoneSummary = [ordered]@{
        milestone = 4
        status = 'passed'
        commit_sha = $evidenceCommit
        source_provenance = [ordered]@{ clean_committed_tree = $true; exact_head_captured_before_build = $true }
        topology = [ordered]@{
            api_replicas = 3
            admission_workers = 2
            read_model_workers = 2
            hold_expirers = 1
            outbox_workers = 1
            logical_booking_shards = 3
            physical_postgresql_processes = 1
        }
        barriers = [ordered]@{
            train_a_copy = 'cutover_ready_before_overlap_workload'
            overlap_workload = 'route_metric_increment_observed_before_cutover'
            train_a_cutover = 'rollback_window'
            train_b_cutover = 'rollback_window'
            availability_rotation = 'prior_events_receipted_then_exact_shard_cutover_event_receipted'
            outage_restore = 'completed_before_reconciliation'
        }
        fixture_lifecycle = $fixtureLifecycleEvidence
        route_cache = [ordered]@{
            hit_count_total = $routeCacheHits
            hit_count_delta = $routeCacheHitDelta
            miss_count_delta = $routeCacheMissDelta
            lookup_count_delta = $routeCacheLookups
            hit_ratio = $routeCacheHitRatio
        }
        stale_router_refresh_count_delta = $staleRouterRefreshCountDelta
        refresh_latency_mean_ms = $refreshLatencyMeanMilliseconds
        cutover_rejection = $cutoverRejectionEvidence
        replica_route_evidence = $replicaRouteEvidence
        availability_generation_evidence = $availabilityEvidence
        retained_legacy_source_immutability = $legacySourceImmutabilityEvidence
        migration_evidence = $migrationEvidence
        admin_fanout_evidence = $adminFanoutEvidence
        operator_health_evidence = $operatorHealthEvidence
        admin_fanout_artifacts = @(
            'admin-fanout-complete-before.json', 'admin-fanout-partial.json',
            'admin-fanout-complete-after.json'
        )
        reconciliation_evidence = $reconciliationEvidence
        postgres_connections = [ordered]@{
            maximum_observed = $maximumPostgresConnections
            samples = $postgresConnectionSamples
        }
        redis_latency = $redisLatencyEvidence
        workloads = $workloads
        integrity_counts = $integrity
        integrity_artifact = 'integrity-evidence.json'
        reconciliation_artifacts = @(
            'train-a-reconcile.json', 'train-b-reconcile.json',
            'reconcile-shard-assignments.json', 'reconcile-shard-locators.json',
            'reconcile-train-a-migration.json', 'reconcile-train-b-migration.json',
            'reconcile-admission-state.json', 'reconcile-train-a-read-model.json',
            'reconcile-train-b-read-model.json', 'reconcile-train-a-cache-versions.json',
            'reconcile-train-b-cache-versions.json'
        )
        limitations = @(
            'Bounded local functional and latency smoke; it is not production capacity evidence.',
            'Schema shards share one PostgreSQL process, so outage injection proves logical catalog isolation rather than physical database-host isolation.',
            'Reported p50, p95, and p99 values apply only to this synthetic fixture, topology, and bounded duration.',
            'Seat-inventory and reservation-quota legacy reconcilers are not used as shard-aware proof; migration reconciliation and integrity SQL cover the fixed six-table shard boundary.',
            'A zero cutover rejection count means this bounded run observed no 503 interruption; it is not a zero-downtime guarantee.'
        )
    }

    $succeeded = $true
    $failureCategory = 'none'
}
catch {
    $succeeded = $false
    if ($failureCategory -eq 'not_started') { $failureCategory = 'evidence_step_failed' }
    $status = Get-Milestone4EvidenceFailureStatus -Category $failureCategory
    Write-EvidenceStatus -Status $status -Reason $failureCategory
}
finally {
    if ($started -and -not $KeepEnvironment) {
        try {
            $teardown = Invoke-Compose -AllowFailure -Arguments @('down', '--volumes', '--remove-orphans') `
                -CapturePath (Join-Path $EvidenceDirectory 'compose-down.log')
            $teardownCompleted = $teardown.ExitCode -eq 0
            if (-not $teardownCompleted) {
                $succeeded = $false
                $failureCategory = 'compose_teardown_failed'
                Write-EvidenceStatus -Status 'failed' -Reason $failureCategory
            }
        } catch {
            $succeeded = $false
            $failureCategory = 'compose_teardown_failed'
            Write-EvidenceStatus -Status 'failed' -Reason $failureCategory
        }
    }
    try {
        Assert-ArtifactsSanitized
        [ordered]@{
            status = 'passed'
            scanned_after_teardown = $teardownCompleted
        } | ConvertTo-Json -Depth 2 |
            Out-File -LiteralPath (Join-Path $EvidenceDirectory 'artifact-sanitization.json') -Encoding utf8
        $sanitizationCompleted = $true
    } catch {
        $succeeded = $false
        $failureCategory = 'artifact_sanitization_failed'
        Write-EvidenceStatus -Status 'failed' -Reason $failureCategory
        [ordered]@{
            status = 'failed'
            scanned_after_teardown = $teardownCompleted
        } | ConvertTo-Json -Depth 2 |
            Out-File -LiteralPath (Join-Path $EvidenceDirectory 'artifact-sanitization.json') -Encoding utf8
    }
    foreach ($name in $environmentNames) {
        [Environment]::SetEnvironmentVariable($name, $savedEnvironment[$name], 'Process')
    }
    if ((Get-Location).Path -eq $root) { Pop-Location }
}

$teardownRequired = $started -and -not $KeepEnvironment
$summaryReady = Test-Milestone4CanonicalSummaryReady `
    -RunSucceeded $succeeded `
    -SummaryPrepared ($null -ne $milestoneSummary) `
    -SanitizationCompleted $sanitizationCompleted `
    -TeardownRequired $teardownRequired `
    -TeardownCompleted $teardownCompleted
if (-not $summaryReady) {
    if ($succeeded) {
        $succeeded = $false
        $failureCategory = 'summary_finalization_failed'
        Write-EvidenceStatus -Status 'failed' -Reason $failureCategory
    }
    $secretValues.Clear()
    throw "Milestone 4 evidence did not complete; inspect only artifacts marked sanitized (category=$failureCategory)"
}
try {
    $milestoneSummary | ConvertTo-Json -Depth 12 |
        Out-File -LiteralPath $summaryCandidatePath -Encoding utf8
    Assert-ArtifactsSanitized
    [ordered]@{
        status = 'passed'
        scanned_after_teardown = $teardownCompleted
    } | ConvertTo-Json -Depth 2 |
        Out-File -LiteralPath (Join-Path $EvidenceDirectory 'artifact-sanitization.json') -Encoding utf8
    Write-EvidenceStatus -Status 'passed' -Reason 'bounded_evidence_completed'
    $secretValues.Clear()
    # Publish the already scanned bytes only after every other fallible final
    # artifact write has completed.
    Move-Item -LiteralPath $summaryCandidatePath -Destination $canonicalSummaryPath -ErrorAction Stop
} catch {
    foreach ($path in @($summaryCandidatePath, $canonicalSummaryPath)) {
        if (Test-Path -LiteralPath $path) {
            Remove-Item -Force -LiteralPath $path -ErrorAction SilentlyContinue
        }
    }
    $canonicalSummaryRemains = Test-Path -LiteralPath $canonicalSummaryPath
    $succeeded = $false
    $failureCategory = 'summary_finalization_failed'
    Write-EvidenceStatus -Status 'failed' -Reason $failureCategory
    [ordered]@{
        status = 'failed'
        scanned_after_teardown = $teardownCompleted
    } | ConvertTo-Json -Depth 2 |
        Out-File -LiteralPath (Join-Path $EvidenceDirectory 'artifact-sanitization.json') -Encoding utf8
    $secretValues.Clear()
    if ($canonicalSummaryRemains) {
        throw 'Milestone 4 summary finalization failed and the canonical passed summary could not be revoked'
    }
    throw "Milestone 4 evidence did not complete; inspect only artifacts marked sanitized (category=$failureCategory)"
}
