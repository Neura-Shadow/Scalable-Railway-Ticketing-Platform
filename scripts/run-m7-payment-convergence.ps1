[CmdletBinding()]
param(
    [string]$ProjectName = '',
    [ValidateSet(1, 2)][int]$WorkerReplicas = 1,
    [switch]$SkipBuild,
    [switch]$IntegrationProbe,
    [switch]$NativeSelfTest
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$root = [System.IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$composeFile = Join-Path $root 'docker-compose.payment.yml'
$focusedOverride = Join-Path $root 'deploy/compose/m7-payment-convergence.override.yml'
$driverPath = Join-Path $PSScriptRoot 'milestone-5-physical-shard-evidence-driver.ps1'
. $driverPath

$suffix = [guid]::NewGuid().ToString('N').Substring(0, 10)
if ([string]::IsNullOrWhiteSpace($ProjectName)) { $ProjectName = "railway-m7-payment-$suffix" }
if ($ProjectName -notmatch '^[a-z0-9][a-z0-9_-]{2,62}$') { throw 'ProjectName is invalid' }
$scratch = Join-Path ([System.IO.Path]::GetTempPath()) "railway-m7-payment-$suffix"
$composeArguments = @('compose', '-p', $ProjectName, '-f', $composeFile, '-f', $focusedOverride)
$context = [pscustomobject]@{
    RepositoryPath = $root
    RawDirectory = $scratch
    ProjectName = $ProjectName
    ComposeFile = $composeFile
    ComposeArguments = [string[]]$composeArguments
}
$originalJWTSecret = $env:JWT_SECRET
$originalIntegrationDatabaseURL = $env:M7_PAYMENT_INTEGRATION_DATABASE_URL
$originalIntegrationReservationID = $env:M7_PAYMENT_INTEGRATION_RESERVATION_ID
$started = $false
$runFailed = $false
$probeImage = ''
$probeImageBuilt = $false

function Invoke-M7FocusedNative {
    param([scriptblock]$Command, [string]$Label='native-command', [switch]$AllowFailure)
    $previous = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = @(& $Command 2>&1 | ForEach-Object { [string]$_ })
        $exitCode = $LASTEXITCODE
    } finally { $ErrorActionPreference = $previous }
    if ($exitCode -ne 0 -and -not $AllowFailure) { throw "$Label failed with exit code $exitCode" }
    return [pscustomobject]@{ Output=[string[]]$output; ExitCode=$exitCode }
}

function Invoke-M7FocusedDocker {
    param(
        [Parameter(Mandatory=$true)][string[]]$Arguments,
        [string]$Label='docker-command',
        [ValidateRange(1, 900)][int]$TimeoutSeconds=60,
        [switch]$AllowFailure
    )
    $dockerCommand = Get-Command docker -ErrorAction Stop
    $start = [System.Diagnostics.ProcessStartInfo]::new()
    $start.FileName = $dockerCommand.Source
    $start.UseShellExecute = $false
    $start.CreateNoWindow = $true
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    foreach ($argument in $Arguments) { $start.ArgumentList.Add($argument) }
    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $start
    try {
        if (-not $process.Start()) { throw "$Label did not start" }
        $stdoutTask = $process.StandardOutput.ReadToEndAsync()
        $stderrTask = $process.StandardError.ReadToEndAsync()
        if (-not $process.WaitForExit($TimeoutSeconds * 1000)) {
            try { $process.Kill($true); $process.WaitForExit() } catch { }
            if ($AllowFailure) {
                return [pscustomobject]@{Output=[string[]]@("$Label timed out");ExitCode=124}
            }
            throw "$Label timed out"
        }
        $stdout = $stdoutTask.GetAwaiter().GetResult()
        $stderr = $stderrTask.GetAwaiter().GetResult()
        $lines = @($stdout, $stderr) | ForEach-Object { [string]$_ -split "`r?`n" } | Where-Object { $_ -ne '' }
        if ($process.ExitCode -ne 0 -and -not $AllowFailure) { throw "$Label failed with exit code $($process.ExitCode)" }
        return [pscustomobject]@{Output=[string[]]$lines;ExitCode=$process.ExitCode}
    } finally { $process.Dispose() }
}

function Get-M7FocusedPortURL {
    $container = Get-M7FocusedContainer -Service 'api-1'
    $result = Invoke-M7FocusedDocker -Label 'api-port-query' -Arguments @('inspect','--format','{{(index (index .NetworkSettings.Ports "8080/tcp") 0).HostPort}}',$container)
    $value = [string](@($result.Output | Where-Object { $_.Trim() }) | Select-Object -Last 1)
    if ($value -notmatch '^\d{2,5}$') { throw 'api-1 did not publish a bounded localhost port' }
    return "http://127.0.0.1:$value"
}

function Get-M7FocusedContainer {
    param([string]$Service)
    if ($Service -notmatch '^[a-z0-9][a-z0-9-]{1,63}$') { throw 'focused service name is invalid' }
    $name = "$ProjectName-$Service-1"
    if ($name -notmatch '^[a-z0-9][a-z0-9_-]{2,127}$') { throw 'focused container name is invalid' }
    $result = Invoke-M7FocusedDocker -Label "container-query-$Service" -Arguments @('inspect','--format','{{.Id}}',$name)
    $container = [string](@($result.Output | Where-Object { $_.Trim() }) | Select-Object -Last 1).Trim()
    if ($container -notmatch '^[a-f0-9]{12,64}$') { throw "focused container identity is unavailable for $Service" }
    return $container
}

function Get-M7FocusedScalar {
    param([string]$Service, [string]$User, [string]$Database, [string]$SQL)
    if ($SQL.Length -gt 12000) { throw 'focused SQL exceeds the bounded diagnostic size' }
    $container = Get-M7FocusedContainer -Service $Service
    $result = Invoke-M7FocusedDocker -Label "psql-$Service" -Arguments @(
        'exec',$container,'psql','-X','-q','-A','-t','-v','ON_ERROR_STOP=1','-U',$User,'-d',$Database,'-c',$SQL
    )
    return [string](@($result.Output | Where-Object { $_.Trim() }) | Select-Object -Last 1).Trim()
}

function Wait-M7FocusedScalar {
    param([string]$Service='control-postgres', [string]$SQL, [scriptblock]$Accept, [int]$Attempts=120)
    for ($attempt=1; $attempt -le $Attempts; $attempt++) {
        $value = Get-M7FocusedScalar -Service $Service -User $(if ($Service -eq 'control-postgres') {'railway_control'} else {'railway_booking'}) -Database $(if ($Service -eq 'control-postgres') {'railway_control'} else {'railway_booking'}) -SQL $SQL
        if (& $Accept $value) { return $value }
        Start-Sleep -Milliseconds 250
    }
    throw 'focused state did not converge within the bounded poll window'
}

function Protect-M7FocusedDiagnostic {
    param([string[]]$Lines)
    return @($Lines | Select-Object -Last 40 | ForEach-Object {
        ([string]$_) `
            -replace '(?i)postgres(?:ql)?://[^\s''"]+', '[REDACTED_DSN]' `
            -replace '\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b', '[REDACTED_UUID]' `
            -replace '(?i)(password|secret|token|api[_-]?key)[=:][^\s,}]+', '$1=[REDACTED]'
    })
}

function Get-M7FocusedWorkerLogs {
    param([string[]]$Services)
    $lines = [System.Collections.Generic.List[string]]::new()
    foreach ($service in $Services) {
        $container = Get-M7FocusedContainer -Service $service
        # Project preflight proves these containers are fresh, so the complete
        # container log also retains failures emitted before a worker restart.
        $result = Invoke-M7FocusedDocker -Label "worker-logs-$service" -Arguments @('logs',$container)
        foreach ($line in $result.Output) { $lines.Add([string]$line) }
    }
    return [string[]]$lines
}

function Assert-M7FocusedRuntimePrivileges {
    $sql = @"
SELECT current_setting('railway.deployment_region')||'|'||current_setting('railway.deployment_role')||'|'||
       current_setting('railway.region_epoch')||'|'||current_setting('railway.regional_writes_enabled')||'|'||
       (SELECT region||'|'||state||'|'||writes_enabled::text||'|'||epoch::text FROM public.regional_write_authority WHERE singleton)||'|'||
       has_table_privilege(current_user,'public.payment_operations','SELECT,INSERT,UPDATE')::text||'|'||
       has_table_privilege(current_user,'public.payment_webhook_inbox','SELECT,UPDATE')::text||'|'||
       has_table_privilege(current_user,'public.payment_sagas','SELECT,UPDATE')::text||'|'||
       has_table_privilege(current_user,'public.payment_intents','SELECT,UPDATE')::text||'|'||
       has_table_privilege(current_user,'public.payment_saga_actions','SELECT,INSERT,UPDATE')::text||'|'||
       has_table_privilege(current_user,'public.financial_ledger_transactions','SELECT,INSERT')::text||'|'||
       has_table_privilege(current_user,'public.financial_ledger_postings','SELECT,INSERT')::text||'|'||
       has_table_privilege(current_user,'public.financial_ledger_reversals','SELECT')::text||'|'||
       has_table_privilege(current_user,'public.reservation_shard_locators','SELECT')::text||'|'||
       has_table_privilege(current_user,'public.ticket_order_shard_locators','SELECT,INSERT,UPDATE')::text||'|'||
       has_table_privilege(current_user,'public.ticket_shard_locators','SELECT,INSERT,UPDATE')::text||'|'||
       has_table_privilege(current_user,'public.ticket_code_directory','SELECT,INSERT,UPDATE')::text||'|'||
       has_function_privilege(current_user,'public.lock_regional_write_authority()','EXECUTE')::text||'|'||
       has_table_privilege(current_user,'public.regional_write_authority','UPDATE')::text;
"@
    $container = Get-M7FocusedContainer -Service 'control-postgres'
    $regionalOptions = '-c railway.deployment_region=region-a -c railway.deployment_role=active -c railway.region_epoch=1 -c railway.regional_writes_enabled=true'
    $result = Invoke-M7FocusedDocker -Label 'runtime-privilege-preflight' -Arguments @(
        'exec','-e','PGPASSWORD=runtime-local-only','-e',"PGOPTIONS=$regionalOptions",$container,
        'psql','-X','-q','-A','-t','-v','ON_ERROR_STOP=1','-U','railway_runtime','-d','railway_control','-c',$sql
    )
    $value = [string](@($result.Output | Where-Object { $_.Trim() }) | Select-Object -Last 1).Trim()
    if ($value -ne 'region-a|active|1|true|region-a|active|true|1|true|true|true|true|true|true|true|true|true|true|true|true|true|false') {
        throw "runtime privilege preflight failed: $value"
    }
}

function New-M7FocusedCustomerReservation {
    param([string]$BaseURL, [string]$TrainRunID)
    $password = "M7-$([guid]::NewGuid().ToString('N').Substring(0, 14))-Aa1!"
    $email = "m7-focused-$suffix@example.test"
    $ip = '198.19.77.41'
    Invoke-Milestone5DriverAPI -BaseURL $BaseURL -Method POST -Path '/api/v1/auth/register' -ForwardedFor $ip `
        -Body @{email=$email;password=$password;display_name='M7 Focused Rider'} -ExpectedStatus @(202) | Out-Null
    $login = Invoke-Milestone5DriverAPI -BaseURL $BaseURL -Method POST -Path '/api/v1/auth/login' -ForwardedFor $ip `
        -Body @{email=$email;password=$password} -ExpectedStatus @(200)
    $token = [string]$login.Body.access_token
    if ([string]::IsNullOrWhiteSpace($token)) { throw 'focused login omitted access token' }
    $passengers = [System.Collections.Generic.List[string]]::new()
    foreach ($index in 0,1) {
        $passenger = Invoke-Milestone5DriverAPI -BaseURL $BaseURL -Method POST -Path '/api/v1/passengers' -Token $token `
            -Body @{display_name="M7 Focused Passenger $index"} -ExpectedStatus @(201)
        $passengers.Add([string]$passenger.Body.id)
    }
    $reservation = Invoke-Milestone5DriverAPI -BaseURL $BaseURL -Method POST -Path '/api/v1/reservations' -Token $token `
        -IdempotencyKey "m7-focused-reservation-$suffix" -Body @{
            train_run_id=$TrainRunID;origin_station_code='M2A';destination_station_code='M2B';
            seat_class='standard';passenger_ids=[string[]]$passengers
        } -ExpectedStatus @(201)
    if ([string]$reservation.Body.id -notmatch '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$') {
        throw 'reservation response omitted a bounded UUID'
    }
    $password = $null
    return [pscustomobject]@{Token=$token;ReservationID=[string]$reservation.Body.id}
}

function Complete-M7FocusedSandboxPayment {
    param([string]$BaseURL, [string]$ReservationID)
    $hosted = Wait-M7FocusedScalar -SQL "SELECT coalesce(hosted_session_ref,'') FROM public.payment_intents WHERE reservation_id='$ReservationID'::uuid ORDER BY created_at DESC LIMIT 1" -Accept { param($v) $v -match '^sandbox-checkout:([A-Za-z0-9._:-]+)$' }
    if ($hosted -notmatch '^sandbox-checkout:([A-Za-z0-9._:-]+)$') { throw 'hosted reference was not bounded' }
    $providerPaymentID = $Matches[1]
    $sandboxContainer = Get-M7FocusedContainer -Service 'payment-sandbox'
    Invoke-M7FocusedDocker -Label 'sandbox-authorize' -Arguments @(
        'exec',$sandboxContainer,'wget','-q','-O','/dev/null','--post-data=',"http://127.0.0.1:8099/hosted/checkouts/$providerPaymentID/authorize"
    ) | Out-Null
    $drained = Invoke-M7FocusedDocker -Label 'sandbox-webhook-drain' -Arguments @(
        'exec',$sandboxContainer,'wget','-q','-O','-','--header=X-Sandbox-Control-Token: synthetic-disposable-fault-token','http://127.0.0.1:8099/_sandbox/webhooks'
    )
    $events = @(($drained.Output -join "`n") | ConvertFrom-Json)
    if ($events.Count -lt 1 -or $events.Count -gt 20) { throw 'webhook drain count is outside the focused bound' }
    foreach ($event in $events) {
        $body = [Convert]::FromBase64String([string]$event.Body)
        $response = Invoke-WebRequest -UseBasicParsing -Method Post -Uri "$BaseURL/webhooks/payments/sandbox" -ContentType 'application/json' -Body $body -Headers @{
            'X-Payment-Key-ID'=[string]$event.Headers.key_id
            'X-Payment-Timestamp'=[string]$event.Headers.timestamp
            'X-Payment-Signature'=[string]$event.Headers.signature
        }
        if ([int]$response.StatusCode -notin @(200,202)) { throw 'webhook was not durably acknowledged' }
    }
}

function Assert-M7FocusedConvergence {
    param([string]$ReservationID)
    Wait-M7FocusedScalar -SQL "SELECT intent.state||'|'||saga.state||'|'||saga.current_step FROM public.payment_intents AS intent JOIN public.payment_sagas AS saga USING(payment_intent_id) WHERE intent.reservation_id='$ReservationID'::uuid" -Accept { param($v) $v -eq 'completed|completed|complete' } | Out-Null
    $control = Get-M7FocusedScalar -Service control-postgres -User railway_control -Database railway_control -SQL @"
SELECT (SELECT count(*) FROM public.payment_operations AS operation JOIN public.payment_intents AS intent USING(payment_intent_id) WHERE intent.reservation_id='$ReservationID'::uuid AND operation.operation_type='capture' AND operation.state='succeeded')::text||'|'||
       (SELECT count(*) FROM public.payment_saga_actions AS action JOIN public.payment_sagas AS saga ON saga.saga_id=action.saga_id JOIN public.payment_intents AS intent ON intent.payment_intent_id=saga.payment_intent_id WHERE intent.reservation_id='$ReservationID'::uuid AND action.action_type='issue_tickets' AND action.state='succeeded')::text;
"@
    if ($control -ne '1|1') { throw "control convergence invariant failed: $control" }
    $shard = Get-M7FocusedScalar -Service booking-shard-0-postgres -User railway_booking -Database railway_booking -SQL @"
SELECT (SELECT count(*) FROM public.ticket_issuance_receipts WHERE reservation_id='$ReservationID'::uuid)::text||'|'||
       (SELECT count(*) FROM public.ticket_orders WHERE reservation_id='$ReservationID'::uuid AND status='issued')::text||'|'||
       (SELECT count(*) FROM public.tickets AS ticket JOIN public.ticket_orders AS orders ON orders.id=ticket.ticket_order_id WHERE orders.reservation_id='$ReservationID'::uuid AND ticket.status='active')::text||'|'||
       (SELECT count(*)-count(DISTINCT ticket.ticket_code) FROM public.tickets AS ticket JOIN public.ticket_orders AS orders ON orders.id=ticket.ticket_order_id WHERE orders.reservation_id='$ReservationID'::uuid)::text;
"@
    if ($shard -ne '1|1|2|0') { throw "shard convergence invariant failed: $shard" }
}

if ($NativeSelfTest) {
    $version = Invoke-M7FocusedDocker -Label 'native-self-test-version' -TimeoutSeconds 10 -Arguments @('version','--format','{{.Server.Version}}')
    if ($version.ExitCode -ne 0 -or @($version.Output | Where-Object { $_ -match '^\d+\.\d+' }).Count -ne 1) { throw 'native self-test version probe failed' }
    $nonzero = Invoke-M7FocusedDocker -AllowFailure -Label 'native-self-test-nonzero' -TimeoutSeconds 10 -Arguments @('inspect','m7-focused-container-that-must-not-exist')
    if ($nonzero.ExitCode -eq 0) { throw 'native self-test accepted misleading success' }
    $timedOut = $false
    try { Invoke-M7FocusedDocker -Label 'native-self-test-timeout' -TimeoutSeconds 1 -Arguments @('events') | Out-Null } catch {
        $timedOut = $_.Exception.Message -eq 'native-self-test-timeout timed out'
    }
    if (-not $timedOut) { throw 'native self-test did not enforce its hard timeout' }
    [ordered]@{status='passed';normal_exit=$version.ExitCode;nonzero_exit=$nonzero.ExitCode;timeout_enforced=$true} | ConvertTo-Json -Compress
    return
}

try {
    New-Item -ItemType Directory -Path $scratch | Out-Null
    $env:JWT_SECRET = "m7-focused-$([guid]::NewGuid().ToString('N'))-$([guid]::NewGuid().ToString('N'))"
    $projectLabel = "label=com.docker.compose.project=$ProjectName"
    foreach ($query in @(@('ps','-a','-q','--filter',$projectLabel),@('volume','ls','-q','--filter',$projectLabel),@('network','ls','-q','--filter',$projectLabel))) {
        $owned = Invoke-M7FocusedDocker -Label 'project-resource-preflight' -Arguments $query
        if (@($owned.Output | Where-Object { $_ }).Count -ne 0) { throw 'ProjectName already owns Docker resources' }
    }
    Invoke-M7FocusedDocker -Label 'compose-config' -Arguments (@($composeArguments) + @('config','--quiet')) | Out-Null
    if ($IntegrationProbe) {
        $probeImage = "railway-m7-probe-$suffix"
        if ($probeImage -notmatch '^[a-z0-9][a-z0-9_-]{2,127}$') { throw 'focused integration image name is invalid' }
        $existingProbeImage = Invoke-M7FocusedDocker -AllowFailure -Label 'integration-probe-image-preflight' -Arguments @('image','inspect',$probeImage)
        if ($existingProbeImage.ExitCode -eq 0) { throw 'focused integration image already exists' }
        Invoke-M7FocusedDocker -Label 'integration-probe-image-build' -TimeoutSeconds 900 -Arguments @(
            'build','--pull=false','-f',(Join-Path $root 'deploy/docker/m7-payment-integration.Dockerfile'),'-t',$probeImage,$root
        ) | Out-Null
        $probeImageBuilt = $true
    }
    $services = @('api-1')
    if (-not $IntegrationProbe) {
        $services += 'payment-worker-1'
        if ($WorkerReplicas -eq 2) { $services += 'payment-worker-2' }
    }
    $up = @('up','-d','--wait')
    if (-not $SkipBuild) { $up += '--build' }
    $up += $services
    $started = $true
    Invoke-M7FocusedDocker -Label 'compose-up' -TimeoutSeconds 900 -Arguments (@($composeArguments) + $up) | Out-Null
    if (-not $SkipBuild) {
        Invoke-M7FocusedDocker -Label 'tool-image-build' -TimeoutSeconds 900 -Arguments (@($composeArguments) + @('--profile','tools','build','physical-shard-admin','payment-reconciler')) | Out-Null
    }
    Assert-M7FocusedRuntimePrivileges
    Initialize-Milestone5DriverFixture -Context $context
    $trainRunID = '21000000-0000-4000-8000-000000000401'
    $migrationID = '68000000-0000-4000-8000-000000000701'
    New-Milestone5Migration -Context $context -TrainRunID $trainRunID -TargetShard 'physical-shard-0' -MigrationID $migrationID -Prefix 'm7-focused-train'
    Move-Milestone5Migration -Context $context -MigrationID $migrationID -Target rollback_window -Prefix 'm7-focused-train'
    $baseURL = Get-M7FocusedPortURL
    $fixture = New-M7FocusedCustomerReservation -BaseURL $baseURL -TrainRunID $trainRunID
    $intent = Invoke-Milestone5DriverAPI -BaseURL $baseURL -Method POST -Path "/api/v1/reservations/$($fixture.ReservationID)/payment-intents" `
        -Token $fixture.Token -IdempotencyKey "m7-focused-payment-$suffix" -Body @{} -ExpectedStatus @(202)
    if ([string]$intent.Body.id -notmatch '^[0-9a-f-]{36}$') { throw 'payment intent omitted durable identity' }
    if ($IntegrationProbe) {
        $env:M7_PAYMENT_INTEGRATION_DATABASE_URL = 'postgresql://railway_runtime:runtime-local-only@control-postgres:5432/railway_control?sslmode=disable&connect_timeout=3'
        $env:M7_PAYMENT_INTEGRATION_RESERVATION_ID = [string]$fixture.ReservationID
        $network = "${ProjectName}_backend"
        if ($network -notmatch '^[a-z0-9][a-z0-9_-]{2,127}$') { throw 'focused integration network name is invalid' }
        $mount = "type=bind,source=$root,target=/src,readonly"
        $probeCommand = 'go test ./internal/payment/worker/postgres -run ^TestM7PaymentWorkerRunOnceV11Lanes$ -count=10 -timeout 300s && go test -race ./internal/payment/worker/postgres -run ^TestM7PaymentWorkerRunOnceV11Lanes$ -count=3 -timeout 420s'
        $probe = Invoke-M7FocusedDocker -AllowFailure -Label 'm7-payment-integration-probe' -TimeoutSeconds 900 -Arguments @(
            'run','--rm','--pull=never','--network',$network,'--mount',$mount,'-w','/src',
            '-e','M7_PAYMENT_INTEGRATION_DATABASE_URL','-e','M7_PAYMENT_INTEGRATION_RESERVATION_ID',
            $probeImage,'sh','-ec',$probeCommand
        )
        if ($probe.ExitCode -ne 0) {
            $boundedProbe = Protect-M7FocusedDiagnostic -Lines $probe.Output
            throw ("m7 payment integration probe failed`n" + ($boundedProbe -join "`n"))
        }
        $probe.Output | Where-Object { $_ -match '^(ok|FAIL|--- FAIL:)' }
        [ordered]@{status='passed';operation_runs=13;webhook_runs=13;action_runs=13;runtime_role='railway_runtime';region='region-a';epoch=1;writes_enabled=$true} | ConvertTo-Json -Compress
        return
    }
    Complete-M7FocusedSandboxPayment -BaseURL $baseURL -ReservationID $fixture.ReservationID
    Assert-M7FocusedConvergence -ReservationID $fixture.ReservationID
    $reconcile = Invoke-M7FocusedDocker -Label 'payment-reconciliation' -TimeoutSeconds 60 -Arguments (@($composeArguments) + @(
        'run','--rm','-e','PAYMENT_PROCESSING_GRACE_SECONDS=1','payment-reconciler',
        '--once','--scope','payment-all','--batch-size','100','--timeout','30s'
    ))
    $reconcileLine = [string](@($reconcile.Output | Where-Object { ([string]$_).TrimStart().StartsWith('{') }) | Select-Object -Last 1)
    if ([string]::IsNullOrWhiteSpace($reconcileLine)) { throw 'payment reconciliation omitted its bounded JSON result' }
    $reconcileResult = $reconcileLine | ConvertFrom-Json
    if ([string]$reconcileResult.status -ne 'completed' -or -not [bool]$reconcileResult.read_only -or
        [int]$reconcileResult.rows_examined -lt 1 -or [int]$reconcileResult.shard_rows_found -lt 1 -or
        [int]$reconcileResult.issued_orders -lt 1 -or [int]$reconcileResult.mismatch_count -ne 0 -or
        [int]$reconcileResult.manual_reviews -ne 0 -or [bool]$reconcileResult.truncated) {
        throw ('payment reconciliation did not prove a clean non-empty pass: status={0};read_only={1};rows={2};shard_rows={3};issued={4};mismatches={5};manual={6};truncated={7}' -f
            $reconcileResult.status,$reconcileResult.read_only,$reconcileResult.rows_examined,$reconcileResult.shard_rows_found,
            $reconcileResult.issued_orders,$reconcileResult.mismatch_count,$reconcileResult.manual_reviews,$reconcileResult.truncated)
    }
    $workerLogs = Get-M7FocusedWorkerLogs -Services $services
    $boundedFailures = @($workerLogs | Select-String 'payment pass completed with isolated failures')
    if ($boundedFailures.Count -ne 0) { throw 'payment worker retained a bounded lane failure after convergence' }
    [ordered]@{status='passed';workers=$WorkerReplicas;capture_count=1;issue_action_count=1;issuance_receipt_count=1;issued_order_count=1;active_ticket_count=2;duplicate_ticket_codes=0;reconciliation_mismatches=[int]$reconcileResult.mismatch_count} | ConvertTo-Json -Compress
} catch {
    $runFailed = $true
    $rootFailure = Protect-M7FocusedDiagnostic -Lines @([string]$_.Exception.Message)
    if ($started) {
        try { $state = Get-M7FocusedScalar -Service control-postgres -User railway_control -Database railway_control -SQL "SELECT coalesce((SELECT intent.state||'|'||saga.state||'|'||saga.current_step FROM public.payment_intents AS intent JOIN public.payment_sagas AS saga USING(payment_intent_id) ORDER BY intent.created_at DESC LIMIT 1),'missing')" } catch { $state = 'state=diagnostic_unavailable' }
        try { $counts = Get-M7FocusedScalar -Service control-postgres -User railway_control -Database railway_control -SQL "SELECT 'operations='||coalesce((SELECT string_agg(operation_type||':'||state||':'||count::text,',' ORDER BY operation_type,state) FROM (SELECT operation_type,state,count(*) AS count FROM public.payment_operations GROUP BY operation_type,state) grouped),'none')||'|webhooks='||coalesce((SELECT string_agg(state||':'||count::text,',' ORDER BY state) FROM (SELECT state,count(*) AS count FROM public.payment_webhook_inbox GROUP BY state) grouped),'none')||'|actions='||coalesce((SELECT string_agg(action_type||':'||state||':'||count::text,',' ORDER BY action_type,state) FROM (SELECT action_type,state,count(*) AS count FROM public.payment_saga_actions GROUP BY action_type,state) grouped),'none')" } catch { $counts = 'counts=diagnostic_unavailable' }
        $logs = @()
        if (-not $IntegrationProbe) {
            $availableWorkers = @('payment-worker-1')
            if ($WorkerReplicas -eq 2) { $availableWorkers += 'payment-worker-2' }
            try { $logs = Get-M7FocusedWorkerLogs -Services $availableWorkers } catch { $logs = @('worker_logs=diagnostic_unavailable') }
        }
        $safe = @($rootFailure) + @(Protect-M7FocusedDiagnostic -Lines (@($state,$counts) + @($logs)))
        throw ("focused payment convergence failed`n" + ($safe -join "`n"))
    } else {
        throw ("focused payment convergence failed`n" + ($rootFailure -join "`n"))
    }
} finally {
    $cleanupFailures = [System.Collections.Generic.List[string]]::new()
    try {
        if ($started) {
            try {
                $down = Invoke-M7FocusedDocker -AllowFailure -Label 'compose-down' -TimeoutSeconds 120 -Arguments (@($composeArguments) + @('down','-v','--remove-orphans'))
                if ($down.ExitCode -ne 0) { $cleanupFailures.Add('compose_down_failed') }
            } catch { $cleanupFailures.Add('compose_down_failed') }
        }
        if ($probeImageBuilt) {
            try {
                $imageRemove = Invoke-M7FocusedDocker -AllowFailure -Label 'integration-probe-image-remove' -TimeoutSeconds 60 -Arguments @('image','rm',$probeImage)
                if ($imageRemove.ExitCode -ne 0) { $cleanupFailures.Add('probe_image_remove_failed') }
            } catch { $cleanupFailures.Add('probe_image_remove_failed') }
        }
        if (Test-Path -LiteralPath $scratch) {
            try {
                $resolved = [System.IO.Path]::GetFullPath($scratch)
                if (-not $resolved.StartsWith([System.IO.Path]::GetTempPath(), [System.StringComparison]::OrdinalIgnoreCase)) { throw 'unsafe scratch path' }
                [System.IO.Directory]::Delete($resolved, $true)
            } catch { $cleanupFailures.Add('scratch_remove_failed') }
        }
    } finally {
        $env:JWT_SECRET = $originalJWTSecret
        $env:M7_PAYMENT_INTEGRATION_DATABASE_URL = $originalIntegrationDatabaseURL
        $env:M7_PAYMENT_INTEGRATION_RESERVATION_ID = $originalIntegrationReservationID
    }
    if ($cleanupFailures.Count -ne 0) {
        $cleanupMessage = 'focused cleanup failed: ' + ($cleanupFailures -join ',')
        if ($runFailed) { Write-Warning $cleanupMessage } else { throw $cleanupMessage }
    }
}
