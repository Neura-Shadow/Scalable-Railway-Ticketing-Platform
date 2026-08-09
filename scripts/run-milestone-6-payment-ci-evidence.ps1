[CmdletBinding()]
param(
    [string]$ProjectName = '',
    [string]$EvidenceDirectory = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$root = [System.IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$composeFile = Join-Path $root 'docker-compose.payment.yml'
$driverPath = Join-Path $PSScriptRoot 'milestone-5-physical-shard-evidence-driver.ps1'
. $driverPath

$suffix = [guid]::NewGuid().ToString('N').Substring(0, 10)
if ([string]::IsNullOrWhiteSpace($ProjectName)) { $ProjectName = "railway-m6-ci-$suffix" }
if ($ProjectName -notmatch '^[a-z0-9][a-z0-9_-]{2,62}$') { throw 'ProjectName is invalid' }
if ([string]::IsNullOrWhiteSpace($EvidenceDirectory)) {
    $EvidenceDirectory = Join-Path ([System.IO.Path]::GetTempPath()) "railway-m6-ci-$suffix"
}
$EvidenceDirectory = [System.IO.Path]::GetFullPath($EvidenceDirectory)
if (Test-Path -LiteralPath $EvidenceDirectory) { throw 'EvidenceDirectory must not already exist' }
New-Item -ItemType Directory -Path $EvidenceDirectory | Out-Null

$composeArguments = @('compose', '-p', $ProjectName, '-f', $composeFile)
$context = [pscustomobject]@{
    RepositoryPath = $root
    RawDirectory = $EvidenceDirectory
    ProjectName = $ProjectName
    ComposeFile = $composeFile
    ComposeArguments = [string[]]$composeArguments
}
$started = $false
$migrationJob = $null
$originalJWTSecret = $env:JWT_SECRET
try {
$runJWTSecret = "m6-$([guid]::NewGuid().ToString('N'))-$([guid]::NewGuid().ToString('N'))"
$env:JWT_SECRET = $runJWTSecret
if ($env:GITHUB_ACTIONS -eq 'true') { Write-Output "::add-mask::$runJWTSecret" }

function Invoke-M6Native {
    param([scriptblock]$Command, [switch]$AllowFailure)
    $previous = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = @(& $Command 2>&1 | ForEach-Object { [string]$_ })
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    if ($exitCode -ne 0 -and -not $AllowFailure) { throw "native command failed with exit code $exitCode" }
    [pscustomobject]@{ Output = $output; ExitCode = $exitCode }
}

function New-M6CustomerReservation {
    param([string]$BaseURL, [string]$TrainRunID, [int]$Index)
    $password = "M6-$([guid]::NewGuid().ToString('N').Substring(0, 14))-Aa1!"
    $email = "m6-ci-$suffix-$Index@example.test"
    $ip = "198.19.6.$($Index + 20)"
    Invoke-Milestone5DriverAPI -BaseURL $BaseURL -Method POST -Path '/api/v1/auth/register' `
        -ForwardedFor $ip -Body @{ email=$email; password=$password; display_name="M6 CI Rider $Index" } `
        -ExpectedStatus @(202) | Out-Null
    $login = Invoke-Milestone5DriverAPI -BaseURL $BaseURL -Method POST -Path '/api/v1/auth/login' `
        -ForwardedFor $ip -Body @{ email=$email; password=$password } -ExpectedStatus @(200)
    $token = [string]$login.Body.access_token
    if (-not $token) { throw 'synthetic customer login omitted its access token' }
    $passenger = Invoke-Milestone5DriverAPI -BaseURL $BaseURL -Method POST -Path '/api/v1/passengers' `
        -Token $token -Body @{ display_name="M6 CI Passenger $Index" } -ExpectedStatus @(201)
    $reservation = Invoke-Milestone5DriverAPI -BaseURL $BaseURL -Method POST -Path '/api/v1/reservations' `
        -Token $token -IdempotencyKey "m6-ci-reservation-$suffix-$Index" -Body @{
            train_run_id=$TrainRunID; origin_station_code='M2A'; destination_station_code='M2B'
            seat_class='standard'; passenger_ids=@([string]$passenger.Body.id)
        } -ExpectedStatus @(201)
    $password = $null
    [pscustomobject]@{ Token=$token; ReservationID=[string]$reservation.Body.id }
}

function Invoke-M6K6 {
    param([string]$Script, [hashtable]$Environment)
    $network = "${ProjectName}_backend"
    $userArguments = @()
    if ([System.Environment]::OSVersion.Platform -ne [System.PlatformID]::Win32NT) {
        $uid = [string](@((Invoke-M6Native -Command { & id -u }).Output) | Select-Object -Last 1)
        $gid = [string](@((Invoke-M6Native -Command { & id -g }).Output) | Select-Object -Last 1)
        if ($uid.Trim() -notmatch '^[0-9]+$' -or $gid.Trim() -notmatch '^[0-9]+$') {
            throw 'Docker evidence user identity is malformed'
        }
        $userArguments = @('--user', "$($uid.Trim()):$($gid.Trim())")
    }
    $arguments = @('run', '--rm') + $userArguments + @('--network', $network,
        '-v', "${root}:/repo:ro", '-v', "${EvidenceDirectory}:/evidence", '-w', '/repo')
    foreach ($entry in $Environment.GetEnumerator()) {
        $arguments += @('-e', "$($entry.Key)=$($entry.Value)")
    }
    $name = [System.IO.Path]::GetFileNameWithoutExtension($Script)
    $arguments += @('grafana/k6:0.55.0', 'run', '--quiet',
        '--summary-export', "/evidence/$name-summary.json", "/repo/loadtest/k6/$Script")
    $result = Invoke-M6Native -AllowFailure -Command { & docker @arguments }
    $result.Output | Set-Content -LiteralPath (Join-Path $EvidenceDirectory "$name.log") -Encoding utf8
    if ($result.ExitCode -ne 0) { throw "$Script failed" }
}

try {
    $projectLabel = "label=com.docker.compose.project=$ProjectName"
    foreach ($query in @(
        @('ps','-a','-q','--filter',$projectLabel),
        @('volume','ls','-q','--filter',$projectLabel),
        @('network','ls','-q','--filter',$projectLabel)
    )) {
        $owned = Invoke-M6Native -Command { & docker @query }
        if (@($owned.Output | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count -ne 0) {
            throw 'ProjectName already owns Docker resources; refusing destructive reuse'
        }
    }
    # Mark ownership before Compose starts so a failed partial build/start is
    # still followed by a scoped, project-name-qualified teardown.
    $started = $true
    $up = Invoke-M6Native -AllowFailure -Command { & docker @composeArguments up -d --build --wait }
    $up.Output | Set-Content -LiteralPath (Join-Path $EvidenceDirectory 'compose-up.log') -Encoding utf8
    if ($up.ExitCode -ne 0) { throw 'payment topology did not start' }
    $running = Invoke-Milestone5DriverCompose -Context $context -Arguments @('ps','--status','running','--services') `
        -Artifact 'topology-services.log'
    $runningServices = @($running.Output | ForEach-Object { ([string]$_).Trim() } | Where-Object { $_ })
    foreach ($service in @(
        'api-1','api-2','api-3','payment-worker-1','payment-worker-2',
        'payment-sandbox','payment-reconciler','redis','control-postgres',
        'booking-shard-0-postgres','booking-shard-1-postgres'
    )) {
        if ($service -notin $runningServices) { throw "required payment topology service $service is not running" }
    }
    Wait-Milestone5DriverReady -Context $context
    Invoke-Milestone5DriverCompose -Context $context -Arguments @('--profile','tools','build','physical-shard-admin') `
        -Artifact 'physical-shard-admin-build.log' | Out-Null
    Initialize-Milestone5DriverFixture -Context $context

    $trainA = '21000000-0000-4000-8000-000000000401'
    $trainC = '21000000-0000-4000-8000-000000000403'
    $migrationA = '61000000-0000-4000-8000-000000000401'
    $migrationCInitial = '61000000-0000-4000-8000-000000000402'
    $migrationC = '61000000-0000-4000-8000-000000000403'
    New-Milestone5Migration -Context $context -TrainRunID $trainA -TargetShard 'physical-shard-0' `
        -MigrationID $migrationA -Prefix 'm6-ci-train-a'
    Move-Milestone5Migration -Context $context -MigrationID $migrationA -Target rollback_window `
        -Prefix 'm6-ci-train-a'

    New-Milestone5Migration -Context $context -TrainRunID $trainC -TargetShard 'physical-shard-1' `
        -MigrationID $migrationCInitial -Prefix 'm6-ci-train-c-initial'
    $accelerated = Get-Milestone5DriverScalar -Context $context -Service 'control-postgres' `
        -Artifact 'm6-ci-train-c-initial-bounded-rollback.log' -SQL "WITH updated AS (UPDATE public.physical_shard_migrations SET rollback_window_seconds=1 WHERE migration_id='$migrationCInitial'::uuid AND state='planned' RETURNING 1) SELECT count(*) FROM updated;"
    if ($accelerated -ne 1) { throw 'initial physical fixture rollback window was not bounded' }
    Move-Milestone5Migration -Context $context -MigrationID $migrationCInitial -Target rollback_window `
        -Prefix 'm6-ci-train-c-initial'
    Complete-Milestone5MigrationAfterRollbackWindow -Context $context -MigrationID $migrationCInitial `
        -Prefix 'm6-ci-train-c-initial'

    $baseURL = Get-Milestone5DriverPublishedURL -Context $context
    $multi = New-M6CustomerReservation -BaseURL $baseURL -TrainRunID $trainA -Index 0
    $migrating = New-M6CustomerReservation -BaseURL $baseURL -TrainRunID $trainC -Index 1

    New-Milestone5Migration -Context $context -TrainRunID $trainC -TargetShard 'physical-shard-0' `
        -MigrationID $migrationC -Prefix 'm6-ci-train-c'
    Move-Milestone5Migration -Context $context -MigrationID $migrationC -Target validating_online `
        -Prefix 'm6-ci-train-c'

    $common = @{
        SANDBOX_URL='http://payment-sandbox:8099'
        SANDBOX_CONTROL_TOKEN='synthetic-disposable-fault-token'
        VUS='1'; ITERATIONS_PER_VU='1'; DURATION='2m'; POLL_ATTEMPTS='80'
    }
    $multiEnvironment = @{} + $common
    $multiEnvironment.BASE_URLS = 'http://api-1:8080,http://api-2:8080,http://api-3:8080'
    $multiEnvironment.CUSTOMER_TOKENS = $multi.Token
    $multiEnvironment.RESERVATION_IDS = $multi.ReservationID
    Invoke-M6K6 -Script 'multi-replica-payment.js' -Environment $multiEnvironment

    $jobContext = $context
    $migrationJob = Start-Job -ScriptBlock {
        param($trustedDriver, $ctx, $migrationID)
        . $trustedDriver
        Start-Sleep -Milliseconds 750
        Move-Milestone5Migration -Context $ctx -MigrationID $migrationID -Target rollback_window `
            -Prefix 'm6-ci-train-c-cutover'
    } -ArgumentList $driverPath,$jobContext,$migrationC
    $migrationEnvironment = @{} + $common
    $migrationEnvironment.BASE_URL = 'http://api-1:8080'
    $migrationEnvironment.CUSTOMER_TOKENS = $migrating.Token
    $migrationEnvironment.RESERVATION_IDS = $migrating.ReservationID
    $migrationEnvironment.MIGRATION_PHASE = 'validating_to_cutover'
    $migrationEnvironment.MIGRATION_POLL_ATTEMPTS = '100'
    Invoke-M6K6 -Script 'payment-during-migration.js' -Environment $migrationEnvironment
    if (-not (Wait-Job -Job $migrationJob -Timeout 180)) { throw 'physical migration cutover timed out' }
    Receive-Job -Job $migrationJob -ErrorAction Stop | Out-Null
    if ($migrationJob.State -ne 'Completed') { throw "physical migration job ended in $($migrationJob.State)" }

    $reservationList = "'$($multi.ReservationID)'::uuid,'$($migrating.ReservationID)'::uuid"
    $completed = Get-Milestone5DriverScalar -Context $context -Service 'control-postgres' `
        -Artifact 'completed-payment-intents.log' -SQL "SELECT count(*) FROM public.payment_intents WHERE reservation_id IN ($reservationList) AND state='completed';"
    $captures = Get-Milestone5DriverScalar -Context $context -Service 'control-postgres' `
        -Artifact 'unique-captures.log' -SQL "SELECT count(*) FROM public.payment_operations o JOIN public.payment_intents i USING(payment_intent_id) WHERE i.reservation_id IN ($reservationList) AND o.operation_type='capture' AND o.state='succeeded';"
    $orders = Get-Milestone5DriverScalar -Context $context -Service 'control-postgres' `
        -Artifact 'ticket-order-locators.log' -SQL "SELECT count(*) FROM public.ticket_order_shard_locators WHERE reservation_id IN ($reservationList) AND status='confirmed';"
    $locators = Get-Milestone5DriverScalar -Context $context -Service 'control-postgres' `
        -Artifact 'ticket-locators.log' -SQL "SELECT count(*) FROM public.ticket_shard_locators WHERE reservation_id IN ($reservationList);"
    $authoritativeOrders = 0L
    $authoritativeTickets = 0L
    foreach ($service in @('booking-shard-0-postgres','booking-shard-1-postgres')) {
        $authoritativeOrders += Get-Milestone5DriverScalar -Context $context -Service $service `
            -Artifact "$service-issued-ticket-orders.log" -SQL "SELECT count(*) FROM public.ticket_orders AS ticket_order JOIN public.train_run_write_fences AS fence ON fence.train_run_id=ticket_order.train_run_id AND fence.assignment_generation=ticket_order.assignment_generation AND fence.write_enabled WHERE ticket_order.reservation_id IN ($reservationList) AND ticket_order.status='issued';"
        $authoritativeTickets += Get-Milestone5DriverScalar -Context $context -Service $service `
            -Artifact "$service-active-tickets.log" -SQL "SELECT count(*) FROM public.tickets AS ticket JOIN public.ticket_orders AS ticket_order ON ticket_order.id=ticket.ticket_order_id JOIN public.train_run_write_fences AS fence ON fence.train_run_id=ticket_order.train_run_id AND fence.assignment_generation=ticket_order.assignment_generation AND fence.write_enabled WHERE ticket_order.reservation_id IN ($reservationList) AND ticket.status='active';"
    }
    $migrationState = Get-Milestone5MigrationState -Context $context -MigrationID $migrationC -Artifact 'migration-final-state.log'
    if ($completed -ne 2 -or $captures -ne 2 -or $orders -ne 2 -or $locators -ne 2 -or
        $authoritativeOrders -ne 2 -or $authoritativeTickets -ne 2 -or $migrationState -ne 'rollback_window') {
        throw 'payment or physical-migration invariants did not converge'
    }
    $migratingIntent = Get-Milestone5DriverUUID -Context $context -Service 'control-postgres' `
        -Artifact 'migrating-payment-intent-id.log' -SQL "SELECT payment_intent_id FROM public.payment_intents WHERE reservation_id='$($migrating.ReservationID)'::uuid;"
    $visible = Invoke-Milestone5DriverAPI -BaseURL $baseURL -Method GET `
        -Path "/api/v1/payment-intents/$migratingIntent" -Token $migrating.Token -ExpectedStatus @(200)
    if ([string]$visible.Body.state -ne 'completed') { throw 'migrated payment intent was not addressable after cutover' }

    [ordered]@{
        status='passed'; api_replicas=3; payment_workers=2; completed_payment_intents=$completed
        succeeded_capture_operations=$captures; ticket_order_locators=$orders; ticket_locators=$locators
        authoritative_issued_ticket_orders=$authoritativeOrders; authoritative_active_tickets=$authoritativeTickets
        payment_during_migration_state=$migrationState; post_cutover_intent_state=[string]$visible.Body.state
        scenarios=@('multi-replica-payment','payment-during-migration')
    } | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $EvidenceDirectory 'milestone-6-ci-summary.json') -Encoding utf8
} catch {
    if ($started) {
        try {
            Invoke-Milestone5DriverPSQL -Context $context -Service 'control-postgres' `
                -Artifact 'failure-control-snapshot.log' -SQL @"
SELECT 'intent|'||state||'|'||count(*) FROM public.payment_intents GROUP BY state
UNION ALL SELECT 'saga|'||state||'|'||current_step||'|'||coalesce(bounded_error_category,'none')||'|'||count(*) FROM public.payment_sagas GROUP BY state,current_step,bounded_error_category
UNION ALL SELECT 'operation|'||operation_type||'|'||state||'|'||coalesce(bounded_error_category,'none')||'|'||count(*) FROM public.payment_operations GROUP BY operation_type,state,bounded_error_category
UNION ALL SELECT 'readiness|'||state||'|'||claimed_ticket_count FROM public.ticket_code_claim_readiness
UNION ALL SELECT 'locator|orders|'||count(*) FROM public.ticket_order_shard_locators
UNION ALL SELECT 'locator|tickets|'||count(*) FROM public.ticket_shard_locators
UNION ALL SELECT 'directory|codes|'||count(*) FROM public.ticket_code_directory
UNION ALL SELECT 'review|'||reason_category||'|'||count(*) FROM public.payment_manual_review_cases GROUP BY reason_category;
"@ | Out-Null
            foreach ($service in @('booking-shard-0-postgres','booking-shard-1-postgres')) {
                Invoke-Milestone5DriverPSQL -Context $context -Service $service `
                    -Artifact "failure-$service-snapshot.log" -SQL @"
SELECT 'reservation|'||status||'|'||count(*) FROM public.reservations GROUP BY status
UNION ALL SELECT 'order|'||status||'|'||count(*) FROM public.ticket_orders GROUP BY status
UNION ALL SELECT 'receipt|issuance|'||count(*) FROM public.ticket_issuance_receipts
UNION ALL SELECT 'receipt|refund|'||count(*) FROM public.payment_refund_receipts
UNION ALL SELECT 'receipt|compensation|'||count(*) FROM public.payment_compensation_receipts;
"@ | Out-Null
            }
            foreach ($service in @(
                'api-1','api-2','api-3','reverse-proxy','payment-sandbox',
                'payment-worker-1','payment-worker-2','control-postgres',
                'booking-shard-0-postgres','booking-shard-1-postgres'
            )) {
                Invoke-Milestone5DriverCompose -Context $context -AllowFailure `
                    -Arguments @('logs','--no-color','--tail','200',$service) `
                    -Artifact "failure-$service.log" | Out-Null
            }
        } catch {
            # Preserve the original scenario failure; snapshots are best-effort.
        }
    }
    throw
} finally {
    if ($null -ne $migrationJob) { Remove-Job -Job $migrationJob -Force -ErrorAction SilentlyContinue }
    if ($started) {
        $down = Invoke-M6Native -AllowFailure -Command { & docker @composeArguments down -v --remove-orphans }
        $down.Output | Set-Content -LiteralPath (Join-Path $EvidenceDirectory 'compose-down.log') -Encoding utf8
        if ($down.ExitCode -ne 0) { Write-Error 'payment topology teardown failed' }
        foreach ($query in @(
            @('ps','-a','-q','--filter',$projectLabel),
            @('volume','ls','-q','--filter',$projectLabel),
            @('network','ls','-q','--filter',$projectLabel)
        )) {
            $remaining = Invoke-M6Native -AllowFailure -Command { & docker @query }
            if ($remaining.ExitCode -ne 0 -or
                @($remaining.Output | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count -ne 0) {
                Write-Error 'payment topology teardown left project-scoped Docker resources'
            }
        }
    }
}
} finally {
    if ($null -eq $originalJWTSecret) {
        Remove-Item Env:JWT_SECRET -ErrorAction SilentlyContinue
    } else {
        $env:JWT_SECRET = $originalJWTSecret
    }
}
