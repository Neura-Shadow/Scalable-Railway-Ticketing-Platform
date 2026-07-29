# Trusted repository-local controller for the disposable Milestone 5 evidence
# topology. The runner dot-sources this file and owns topology start/teardown,
# artifact publication, sanitization and the ten k6 invocations.
Set-StrictMode -Version Latest

$script:M5TrainA = '21000000-0000-4000-8000-000000000401'
$script:M5TrainB = '21000000-0000-4000-8000-000000000402'
$script:M5TrainC = '21000000-0000-4000-8000-000000000403'
$script:M5TrainD = '21000000-0000-4000-8000-000000000404'
$script:M5TrainE = '21000000-0000-4000-8000-000000000405'
$script:M5TrainF = '21000000-0000-4000-8000-000000000406'
$script:M5TrainG = '21000000-0000-4000-8000-000000000407'
$script:M5MigrationA = '51000000-0000-4000-8000-000000000401'
$script:M5MigrationB = '51000000-0000-4000-8000-000000000402'
$script:M5MigrationC = '51000000-0000-4000-8000-000000000403'
$script:M5MigrationCSecond = '51000000-0000-4000-8000-000000000404'
$script:M5MigrationCReverse = '51000000-0000-4000-8000-000000000405'
$script:M5FareA = '21000000-0000-4000-8000-000000000601'

function Assert-Milestone5DriverContext {
    param([Parameter(Mandatory = $true)][object]$Context)
    foreach ($name in @('RepositoryPath', 'RawDirectory', 'ProjectName', 'ComposeFile', 'ComposeArguments')) {
        $property = $Context.PSObject.Properties[$name]
        if ($null -eq $property -or $null -eq $property.Value) {
            throw "Milestone 5 driver context omitted $name"
        }
    }
    if (-not (Test-Path -LiteralPath ([string]$Context.RepositoryPath) -PathType Container) -or
        -not (Test-Path -LiteralPath ([string]$Context.RawDirectory) -PathType Container)) {
        throw 'Milestone 5 driver context paths are unavailable'
    }
}

function Invoke-Milestone5DriverNative {
    param(
        [Parameter(Mandatory = $true)][scriptblock]$Command,
        [switch]$AllowFailure
    )
    $previous = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = @(& $Command 2>&1 | ForEach-Object { [string]$_ })
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    if ($exitCode -ne 0 -and -not $AllowFailure) {
        throw "Milestone 5 driver command failed with exit code $exitCode"
    }
    return [pscustomobject]@{ Output = @($output); ExitCode = $exitCode }
}

function Write-Milestone5DriverArtifact {
    param(
        [Parameter(Mandatory = $true)][object]$Context,
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][AllowNull()][AllowEmptyCollection()][AllowEmptyString()][string[]]$Lines
    )
    if ($Name -notmatch '^[a-z0-9][a-z0-9._-]{0,95}$') { throw 'invalid driver artifact name' }
    @($Lines) | Out-File -LiteralPath (Join-Path ([string]$Context.RawDirectory) $Name) -Encoding utf8
}

function Invoke-Milestone5DriverCompose {
    param(
        [Parameter(Mandatory = $true)][object]$Context,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [string]$Artifact = '',
        [switch]$AllowFailure
    )
    Assert-Milestone5DriverContext -Context $Context
    $composeArguments = [string[]]@($Context.ComposeArguments)
    $result = Invoke-Milestone5DriverNative -AllowFailure -Command {
        & docker @composeArguments @Arguments
    }
    if (-not [string]::IsNullOrWhiteSpace($Artifact)) {
        Write-Milestone5DriverArtifact -Context $Context -Name $Artifact -Lines $result.Output
    }
    if ($result.ExitCode -ne 0 -and -not $AllowFailure) {
        throw 'Milestone 5 Docker Compose command failed'
    }
    return $result
}

function Get-Milestone5DriverDatabaseIdentity {
    param([Parameter(Mandatory = $true)][string]$Service)
    switch ($Service) {
        'control-postgres' { return @('railway_control', 'railway_control') }
        'booking-shard-0-postgres' { return @('railway_booking', 'railway_booking') }
        'booking-shard-1-postgres' { return @('railway_booking', 'railway_booking') }
        default { throw 'unknown Milestone 5 PostgreSQL service' }
    }
}

function Invoke-Milestone5DriverPSQL {
    param(
        [Parameter(Mandatory = $true)][object]$Context,
        [Parameter(Mandatory = $true)][ValidateSet('control-postgres', 'booking-shard-0-postgres', 'booking-shard-1-postgres')][string]$Service,
        [Parameter(Mandatory = $true)][string]$SQL,
        [Parameter(Mandatory = $true)][string]$Artifact,
        [switch]$AsInput
    )
    $identity = Get-Milestone5DriverDatabaseIdentity -Service $Service
    $composeArguments = [string[]]@($Context.ComposeArguments)
    if ($AsInput) {
        $result = Invoke-Milestone5DriverNative -AllowFailure -Command {
            $SQL | & docker @composeArguments exec -T $Service psql -U $identity[0] -d $identity[1] -v ON_ERROR_STOP=1
        }
    } else {
        $result = Invoke-Milestone5DriverCompose -Context $Context -AllowFailure -Arguments @(
            'exec', '-T', $Service, 'psql', '-U', $identity[0], '-d', $identity[1],
            '-v', 'ON_ERROR_STOP=1', '-At', '-c', $SQL
        )
    }
    Write-Milestone5DriverArtifact -Context $Context -Name $Artifact -Lines $result.Output
    if ($result.ExitCode -ne 0) { throw "Milestone 5 PostgreSQL command failed for $Artifact" }
    return $result
}

function Get-Milestone5DriverScalar {
    param(
        [Parameter(Mandatory = $true)][object]$Context,
        [Parameter(Mandatory = $true)][string]$Service,
        [Parameter(Mandatory = $true)][string]$SQL,
        [Parameter(Mandatory = $true)][string]$Artifact
    )
    $result = Invoke-Milestone5DriverPSQL -Context $Context -Service $Service -SQL $SQL -Artifact $Artifact
    $value = [string](@($result.Output | Where-Object { ([string]$_).Trim() -match '^-?[0-9]+$' }) | Select-Object -Last 1)
    $parsed = 0L
    if (-not [int64]::TryParse($value.Trim(), [ref]$parsed)) { throw "database scalar missing for $Artifact" }
    return $parsed
}

function Get-Milestone5DriverUUID {
    param(
        [Parameter(Mandatory = $true)][object]$Context,
        [Parameter(Mandatory = $true)][string]$Service,
        [Parameter(Mandatory = $true)][string]$SQL,
        [Parameter(Mandatory = $true)][string]$Artifact
    )
    $result = Invoke-Milestone5DriverPSQL -Context $Context -Service $Service -SQL $SQL -Artifact $Artifact
    $value = [string](@($result.Output | Where-Object {
        ([string]$_).Trim() -match '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
    }) | Select-Object -Last 1)
    $parsed = [guid]::Empty
    if (-not [guid]::TryParse($value.Trim(), [ref]$parsed) -or $parsed -eq [guid]::Empty) {
        throw "database UUID missing for $Artifact"
    }
    return $parsed.ToString()
}

function Get-Milestone5DriverPublishedURL {
    param([Parameter(Mandatory = $true)][object]$Context)
    $result = Invoke-Milestone5DriverCompose -Context $Context -Arguments @('port', 'reverse-proxy', '8080')
    $endpoint = [string](@($result.Output | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) }) | Select-Object -Last 1)
    if ($endpoint.Trim() -notmatch '^(127\.0\.0\.1|0\.0\.0\.0|\[::\]|::):(?<port>[1-9][0-9]{1,4})$') {
        throw 'reverse-proxy did not publish a bounded loopback endpoint'
    }
    return "http://127.0.0.1:$($matches.port)"
}

function Invoke-Milestone5DriverAPI {
    param(
        [Parameter(Mandatory = $true)][string]$BaseURL,
        [Parameter(Mandatory = $true)][ValidateSet('GET', 'POST', 'PATCH')][string]$Method,
        [Parameter(Mandatory = $true)][string]$Path,
        [object]$Body = $null,
        [string]$Token = '',
        [string]$IdempotencyKey = '',
        [string]$ForwardedFor = '',
        [int[]]$ExpectedStatus = @(200)
    )
    $headers = @{ Accept = 'application/json' }
    if ($Token) { $headers.Authorization = "Bearer $Token" }
    if ($IdempotencyKey) { $headers['Idempotency-Key'] = $IdempotencyKey }
    if ($ForwardedFor) { $headers['X-Forwarded-For'] = $ForwardedFor }
    $parameters = @{
        Uri = "$($BaseURL.TrimEnd('/'))$Path"; Method = $Method; Headers = $headers
        UseBasicParsing = $true; TimeoutSec = 20
    }
    if ($null -ne $Body) {
        $parameters.ContentType = 'application/json'
        $parameters.Body = $Body | ConvertTo-Json -Compress -Depth 8
    }
    try {
        $response = Invoke-WebRequest @parameters
        $status = [int]$response.StatusCode
        $content = [string]$response.Content
    } catch {
        if ($null -eq $_.Exception.Response) { throw }
        $status = [int]$_.Exception.Response.StatusCode
        $stream = $_.Exception.Response.GetResponseStream()
        $reader = [System.IO.StreamReader]::new($stream)
        try { $content = $reader.ReadToEnd() } finally { $reader.Dispose() }
    }
    $decoded = $null
    if (-not [string]::IsNullOrWhiteSpace($content)) { $decoded = $content | ConvertFrom-Json }
    if ($status -notin $ExpectedStatus) { throw "API $Method $Path returned unexpected status $status" }
    return [pscustomobject]@{ StatusCode = $status; Body = $decoded }
}

function Wait-Milestone5DriverReady {
    param([Parameter(Mandatory = $true)][object]$Context)
    foreach ($service in @('api-1', 'api-2', 'api-3')) {
        $ready = $false
        for ($attempt = 1; $attempt -le 90; $attempt++) {
            $probe = Invoke-Milestone5DriverCompose -Context $Context -AllowFailure -Arguments @(
                'exec', '-T', $service, 'wget', '-q', '-T', '2', '-O', '/dev/null', 'http://127.0.0.1:8080/readyz'
            )
            if ($probe.ExitCode -eq 0) { $ready = $true; break }
            Start-Sleep -Seconds 1
        }
        if (-not $ready) { throw "$service did not become ready" }
    }
}

function Invoke-Milestone5DriverAdmin {
    param(
        [Parameter(Mandatory = $true)][object]$Context,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$Artifact
    )
    $composeRunArguments = @(
        '--profile', 'tools', 'run', '--rm', '--no-deps', '-T', 'physical-shard-admin'
    ) + $Arguments
    $result = Invoke-Milestone5DriverCompose -Context $Context `
        -Arguments $composeRunArguments -Artifact $Artifact
    $line = [string](@($result.Output | Where-Object { ([string]$_).TrimStart().StartsWith('{') }) | Select-Object -Last 1)
    if ([string]::IsNullOrWhiteSpace($line)) { throw "physical-shard-admin omitted JSON for $Artifact" }
    $envelope = $line | ConvertFrom-Json
    if ([string]$envelope.status -notin @('completed', 'dry-run')) { throw "physical-shard-admin failed for $Artifact" }
    return $envelope
}

function Get-Milestone5MigrationState {
    param([object]$Context, [string]$MigrationID, [string]$Artifact)
    $result = Invoke-Milestone5DriverPSQL -Context $Context -Service 'control-postgres' -Artifact $Artifact -SQL "SELECT state FROM public.physical_shard_migrations WHERE migration_id='$MigrationID'::uuid;"
    $state = [string](@($result.Output | Where-Object { ([string]$_).Trim() -match '^[a-z_]+$' }) | Select-Object -Last 1)
    if (-not $state) { throw 'physical migration state is missing' }
    return $state.Trim()
}

function Move-Milestone5Migration {
    param(
        [Parameter(Mandatory = $true)][object]$Context,
        [Parameter(Mandatory = $true)][string]$MigrationID,
        [Parameter(Mandatory = $true)][ValidateSet('base_copying', 'catching_up', 'validating_online', 'draining', 'rollback_window')][string]$Target,
        [Parameter(Mandatory = $true)][string]$Prefix,
        [switch]$ReverseStart
    )
    $rank = @{
        planned=0; preparing_target=1; capture_enabled=2; base_copying=3; catching_up=4
        validating_online=5; draining=6; source_fenced=7; final_catchup=8
        final_validating=9; target_enabled=10; switching_assignment=11; rollback_window=12
    }
    for ($step = 1; $step -le 80; $step++) {
        $state = Get-Milestone5MigrationState -Context $Context -MigrationID $MigrationID -Artifact "$Prefix-state-$step.log"
        if ($state -eq $Target) { return }
        if (-not $rank.ContainsKey($state) -or $rank[$state] -gt $rank[$Target]) {
            throw "$Prefix migration passed or left target state $Target from $state"
        }
        $command = switch ($state) {
            'planned' { if ($ReverseStart) { 'start-reverse-migration' } else { 'enable-capture' } }
            'preparing_target' { 'enable-capture' }
            'capture_enabled' { 'start-base-copy' }
            'base_copying' { 'resume-base-copy' }
            'catching_up' { 'replay-journal' }
            'validating_online' { 'validate-online' }
            'draining' { 'begin-quiesce' }
            'source_fenced' { 'final-catchup' }
            default { 'cutover' }
        }
        Invoke-Milestone5DriverAdmin -Context $Context -Arguments @(
            $command, '--migration-id', $MigrationID, '--confirm', '--timeout', '2m'
        ) -Artifact "$Prefix-$step-$command.log" | Out-Null
        $ReverseStart = $false
    }
    throw "$Prefix migration did not reach $Target within the bounded operation count"
}

function New-Milestone5Migration {
    param([object]$Context, [string]$TrainRunID, [string]$TargetShard, [string]$MigrationID, [string]$Prefix)
    Invoke-Milestone5DriverAdmin -Context $Context -Arguments @(
        'plan-migration', '--train-run-id', $TrainRunID, '--target-shard', $TargetShard,
        '--migration-id', $MigrationID, '--confirm', '--timeout', '2m'
    ) -Artifact "$Prefix-plan.log" | Out-Null
}

function New-Milestone5DriverCustomers {
    param([object]$State, [int]$Count)
    $password = "M5-$([guid]::NewGuid().ToString('N').Substring(0,12))-Aa1!"
    $State.SecretValues.Add($password)
    $customers = @()
    for ($index = 0; $index -lt $Count; $index++) {
        $email = "m5-$($State.Suffix)-$index@example.test"
        $forwardedFor = "198.19.0.$($index+20)"
        Invoke-Milestone5DriverAPI -BaseURL $State.BaseURL -Method POST -Path '/api/v1/auth/register' -Body @{
            email=$email; password=$password; display_name="M5 Synthetic Rider $index"
        } -ForwardedFor $forwardedFor -ExpectedStatus @(202) | Out-Null
        $login = Invoke-Milestone5DriverAPI -BaseURL $State.BaseURL -Method POST -Path '/api/v1/auth/login' -Body @{
            email=$email; password=$password
        } -ForwardedFor $forwardedFor -ExpectedStatus @(200)
        $token = [string]$login.Body.access_token
        if (-not $token) { throw 'synthetic login omitted token' }
        $State.SecretValues.Add($token)
        $passengers = @()
        $passengerCount = if ($index -lt 2) { 11 } else { 7 }
        foreach ($passengerIndex in 0..($passengerCount-1)) {
            $passenger = Invoke-Milestone5DriverAPI -BaseURL $State.BaseURL -Method POST -Path '/api/v1/passengers' -Token $token -Body @{
                display_name="M5 Synthetic Passenger $index-$passengerIndex"
            } -ExpectedStatus @(201)
            $passengers += [string]$passenger.Body.id
        }
        $customers += [pscustomobject]@{ Token=$token; PassengerIDs=[string[]]$passengers }
    }
    $password = $null
    return $customers
}

function Invoke-Milestone5OperatorDurableSmoke {
    param(
        [Parameter(Mandatory = $true)][object]$Context,
        [Parameter(Mandatory = $true)][object]$State
    )
    $email = "m5-operator-$($State.Suffix)@example.test"
    $password = "M5-Op-$([guid]::NewGuid().ToString('N').Substring(0,12))-Aa1!"
    $forwardedFor = '198.19.1.240'
    $State.SecretValues.Add($password)

    Invoke-Milestone5DriverAPI -BaseURL $State.BaseURL -Method POST -Path '/api/v1/auth/register' -Body @{
        email=$email; password=$password; display_name='M5 Synthetic Operator'
    } -ForwardedFor $forwardedFor -ExpectedStatus @(202) | Out-Null

    # Promotion is fixture setup only. The command and receipt below must be
    # created exclusively by the authenticated public operator API.
    $promoted = Get-Milestone5DriverScalar -Context $Context -Service 'control-postgres' `
        -Artifact 'operator-role-promotion.log' -SQL "WITH promoted AS (UPDATE public.users SET role='operator',token_version=token_version+1 WHERE email='$email' AND role='customer' RETURNING id) SELECT count(*) FROM promoted;"
    if ($promoted -ne 1) { throw 'dedicated fixture operator promotion was not unique' }

    $login = Invoke-Milestone5DriverAPI -BaseURL $State.BaseURL -Method POST -Path '/api/v1/auth/login' -Body @{
        email=$email; password=$password
    } -ForwardedFor $forwardedFor -ExpectedStatus @(200)
    $token = [string]$login.Body.access_token
    if (-not $token) { throw 'dedicated fixture operator login omitted token' }
    $State.SecretValues.Add($token)

    $path = "/api/v1/operator/train-runs/$script:M5TrainA/fares/$script:M5FareA"
    $before = Invoke-Milestone5DriverAPI -BaseURL $State.BaseURL -Method GET -Path $path -Token $token -ExpectedStatus @(200)
    $sourceVersion = [int64]$before.Body.source_version
    $generation = [int64]$before.Body.assignment_generation
    $amountMinor = [int64]$before.Body.amount_minor
    if ($sourceVersion -lt 1 -or $generation -ne 2 -or $amountMinor -lt 0 -or
        [int]$before.Body.from_stop_index -ne 0 -or [int]$before.Body.to_stop_index -ne 1 -or
        [string]$before.Body.seat_class -ne 'standard' -or [string]$before.Body.currency -ne 'TWD') {
        throw 'operator GET did not return the expected authoritative physical fare state'
    }
    $newAmountMinor = $amountMinor + 1
    $key = "m5-operator-fare-$($State.Suffix)"
    $State.SecretValues.Add($key)
    $request = [ordered]@{
        expected_source_version=$sourceVersion; from_stop_index=0; to_stop_index=1
        seat_class='standard'; amount_minor=$newAmountMinor; currency='TWD'
    }

    $controlBefore = Get-Milestone5DriverScalar -Context $Context -Service 'control-postgres' `
        -Artifact 'operator-command-count-before.log' -SQL "SELECT count(*) FROM public.operator_booking_commands WHERE actor_id=(SELECT id FROM public.users WHERE email='$email') AND operation='fare.install' AND train_run_id='$script:M5TrainA'::uuid AND resource_id='$script:M5FareA'::uuid;"
    $receiptBefore = Get-Milestone5DriverScalar -Context $Context -Service 'booking-shard-0-postgres' `
        -Artifact 'operator-receipt-count-before.log' -SQL "SELECT count(*) FROM public.booking_command_receipts WHERE train_run_id='$script:M5TrainA'::uuid AND assignment_generation=$generation AND command_type='fare.install';"

    $first = Invoke-Milestone5DriverAPI -BaseURL $State.BaseURL -Method PATCH -Path $path -Token $token `
        -IdempotencyKey $key -Body $request -ExpectedStatus @(200)
    if (-not [string]$first.Body.id) { throw 'operator PATCH omitted its result identity' }

    $commandID = Get-Milestone5DriverUUID -Context $Context -Service 'control-postgres' `
        -Artifact 'operator-command-id.log' -SQL "SELECT command_id FROM public.operator_booking_commands WHERE actor_id=(SELECT id FROM public.users WHERE email='$email') AND operation='fare.install' AND train_run_id='$script:M5TrainA'::uuid AND resource_id='$script:M5FareA'::uuid ORDER BY created_at DESC LIMIT 1;"
    $physicalFareID = Get-Milestone5DriverUUID -Context $Context -Service 'control-postgres' `
        -Artifact 'operator-physical-fare-id.log' -SQL "SELECT public.physical_source_entity_id('$script:M5TrainA'::uuid,'fare','$script:M5FareA'::uuid);"
    $controlValid = Get-Milestone5DriverScalar -Context $Context -Service 'control-postgres' `
        -Artifact 'operator-command-finalized.log' -SQL "SELECT count(*) FROM public.operator_booking_commands c JOIN public.fares f ON f.id=c.resource_id WHERE c.command_id='$commandID'::uuid AND c.state='finalized' AND c.target_shard_id='physical-shard-0' AND c.assignment_generation=$generation AND c.expected_source_version=$sourceVersion AND c.result_source_version=$($sourceVersion+1) AND f.source_version=c.result_source_version AND f.last_booking_command_id=c.command_id AND f.amount_minor=$newAmountMinor;"
    if ($controlValid -ne 1) { throw 'control operator command and fare version did not finalize consistently' }
    $receiptValid = Get-Milestone5DriverScalar -Context $Context -Service 'booking-shard-0-postgres' `
        -Artifact 'operator-receipt-succeeded.log' -SQL "SELECT count(*) FROM public.booking_command_receipts r JOIN public.booking_fare_snapshots f ON f.id='$physicalFareID'::uuid AND f.train_run_id=r.train_run_id AND f.assignment_generation=r.assignment_generation WHERE r.command_id='$commandID'::uuid AND r.train_run_id='$script:M5TrainA'::uuid AND r.assignment_generation=$generation AND r.command_type='fare.install' AND r.status='succeeded' AND r.result_id='$script:M5FareA'::uuid AND r.result_source_version=$($sourceVersion+1) AND f.source_version=r.result_source_version AND f.amount_minor=$newAmountMinor;"
    if ($receiptValid -ne 1) { throw 'physical operator receipt and fare snapshot version did not succeed consistently' }
    $controlAfterFirst = Get-Milestone5DriverScalar -Context $Context -Service 'control-postgres' `
        -Artifact 'operator-command-count-after-first.log' -SQL "SELECT count(*) FROM public.operator_booking_commands WHERE actor_id=(SELECT id FROM public.users WHERE email='$email') AND operation='fare.install' AND train_run_id='$script:M5TrainA'::uuid AND resource_id='$script:M5FareA'::uuid;"
    $receiptAfterFirst = Get-Milestone5DriverScalar -Context $Context -Service 'booking-shard-0-postgres' `
        -Artifact 'operator-receipt-count-after-first.log' -SQL "SELECT count(*) FROM public.booking_command_receipts WHERE train_run_id='$script:M5TrainA'::uuid AND assignment_generation=$generation AND command_type='fare.install';"
    if ($controlAfterFirst -ne $controlBefore + 1 -or $receiptAfterFirst -ne $receiptBefore + 1) {
        throw 'operator PATCH did not add exactly one control command and one physical receipt'
    }

    $after = Invoke-Milestone5DriverAPI -BaseURL $State.BaseURL -Method GET -Path $path -Token $token -ExpectedStatus @(200)
    if ([int64]$after.Body.source_version -ne $sourceVersion + 1 -or [int64]$after.Body.amount_minor -ne $newAmountMinor) {
        throw 'operator GET did not expose the newly authoritative fare version'
    }

    $replay = Invoke-Milestone5DriverAPI -BaseURL $State.BaseURL -Method PATCH -Path $path -Token $token `
        -IdempotencyKey $key -Body $request -ExpectedStatus @(200)
    if ([string]$replay.Body.id -ne [string]$first.Body.id) { throw 'operator idempotent replay changed result identity' }
    $controlAfter = Get-Milestone5DriverScalar -Context $Context -Service 'control-postgres' `
        -Artifact 'operator-command-count-after-replay.log' -SQL "SELECT count(*) FROM public.operator_booking_commands WHERE actor_id=(SELECT id FROM public.users WHERE email='$email') AND operation='fare.install' AND train_run_id='$script:M5TrainA'::uuid AND resource_id='$script:M5FareA'::uuid;"
    $receiptAfter = Get-Milestone5DriverScalar -Context $Context -Service 'booking-shard-0-postgres' `
        -Artifact 'operator-receipt-count-after-replay.log' -SQL "SELECT count(*) FROM public.booking_command_receipts WHERE train_run_id='$script:M5TrainA'::uuid AND assignment_generation=$generation AND command_type='fare.install';"
    if ($controlAfter -ne $controlAfterFirst -or $receiptAfter -ne $receiptAfterFirst) {
        throw 'operator idempotent replay added a control command or physical receipt'
    }

    Write-Milestone5DriverArtifact -Context $Context -Name 'operator-durable-smoke.log' -Lines @(
        ([ordered]@{
            get_status=$before.StatusCode; patch_status=$first.StatusCode; replay_status=$replay.StatusCode
            assignment_generation=$generation; initial_source_version=$sourceVersion
            result_source_version=($sourceVersion+1); control_commands_added=($controlAfterFirst-$controlBefore)
            physical_receipts_added=($receiptAfterFirst-$receiptBefore)
            replay_control_commands_added=($controlAfter-$controlAfterFirst)
            replay_physical_receipts_added=($receiptAfter-$receiptAfterFirst)
        } | ConvertTo-Json -Compress)
    )
    $password = $null
    $token = $null
    $key = $null
}

function Initialize-Milestone5DriverFixture {
    param([object]$Context)
    $fixture = Get-Content -Raw -LiteralPath (Join-Path ([string]$Context.RepositoryPath) 'loadtest/fixtures/milestone-2-multi-replica.sql')
    Invoke-Milestone5DriverPSQL -Context $Context -Service 'control-postgres' -SQL $fixture -Artifact 'fixture-m2.log' -AsInput | Out-Null
    $sql = @"
BEGIN;
INSERT INTO public.train_runs(id,train_id,route_id,service_date,scheduled_departure_at,status,segment_count)
SELECT id,'21000000-0000-4000-8000-000000000200'::uuid,'21000000-0000-4000-8000-000000000100'::uuid,
       CURRENT_DATE+offset_days,(CURRENT_DATE+offset_days)::timestamp AT TIME ZONE 'UTC','scheduled',1
FROM (VALUES
 ('$script:M5TrainC'::uuid,32),('$script:M5TrainD'::uuid,33),('$script:M5TrainE'::uuid,34),
 ('$script:M5TrainF'::uuid,35),('$script:M5TrainG'::uuid,36)
) AS input(id,offset_days)
ON CONFLICT(id) DO UPDATE SET service_date=EXCLUDED.service_date,scheduled_departure_at=EXCLUDED.scheduled_departure_at,status='scheduled',segment_count=1;
INSERT INTO public.seat_inventory(train_run_id,segment_count,seat_id,seat_class,occupied_segments)
SELECT run.id,1,seat.id,'standard',B'0'
FROM (VALUES ('$script:M5TrainC'::uuid),('$script:M5TrainD'::uuid),('$script:M5TrainE'::uuid),('$script:M5TrainF'::uuid),('$script:M5TrainG'::uuid)) run(id)
CROSS JOIN public.seats seat
WHERE seat.coach_id='21000000-0000-4000-8000-000000000300'::uuid
ON CONFLICT(train_run_id,seat_id) DO UPDATE SET segment_count=1,seat_class='standard';
INSERT INTO public.fares(id,train_run_id,route_id,from_stop_index,to_stop_index,seat_class,amount_minor,currency,active)
VALUES
 ('21000000-0000-4000-8000-000000000603','$script:M5TrainC',NULL,0,1,'standard',10000,'TWD',true),
 ('21000000-0000-4000-8000-000000000604','$script:M5TrainD',NULL,0,1,'standard',10000,'TWD',true),
 ('21000000-0000-4000-8000-000000000605','$script:M5TrainE',NULL,0,1,'standard',10000,'TWD',true),
 ('21000000-0000-4000-8000-000000000606','$script:M5TrainF',NULL,0,1,'standard',10000,'TWD',true),
 ('21000000-0000-4000-8000-000000000607','$script:M5TrainG',NULL,0,1,'standard',10000,'TWD',true)
ON CONFLICT(id) DO UPDATE SET train_run_id=EXCLUDED.train_run_id,from_stop_index=0,to_stop_index=1,seat_class='standard',amount_minor=10000,currency='TWD',active=true;
UPDATE public.hot_train_policies SET enabled=false,updated_at=clock_timestamp()
WHERE train_run_id IN ('$script:M5TrainA'::uuid,'$script:M5TrainB'::uuid);
COMMIT;
SELECT count(*) FROM public.train_run_shard_assignments
WHERE train_run_id IN ('$script:M5TrainA'::uuid,'$script:M5TrainB'::uuid,'$script:M5TrainC'::uuid,'$script:M5TrainD'::uuid,'$script:M5TrainE'::uuid,'$script:M5TrainF'::uuid,'$script:M5TrainG'::uuid);
"@
    $loaded = Get-Milestone5DriverScalar -Context $Context -Service 'control-postgres' -SQL $sql -Artifact 'fixture-m5.log'
    if ($loaded -ne 7) { throw 'Milestone 5 fixture did not create seven routed train runs' }
}

function Add-Milestone5DriverQuotaBaseline {
    param([object]$State)
    foreach ($customerIndex in 0..1) {
        $passengerIndex = 0
        foreach ($run in @($script:M5TrainC, $script:M5TrainD, $script:M5TrainE)) {
            foreach ($hold in 1..3) {
                $created = Invoke-Milestone5DriverAPI -BaseURL $State.BaseURL -Method POST -Path '/api/v1/reservations' `
                    -Token $State.Customers[$customerIndex].Token -IdempotencyKey "m5-quota-baseline-$customerIndex-$($run.Substring(35))-$hold" -Body @{
                        train_run_id=$run; origin_station_code='M2A'; destination_station_code='M2B';
                        seat_class='standard'; passenger_ids=@($State.Customers[$customerIndex].PassengerIDs[$passengerIndex])
                    } -ExpectedStatus @(201)
                if (-not [string]$created.Body.id) { throw 'quota baseline reservation omitted identity' }
                $passengerIndex++
            }
        }
    }
}

function New-Milestone5EnvironmentMap {
    param([object]$State)
    $tokens = [string[]]@($State.Customers[2..7] | ForEach-Object Token)
    $passengers = [string[]]@($State.Customers[2..7] | ForEach-Object { $_.PassengerIDs[0] })
    $common = [ordered]@{
        BASE_URL='http://reverse-proxy:8080'; ORIGIN_CODE='M2A'; DESTINATION_CODE='M2B'; SEAT_CLASS='standard'
        CUSTOMER_TOKENS=($tokens -join ','); PASSENGER_IDS=($passengers -join ','); TRAIN_RUN_IDS="$script:M5TrainA,$script:M5TrainB"
    }
    $map = [ordered]@{}
    $routing = [ordered]@{}
    foreach ($entry in $common.GetEnumerator()) { $routing[[string]$entry.Key] = [string]$entry.Value }
    # Each iteration performs a create plus an idempotent replay. Six bounded
    # identities keep the 12-iteration proof below the production per-user
    # reservation limit without weakening that control for evidence.
    $routing['VUS']='6'; $routing['ITERATIONS']='12'
    $map['physical-shard-routing'] = $routing
    $quotaPassengers = @(
        $State.Customers[0].PassengerIDs[9], $State.Customers[0].PassengerIDs[10],
        $State.Customers[1].PassengerIDs[9], $State.Customers[1].PassengerIDs[10]
    )
    $map['cross-shard-global-quota'] = [ordered]@{
        BASE_URL=$common.BASE_URL; ORIGIN_CODE='M2A'; DESTINATION_CODE='M2B'; SEAT_CLASS='standard'; TRAIN_RUN_IDS=$common.TRAIN_RUN_IDS
        CUSTOMER_TOKENS=($State.Customers[0].Token,$State.Customers[1].Token -join ','); PASSENGER_IDS=($quotaPassengers -join ',')
        VUS='2'; MAX_ACTIVE_HOLDS_PER_CUSTOMER='1'; RATE_LIMIT_SETTLE_SECONDS='61'
    }
    $map['booking-command-recovery'] = [ordered]@{
        BASE_URL=$common.BASE_URL; ORIGIN_CODE='M2A'; DESTINATION_CODE='M2B'; SEAT_CLASS='standard'; TRAIN_RUN_ID=$script:M5TrainA
        CUSTOMER_TOKENS=$State.Customers[2].Token; PASSENGER_IDS=$State.Customers[2].PassengerIDs[1]
        IDEMPOTENCY_KEY="m5-recovery-$($State.Suffix)"; DEFERRED_ERROR_CODE='unavailable'; MAX_REPLAY_ATTEMPTS='20'; REPLAY_INTERVAL_SECONDS='0.25'
    }
    $map['physical-shard-outage'] = [ordered]@{
        BASE_URL=$common.BASE_URL; ORIGIN_CODE='M2A'; DESTINATION_CODE='M2B'; SEAT_CLASS='standard'; OUTAGE_TRAIN_RUN_ID=$script:M5TrainA; HEALTHY_TRAIN_RUN_ID=$script:M5TrainB
        CUSTOMER_TOKENS=($State.Customers[3].Token,$State.Customers[4].Token -join ','); PASSENGER_IDS=($State.Customers[3].PassengerIDs[0],$State.Customers[4].PassengerIDs[0] -join ','); VUS_PER_SHARD='2'
    }
    foreach ($scenarioSpec in @(
        @('online-base-copy',2), @('journal-catchup',3), @('physical-cutover',4)
    )) {
        $scenario = [string]$scenarioSpec[0]
        $passengerIndex = [int]$scenarioSpec[1]
        $map[$scenario] = [ordered]@{
            BASE_URL=$common.BASE_URL; ORIGIN_CODE='M2A'; DESTINATION_CODE='M2B'; SEAT_CLASS='standard'; TRAIN_RUN_ID=$script:M5TrainC
            CUSTOMER_TOKENS=($State.Customers[2].Token,$State.Customers[3].Token,$State.Customers[4].Token -join ',')
            PASSENGER_IDS=($State.Customers[2].PassengerIDs[$passengerIndex],$State.Customers[3].PassengerIDs[$passengerIndex],$State.Customers[4].PassengerIDs[$passengerIndex] -join ',')
            VUS='2'; ITERATIONS='8'; MAX_PAUSE_MS='30000'
        }
    }
    $map['stale-router-physical'] = [ordered]@{
        API_URLS='http://api-1:8080,http://api-2:8080,http://api-3:8080'; ORIGIN_CODE='M2A'; DESTINATION_CODE='M2B'; SEAT_CLASS='standard'; TRAIN_RUN_ID=$script:M5TrainC
        CUSTOMER_TOKENS=($State.Customers[2].Token,$State.Customers[3].Token,$State.Customers[4].Token -join ',')
        PASSENGER_IDS=($State.Customers[2].PassengerIDs[5],$State.Customers[3].PassengerIDs[5],$State.Customers[4].PassengerIDs[5] -join ',')
    }
    $map['reverse-migration'] = [ordered]@{
        BASE_URL=$common.BASE_URL; ORIGIN_CODE='M2A'; DESTINATION_CODE='M2B'; SEAT_CLASS='standard'; TRAIN_RUN_ID=$script:M5TrainC
        CUSTOMER_TOKENS=$State.Customers[7].Token; PASSENGER_IDS=$State.Customers[7].PassengerIDs[0]
        IDEMPOTENCY_KEY="m5-target-era-$($State.Suffix)"; TARGET_ERA_RESERVATION_ID='pending'
    }
    $map['legacy-vs-physical'] = [ordered]@{
        BASE_URL=$common.BASE_URL; ORIGIN_CODE='M2A'; DESTINATION_CODE='M2B'; SEAT_CLASS='standard'; LEGACY_TRAIN_RUN_ID=$script:M5TrainD; PHYSICAL_TRAIN_RUN_ID=$script:M5TrainC
        LEGACY_CUSTOMER_TOKENS=($State.Customers[5].Token,$State.Customers[6].Token -join ','); LEGACY_PASSENGER_IDS=($State.Customers[5].PassengerIDs[0],$State.Customers[6].PassengerIDs[0] -join ',')
        PHYSICAL_CUSTOMER_TOKENS=($State.Customers[2].Token,$State.Customers[3].Token -join ','); PHYSICAL_PASSENGER_IDS=($State.Customers[2].PassengerIDs[6],$State.Customers[3].PassengerIDs[6] -join ','); VUS_PER_PATH='2'
    }
    return $map
}

function Initialize-Milestone5Evidence {
    param([Parameter(Mandatory = $true)][object]$Context)
    Assert-Milestone5DriverContext -Context $Context
    Wait-Milestone5DriverReady -Context $Context
    Invoke-Milestone5DriverCompose -Context $Context -Arguments @('--profile','tools','build','physical-shard-admin') -Artifact 'physical-shard-admin-build.log' | Out-Null
    Initialize-Milestone5DriverFixture -Context $Context
    $state = [pscustomobject]@{
        Suffix=[guid]::NewGuid().ToString('N').Substring(0,10); BaseURL=Get-Milestone5DriverPublishedURL -Context $Context
        SecretValues=[System.Collections.Generic.List[string]]::new(); Customers=@(); EnvironmentByScenario=[ordered]@{}; Jobs=@{}
        FinalWritePauseMs=0.0; MaximumFinalWritePauseMs=30000.0; TargetEraReservationID=''; TargetWriteObservedBeforeReverse=$false
        OnlineCopyReservationCountBefore=-1L; OnlineCopyJournalCountBefore=-1L
        OnlineCopyMutationDelta=-1L; OnlineCopyJournalDelta=-1L
    }
    $state.Customers = New-Milestone5DriverCustomers -State $state -Count 8
    Add-Milestone5DriverQuotaBaseline -State $state
    New-Milestone5Migration -Context $Context -TrainRunID $script:M5TrainA -TargetShard 'physical-shard-0' -MigrationID $script:M5MigrationA -Prefix 'train-a'
    Move-Milestone5Migration -Context $Context -MigrationID $script:M5MigrationA -Target rollback_window -Prefix 'train-a'
    New-Milestone5Migration -Context $Context -TrainRunID $script:M5TrainB -TargetShard 'physical-shard-1' -MigrationID $script:M5MigrationB -Prefix 'train-b'
    Move-Milestone5Migration -Context $Context -MigrationID $script:M5MigrationB -Target rollback_window -Prefix 'train-b'
    Invoke-Milestone5OperatorDurableSmoke -Context $Context -State $state
    $state.EnvironmentByScenario = New-Milestone5EnvironmentMap -State $state
    return $state
}

function Start-Milestone5DriverJob {
    param([object]$Context,[object]$State,[string]$Scenario,[string]$MigrationID,[string]$Target,[int]$DelayMilliseconds,[switch]$MeasurePause)
    if ($State.Jobs.ContainsKey($Scenario)) { throw "$Scenario already owns a transition job" }
    $driverPath = $MyInvocation.MyCommand.Path
    if (-not $driverPath) { $driverPath = Join-Path ([string]$Context.RepositoryPath) 'scripts/milestone-5-physical-shard-evidence-driver.ps1' }
    $jobContext = [pscustomobject]@{
        RepositoryPath=[string]$Context.RepositoryPath; RawDirectory=[string]$Context.RawDirectory; ProjectName=[string]$Context.ProjectName
        ComposeFile=[string]$Context.ComposeFile; ComposeArguments=[string[]]@($Context.ComposeArguments)
    }
    $job = Start-Job -ScriptBlock {
        param($DriverPath,$JobContext,$Scenario,$MigrationID,$Target,$DelayMilliseconds,$MeasurePause)
        . $DriverPath
        Start-Sleep -Milliseconds $DelayMilliseconds
        if ($MeasurePause) {
            Move-Milestone5Migration -Context $JobContext -MigrationID $MigrationID -Target draining -Prefix "$Scenario-job"
            $started = [DateTimeOffset]::UtcNow
            Move-Milestone5Migration -Context $JobContext -MigrationID $MigrationID -Target rollback_window -Prefix "$Scenario-job-cutover"
            return [pscustomobject]@{ final_write_pause_ms=([DateTimeOffset]::UtcNow-$started).TotalMilliseconds }
        }
        Move-Milestone5Migration -Context $JobContext -MigrationID $MigrationID -Target $Target -Prefix "$Scenario-job"
        return [pscustomobject]@{ state=$Target }
    } -ArgumentList $driverPath,$jobContext,$Scenario,$MigrationID,$Target,$DelayMilliseconds,[bool]$MeasurePause
    $State.Jobs[$Scenario] = $job
}

function Stop-Milestone5DriverJob {
    param([object]$State,[string]$Scenario,[int]$TimeoutSeconds=180)
    if (-not $State.Jobs.ContainsKey($Scenario)) { throw "$Scenario transition job is missing" }
    $job = $State.Jobs[$Scenario]
    Wait-Job -Job $job -Timeout $TimeoutSeconds | Out-Null
    if ($job.State -ne 'Completed') {
        Stop-Job -Job $job -ErrorAction SilentlyContinue
        Remove-Job -Job $job -Force -ErrorAction SilentlyContinue
        throw "$Scenario transition job did not complete: $($job.State)"
    }
    try { return @(Receive-Job -Job $job -ErrorAction Stop) } finally {
        Remove-Job -Job $job -Force -ErrorAction SilentlyContinue
        $State.Jobs.Remove($Scenario)
    }
}

function Enable-Milestone5RecoveryFault {
    param([object]$Context)
    Invoke-Milestone5DriverPSQL -Context $Context -Service 'control-postgres' -Artifact 'recovery-fault-enable.log' -SQL @'
DROP TRIGGER IF EXISTS m5_evidence_finalize_once ON public.booking_commands;
DROP FUNCTION IF EXISTS public.m5_evidence_fail_finalize_once();
DROP SEQUENCE IF EXISTS public.m5_evidence_finalize_fail_seq;
CREATE SEQUENCE public.m5_evidence_finalize_fail_seq START 1;
CREATE FUNCTION public.m5_evidence_fail_finalize_once() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF NEW.state='finalized' AND OLD.state<>'finalized' AND nextval('public.m5_evidence_finalize_fail_seq')=1 THEN
  RAISE EXCEPTION 'bounded evidence finalization fault' USING ERRCODE='40001';
 END IF;
 RETURN NEW;
END; $$;
CREATE TRIGGER m5_evidence_finalize_once BEFORE UPDATE OF state ON public.booking_commands
FOR EACH ROW EXECUTE FUNCTION public.m5_evidence_fail_finalize_once();
SELECT 1;
'@ | Out-Null
}

function Disable-Milestone5RecoveryFault {
    param([object]$Context)
    Invoke-Milestone5DriverPSQL -Context $Context -Service 'control-postgres' -Artifact 'recovery-fault-disable.log' -SQL @'
DROP TRIGGER IF EXISTS m5_evidence_finalize_once ON public.booking_commands;
DROP FUNCTION IF EXISTS public.m5_evidence_fail_finalize_once();
DROP SEQUENCE IF EXISTS public.m5_evidence_finalize_fail_seq;
SELECT 1;
'@ | Out-Null
}

function Start-Milestone5Scenario {
    param([Parameter(Mandatory=$true)][object]$Context,[Parameter(Mandatory=$true)][object]$State,[Parameter(Mandatory=$true)][string]$Scenario,[Parameter(Mandatory=$true)][object]$Environment)
    Assert-Milestone5DriverContext -Context $Context
    if ($null -eq $State.PSObject.Properties['EnvironmentByScenario']) { throw 'Milestone 5 driver state is invalid' }
    switch ($Scenario) {
        'physical-shard-routing' { return }
        'cross-shard-global-quota' {
            $settleSeconds = 0
            if (-not [int]::TryParse([string]$Environment.RATE_LIMIT_SETTLE_SECONDS, [ref]$settleSeconds) -or
                $settleSeconds -lt 60 -or $settleSeconds -gt 90) {
                throw 'global quota evidence rate-limit settle interval must be between 60 and 90 seconds'
            }
            # Baseline holds are created through the public API for the same
            # authenticated subjects. Let that fixed reservation-rate window
            # expire so the subsequent rejection proves global quota rather
            # than an unrelated HTTP rate limit.
            Start-Sleep -Seconds $settleSeconds
            return
        }
        'booking-command-recovery' { Enable-Milestone5RecoveryFault -Context $Context; return }
        'physical-shard-outage' {
            Invoke-Milestone5DriverCompose -Context $Context -Arguments @('stop','booking-shard-0-postgres') -Artifact 'outage-stop.log' | Out-Null
            $healthy = Get-Milestone5DriverScalar -Context $Context -Service 'booking-shard-1-postgres' -SQL 'SELECT 1;' -Artifact 'outage-peer-ready.log'
            if ($healthy -ne 1) { throw 'healthy shard was not queryable during outage injection' }
            return
        }
        'online-base-copy' {
            New-Milestone5Migration -Context $Context -TrainRunID $script:M5TrainC -TargetShard 'physical-shard-0' -MigrationID $script:M5MigrationC -Prefix 'train-c'
            Move-Milestone5Migration -Context $Context -MigrationID $script:M5MigrationC -Target base_copying -Prefix 'train-c'
            $State.OnlineCopyReservationCountBefore = Get-Milestone5DriverScalar -Context $Context -Service 'control-postgres' -Artifact 'online-copy-reservations-before.log' -SQL "SELECT count(*) FROM public.reservations WHERE train_run_id='$script:M5TrainC'::uuid;"
            $State.OnlineCopyJournalCountBefore = Get-Milestone5DriverScalar -Context $Context -Service 'control-postgres' -Artifact 'online-copy-journal-before.log' -SQL "SELECT count(*) FROM public.physical_source_train_run_mutation_journal WHERE migration_id='$script:M5MigrationC'::uuid;"
            Start-Milestone5DriverJob -Context $Context -State $State -Scenario $Scenario -MigrationID $script:M5MigrationC -Target catching_up -DelayMilliseconds 1000
            return
        }
        'journal-catchup' {
            Start-Milestone5DriverJob -Context $Context -State $State -Scenario $Scenario -MigrationID $script:M5MigrationC -Target validating_online -DelayMilliseconds 1500
            return
        }
        'physical-cutover' {
            Start-Milestone5DriverJob -Context $Context -State $State -Scenario $Scenario -MigrationID $script:M5MigrationC -Target rollback_window -DelayMilliseconds 1000 -MeasurePause
            return
        }
        'stale-router-physical' {
            foreach ($index in 0..2) {
                $customer = $State.Customers[$index+2]
                $body = @{train_run_id=$script:M5TrainC;origin_station_code='M2A';destination_station_code='M2B';seat_class='standard';passenger_ids=@($customer.PassengerIDs[0])} | ConvertTo-Json -Compress
                $shell = "wget -q -O /dev/null --header='Authorization: Bearer `$M5_TOKEN' --header='Content-Type: application/json' --header='Idempotency-Key: m5-prewarm-$index' --post-data='$body' http://127.0.0.1:8080/api/v1/reservations"
                Invoke-Milestone5DriverCompose -Context $Context -Arguments @('exec','-T','-e',"M5_TOKEN=$($customer.Token)","api-$($index+1)",'sh','-c',$shell) | Out-Null
            }
            New-Milestone5Migration -Context $Context -TrainRunID $script:M5TrainC -TargetShard 'physical-shard-1' -MigrationID $script:M5MigrationCSecond -Prefix 'train-c-second'
            Move-Milestone5Migration -Context $Context -MigrationID $script:M5MigrationCSecond -Target rollback_window -Prefix 'train-c-second'
            return
        }
        'reverse-migration' { return }
        'legacy-vs-physical' { return }
        default { throw 'unknown Milestone 5 scenario' }
    }
}

function Stop-Milestone5Scenario {
    param([Parameter(Mandatory=$true)][object]$Context,[Parameter(Mandatory=$true)][object]$State,[Parameter(Mandatory=$true)][string]$Scenario,[Parameter(Mandatory=$true)][bool]$Started)
    if (-not $Started) { return }
    switch ($Scenario) {
        'booking-command-recovery' { Disable-Milestone5RecoveryFault -Context $Context }
        'physical-shard-outage' {
            Invoke-Milestone5DriverCompose -Context $Context -Arguments @('start','booking-shard-0-postgres') -Artifact 'outage-start.log' | Out-Null
            for ($attempt=1;$attempt -le 60;$attempt++) {
                $probe = Invoke-Milestone5DriverCompose -Context $Context -AllowFailure -Arguments @('exec','-T','booking-shard-0-postgres','pg_isready','-U','railway_booking','-d','railway_booking')
                if ($probe.ExitCode -eq 0) { return }
                Start-Sleep -Seconds 1
            }
            throw 'outage shard did not recover'
        }
        'online-base-copy' {
            Stop-Milestone5DriverJob -State $State -Scenario $Scenario | Out-Null
            $reservationAfter = Get-Milestone5DriverScalar -Context $Context -Service 'control-postgres' -Artifact 'online-copy-reservations-after.log' -SQL "SELECT count(*) FROM public.reservations WHERE train_run_id='$script:M5TrainC'::uuid;"
            $journalAfter = Get-Milestone5DriverScalar -Context $Context -Service 'control-postgres' -Artifact 'online-copy-journal-after.log' -SQL "SELECT count(*) FROM public.physical_source_train_run_mutation_journal WHERE migration_id='$script:M5MigrationC'::uuid;"
            $State.OnlineCopyMutationDelta = $reservationAfter - [int64]$State.OnlineCopyReservationCountBefore
            $State.OnlineCopyJournalDelta = $journalAfter - [int64]$State.OnlineCopyJournalCountBefore
            if ($State.OnlineCopyMutationDelta -lt 2 -or $State.OnlineCopyJournalDelta -lt 1) {
                throw 'online base copy did not produce real source mutations and journal progress'
            }
        }
        'journal-catchup' { Stop-Milestone5DriverJob -State $State -Scenario $Scenario | Out-Null }
        'physical-cutover' {
            $output = @(Stop-Milestone5DriverJob -State $State -Scenario $Scenario)
            $measurement = @($output | Where-Object { $null -ne $_.PSObject.Properties['final_write_pause_ms'] }) | Select-Object -Last 1
            if ($null -eq $measurement -or [double]$measurement.final_write_pause_ms -le 0) { throw 'cutover job omitted measured pause' }
            $State.FinalWritePauseMs = [double]$measurement.final_write_pause_ms
        }
        'stale-router-physical' {
            $environment = $State.EnvironmentByScenario['reverse-migration']
            $created = Invoke-Milestone5DriverAPI -BaseURL $State.BaseURL -Method POST -Path '/api/v1/reservations' -Token $State.Customers[7].Token -IdempotencyKey $environment.IDEMPOTENCY_KEY -Body @{
                train_run_id=$script:M5TrainC;origin_station_code='M2A';destination_station_code='M2B';seat_class='standard';passenger_ids=@($State.Customers[7].PassengerIDs[0])
            } -ExpectedStatus @(201)
            $State.TargetEraReservationID = [string]$created.Body.id
            if (-not $State.TargetEraReservationID) { throw 'target-era write omitted reservation identity' }
            $observed = Get-Milestone5DriverScalar -Context $Context -Service 'booking-shard-1-postgres' -Artifact 'target-write-before-reverse.log' -SQL "SELECT count(*) FROM public.reservations r JOIN public.train_run_target_write_evidence e ON e.train_run_id=r.train_run_id AND e.assignment_generation=r.assignment_generation WHERE r.id='$($State.TargetEraReservationID)'::uuid AND r.train_run_id='$script:M5TrainC'::uuid AND e.successful_write_count>0;"
            $State.TargetWriteObservedBeforeReverse = ($observed -eq 1)
            if (-not $State.TargetWriteObservedBeforeReverse) { throw 'target-era write evidence was not durable before reverse' }
            $environment.TARGET_ERA_RESERVATION_ID = $State.TargetEraReservationID
            Invoke-Milestone5DriverAdmin -Context $Context -Arguments @(
                'plan-reverse-migration','--migration-id',$script:M5MigrationCSecond,'--reverse-migration-id',$script:M5MigrationCReverse,'--generation','4','--confirm','--timeout','2m'
            ) -Artifact 'train-c-reverse-plan.log' | Out-Null
            Move-Milestone5Migration -Context $Context -MigrationID $script:M5MigrationCReverse -Target rollback_window -Prefix 'train-c-reverse' -ReverseStart
        }
    }
}

function Get-Milestone5WriterCounts {
    param([object]$Context)
    $runs = @($script:M5TrainA,$script:M5TrainB,$script:M5TrainC,$script:M5TrainD,$script:M5TrainE,$script:M5TrainF,$script:M5TrainG)
    $counts = @{}; foreach($run in $runs){$counts[$run]=0L}
    foreach ($probe in @(
        @{Service='control-postgres';SQL="SELECT train_run_id::text||'|'||write_enabled::int FROM public.train_run_write_fences WHERE train_run_id IN ('$($runs -join "'::uuid,'")'::uuid);";Artifact='writers-control.log'},
        @{Service='booking-shard-0-postgres';SQL="SELECT train_run_id::text||'|'||write_enabled::int FROM public.train_run_write_fences WHERE train_run_id IN ('$($runs -join "'::uuid,'")'::uuid);";Artifact='writers-shard-0.log'},
        @{Service='booking-shard-1-postgres';SQL="SELECT train_run_id::text||'|'||write_enabled::int FROM public.train_run_write_fences WHERE train_run_id IN ('$($runs -join "'::uuid,'")'::uuid);";Artifact='writers-shard-1.log'}
    )) {
        $result=Invoke-Milestone5DriverPSQL -Context $Context -Service $probe.Service -SQL $probe.SQL -Artifact $probe.Artifact
        foreach($line in $result.Output){if(([string]$line).Trim() -match '^(?<run>[0-9a-f-]{36})\|(?<enabled>[01])$'){$counts[$matches.run]+=[int64]$matches.enabled}}
    }
    return $counts
}

function Get-Milestone5DatabaseEvidence {
    param([Parameter(Mandatory=$true)][object]$Context,[Parameter(Mandatory=$true)][object]$State)
    $writers=Get-Milestone5WriterCounts -Context $Context
    $dual=@($writers.GetEnumerator()|Where-Object{$_.Value -gt 1}).Count
    $assignmentMismatch=Get-Milestone5DriverScalar -Context $Context -Service 'control-postgres' -Artifact 'assignment-ledger-mismatches.log' -SQL @"
WITH latest AS (SELECT DISTINCT ON(train_run_id) train_run_id,target_shard_id,target_generation FROM public.physical_shard_migrations WHERE state IN('rollback_window','completed','reverse_migration_required') ORDER BY train_run_id,created_at DESC,migration_id DESC)
SELECT count(*) FROM latest JOIN public.train_run_shard_assignments a USING(train_run_id) WHERE a.shard_id<>latest.target_shard_id OR a.assignment_generation<>latest.target_generation OR a.assignment_state NOT IN('stable','rollback_window');
"@
    $directoryMismatch=Get-Milestone5DriverScalar -Context $Context -Service 'control-postgres' -Artifact 'directory-mismatches.log' -SQL "SELECT count(*) FROM public.reservation_directory d JOIN public.booking_commands c ON c.command_id=d.command_id WHERE c.train_run_id IN ('$script:M5TrainA'::uuid,'$script:M5TrainB'::uuid,'$script:M5TrainC'::uuid) AND (d.reservation_id<>c.reservation_id OR d.last_known_shard_id<>c.target_shard_id OR d.last_known_generation<>c.assignment_generation OR d.state<>'active' OR c.state<>'finalized');"
    $quotaViolations=Get-Milestone5DriverScalar -Context $Context -Service 'control-postgres' -Artifact 'quota-violations.log' -SQL "WITH active AS(SELECT owner_user_id,train_run_id,passenger_count FROM public.booking_quota_leases WHERE state IN('pending','active_hold','repair_required') AND expires_at>clock_timestamp()), violations AS(SELECT owner_user_id FROM active GROUP BY owner_user_id HAVING count(*)>10 OR sum(passenger_count)>24 UNION SELECT owner_user_id FROM active GROUP BY owner_user_id,train_run_id HAVING count(*)>3) SELECT count(*) FROM violations;"
    $journalGaps=0L; $applyConflicts=0L; $receiptConflicts=0L
    $journalGaps += Get-Milestone5DriverScalar -Context $Context -Service 'control-postgres' -Artifact 'journal-gaps-control.log' -SQL "SELECT count(*) FROM (SELECT migration_id FROM public.physical_source_train_run_mutation_journal GROUP BY migration_id HAVING max(mutation_sequence)-min(mutation_sequence)+1<>count(*)) gaps;"
    foreach($entry in @(@('booking-shard-0-postgres','0'),@('booking-shard-1-postgres','1'))){
        $journalGaps += Get-Milestone5DriverScalar -Context $Context -Service $entry[0] -Artifact "journal-gaps-shard-$($entry[1]).log" -SQL "SELECT count(*) FROM (SELECT migration_id FROM public.train_run_mutation_journal GROUP BY migration_id HAVING max(mutation_sequence)-min(mutation_sequence)+1<>count(*)) gaps;"
        $applyConflicts += Get-Milestone5DriverScalar -Context $Context -Service $entry[0] -Artifact "apply-conflicts-shard-$($entry[1]).log" -SQL "SELECT count(*) FROM public.migration_apply_receipts WHERE octet_length(apply_fingerprint)<>32 OR mutation_sequence<=0;"
        $receiptConflicts += Get-Milestone5DriverScalar -Context $Context -Service $entry[0] -Artifact "receipt-conflicts-shard-$($entry[1]).log" -SQL "SELECT count(*) FROM public.booking_command_receipts r WHERE (r.status='succeeded' AND (r.result_id IS NULL OR r.completed_at IS NULL)) OR octet_length(r.request_fingerprint)<>32;"
    }
    $unreconciled=Get-Milestone5DriverScalar -Context $Context -Service 'control-postgres' -Artifact 'unreconciled-commands.log' -SQL "SELECT count(*) FROM public.booking_commands WHERE train_run_id IN ('$script:M5TrainA'::uuid,'$script:M5TrainB'::uuid,'$script:M5TrainC'::uuid) AND state NOT IN('finalized','failed','expired');"
    return [ordered]@{
        enabled_writer_fences=[int64]$writers[$script:M5TrainC]; dual_writer_violations=[int64]$dual
        assignment_ledger_mismatches=$assignmentMismatch; directory_mismatches=$directoryMismatch; quota_violations=$quotaViolations
        journal_gaps=$journalGaps; apply_receipt_conflicts=$applyConflicts; command_receipt_conflicts=$receiptConflicts; unreconciled_commands=$unreconciled
        online_copy_mutation_delta=[int64]$State.OnlineCopyMutationDelta; online_copy_journal_delta=[int64]$State.OnlineCopyJournalDelta
    }
}

function Get-Milestone5MigrationEvidence {
    param([Parameter(Mandatory=$true)][object]$Context,[Parameter(Mandatory=$true)][object]$State)
    $targetGeneration=Get-Milestone5DriverScalar -Context $Context -Service 'control-postgres' -Artifact 'target-generation.log' -SQL "SELECT target_generation FROM public.physical_shard_migrations WHERE migration_id='$script:M5MigrationCSecond'::uuid;"
    $reverseGeneration=Get-Milestone5DriverScalar -Context $Context -Service 'control-postgres' -Artifact 'reverse-generation.log' -SQL "SELECT target_generation FROM public.physical_shard_migrations WHERE migration_id='$script:M5MigrationCReverse'::uuid;"
    $preserved=Get-Milestone5DriverScalar -Context $Context -Service 'booking-shard-0-postgres' -Artifact 'target-write-after-reverse.log' -SQL "SELECT count(*) FROM public.reservations WHERE id='$($State.TargetEraReservationID)'::uuid AND train_run_id='$script:M5TrainC'::uuid AND assignment_generation=$reverseGeneration;"
    $observedBefore=[bool]$State.TargetWriteObservedBeforeReverse
    $preservedAfter=($preserved -eq 1)
    return [ordered]@{
        final_write_pause_ms=[double]$State.FinalWritePauseMs; maximum_final_write_pause_ms=[double]$State.MaximumFinalWritePauseMs
        target_write_observed_before_reverse=$observedBefore; target_write_preserved_after_reverse=$preservedAfter
        target_generation=$targetGeneration; reverse_generation=$reverseGeneration
    }
}
