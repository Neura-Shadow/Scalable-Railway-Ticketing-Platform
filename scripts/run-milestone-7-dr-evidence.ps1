[CmdletBinding()]
param(
    [string]$ProjectName = '',
    [string]$EvidenceDirectory = '',
    [switch]$SkipBuild,
    [switch]$ConfirmDestructiveDrill
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

if (-not $ConfirmDestructiveDrill) {
    throw 'ConfirmDestructiveDrill is required for the project-scoped fence, promotion, reseed, and failback drill'
}

$root = [System.IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$composeFile = Join-Path $root 'docker-compose.dr.yml'
$composeFiles = @(
    (Join-Path $root 'docker-compose.physical-shards.yml'),
    (Join-Path $root 'deploy/compose/payment.override.yml'),
    $composeFile,
    (Join-Path $root 'deploy/compose/dr-app.override.yml')
)
$driverPath = Join-Path $PSScriptRoot 'milestone-5-physical-shard-evidence-driver.ps1'
. $driverPath
$suffix = [guid]::NewGuid().ToString('N').Substring(0, 10)
if ([string]::IsNullOrWhiteSpace($ProjectName)) { $ProjectName = "railway-m7-dr-$suffix" }
if ($ProjectName -notmatch '^[a-z0-9][a-z0-9_-]{2,62}$') { throw 'ProjectName is invalid' }
if ([string]::IsNullOrWhiteSpace($EvidenceDirectory)) {
    $EvidenceDirectory = Join-Path ([System.IO.Path]::GetTempPath()) "railway-m7-dr-evidence-$suffix"
}
$EvidenceDirectory = [System.IO.Path]::GetFullPath($EvidenceDirectory)
$rootPrefix = $root.TrimEnd([System.IO.Path]::DirectorySeparatorChar) + [System.IO.Path]::DirectorySeparatorChar
if ($EvidenceDirectory.StartsWith($rootPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw 'EvidenceDirectory must be outside the source repository'
}
if (Test-Path -LiteralPath $EvidenceDirectory) { throw 'EvidenceDirectory must not already exist' }
New-Item -ItemType Directory -Path $EvidenceDirectory | Out-Null

$secretDirectory = Join-Path ([System.IO.Path]::GetTempPath()) "railway-m7-dr-secrets-$suffix"
New-Item -ItemType Directory -Path $secretDirectory | Out-Null
$controlReplicationSecretPath = Join-Path $secretDirectory 'control-replication-password'
$shard0ReplicationSecretPath = Join-Path $secretDirectory 'shard-0-replication-password'
$shard1ReplicationSecretPath = Join-Path $secretDirectory 'shard-1-replication-password'
$replicationCAKeyPath = Join-Path $secretDirectory 'replication-ca.key'
$replicationCACertPath = Join-Path $secretDirectory 'replication-ca.crt'
$tlsEndpointSpecs = @(
    [pscustomobject]@{ Prefix='CONTROL_REGION_A'; File='control-region-a'; DNS='control-postgres'; ReplicationDNS='control-postgres-replication' },
    [pscustomobject]@{ Prefix='CONTROL_REGION_B'; File='control-region-b'; DNS='control-postgres-region-b'; ReplicationDNS='control-postgres-region-b-replication' },
    [pscustomobject]@{ Prefix='CONTROL_REGION_A_RESEED'; File='control-region-a-reseed'; DNS='control-postgres-region-a-reseed'; ReplicationDNS='control-postgres-region-a-reseed-replication' },
    [pscustomobject]@{ Prefix='SHARD_0_REGION_A'; File='shard-0-region-a'; DNS='booking-shard-0-postgres'; ReplicationDNS='booking-shard-0-postgres-replication' },
    [pscustomobject]@{ Prefix='SHARD_0_REGION_B'; File='shard-0-region-b'; DNS='booking-shard-0-postgres-region-b'; ReplicationDNS='booking-shard-0-postgres-region-b-replication' },
    [pscustomobject]@{ Prefix='SHARD_0_REGION_A_RESEED'; File='shard-0-region-a-reseed'; DNS='booking-shard-0-postgres-region-a-reseed'; ReplicationDNS='booking-shard-0-postgres-region-a-reseed-replication' },
    [pscustomobject]@{ Prefix='SHARD_1_REGION_A'; File='shard-1-region-a'; DNS='booking-shard-1-postgres'; ReplicationDNS='booking-shard-1-postgres-replication' },
    [pscustomobject]@{ Prefix='SHARD_1_REGION_B'; File='shard-1-region-b'; DNS='booking-shard-1-postgres-region-b'; ReplicationDNS='booking-shard-1-postgres-region-b-replication' },
    [pscustomobject]@{ Prefix='SHARD_1_REGION_A_RESEED'; File='shard-1-region-a-reseed'; DNS='booking-shard-1-postgres-region-a-reseed'; ReplicationDNS='booking-shard-1-postgres-region-a-reseed-replication' }
)
$controlCipherSecretPath = Join-Path $secretDirectory 'pgbackrest-control-cipher-pass'
$shard0CipherSecretPath = Join-Path $secretDirectory 'pgbackrest-shard-0-cipher-pass'
$shard1CipherSecretPath = Join-Path $secretDirectory 'pgbackrest-shard-1-cipher-pass'
$fencingAttestationPrivateKeyPath = Join-Path $secretDirectory 'fencing-attestation.key'
$fencingAttestationPublicDERPath = Join-Path $secretDirectory 'fencing-attestation-public.der'
$controlReplicationSecret = "m7-control-repl-$([guid]::NewGuid().ToString('N'))-$([guid]::NewGuid().ToString('N'))"
$shard0ReplicationSecret = "m7-shard0-repl-$([guid]::NewGuid().ToString('N'))-$([guid]::NewGuid().ToString('N'))"
$shard1ReplicationSecret = "m7-shard1-repl-$([guid]::NewGuid().ToString('N'))-$([guid]::NewGuid().ToString('N'))"
$controlCipherSecret = "m7-control-cipher-$([guid]::NewGuid().ToString('N'))-$([guid]::NewGuid().ToString('N'))"
$shard0CipherSecret = "m7-shard0-cipher-$([guid]::NewGuid().ToString('N'))-$([guid]::NewGuid().ToString('N'))"
$shard1CipherSecret = "m7-shard1-cipher-$([guid]::NewGuid().ToString('N'))-$([guid]::NewGuid().ToString('N'))"
[System.IO.File]::WriteAllText($controlReplicationSecretPath, $controlReplicationSecret)
[System.IO.File]::WriteAllText($shard0ReplicationSecretPath, $shard0ReplicationSecret)
[System.IO.File]::WriteAllText($shard1ReplicationSecretPath, $shard1ReplicationSecret)
[System.IO.File]::WriteAllText($controlCipherSecretPath, $controlCipherSecret)
[System.IO.File]::WriteAllText($shard0CipherSecretPath, $shard0CipherSecret)
[System.IO.File]::WriteAllText($shard1CipherSecretPath, $shard1CipherSecret)
$openssl = Get-Command openssl -ErrorAction Stop
& $openssl.Source genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 -out $replicationCAKeyPath 2>$null
if ($LASTEXITCODE -ne 0) { throw 'failed to generate the ephemeral replication CA key' }
& $openssl.Source req -x509 -new -sha256 -key $replicationCAKeyPath -subj '/CN=railway-m7-dr-replication-ca' -days 2 -out $replicationCACertPath 2>$null
if ($LASTEXITCODE -ne 0) { throw 'failed to generate the ephemeral replication CA certificate' }
foreach ($endpoint in $tlsEndpointSpecs) {
    $keyPath = Join-Path $secretDirectory "$($endpoint.File).key"
    $csrPath = Join-Path $secretDirectory "$($endpoint.File).csr"
    $certPath = Join-Path $secretDirectory "$($endpoint.File).crt"
    $extPath = Join-Path $secretDirectory "$($endpoint.File).ext"
    [System.IO.File]::WriteAllText($extPath, "basicConstraints=critical,CA:FALSE`nkeyUsage=critical,digitalSignature,keyEncipherment`nextendedKeyUsage=serverAuth`nsubjectAltName=DNS:$($endpoint.DNS),DNS:$($endpoint.ReplicationDNS)`n")
    & $openssl.Source genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 -out $keyPath 2>$null
    if ($LASTEXITCODE -ne 0) { throw "failed to generate the ephemeral replication TLS key for $($endpoint.DNS)" }
    & $openssl.Source req -new -sha256 -key $keyPath -subj "/CN=$($endpoint.DNS)" -out $csrPath 2>$null
    if ($LASTEXITCODE -ne 0) { throw "failed to generate the ephemeral replication TLS request for $($endpoint.DNS)" }
    & $openssl.Source x509 -req -sha256 -in $csrPath -CA $replicationCACertPath -CAkey $replicationCAKeyPath -CAcreateserial -days 2 -extfile $extPath -out $certPath 2>$null
    if ($LASTEXITCODE -ne 0) { throw "failed to issue the ephemeral replication TLS certificate for $($endpoint.DNS)" }
    $endpoint | Add-Member -NotePropertyName KeyPath -NotePropertyValue $keyPath
    $endpoint | Add-Member -NotePropertyName CertPath -NotePropertyValue $certPath
}
& $openssl.Source genpkey -algorithm ED25519 -out $fencingAttestationPrivateKeyPath 2>$null
if ($LASTEXITCODE -ne 0) { throw 'failed to generate the ephemeral fencing attestation key' }
& $openssl.Source pkey -in $fencingAttestationPrivateKeyPath -pubout -outform DER -out $fencingAttestationPublicDERPath 2>$null
if ($LASTEXITCODE -ne 0) { throw 'failed to export the fencing attestation public key' }
$fencingPublicDER = [System.IO.File]::ReadAllBytes($fencingAttestationPublicDERPath)
if ($fencingPublicDER.Length -lt 32) { throw 'fencing attestation public key export was malformed' }
$fencingPublicRaw = [byte[]]::new(32)
[Array]::Copy($fencingPublicDER, $fencingPublicDER.Length - 32, $fencingPublicRaw, 0, 32)

$originalEnvironment = @{
    DR_CONTROL_REPLICATION_PASSWORD_FILE = $env:DR_CONTROL_REPLICATION_PASSWORD_FILE
    DR_SHARD_0_REPLICATION_PASSWORD_FILE = $env:DR_SHARD_0_REPLICATION_PASSWORD_FILE
    DR_SHARD_1_REPLICATION_PASSWORD_FILE = $env:DR_SHARD_1_REPLICATION_PASSWORD_FILE
    DR_REPLICATION_TLS_ROOT_CERT_FILE = $env:DR_REPLICATION_TLS_ROOT_CERT_FILE
    PGBACKREST_CONTROL_CIPHER_FILE = $env:PGBACKREST_CONTROL_CIPHER_FILE
    PGBACKREST_SHARD_0_CIPHER_FILE = $env:PGBACKREST_SHARD_0_CIPHER_FILE
    PGBACKREST_SHARD_1_CIPHER_FILE = $env:PGBACKREST_SHARD_1_CIPHER_FILE
    JWT_SECRET = $env:JWT_SECRET
    PAYMENT_CONTRACT_API_KEY = $env:PAYMENT_CONTRACT_API_KEY
    CONTROL_PRIMARY_HOST = $env:CONTROL_PRIMARY_HOST
    SHARD_0_PRIMARY_HOST = $env:SHARD_0_PRIMARY_HOST
    SHARD_1_PRIMARY_HOST = $env:SHARD_1_PRIMARY_HOST
    REGION_B_DEPLOYMENT_ROLE = $env:REGION_B_DEPLOYMENT_ROLE
    REGION_B_EPOCH = $env:REGION_B_EPOCH
    REGION_B_WRITES_ENABLED = $env:REGION_B_WRITES_ENABLED
    REGION_A_DEPLOYMENT_ROLE = $env:REGION_A_DEPLOYMENT_ROLE
    REGION_A_EPOCH = $env:REGION_A_EPOCH
    REGION_A_WRITES_ENABLED = $env:REGION_A_WRITES_ENABLED
    REGION_A_SETTLEMENT_ENABLED = $env:REGION_A_SETTLEMENT_ENABLED
    REGION_A_CONTROL_DATABASE_URL = $env:REGION_A_CONTROL_DATABASE_URL
    REGION_A_RECONCILER_CONTROL_DATABASE_URL = $env:REGION_A_RECONCILER_CONTROL_DATABASE_URL
    REGION_A_RECONCILER_SHARD_0_DATABASE_URL = $env:REGION_A_RECONCILER_SHARD_0_DATABASE_URL
    REGION_A_RECONCILER_SHARD_1_DATABASE_URL = $env:REGION_A_RECONCILER_SHARD_1_DATABASE_URL
    CONTROL_DATABASE_URL = $env:CONTROL_DATABASE_URL
    BOOKING_SHARD_0_DATABASE_URL = $env:BOOKING_SHARD_0_DATABASE_URL
    BOOKING_SHARD_1_DATABASE_URL = $env:BOOKING_SHARD_1_DATABASE_URL
    ACTIVE_REGION_UPSTREAM = $env:ACTIVE_REGION_UPSTREAM
    DR_EVIDENCE_DIRECTORY = $env:DR_EVIDENCE_DIRECTORY
    M7_WEBHOOK_KEYRING = $env:M7_WEBHOOK_KEYRING
    M7_WEBHOOK_ACCEPT_KEY_IDS = $env:M7_WEBHOOK_ACCEPT_KEY_IDS
    M7_STRIPE_WEBHOOK_KEYRING = $env:M7_STRIPE_WEBHOOK_KEYRING
    M7_STRIPE_WEBHOOK_PRIMARY_KEY_ID = $env:M7_STRIPE_WEBHOOK_PRIMARY_KEY_ID
    M7_STRIPE_WEBHOOK_ACCEPT_KEY_IDS = $env:M7_STRIPE_WEBHOOK_ACCEPT_KEY_IDS
    M7_STRIPE_WEBHOOK_GRACE_SECONDS = $env:M7_STRIPE_WEBHOOK_GRACE_SECONDS
    DR_RECOVERY_EPOCH = $env:DR_RECOVERY_EPOCH
    DR_JOURNAL_REGION = $env:DR_JOURNAL_REGION
    DR_JOURNAL_DATABASE_URL = $env:DR_JOURNAL_DATABASE_URL
    DR_FENCE_ATTESTATION_ISSUER = $env:DR_FENCE_ATTESTATION_ISSUER
    DR_FENCE_ATTESTATION_KEY_ID = $env:DR_FENCE_ATTESTATION_KEY_ID
    DR_FENCE_ATTESTATION_PUBLIC_KEY_B64 = $env:DR_FENCE_ATTESTATION_PUBLIC_KEY_B64
    SETTLEMENT_ADMIN_DATABASE_URL = $env:SETTLEMENT_ADMIN_DATABASE_URL
    SETTLEMENT_ADMIN_REGION = $env:SETTLEMENT_ADMIN_REGION
    SETTLEMENT_ADMIN_EPOCH = $env:SETTLEMENT_ADMIN_EPOCH
}
foreach ($endpoint in $tlsEndpointSpecs) {
    $certVariable = "DR_$($endpoint.Prefix)_TLS_CERT_FILE"
    $keyVariable = "DR_$($endpoint.Prefix)_TLS_KEY_FILE"
    $originalEnvironment[$certVariable] = [Environment]::GetEnvironmentVariable($certVariable)
    $originalEnvironment[$keyVariable] = [Environment]::GetEnvironmentVariable($keyVariable)
}
$env:DR_CONTROL_REPLICATION_PASSWORD_FILE = $controlReplicationSecretPath
$env:DR_SHARD_0_REPLICATION_PASSWORD_FILE = $shard0ReplicationSecretPath
$env:DR_SHARD_1_REPLICATION_PASSWORD_FILE = $shard1ReplicationSecretPath
$env:DR_REPLICATION_TLS_ROOT_CERT_FILE = $replicationCACertPath
$env:PGBACKREST_CONTROL_CIPHER_FILE = $controlCipherSecretPath
$env:PGBACKREST_SHARD_0_CIPHER_FILE = $shard0CipherSecretPath
$env:PGBACKREST_SHARD_1_CIPHER_FILE = $shard1CipherSecretPath
foreach ($endpoint in $tlsEndpointSpecs) {
    Set-Item "Env:DR_$($endpoint.Prefix)_TLS_CERT_FILE" -Value $endpoint.CertPath
    Set-Item "Env:DR_$($endpoint.Prefix)_TLS_KEY_FILE" -Value $endpoint.KeyPath
}
$env:JWT_SECRET = "m7-jwt-$([guid]::NewGuid().ToString('N'))-$([guid]::NewGuid().ToString('N'))"
$env:PAYMENT_CONTRACT_API_KEY = "rk_test_m7_$([guid]::NewGuid().ToString('N'))$([guid]::NewGuid().ToString('N'))"
$env:DR_EVIDENCE_DIRECTORY = $EvidenceDirectory
$env:DR_FENCE_ATTESTATION_ISSUER = 'railway-local-fence-authority'
$env:DR_FENCE_ATTESTATION_KEY_ID = 'ephemeral-m7'
$env:DR_FENCE_ATTESTATION_PUBLIC_KEY_B64 = [Convert]::ToBase64String($fencingPublicRaw)
$env:REGION_A_SETTLEMENT_ENABLED = 'false'
$webhookPreviousKey = [System.Text.Encoding]::UTF8.GetBytes("m7-prev-$([guid]::NewGuid().ToString('N'))")
$webhookCurrentKey = [System.Text.Encoding]::UTF8.GetBytes("m7-curr-$([guid]::NewGuid().ToString('N'))")
$webhookPreviousB64 = [Convert]::ToBase64String($webhookPreviousKey)
$webhookCurrentB64 = [Convert]::ToBase64String($webhookCurrentKey)
$env:M7_WEBHOOK_KEYRING = "previous=$webhookPreviousB64,current=$webhookCurrentB64"
$env:M7_WEBHOOK_ACCEPT_KEY_IDS = 'previous,current'
$stripeWebhookPrevious = "whsec_m7_previous_$([guid]::NewGuid().ToString('N'))"
$stripeWebhookCurrent = "whsec_m7_current_$([guid]::NewGuid().ToString('N'))"
$env:M7_STRIPE_WEBHOOK_KEYRING = "previous=$stripeWebhookPrevious"
$env:M7_STRIPE_WEBHOOK_PRIMARY_KEY_ID = 'previous'
$env:M7_STRIPE_WEBHOOK_ACCEPT_KEY_IDS = 'previous'
$env:M7_STRIPE_WEBHOOK_GRACE_SECONDS = '5'
$sensitiveValues = @(
    $controlReplicationSecret, $shard0ReplicationSecret, $shard1ReplicationSecret,
    $controlCipherSecret, $shard0CipherSecret, $shard1CipherSecret, $env:JWT_SECRET, $env:PAYMENT_CONTRACT_API_KEY,
    $webhookPreviousB64, $webhookCurrentB64, $env:M7_WEBHOOK_KEYRING,
    $stripeWebhookPrevious, $stripeWebhookCurrent, $env:M7_STRIPE_WEBHOOK_KEYRING
)
if ($env:GITHUB_ACTIONS -eq 'true') {
    foreach ($value in $sensitiveValues) { Write-Output "::add-mask::$value" }
}

$composeArguments = @('compose','-p',$ProjectName)
foreach ($file in $composeFiles) { $composeArguments += @('-f',$file) }
$driverContext = [pscustomobject]@{
    RepositoryPath=$root; RawDirectory=$EvidenceDirectory; ProjectName=$ProjectName
    ComposeFile=$composeFile; ComposeArguments=[string[]]$composeArguments
}
$started = $false
$runError = $null
$start = [DateTimeOffset]::UtcNow
$phaseEvidence = [System.Collections.Generic.List[object]]::new()
$backupEvidence = [System.Collections.Generic.List[object]]::new()
$restoreEvidence = [System.Collections.Generic.List[object]]::new()
$settlementEvidence = [System.Collections.Generic.List[object]]::new()
$refundEvidence = [System.Collections.Generic.List[object]]::new()
$metricsEvidence = [System.Collections.Generic.List[object]]::new()
$replicationEvidence = [System.Collections.Generic.List[object]]::new()
$journalCrashEvidence = [System.Collections.Generic.List[object]]::new()
$applicationCrashEvidence = [System.Collections.Generic.List[object]]::new()
$rpoEvidence = [System.Collections.Generic.List[object]]::new()
$failbackRPOEvidence = [System.Collections.Generic.List[object]]::new()
$failoverStart = $null
$failbackStart = $null
$webhookOutageStartedAt = $null
$webhookOutageEndedAt = $null
$sourceState = [pscustomobject]@{ FileCount=0; SHA256=''; Excluded=[string[]]@('docs/benchmark-report-milestone-7.md','docs/milestone-7-load-testing.md') }
$sourceCommit = ''
$sourceDirtyAtStart = $true
$renderedDigest = ''
$m7Customer = $null
$m7Orders = @()
$m7HealthyCustomer = $null
$m7HealthyOrderID = ''
$failoverOperationID = [guid]::NewGuid().ToString()
$failoverIncidentID = [guid]::NewGuid().ToString()
$failbackOperationID = [guid]::NewGuid().ToString()
$failbackIncidentID = [guid]::NewGuid().ToString()
$failoverPositions = @{}
$standbyReplayPositions = @{}
$failbackPositions = @{}
$failbackReplayPositions = @{}
$settlementReconciliationEvidence = $null
$interruptedPaymentEvidence = $null
$interruptedRefundEvidence = $null
$interruptedFullRefundEvidence = $null
$singleShardOutageEvidence = $null
$redisRecoveryEvidence = $null
$stripeRotationEvidence = $null

function Protect-M7Diagnostic {
    param([string[]]$Lines)
    $value = [string](@($Lines | Select-Object -Last 12) -join "`n")
    foreach ($secret in $sensitiveValues) {
        if (-not [string]::IsNullOrEmpty($secret)) { $value = $value.Replace($secret, '[redacted]') }
    }
    $value = $value -replace '(?i)(postgres(?:ql)?://)[^\s/@:]+:[^\s/@]+@', '$1[redacted]@'
    $value = $value -replace '(?i)(password|cipher[_-]?pass|secret)(\s*[:=]\s*)[^\s,;]+', '$1$2[redacted]'
    if ($value.Length -gt 2048) { $value = $value.Substring($value.Length - 2048) }
    return $value
}

function Invoke-M7Native {
    param([scriptblock]$Command, [switch]$AllowFailure)
    $previous = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = @(& $Command 2>&1 | ForEach-Object { [string]$_ })
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    if ($exitCode -ne 0 -and -not $AllowFailure) {
        $diagnostic = Protect-M7Diagnostic -Lines $output
        throw "native command failed with exit code $exitCode`n$diagnostic"
    }
    [pscustomobject]@{ Output=[string[]]$output; ExitCode=[int]$exitCode }
}

function Invoke-M7Compose {
    param([string[]]$Arguments, [switch]$AllowFailure)
    $base = [string[]]$composeArguments
    return Invoke-M7Native -AllowFailure:$AllowFailure -Command { & docker @base @Arguments }
}

function Get-M7SHA256 {
    param([Parameter(Mandatory=$true)][string]$Text)
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try { $bytes = $sha.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($Text)) } finally { $sha.Dispose() }
    return (($bytes | ForEach-Object { $_.ToString('x2') }) -join '')
}

function Get-M7SourceState {
    $excluded = @('docs/benchmark-report-milestone-7.md','docs/milestone-7-load-testing.md')
    $paths = @(& git -C $root ls-files --cached --others --exclude-standard)
    if ($LASTEXITCODE -ne 0 -or $paths.Count -eq 0) { throw 'source-state inventory failed' }
    $entries = [System.Collections.Generic.List[string]]::new()
    foreach ($relative in @($paths | Sort-Object -Unique)) {
        $normalized = ([string]$relative).Replace('\','/')
        if ($normalized -in $excluded) { continue }
        $full = Join-Path $root ([string]$relative)
        if (-not [System.IO.File]::Exists($full)) { $entries.Add("$normalized|missing"); continue }
        $file = [System.IO.FileInfo]::new($full)
        $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $full).Hash.ToLowerInvariant()
        $entries.Add("$normalized|$($file.Length)|$hash")
    }
    [pscustomobject]@{ FileCount=$entries.Count; SHA256=(Get-M7SHA256 -Text ($entries -join "`n")); Excluded=[string[]]$excluded }
}

function Write-M7JSON {
    param([Parameter(Mandatory=$true)][string]$Name, [Parameter(Mandatory=$true)][object]$Value)
    $json = $Value | ConvertTo-Json -Depth 12
    [System.IO.File]::WriteAllText((Join-Path $EvidenceDirectory $Name), $json, [System.Text.UTF8Encoding]::new($false))
}

function Get-M7Scalar {
    param([string]$Service, [string]$User, [string]$Database, [string]$SQL)
    $result = Invoke-M7Compose -Arguments @('exec','-T',$Service,'psql','-X','-v','ON_ERROR_STOP=1','-U',$User,'-d',$Database,'-tAc',$SQL)
    $values = @($result.Output | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' })
    if ($values.Count -ne 1) { throw "database scalar was not exactly one row for $Service" }
    return [string]$values[0]
}

function Invoke-M7SQL {
    param([string]$Service, [string]$User, [string]$Database, [string]$SQL)
    Invoke-M7Compose -Arguments @('exec','-T',$Service,'psql','-X','-v','ON_ERROR_STOP=1','-U',$User,'-d',$Database,'-c',$SQL) | Out-Null
}

function Invoke-M7SQLFile {
    param([string]$Service, [string]$User, [string]$Database, [string]$Path)
    $fullPath = Join-Path $root $Path
    if (-not [System.IO.File]::Exists($fullPath)) { throw "required SQL evidence fixture is missing: $Path" }
    $sql = [System.IO.File]::ReadAllText($fullPath)
    $base = [string[]]$composeArguments
    $result = Invoke-M7Native -Command { $sql | & docker @base exec -T $Service psql -X -v ON_ERROR_STOP=1 -U $User -d $Database }
    if ($result.ExitCode -ne 0) { throw "SQL evidence fixture failed: $Path" }
}

function Invoke-M7AuthorityTransition {
    param([string]$Service, [string]$User, [string]$Database, [string]$Region, [int64]$Epoch, [string]$State='active', [bool]$Writes=$true)
    if ($Region -notin @('region-a','region-b') -or $Epoch -lt 1 -or $State -notin @('recovery','active') -or ($Writes -and $State -ne 'active')) { throw 'authority transition context is invalid' }
    Assert-M7DRFenceFresh -OperationID $(if($Region -eq 'region-b'){$failoverOperationID}else{$failbackOperationID}) `
        -JournalRegion $Region -RecoveryEpoch $Epoch
    $writesSQL = $Writes.ToString().ToLowerInvariant()
    $sql = @"
WITH recovery_context AS MATERIALIZED (
    SELECT set_config('railway.deployment_region','$Region',true),
           set_config('railway.deployment_role','recovery',true),
           set_config('railway.region_epoch','$Epoch',true),
           set_config('railway.regional_writes_enabled','false',true)
), changed AS (
    UPDATE public.regional_write_authority
       SET region='$Region',epoch=$Epoch,state='$State',writes_enabled=$writesSQL
      FROM recovery_context
     WHERE singleton
 RETURNING 1
)
SELECT count(*) FROM changed
"@
    return Get-M7Scalar -Service $Service -User $User -Database $Database -SQL $sql
}

function Wait-M7Role {
    param([string]$Service, [string]$User, [string]$Database, [bool]$Recovery)
    $expected = 'f'
    if ($Recovery) { $expected = 't' }
    for ($attempt=1; $attempt -le 90; $attempt++) {
        $probe = Invoke-M7Compose -AllowFailure -Arguments @('exec','-T',$Service,'psql','-X','-U',$User,'-d',$Database,'-tAc','SELECT pg_is_in_recovery()')
        $value = [string](@($probe.Output | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' }) | Select-Object -Last 1)
        if ($probe.ExitCode -eq 0 -and $value -eq $expected) { return }
        Start-Sleep -Seconds 1
    }
    $logs = Invoke-M7Compose -AllowFailure -Arguments @('logs','--no-color','--tail','40',$Service)
    $diagnostic = Protect-M7Diagnostic -Lines $logs.Output
    throw "$Service did not reach expected recovery role $expected`n$diagnostic"
}

function Wait-M7Replay {
    param([string]$Service, [string]$User, [string]$Database, [string]$LSN)
    if ($LSN -notmatch '^[0-9A-F]+/[0-9A-F]+$') { throw 'source LSN is malformed' }
    for ($attempt=1; $attempt -le 120; $attempt++) {
        $probe = Invoke-M7Compose -AllowFailure -Arguments @('exec','-T',$Service,'psql','-X','-U',$User,'-d',$Database,'-tAc',"SELECT pg_last_wal_replay_lsn()>='$LSN'::pg_lsn")
        $value = [string](@($probe.Output | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' }) | Select-Object -Last 1)
        if ($probe.ExitCode -eq 0 -and $value -eq 't') { return }
        Start-Sleep -Milliseconds 500
    }
    throw "$Service did not replay the required LSN"
}

function Wait-M7ServiceHTTP {
    param([string]$Service, [string]$URL)
    for ($attempt=1; $attempt -le 90; $attempt++) {
        $probe = Invoke-M7Compose -AllowFailure -Arguments @('exec','-T',$Service,'wget','-q','-T','2','-O','/dev/null',$URL)
        if ($probe.ExitCode -eq 0) { return }
        Start-Sleep -Seconds 1
    }
    throw "$Service did not expose its bounded HTTP evidence endpoint"
}

function Get-M7PublishedURL {
    $result = Invoke-M7Compose -Arguments @('port','global-test-ingress','8080')
    $value = [string](@($result.Output | Where-Object { $_.Trim() -ne '' }) | Select-Object -Last 1)
    if ($value -notmatch ':(\d+)$') { throw 'global test ingress did not publish a bounded port' }
    return "http://127.0.0.1:$($Matches[1])"
}

function New-M7CustomerFixtures {
    param([string]$BaseURL, [string]$TrainRunID, [int]$Count, [string]$Label='primary')
    if ($Count -lt 1 -or $Count -gt 10) { throw 'M7 customer fixture count is outside the bound' }
    if ($Label -notmatch '^[a-z][a-z0-9-]{0,20}$') { throw 'M7 customer fixture label is invalid' }
    $password = "M7-$([guid]::NewGuid().ToString('N').Substring(0, 14))-Aa1!"
    $email = "m7-dr-$Label-$suffix@example.test"
    Invoke-Milestone5DriverAPI -BaseURL $BaseURL -Method POST -Path '/api/v1/auth/register' -ForwardedFor '198.19.7.41' `
        -Body @{ email=$email; password=$password; display_name='M7 DR Rider' } -ExpectedStatus @(202) | Out-Null
    $login = Invoke-Milestone5DriverAPI -BaseURL $BaseURL -Method POST -Path '/api/v1/auth/login' -ForwardedFor '198.19.7.41' `
        -Body @{ email=$email; password=$password } -ExpectedStatus @(200)
    $token = [string]$login.Body.access_token
    if ([string]::IsNullOrWhiteSpace($token)) { throw 'M7 synthetic login omitted its token' }
    $script:sensitiveValues += $token
    if ($env:GITHUB_ACTIONS -eq 'true') { Write-Output "::add-mask::$token" }
    $reservations = [System.Collections.Generic.List[string]]::new()
    for ($index=0; $index -lt $Count; $index++) {
        $passengers = [System.Collections.Generic.List[string]]::new()
        foreach ($passengerIndex in 0,1) {
            $passenger = Invoke-Milestone5DriverAPI -BaseURL $BaseURL -Method POST -Path '/api/v1/passengers' -Token $token `
                -Body @{ display_name="M7 Passenger $index-$passengerIndex" } -ExpectedStatus @(201)
            $passengers.Add([string]$passenger.Body.id)
        }
        $reservation = Invoke-Milestone5DriverAPI -BaseURL $BaseURL -Method POST -Path '/api/v1/reservations' -Token $token `
                -IdempotencyKey "m7-dr-reservation-$Label-$suffix-$index" -Body @{
                train_run_id=$TrainRunID; origin_station_code='M2A'; destination_station_code='M2B'
                seat_class='standard'; passenger_ids=[string[]]$passengers
            } -ExpectedStatus @(201)
        $reservations.Add([string]$reservation.Body.id)
    }
    $password = $null
    return [pscustomobject]@{ Token=$token; Reservations=[string[]]$reservations }
}

function Assert-M7SettlementImport {
    param([string]$DatabaseService, [string]$WorkerService, [string]$Phase, [switch]$CrashAfterFirstPage)
    $observationStartedAt = [DateTimeOffset]::UtcNow
    $expected = '3|1|1|0|0'
    if ($CrashAfterFirstPage) {
        $barrierReached = $false
        for ($attempt=1; $attempt -le 100; $attempt++) {
            $barrier = Get-M7Scalar -Service $DatabaseService -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT (SELECT cursor FROM public.provider_settlement_import_checkpoints WHERE provider='stripe' AND provider_account_id='acct_m7_contract')||'|'||
       (SELECT count(*) FROM public.provider_balance_transactions WHERE provider='stripe' AND provider_account_id='acct_m7_contract')::text||'|'||
       (SELECT count(*) FROM public.provider_payouts WHERE provider='stripe' AND provider_account_id='acct_m7_contract')::text
"@
            if ($barrier -eq 'b:txn_m7_capture|1|0') { $barrierReached=$true; break }
            Start-Sleep -Milliseconds 25
        }
        if (-not $barrierReached) { throw 'settlement page-1 crash barrier was not reached before page 2' }
        Invoke-M7Compose -Arguments @('kill','-s','KILL',$WorkerService) | Out-Null
        $afterCrash = Get-M7Scalar -Service $DatabaseService -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT cursor||'|'||(SELECT count(*) FROM public.provider_balance_transactions WHERE provider='stripe' AND provider_account_id='acct_m7_contract')::text||'|'||
       (SELECT count(*) FROM public.provider_payouts WHERE provider='stripe' AND provider_account_id='acct_m7_contract')::text
FROM public.provider_settlement_import_checkpoints WHERE provider='stripe' AND provider_account_id='acct_m7_contract'
"@
        $afterCrashLease = Get-M7Scalar -Service $DatabaseService -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT (lease_token IS NOT NULL)::text||'|'||(lease_until>clock_timestamp())::text
FROM public.provider_settlement_import_checkpoints WHERE provider='stripe' AND provider_account_id='acct_m7_contract'
"@
        if ($afterCrash -ne 'b:txn_m7_capture|1|0' -or $afterCrashLease -ne 'true|true') { throw 'settlement page checkpoint or durable lease moved across the forced worker crash' }
        Invoke-M7Compose -Arguments @('start',$WorkerService) | Out-Null
        Wait-M7ServiceHTTP -Service $WorkerService -URL 'http://127.0.0.1:9090/metrics'
        $settlementEvidence.Add([ordered]@{ phase="$Phase-page-1-crash"; cursor='b:txn_m7_capture'; balance_transactions=1; payouts=0; process_killed=$true; durable_lease_survived_crash=$true; external_io_outside_transaction=$true })
    }
    for ($attempt=1; $attempt -le 90; $attempt++) {
        $value = Get-M7Scalar -Service $DatabaseService -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT (SELECT count(*) FROM public.provider_balance_transactions WHERE provider='stripe' AND provider_account_id='acct_m7_contract')::text||'|'||
       (SELECT count(*) FROM public.provider_payouts WHERE provider='stripe' AND provider_account_id='acct_m7_contract')::text||'|'||
       (SELECT count(*) FROM public.provider_settlement_import_checkpoints WHERE provider='stripe' AND provider_account_id='acct_m7_contract')::text||'|'||
       (SELECT count(*) FROM public.provider_settlement_import_conflicts WHERE provider='stripe' AND provider_account_id='acct_m7_contract')::text||'|'||
       (SELECT count(*) FROM public.provider_settlement_import_checkpoints WHERE provider='stripe' AND provider_account_id='acct_m7_contract' AND lease_token IS NOT NULL)::text
"@
        if ($value -eq $expected) { break }
        if ($attempt -eq 90) { throw "settlement import did not converge for $Phase; observed bounded counts $value" }
        Start-Sleep -Seconds 1
    }
    Invoke-M7Compose -Arguments @('restart','-t','10',$WorkerService) | Out-Null
    Wait-M7ServiceHTTP -Service $WorkerService -URL 'http://127.0.0.1:9090/metrics'
    Start-Sleep -Seconds 2
    $replay = Get-M7Scalar -Service $DatabaseService -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT (SELECT count(*) FROM public.provider_balance_transactions WHERE provider='stripe' AND provider_account_id='acct_m7_contract')::text||'|'||
       (SELECT count(*) FROM public.provider_payouts WHERE provider='stripe' AND provider_account_id='acct_m7_contract')::text||'|'||
       (SELECT count(*) FROM public.provider_settlement_import_checkpoints WHERE provider='stripe' AND provider_account_id='acct_m7_contract')::text||'|'||
       (SELECT count(*) FROM public.provider_settlement_import_conflicts WHERE provider='stripe' AND provider_account_id='acct_m7_contract')::text||'|'||
       (SELECT count(*) FROM public.provider_settlement_import_checkpoints WHERE provider='stripe' AND provider_account_id='acct_m7_contract' AND lease_token IS NOT NULL)::text
"@
    if ($replay -ne $expected) { throw "settlement replay was not idempotent for $Phase" }
    $settlementEvidence.Add([ordered]@{ phase=$Phase; resumed_from_cursor=($(if($CrashAfterFirstPage){'b:txn_m7_capture'}else{'completed'})); balance_transactions=3; payouts=1; checkpoints=1; conflicts=0; lease_released=$true; replay='idempotent'; fixture='deterministic-read-only' })
    return [ordered]@{
        imported_records=3
        observation_seconds=[Math]::Max(0.001,[DateTimeOffset]::UtcNow.Subtract($observationStartedAt).TotalSeconds)
    }
}

function Invoke-M7K6 {
    param([string]$Script, [hashtable]$Environment)
    $allowed = @(
        'production-provider-contract.js','settlement-import.js','partial-ticket-refund.js',
        'partial-refund-idempotency.js','webhook-ack-failure.js','webhook-key-rotation.js',
        'regional-failover.js','payment-during-failover.js','refund-during-failover.js','regional-failback.js'
    )
    if ($Script -notin $allowed) { throw "k6 module is not allowlisted: $Script" }
    $name = [System.IO.Path]::GetFileNameWithoutExtension($Script)
    $arguments = @('--profile','dr-tests','run','--rm','--no-deps')
    foreach ($key in @($Environment.Keys | Sort-Object)) {
        if ($key -notmatch '^[A-Z][A-Z0-9_]{0,63}$') { throw 'k6 environment name is invalid' }
        $arguments += @('-e',"$key=$($Environment[$key])")
    }
    $arguments += @('k6-milestone-7','run','--quiet','--summary-export',"/evidence/$name-summary.json","/scripts/$Script")
    $result = Invoke-M7Compose -AllowFailure -Arguments $arguments
    [System.IO.File]::WriteAllLines((Join-Path $EvidenceDirectory "$name.log"), [string[]]$result.Output, [System.Text.UTF8Encoding]::new($false))
    if ($result.ExitCode -ne 0) { throw "$Script failed" }
    $summaryPath = Join-Path $EvidenceDirectory "$name-summary.json"
    if (-not [System.IO.File]::Exists($summaryPath)) { throw "$Script omitted its strict summary" }
    $summary = Get-Content -Raw -LiteralPath $summaryPath | ConvertFrom-Json
    $checks = $summary.metrics.PSObject.Properties['checks'].Value.values
    if ([int64]$checks.fails -ne 0 -or [int64]$checks.passes -lt 1) { throw "$Script did not pass every check" }
    if ($Script -ceq 'settlement-import.js') {
        $records = $summary.metrics.PSObject.Properties['settlement_import_records_observed'].Value.values
        $rate = $summary.metrics.PSObject.Properties['settlement_import_rate_records_per_second'].Value.values
        $lag = $summary.metrics.PSObject.Properties['settlement_import_lag_seconds_observed'].Value.values
        if ([int64]$records.count -lt 1 -or [double]$records.rate -le 0 -or
            [int64]$rate.count -lt 1 -or [double]$rate.avg -le 0 -or
            [int64]$lag.count -lt 1 -or [double]$lag.min -lt 0) {
            throw 'settlement-import.js omitted positive work/rate or bounded lag evidence'
        }
        $settlementEvidence.Add([ordered]@{
            phase='region-a-load-measurement'; imported_records=[int64]$records.count;
            k6_counter_rate=[Math]::Round([double]$records.rate,6);
            import_rate_records_per_second=[Math]::Round([double]$rate.avg,6);
            average_lag_seconds=[Math]::Round([double]$lag.avg,6);
            measurement_source='durable-read-model-plus-bounded-convergence-window'; truncated=$false
        })
    }
}

function Get-M7WebhookSignature {
    param([byte[]]$Key, [string]$Timestamp, [string]$Body)
    $hmac = [System.Security.Cryptography.HMACSHA256]::new($Key)
    try {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes("$Timestamp.$Body")
        return (($hmac.ComputeHash($bytes) | ForEach-Object { $_.ToString('x2') }) -join '')
    } finally { $hmac.Dispose() }
}

function Invoke-M7ContainerWebhookStatus {
    param(
        [string]$Service, [string]$Body, [string]$KeyID, [string]$Timestamp,
        [string]$Signature, [int[]]$ExpectedStatus
    )
    $result = Invoke-M7Compose -AllowFailure -Arguments @(
        'exec','-T',$Service,'wget','-S','-O','/dev/null',
        '--header=Content-Type: application/json',"--header=X-Payment-Key-ID: $KeyID",
        "--header=X-Payment-Timestamp: $Timestamp","--header=X-Payment-Signature: $Signature",
        "--post-data=$Body",'http://127.0.0.1:8080/webhooks/payments/sandbox'
    )
    $text = $result.Output -join "`n"
    $matches = [regex]::Matches($text, '(?im)HTTP/1\.[01]\s+([0-9]{3})')
    if ($matches.Count -lt 1) { throw "webhook status was not observable from $Service" }
    $status = [int]$matches[$matches.Count-1].Groups[1].Value
    if ($status -notin $ExpectedStatus) { throw "webhook status $status from $Service was outside the expected set" }
    return $status
}

function Invoke-M7StripeWebhookStatus {
    param(
        [string]$Service, [string]$Body, [string]$Timestamp,
        [string]$Signature, [int[]]$ExpectedStatus
    )
    $result = Invoke-M7Compose -AllowFailure -Arguments @(
        'exec','-T',$Service,'wget','-S','-O','/dev/null',
        '--header=Content-Type: application/json',"--header=Stripe-Signature: t=$Timestamp,v1=$Signature",
        "--post-data=$Body",'http://127.0.0.1:8080/webhooks/payments/stripe'
    )
    $text = $result.Output -join "`n"
    $matches = [regex]::Matches($text, '(?im)HTTP/1\.[01]\s+([0-9]{3})')
    if ($matches.Count -lt 1) { throw "Stripe webhook status was not observable from $Service" }
    $status = [int]$matches[$matches.Count-1].Groups[1].Value
    if ($status -notin $ExpectedStatus) { throw "Stripe webhook status $status from $Service was outside the expected set" }
    return $status
}

function Assert-M7DurableMetrics {
    param(
        [string]$Service, [string]$Phase, [string[]]$Families,
        [string[]]$AllowZeroFamilies=@(), [string[]]$ExpectedDatabaseTuples=@()
    )
    if ($Phase -notmatch '^[a-z0-9][a-z0-9-]{0,80}$' -or $Families.Count -lt 1 -or $Families.Count -gt 32) { throw 'durable metrics assertion is invalid' }
    foreach ($tuple in $ExpectedDatabaseTuples) {
        if ($tuple -notmatch '^region-[ab]\|(control\|none|booking_shard\|shard-[01])$') { throw 'durable metric database tuple is invalid' }
    }
    $result = Invoke-M7Compose -Arguments @('exec','-T',$Service,'wget','-q','-T','5','-O','-','http://127.0.0.1:9090/metrics')
    $text = [string]($result.Output -join "`n")
    if ([string]::IsNullOrWhiteSpace($text) -or $text.Length -gt 1048576) { throw "durable metrics payload was empty or unbounded for $Service" }
    $observed = [System.Collections.Generic.List[string]]::new()
    $nonzero = [System.Collections.Generic.List[string]]::new()
    $tupleCoverage = [System.Collections.Generic.List[string]]::new()
    foreach ($family in $Families) {
        if ($family -notmatch '^[a-z][a-z0-9_]{2,100}$') { throw 'durable metric family is invalid' }
        $matches = [regex]::Matches($text, "(?m)^$([regex]::Escape($family))(?:_count)?(?:\{(?<labels>[^}`r`n]*)\})?\s+(?<value>[0-9]+(?:\.[0-9]+)?)$")
        if ($matches.Count -lt 1) { throw "durable metric family $family was absent in $Phase" }
        $hasNonzero = @($matches | Where-Object { [double]$_.Groups['value'].Value -gt 0 }).Count -ge 1
        if (-not $hasNonzero -and $family -notin $AllowZeroFamilies) { throw "durable metric family $family had no nonzero observation in $Phase" }
        $observed.Add($family)
        if ($hasNonzero) { $nonzero.Add($family) }
        foreach ($tuple in $ExpectedDatabaseTuples) {
            $parts = $tuple.Split('|')
            $tupleMatch = @($matches | Where-Object {
                $labels = [string]$_.Groups['labels'].Value
                $labels.Contains("region=`"$($parts[0])`"") -and
                $labels.Contains("database_role=`"$($parts[1])`"") -and
                $labels.Contains("shard_id=`"$($parts[2])`"")
            })
            if ($tupleMatch.Count -ne 1) { throw "durable metric family $family did not expose exactly one $tuple sample in $Phase" }
            $tupleCoverage.Add("$family|$tuple")
        }
    }
    [System.IO.File]::WriteAllText((Join-Path $EvidenceDirectory "$Phase-metrics.txt"), $text, [System.Text.UTF8Encoding]::new($false))
    $metricsEvidence.Add([ordered]@{
        phase=$Phase; service=$Service; present_families=[string[]]$observed;
        nonzero_families=[string[]]$nonzero; required_tuples=[string[]]$ExpectedDatabaseTuples;
        tuple_coverage=[string[]]$tupleCoverage; payload_sha256=(Get-M7SHA256 -Text $text); truncated=$false
    })
}

function Assert-M7PartialRefund {
    param(
        [string]$OrderID, [string]$SelectedTicketID, [string]$Scenario,
        [string]$ControlService='control-postgres', [string]$ShardService='booking-shard-0-postgres'
    )
    foreach ($identity in @($OrderID,$SelectedTicketID)) {
        if ($identity -notmatch '^[0-9a-f-]{36}$') { throw 'refund evidence identity is malformed' }
    }
    for ($attempt=1; $attempt -le 120; $attempt++) {
        $control = Get-M7Scalar -Service $ControlService -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT (SELECT count(*) FROM public.ticket_refund_requests WHERE ticket_order_id='$OrderID'::uuid AND state='completed')::text||'|'||
       (SELECT count(*) FROM public.ticket_refund_operations AS operation JOIN public.ticket_refund_requests AS request USING(refund_request_id) WHERE request.ticket_order_id='$OrderID'::uuid AND operation.state='succeeded' AND operation.provider_refund_id IS NOT NULL)::text||'|'||
       (SELECT count(DISTINCT ledger.transaction_id) FROM public.financial_ledger_transactions AS ledger JOIN public.ticket_refund_operations AS operation ON ledger.event_id='partial_refund:'||operation.refund_operation_id::text JOIN public.ticket_refund_requests AS request USING(refund_request_id) WHERE request.ticket_order_id='$OrderID'::uuid AND ledger.purpose='refund')::text||'|'||
       (SELECT count(posting.*) FROM public.financial_ledger_transactions AS ledger JOIN public.financial_ledger_postings AS posting USING(transaction_id) JOIN public.ticket_refund_operations AS operation ON ledger.event_id='partial_refund:'||operation.refund_operation_id::text JOIN public.ticket_refund_requests AS request USING(refund_request_id) WHERE request.ticket_order_id='$OrderID'::uuid)::text||'|'||
       (SELECT (coalesce(sum(posting.amount_minor) FILTER (WHERE posting.side='debit'),0)=coalesce(sum(posting.amount_minor) FILTER (WHERE posting.side='credit'),0))::text FROM public.financial_ledger_transactions AS ledger JOIN public.financial_ledger_postings AS posting USING(transaction_id) JOIN public.ticket_refund_operations AS operation ON ledger.event_id='partial_refund:'||operation.refund_operation_id::text JOIN public.ticket_refund_requests AS request USING(refund_request_id) WHERE request.ticket_order_id='$OrderID'::uuid)
"@
        $shard = Get-M7Scalar -Service $ShardService -User 'railway_booking' -Database 'railway_booking' -SQL @"
SELECT (SELECT count(*) FROM public.selected_ticket_refund_receipts WHERE ticket_id='$SelectedTicketID'::uuid)::text||'|'||
       (SELECT count(*) FROM public.tickets WHERE ticket_order_id='$OrderID'::uuid AND status='refunded')::text||'|'||
       (SELECT count(*) FROM public.tickets WHERE ticket_order_id='$OrderID'::uuid AND status='active')::text
"@
        if ($control -eq '1|1|1|2|true' -and $shard -eq '1|1|1') {
            $refundEvidence.Add([ordered]@{ scenario=$Scenario; request_count=1; provider_refund_count=1; ledger_transactions=1; ledger_postings=2; ledger_balanced=$true; selected_receipts=1; refunded_tickets=1; unselected_active_tickets=1 })
            return
        }
        Start-Sleep -Milliseconds 500
    }
    throw "partial refund did not converge with exact-once evidence for $Scenario"
}

function Complete-M7SandboxPayment {
    param([string]$DatabaseService, [string]$ReservationID, [string]$WebhookBaseURL)
    if ($ReservationID -notmatch '^[0-9a-f-]{36}$') { throw 'payment failover reservation identity is malformed' }
    $hosted = ''
    for ($attempt=1; $attempt -le 120; $attempt++) {
        $hosted = Get-M7Scalar -Service $DatabaseService -User 'railway_control' -Database 'railway_control' -SQL "SELECT coalesce(hosted_session_ref,'') FROM public.payment_intents WHERE reservation_id='$ReservationID'::uuid ORDER BY created_at DESC LIMIT 1"
        if ($hosted -match '^sandbox-checkout:([A-Za-z0-9._:-]+)$') { break }
        Start-Sleep -Milliseconds 500
    }
    if ($hosted -notmatch '^sandbox-checkout:([A-Za-z0-9._:-]+)$') { throw 'failover payment did not expose a bounded hosted reference' }
    $providerPaymentID = $Matches[1]
    Invoke-M7Compose -Arguments @('exec','-T','payment-sandbox','wget','-q','-O','/dev/null','--post-data=',"http://127.0.0.1:8099/hosted/checkouts/$providerPaymentID/authorize") | Out-Null
    $drained = Invoke-M7Compose -Arguments @('exec','-T','payment-sandbox','wget','-q','-O','-',"--header=X-Sandbox-Control-Token: synthetic-disposable-fault-token",'http://127.0.0.1:8099/_sandbox/webhooks')
    $events = @(($drained.Output -join "`n") | ConvertFrom-Json)
    if ($events.Count -lt 1 -or $events.Count -gt 100) { throw 'failover payment webhook drain is outside the bound' }
    foreach ($event in $events) {
        $body = [Convert]::FromBase64String([string]$event.Body)
        $response = Invoke-WebRequest -UseBasicParsing -Method Post -Uri "$WebhookBaseURL/webhooks/payments/sandbox" -ContentType 'application/json' -Body $body -Headers @{
            'X-Payment-Key-ID'=[string]$event.Headers.key_id; 'X-Payment-Timestamp'=[string]$event.Headers.timestamp; 'X-Payment-Signature'=[string]$event.Headers.signature
        }
        if ([int]$response.StatusCode -notin @(200,202)) { throw 'failover payment webhook was not durably acknowledged' }
    }
    for ($attempt=1; $attempt -le 120; $attempt++) {
        $state = Get-M7Scalar -Service $DatabaseService -User 'railway_control' -Database 'railway_control' -SQL "SELECT state FROM public.payment_intents WHERE reservation_id='$ReservationID'::uuid ORDER BY created_at DESC LIMIT 1"
        if ($state -eq 'completed') { return }
        Start-Sleep -Milliseconds 500
    }
    throw 'failover payment did not converge to completed'
}

function Get-M7HostedPayment {
    param([string]$DatabaseService, [string]$ReservationID)
    for ($attempt=1; $attempt -le 120; $attempt++) {
        $value = Get-M7Scalar -Service $DatabaseService -User 'railway_control' -Database 'railway_control' -SQL "SELECT payment_intent_id::text||'|'||coalesce(hosted_session_ref,'') FROM public.payment_intents WHERE reservation_id='$ReservationID'::uuid ORDER BY created_at DESC LIMIT 1"
        $parts = $value -split '\|',2
        if ($parts.Count -eq 2 -and $parts[0] -match '^[0-9a-f-]{36}$' -and $parts[1] -match '^sandbox-checkout:([A-Za-z0-9._:-]+)$') {
            return [pscustomobject]@{ IntentID=$parts[0]; ProviderPaymentID=$Matches[1] }
        }
        Start-Sleep -Milliseconds 500
    }
    throw 'payment crash fixture did not expose its bounded hosted identity'
}

function Send-M7SandboxWebhooks {
    param([string]$WebhookBaseURL)
    $drained = Invoke-M7Compose -Arguments @('exec','-T','payment-sandbox','wget','-q','-O','-','--header=X-Sandbox-Control-Token: synthetic-disposable-fault-token','http://127.0.0.1:8099/_sandbox/webhooks')
    $events = @(($drained.Output -join "`n") | ConvertFrom-Json)
    if ($events.Count -lt 1 -or $events.Count -gt 100) { throw 'sandbox webhook drain was outside the bounded fixture size' }
    foreach ($event in $events) {
        $body = [Convert]::FromBase64String([string]$event.Body)
        $response = Invoke-WebRequest -UseBasicParsing -Method Post -Uri "$WebhookBaseURL/webhooks/payments/sandbox" -ContentType 'application/json' -Body $body -Headers @{
            'X-Payment-Key-ID'=[string]$event.Headers.key_id; 'X-Payment-Timestamp'=[string]$event.Headers.timestamp; 'X-Payment-Signature'=[string]$event.Headers.signature
        }
        if ([int]$response.StatusCode -notin @(200,202)) { throw 'sandbox webhook was not durably acknowledged' }
    }
    return $events.Count
}

function Invoke-M7CrashWorker {
    param([string]$Service, [string]$Point, [string]$TargetID, [string]$Scenario)
    $allowed = @(
        'capture_provider_committed','ticket_issue_shard_committed',
        'refund_provider_committed','refund_compensation_shard_committed',
        'partial_refund_provider_committed','partial_refund_shard_committed'
    )
    if ($Point -notin $allowed -or $TargetID -notmatch '^[0-9a-f-]{36}$' -or $Scenario -notmatch '^[a-z0-9][a-z0-9-]{0,60}$') { throw 'application crash barrier request is invalid' }
    $containerName = "$ProjectName-crash-$Scenario"
    if ($containerName.Length -gt 120) { throw 'application crash container name is unbounded' }
    $run = Invoke-M7Compose -Arguments @(
        '--profile','dr-app','run','-d','--no-deps','--name',$containerName,
        '-e','APP_ENV=test','-e','PAYMENT_WORKER_TEST_CRASH_AFTER_EFFECT_ENABLED=true',
        '-e',"PAYMENT_WORKER_TEST_CRASH_AFTER_EFFECT_POINT=$Point",'-e',"PAYMENT_WORKER_TEST_CRASH_TARGET_ID=$TargetID",$Service
    )
    $containerID = [string](@($run.Output | Where-Object { $_ -match '^[0-9a-f]{12,64}$' }) | Select-Object -Last 1)
    if ([string]::IsNullOrWhiteSpace($containerID)) { throw "application crash worker did not return a container identity for $Scenario" }
    $projectLabel = [string](& docker inspect --format '{{index .Config.Labels "com.docker.compose.project"}}' $containerID 2>$null)
    if ($LASTEXITCODE -ne 0 -or $projectLabel.Trim() -cne $ProjectName) { throw 'application crash worker was not owned by the exact Compose project' }
    $exitCode = $null
    try {
        for ($attempt=1; $attempt -le 300; $attempt++) {
            $state = [string](& docker inspect --format '{{.State.Running}}|{{.State.ExitCode}}' $containerID 2>$null)
            if ($LASTEXITCODE -ne 0) { throw "application crash worker disappeared for $Scenario" }
            $parts = $state.Trim() -split '\|'
            if ($parts.Count -eq 2 -and $parts[0] -eq 'false') { $exitCode=[int]$parts[1]; break }
            Start-Sleep -Milliseconds 500
        }
        $log = [string](@(& docker logs $containerID 2>&1 | Select-Object -Last 100) -join "`n")
        [System.IO.File]::WriteAllText((Join-Path $EvidenceDirectory "$Scenario-crash.log"),(Protect-M7Diagnostic -Lines @($log)),[System.Text.UTF8Encoding]::new($false))
    } finally {
        & docker rm -f $containerID 2>&1 | Out-Null
    }
    if ($null -eq $exitCode) { throw "application crash worker exceeded its 150 second bound for $Scenario" }
    if ($exitCode -ne 86) { throw "application crash worker exited $exitCode instead of the exact test barrier for $Scenario" }
    $applicationCrashEvidence.Add([ordered]@{ scenario=$Scenario; point=$Point; target_id_sha256=(Get-M7SHA256 -Text $TargetID); process_exit=86; external_effect_committed=$true; control_finalize_not_run=$true; resumed=$false })
}

function Invoke-M7TicketConflictWorker {
    param([string]$Service, [string]$TargetID, [string]$Scenario)
    if ($TargetID -notmatch '^[0-9a-f-]{36}$' -or $Scenario -notmatch '^[a-z0-9][a-z0-9-]{0,60}$') { throw 'ticket-conflict worker request is invalid' }
    $containerName = "$ProjectName-conflict-$Scenario"
    $run = Invoke-M7Compose -Arguments @(
        '--profile','dr-app','run','-d','--no-deps','--name',$containerName,
        '-e','APP_ENV=test','-e','PAYMENT_WORKER_TEST_TICKET_ISSUE_CONFLICT_ENABLED=true',
        '-e',"PAYMENT_WORKER_TEST_TICKET_ISSUE_CONFLICT_TARGET_ID=$TargetID",
        '-e','PAYMENT_WORKER_INTERVAL_MILLISECONDS=60000',$Service
    )
    $containerID = [string](@($run.Output | Where-Object { $_ -match '^[0-9a-f]{12,64}$' }) | Select-Object -Last 1)
    if ([string]::IsNullOrWhiteSpace($containerID)) { throw "ticket-conflict worker did not return a container identity for $Scenario" }
    $projectLabel = [string](& docker inspect --format '{{index .Config.Labels "com.docker.compose.project"}}' $containerID 2>$null)
    if ($LASTEXITCODE -ne 0 -or $projectLabel.Trim() -cne $ProjectName) { throw 'ticket-conflict worker was not owned by the exact Compose project' }
    try {
        $observed = $false
        for ($attempt=1; $attempt -le 240; $attempt++) {
            $state = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT intent.state||'|'||saga.state||'|'||saga.current_step
FROM public.payment_intents AS intent JOIN public.payment_sagas AS saga USING(payment_intent_id)
WHERE intent.payment_intent_id='$TargetID'::uuid
"@
            if ($state -eq 'refund_pending|compensating|refund') { $observed=$true; break }
            $running = [string](& docker inspect --format '{{.State.Running}}' $containerID 2>$null)
            if ($LASTEXITCODE -ne 0 -or $running.Trim() -ne 'true') { throw "ticket-conflict worker exited before the compensation transition for $Scenario" }
            Start-Sleep -Milliseconds 500
        }
        if (-not $observed) { throw "ticket-conflict worker exceeded its 120 second transition bound for $Scenario" }
    } finally {
        $log = [string](@(& docker logs $containerID 2>&1 | Select-Object -Last 100) -join "`n")
        [System.IO.File]::WriteAllText((Join-Path $EvidenceDirectory "$Scenario-conflict.log"),(Protect-M7Diagnostic -Lines @($log)),[System.Text.UTF8Encoding]::new($false))
        & docker rm -f $containerID 2>&1 | Out-Null
    }
}

function Set-M7CrashLeaseBarrier {
    param(
        [ValidateSet('payment-operation','payment-action','partial-refund-operation','partial-refund-saga')][string]$Kind,
        [string]$Service, [string]$TargetID, [string]$ExpectedState,
        [ValidateSet('region-a','region-b')][string]$Region='region-a', [int64]$Epoch=1,
        [switch]$Release
    )
    if ($TargetID -notmatch '^[0-9a-f-]{36}$' -or [string]::IsNullOrWhiteSpace($ExpectedState) -or $Epoch -lt 1) { throw 'crash lease barrier request is invalid' }
    $until = if ($Release) { "clock_timestamp()-interval '1 second'" } else { "clock_timestamp()+interval '30 minutes'" }
    $context = "SELECT set_config('railway.deployment_region','$Region',true),set_config('railway.deployment_role','active',true),set_config('railway.region_epoch','$Epoch',true),set_config('railway.regional_writes_enabled','true',true)"
    $sql = switch ($Kind) {
        'payment-operation' { "WITH authority_context AS MATERIALIZED ($context) UPDATE public.payment_operations SET lease_until=$until FROM authority_context WHERE payment_intent_id='$TargetID'::uuid AND state='$ExpectedState' AND lease_owner IS NOT NULL RETURNING '1'" }
        'payment-action' { "WITH authority_context AS MATERIALIZED ($context) UPDATE public.payment_saga_actions AS action SET lease_until=$until FROM public.payment_sagas AS saga,authority_context WHERE saga.saga_id=action.saga_id AND saga.payment_intent_id='$TargetID'::uuid AND action.state='$ExpectedState' AND action.lease_owner IS NOT NULL RETURNING '1'" }
        'partial-refund-operation' { "WITH authority_context AS MATERIALIZED ($context) UPDATE public.ticket_refund_operations SET lease_until=$until FROM authority_context WHERE refund_operation_id='$TargetID'::uuid AND state='$ExpectedState' AND lease_owner IS NOT NULL RETURNING '1'" }
        'partial-refund-saga' { "WITH authority_context AS MATERIALIZED ($context) UPDATE public.ticket_refund_sagas SET lease_until=$until FROM authority_context WHERE refund_request_id='$TargetID'::uuid AND state='$ExpectedState' AND lease_owner IS NOT NULL RETURNING '1'" }
    }
    $updated = Get-M7Scalar -Service $Service -User 'railway_control' -Database 'railway_control' -SQL $sql
    if ($updated -ne '1') { throw "crash lease barrier did not update exactly one $Kind row" }
}

function Get-M7SandboxEffectEvidence {
    param([string]$IntentID, [string]$ExpectedStatus, [int64]$CapturedMinor, [int64]$RefundedMinor)
    $raw = Invoke-M7Compose -Arguments @('exec','-T','payment-sandbox','sh','-lc','test -s /var/lib/payment-sandbox/provider-state.jsonl && cat /var/lib/payment-sandbox/provider-state.jsonl')
    $text = [string]($raw.Output -join "`n")
    if ([string]::IsNullOrWhiteSpace($text) -or $text.Length -gt 16777216) { throw 'sandbox durable state was empty or unbounded' }
    $state = $text | ConvertFrom-Json
    $payments = @($state.payments | Where-Object { [string]$_.intent_id -ceq $IntentID })
    if ($payments.Count -ne 1) { throw 'sandbox did not retain exactly one payment for the interrupted intent' }
    $payment = $payments[0]
    if ([string]$payment.status -cne $ExpectedStatus -or [int64]$payment.captured_minor -ne $CapturedMinor -or [int64]$payment.refunded_minor -ne $RefundedMinor) {
        throw 'sandbox durable financial effect did not match the interrupted intent'
    }
    $operations = @($state.idempotency | Where-Object {
        $null -ne $_.operation -and [string]$_.operation.provider_payment_id -ceq [string]$payment.id -and [string]$_.operation.status -ceq $ExpectedStatus
    })
    if ($operations.Count -ne 1) { throw 'sandbox idempotency state did not contain exactly one terminal provider effect' }
    return [ordered]@{
        provider_payment_id_sha256=(Get-M7SHA256 -Text ([string]$payment.id)); terminal_effect_count=1;
        status=$ExpectedStatus; captured_minor=$CapturedMinor; refunded_minor=$RefundedMinor; truncated=$false
    }
}

function Assert-M7StaleWriterRejected {
    param([string]$Service, [string]$User, [string]$Database, [string]$Region, [int64]$Epoch)
    $sql = "SELECT set_config('railway.deployment_region','$Region',false),set_config('railway.deployment_role','active',false),set_config('railway.region_epoch','$Epoch',false),set_config('railway.regional_writes_enabled','true',false); UPDATE public.regional_write_authority SET updated_at=clock_timestamp() WHERE singleton;"
    $probe = Invoke-M7Compose -AllowFailure -Arguments @('exec','-T',$Service,'psql','-X','-v','ON_ERROR_STOP=1','-U',$User,'-d',$Database,'-c',$sql)
    if ($probe.ExitCode -eq 0) { throw 'stale regional writer was not rejected by the database authority guard' }
}

function Invoke-M7DRAdmin {
    param([string[]]$Arguments)
    $volume = "$EvidenceDirectory`:/evidence:ro"
    $result = Invoke-M7Compose -Arguments (@('--profile','dr-tools','run','--rm','--no-deps','-v',$volume,'dr-admin') + $Arguments)
    $line = [string](@($result.Output | Where-Object { $_.TrimStart().StartsWith('{') }) | Select-Object -Last 1)
    if ([string]::IsNullOrWhiteSpace($line)) { throw 'dr-admin omitted strict JSON output' }
    $value = $line | ConvertFrom-Json
    if ([string]$value.status -notin @('completed','dry-run')) { throw 'dr-admin did not complete the requested phase' }
    return $value
}

function Invoke-M7SettlementAdmin {
    param([string]$Artifact, [string[]]$Arguments)
    if ($Artifact -notmatch '^[a-z0-9][a-z0-9-]{0,80}$') { throw 'settlement admin artifact name is invalid' }
    $result = Invoke-M7Compose -Arguments (@('--profile','dr-tools','run','--rm','--no-deps','settlement-admin') + $Arguments)
    $line = [string](@($result.Output | Where-Object { $_.TrimStart().StartsWith('{') }) | Select-Object -Last 1)
    if ([string]::IsNullOrWhiteSpace($line)) { throw 'settlement-admin omitted strict JSON output' }
    $value = $line | ConvertFrom-Json
    if ([string]$value.status -ne 'completed' -or [bool]$value.financial_mutation) { throw 'settlement-admin did not complete without financial mutation' }
    Write-M7JSON -Name "$Artifact.json" -Value $value
    return $value
}

function Invoke-M7SettlementMismatchEvidence {
	$periodFrom = [DateTime]::UtcNow.Date.AddDays(-1).ToString('yyyy-MM-dd')
	$periodTo = [DateTime]::UtcNow.Date.AddDays(2).ToString('yyyy-MM-dd')
	$operation = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT intent.provider_payment_id||'|'||operation.amount_minor::text
FROM public.payment_operations AS operation
JOIN public.payment_intents AS intent USING(payment_intent_id)
WHERE operation.provider='sandbox' AND operation.operation_type='capture' AND operation.state='succeeded'
  AND intent.provider_payment_id IS NOT NULL
ORDER BY operation.completed_at,operation.operation_id LIMIT 1
"@
    $parts = $operation -split '\|'
    if ($parts.Count -ne 2 -or $parts[0] -notmatch '^[A-Za-z0-9._:-]{1,200}$') { throw 'settlement mismatch operation fixture was malformed' }
    $gross = [int64]$parts[1] + 111
    $net = $gross - 7
    Invoke-M7SQL -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL @"
BEGIN;
SELECT set_config('railway.deployment_region','region-a',true),set_config('railway.deployment_role','active',true),set_config('railway.region_epoch','1',true),set_config('railway.regional_writes_enabled','true',true);
INSERT INTO public.provider_settlement_lines(
 provider,provider_account_id,provider_record_id,payment_correlation,operation_type,gross_minor,fee_minor,net_minor,currency,
 available_at,provider_created_at,provider_settlement_id,provider_payout_id,payout_status,payload_hash,imported_at
) VALUES ('sandbox','acct_m7_detector','line_m7_amount_currency_fee','$($parts[0])','capture',$gross,7,$net,'USD',clock_timestamp(),clock_timestamp(),'batch_m7_mismatch',NULL,NULL,decode(repeat('61',32),'hex'),clock_timestamp());
INSERT INTO public.provider_settlement_lines(
 provider,provider_account_id,provider_record_id,payment_correlation,operation_type,gross_minor,fee_minor,net_minor,currency,
 available_at,provider_created_at,provider_settlement_id,provider_payout_id,payout_status,payload_hash,imported_at
) VALUES ('sandbox','acct_m7_detector','line_m7_provider_only','pi_m7_unmapped','capture',100,0,100,'TWD',clock_timestamp(),clock_timestamp(),'batch_m7_mismatch',NULL,NULL,decode(repeat('63',32),'hex'),clock_timestamp());
INSERT INTO public.provider_payouts(
 provider,provider_account_id,provider_record_id,payment_correlation,operation_type,gross_minor,fee_minor,net_minor,currency,
 available_at,provider_created_at,provider_settlement_id,provider_payout_id,payout_status,payload_hash,imported_at
) VALUES ('sandbox','acct_m7_detector','po_m7_runtime',NULL,'payout',670,0,670,'TWD',clock_timestamp(),clock_timestamp(),NULL,'po_m7_runtime','paid',decode(repeat('64',32),'hex'),clock_timestamp());
INSERT INTO public.provider_payout_lines(
 provider,provider_account_id,provider_record_id,payment_correlation,operation_type,gross_minor,fee_minor,net_minor,currency,
 available_at,provider_created_at,provider_settlement_id,provider_payout_id,payout_status,payload_hash,imported_at
) VALUES ('sandbox','acct_m7_detector','line_m7_payout_mismatch',NULL,'payout',-123,0,-123,'USD',clock_timestamp(),clock_timestamp(),NULL,'po_m7_runtime','paid',decode(repeat('62',32),'hex'),clock_timestamp());
COMMIT;
"@
    $env:SETTLEMENT_ADMIN_DATABASE_URL = 'postgresql://railway_control:control-local-only@control-postgres:5432/railway_control?sslmode=disable&connect_timeout=3'
    $env:SETTLEMENT_ADMIN_REGION = 'region-a'
    $env:SETTLEMENT_ADMIN_EPOCH = '1'
    $report = Invoke-M7SettlementAdmin -Artifact 'settlement-mismatch-detection' -Arguments @(
		'reconcile-period','--from',$periodFrom,'--to',$periodTo,'--page-size','2','--max-pages','20','--timeout','2m'
    )
    if (-not [bool]$report.read_only -or -not [bool]$report.append_only -or -not [bool]$report.completed -or [int]$report.finding_count -lt 6) {
        throw 'settlement detector did not emit bounded read-only mismatch evidence'
    }
	$runID = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL "SELECT run_id::text FROM public.settlement_reconciliation_runs WHERE scope_type='period' AND scope_value='$periodFrom/$periodTo' ORDER BY created_at DESC LIMIT 1"
    if ($runID -notmatch '^[0-9a-f-]{36}$') { throw 'settlement detector run identity was malformed' }
    $matrix = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT count(*) FILTER (WHERE reason='missing' AND evidence_kind='provider')::text||'|'||
       count(*) FILTER (WHERE reason='missing' AND evidence_kind='payment_operation')::text||'|'||
       count(*) FILTER (WHERE reason='amount')::text||'|'||count(*) FILTER (WHERE reason='currency')::text||'|'||
       count(*) FILTER (WHERE reason='fee')::text||'|'||count(*) FILTER (WHERE evidence_kind='payout' AND reason IN ('amount','currency','missing'))::text
FROM public.settlement_reconciliation_mismatches WHERE run_id='$runID'::uuid
"@
    $matrixParts = $matrix -split '\|'
    if ($matrixParts.Count -ne 6 -or @($matrixParts | Where-Object { [int]$_ -lt 1 }).Count -ne 0) { throw "settlement mismatch matrix was incomplete: $matrix" }
    $evidenceHash = (Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $EvidenceDirectory 'settlement-mismatch-detection.json')).Hash.ToLowerInvariant()
    Invoke-M7SettlementAdmin -Artifact 'settlement-manual-review' -Arguments @(
        'mark-reviewed','--run',$runID,'--reviewer','operator:local-dr','--disposition','investigating','--evidence-hash',$evidenceHash,'--timeout','2m'
    ) | Out-Null
    $immutableMismatch = Invoke-M7Compose -AllowFailure -Arguments @('exec','-T','control-postgres','psql','-X','-v','ON_ERROR_STOP=1','-U','railway_control','-d','railway_control','-c',"UPDATE public.settlement_reconciliation_mismatches SET reason='unexpected' WHERE run_id='$runID'::uuid")
    $immutableReview = Invoke-M7Compose -AllowFailure -Arguments @('exec','-T','control-postgres','psql','-X','-v','ON_ERROR_STOP=1','-U','railway_control','-d','railway_control','-c',"UPDATE public.settlement_reconciliation_reviews SET disposition='acknowledged' WHERE run_id='$runID'::uuid")
    $reviewCount = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL "SELECT count(*) FROM public.settlement_reconciliation_reviews WHERE run_id='$runID'::uuid AND disposition='investigating'"
    if ($immutableMismatch.ExitCode -eq 0 -or $immutableReview.ExitCode -eq 0 -or $reviewCount -ne '1') { throw 'settlement mismatch/review evidence was not immutable and append-only' }
    return [ordered]@{
        run_id_sha256=(Get-M7SHA256 -Text $runID); read_only=$true; financial_mutation=$false; completed=$true; bounded=[bool]$report.bounded;
        finding_count=[int]$report.finding_count; missing_provider=[int]$matrixParts[0]; missing_local=[int]$matrixParts[1];
        amount=[int]$matrixParts[2]; currency=[int]$matrixParts[3]; fee=[int]$matrixParts[4]; payout=[int]$matrixParts[5];
        mismatch_immutable=$true; manual_review_append_only=$true; manual_review_count=1
    }
}

function Refresh-M7DRFence {
    param([string]$OperationID, [string]$Prefix, [string]$Boundary)
    $isFailback = $OperationID -eq $failbackOperationID
    $refresh = New-M7SignedFenceEvidence -OperationID $OperationID -SourceRegion $(if($isFailback){'region-b'}else{'region-a'}) `
        -SourceEpoch $(if($isFailback){2}else{1}) -IncidentID $(if($isFailback){$failbackIncidentID}else{$failoverIncidentID}) `
        -OperatorID 'operator:local-dr' -Purpose 'ongoing_source_fence' -ObservationText "ongoing-source-fence|$Prefix|$Boundary"
    $refresh['stage'] = 'fence_refreshed'
    $refreshName = "$Prefix-fence-refresh-$Boundary.json"
    Write-M7JSON -Name $refreshName -Value ([ordered]@{} + $refresh)
    $null = Invoke-M7DRAdmin -Arguments @('refresh-fence','--operation-id',$OperationID,'--evidence-file',"/evidence/$refreshName",'--confirm','--timeout','2m')
}

function Assert-M7DRFenceFresh {
    param([string]$OperationID, [string]$JournalRegion, [int64]$RecoveryEpoch)
    $savedRegion, $savedEpoch, $savedURL = $env:DR_JOURNAL_REGION, $env:DR_RECOVERY_EPOCH, $env:DR_JOURNAL_DATABASE_URL
    try {
        $env:DR_JOURNAL_REGION = $JournalRegion
        $env:DR_RECOVERY_EPOCH = $RecoveryEpoch.ToString([Globalization.CultureInfo]::InvariantCulture)
        $env:DR_JOURNAL_DATABASE_URL = if ($JournalRegion -eq 'region-b') {
            'postgresql://railway_control:control-local-only@control-postgres-region-b:5432/railway_control?sslmode=disable&connect_timeout=3'
        } else {
            'postgresql://railway_control:control-local-only@control-postgres-region-a-reseed:5432/railway_control?sslmode=disable&connect_timeout=3'
        }
        $null = Invoke-M7DRAdmin -Arguments @('verify-fence','--operation-id',$OperationID,'--timeout','2m')
    } finally {
        $env:DR_JOURNAL_REGION, $env:DR_RECOVERY_EPOCH, $env:DR_JOURNAL_DATABASE_URL = $savedRegion, $savedEpoch, $savedURL
    }
}

function Advance-M7DRPhase {
    param([string]$Stage, [hashtable]$Evidence, [string]$OperationID=$failoverOperationID, [string]$Prefix='dr-phase', [switch]$CrashOnce)
    if ($Stage -notin @('external_fencing_verified','source_retained_fenced')) {
        Refresh-M7DRFence -OperationID $OperationID -Prefix $Prefix -Boundary $Stage
    }
    $Evidence['stage'] = $Stage
    $name = "$Prefix-$Stage.json"
    Write-M7JSON -Name $name -Value ([ordered]@{} + $Evidence)
    $arguments = @('advance-phase','--operation-id',$OperationID,'--evidence-file',"/evidence/$name",'--confirm','--timeout','2m')
    if ($CrashOnce) {
        $journalService = if ($env:DR_JOURNAL_REGION -eq 'region-a') { 'control-postgres-region-a-reseed' } elseif ($env:DR_JOURNAL_REGION -eq 'region-b') { 'control-postgres-region-b' } else { throw 'journal crash probe region is invalid' }
        $before = Get-M7Scalar -Service $journalService -User 'railway_control' -Database 'railway_control' -SQL "SELECT stage||'|'||checkpoint_version::text FROM public.regional_failover_operations WHERE operation_id='$OperationID'::uuid"
        $volume = "$EvidenceDirectory`:/evidence:ro"
        $crash = Invoke-M7Compose -AllowFailure -Arguments (@(
            '--profile','dr-tools','run','--rm','--no-deps','-v',$volume,
            '-e','APP_ENV=test','-e','DR_ADMIN_TEST_CRASH_AFTER_LOAD_ENABLED=true',
            '-e',"DR_ADMIN_TEST_CRASH_AFTER_LOAD_STAGE=$Stage",'dr-admin'
        ) + $arguments)
        if ($crash.ExitCode -eq 0) { throw "dr-admin crash-after-load hook did not terminate at $Stage" }
        $after = Get-M7Scalar -Service $journalService -User 'railway_control' -Database 'railway_control' -SQL "SELECT stage||'|'||checkpoint_version::text FROM public.regional_failover_operations WHERE operation_id='$OperationID'::uuid"
        if ($after -cne $before) { throw "dr-admin checkpoint advanced across process crash at $Stage" }
        $journalCrashEvidence.Add([ordered]@{ operation_kind=$(if($OperationID -eq $failoverOperationID){'failover'}else{'failback'}); stage=$Stage; before=$before; after=$after; process_exit_nonzero=$true; resumed=$true })
    }
    $result = Invoke-M7DRAdmin -Arguments $arguments
    if ([string]$result.result.stage -ne $Stage) { throw "dr-admin checkpoint did not reach $Stage" }
}

function Format-M7CanonicalTimestamp {
    param([DateTimeOffset]$Value)
    $formatted = $Value.ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'", [Globalization.CultureInfo]::InvariantCulture)
    $formatted = $formatted -replace '0+Z$','Z'
    return ($formatted -replace '\.Z$','Z')
}

function New-M7SignedFenceEvidence {
    param(
        [string]$OperationID, [string]$SourceRegion, [uint64]$SourceEpoch,
        [string]$IncidentID, [string]$OperatorID,
        [ValidateSet('initial_fence','ongoing_source_fence','retained_source_fence','failback_validation')][string]$Purpose,
        [string]$ObservationText
    )
    $observedAt = [DateTimeOffset]::UtcNow
    $expiresAt = $observedAt.AddMinutes(5)
    $nonce = [guid]::NewGuid().ToString('N')
    $hashes = [ordered]@{
        ingress_sha256=(Get-M7SHA256 "$ObservationText|ingress")
        processes_sha256=(Get-M7SHA256 "$ObservationText|processes")
        credentials_sha256=(Get-M7SHA256 "$ObservationText|credential-consumers")
        database_network_sha256=(Get-M7SHA256 "$ObservationText|database-network")
    }
    $observedText = Format-M7CanonicalTimestamp $observedAt
    $expiresText = Format-M7CanonicalTimestamp $expiresAt
    $payload = @(
        'railway-fence-v1', $Purpose, $OperationID.ToLowerInvariant(), $SourceRegion, $SourceEpoch.ToString([Globalization.CultureInfo]::InvariantCulture),
        $IncidentID.ToLowerInvariant(), $OperatorID, $observedText, $expiresText,
        $env:DR_FENCE_ATTESTATION_ISSUER, $env:DR_FENCE_ATTESTATION_KEY_ID, $nonce,
        $hashes.ingress_sha256, $hashes.processes_sha256, $hashes.credentials_sha256, $hashes.database_network_sha256
    ) -join "`n"
    $payload += "`n"
    $payloadPath = Join-Path $secretDirectory "fence-payload-$nonce"
    $signaturePath = Join-Path $secretDirectory "fence-signature-$nonce"
    [System.IO.File]::WriteAllText($payloadPath, $payload, [System.Text.UTF8Encoding]::new($false))
    & $openssl.Source pkeyutl -sign -rawin -inkey $fencingAttestationPrivateKeyPath -in $payloadPath -out $signaturePath 2>$null
    if ($LASTEXITCODE -ne 0) { throw 'failed to sign fencing attestation' }
    $signature = [Convert]::ToBase64String([System.IO.File]::ReadAllBytes($signaturePath))
    Remove-Item -LiteralPath $payloadPath,$signaturePath -Force
    return @{
        observed_at=$observedText; expires_at=$expiresText; issuer=$env:DR_FENCE_ATTESTATION_ISSUER;
        key_id=$env:DR_FENCE_ATTESTATION_KEY_ID; nonce=$nonce; purpose=$Purpose; signature_b64=$signature;
        ingress_sha256=$hashes.ingress_sha256; processes_sha256=$hashes.processes_sha256;
        credentials_sha256=$hashes.credentials_sha256; database_network_sha256=$hashes.database_network_sha256
    }
}

function Ensure-M7PromotedPrimary {
    param(
        [string]$Service, [string]$User, [string]$Database,
        [string]$OperationKind, [string]$DatabaseName, [switch]$CrashReobserve
    )
    Assert-M7DRFenceFresh -OperationID $(if($OperationKind -eq 'failover'){$failoverOperationID}else{$failbackOperationID}) `
        -JournalRegion $(if($OperationKind -eq 'failover'){'region-b'}else{'region-a'}) -RecoveryEpoch $(if($OperationKind -eq 'failover'){2}else{3})
    $before = Get-M7Scalar -Service $Service -User $User -Database $Database -SQL 'SELECT pg_is_in_recovery()::text'
    if ($before -eq 'true') {
        Invoke-M7Compose -Arguments @('exec','-T',$Service,'gosu','postgres','pg_ctl','-D','/var/lib/postgresql/data','promote','-w','-t','60') | Out-Null
    } elseif ($before -ne 'false') {
        throw "promotion pre-observation was invalid for $Service"
    }
    Wait-M7Role -Service $Service -User $User -Database $Database -Recovery $false
    $after = Get-M7Scalar -Service $Service -User $User -Database $Database -SQL 'SELECT pg_is_in_recovery()::text'
    if ($after -ne 'false') { throw "promotion was not durably observable for $Service" }
    $reobserved = $false
    if ($CrashReobserve) {
        # Exercise the idempotent re-observation decision from durable server
        # state. Actual phase-process termination is injected in dr-admin.
        $resumeObservation = Get-M7Scalar -Service $Service -User $User -Database $Database -SQL 'SELECT pg_is_in_recovery()::text'
        if ($resumeObservation -ne 'false') { throw "promotion resume observation failed for $Service" }
        $reobserved = $true
    }
    $artifact = [ordered]@{
        operation_kind=$OperationKind; database=$DatabaseName; before=$before; after=$after;
        controller_reobservation_probe=[bool]$CrashReobserve; resumed_by_reobservation=$reobserved
    }
    Write-M7JSON -Name "$OperationKind-promotion-$DatabaseName-reobservation.json" -Value $artifact
    return $artifact
}

function Ensure-M7ServicesStopped {
    param(
        [string[]]$Services, [string]$OperationKind, [string]$Boundary,
        [switch]$CrashReobserve
    )
    if ($Services.Count -lt 1 -or $OperationKind -notin @('failover','failback') -or $Boundary -notmatch '^[a-z][a-z0-9-]{0,63}$') {
        throw 'service-fence reobservation context is invalid'
    }
    $runningBefore = [System.Collections.Generic.List[string]]::new()
    foreach ($service in $Services) {
        $running = Invoke-M7Compose -AllowFailure -Arguments @('ps','--status','running','-q',$service)
        if (@($running.Output | Where-Object { $_.Trim() -ne '' }).Count -gt 0) { $runningBefore.Add($service) }
    }
    if ($runningBefore.Count -gt 0) {
        Invoke-M7Compose -Arguments (@('stop','-t','20') + @($runningBefore)) | Out-Null
    }
    foreach ($service in $Services) {
        $running = Invoke-M7Compose -AllowFailure -Arguments @('ps','--status','running','-q',$service)
        if (@($running.Output | Where-Object { $_.Trim() -ne '' }).Count -ne 0) { throw "service fence did not stop $service" }
    }
    $reobserved = $false
    if ($CrashReobserve) {
        # Model loss of the driver after the stop side effect. Resume derives
        # its decision from Compose state and never restarts the fenced source.
        foreach ($service in $Services) {
            $running = Invoke-M7Compose -AllowFailure -Arguments @('ps','--status','running','-q',$service)
            if (@($running.Output | Where-Object { $_.Trim() -ne '' }).Count -ne 0) { throw "service fence resume observation failed for $service" }
        }
        $reobserved = $true
    }
    $artifact = [ordered]@{
        operation_kind=$OperationKind; boundary=$Boundary; service_count=$Services.Count;
        running_before=[string[]]$runningBefore; stopped=$true;
        controller_reobservation_probe=[bool]$CrashReobserve; resumed_by_reobservation=$reobserved
    }
    Write-M7JSON -Name "$OperationKind-service-fence-$Boundary-reobservation.json" -Value $artifact
    return $artifact
}

function Get-M7RunningServiceHash {
    param([string[]]$Services)
    $observations = [System.Collections.Generic.List[string]]::new()
    foreach ($service in $Services) {
        $running = Invoke-M7Compose -Arguments @('ps','--status','running','-q',$service)
        $ids = @($running.Output | ForEach-Object { $_.Trim() } | Where-Object { $_ -match '^[0-9a-f]{12,64}$' })
        if ($ids.Count -ne 1) { throw "service observation was not exactly one running container for $service" }
        $observations.Add("$service|$($ids[0])")
    }
    return Get-M7SHA256 -Text (($observations | Sort-Object) -join "`n")
}

function Get-M7FinancialContinuity {
    param([string]$ControlService, [string]$ShardService)
    $control = Get-M7Scalar -Service $ControlService -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT (SELECT count(*) FROM public.payment_intents WHERE reservation_id='$($m7Customer.Reservations[3])'::uuid AND state='completed')::text||'|'||
       (SELECT count(*) FROM public.payment_intents WHERE reservation_id='$($m7Customer.Reservations[4])'::uuid AND state='completed')::text||'|'||
       (SELECT count(*) FROM public.payment_webhook_inbox WHERE provider_event_id IN ('evt_m7_previous_$suffix','evt_m7_current_$suffix'))::text||'|'||
       (SELECT count(*) FROM public.ticket_refund_requests WHERE ticket_order_id IN ('$($m7Orders[0].OrderID)'::uuid,'$($m7Orders[1].OrderID)'::uuid,'$($m7Orders[2].OrderID)'::uuid) AND state='completed')::text||'|'||
       (SELECT count(*) FROM public.provider_balance_transactions WHERE provider='stripe' AND provider_account_id='acct_m7_contract')::text||'|'||
       (SELECT count(*) FROM public.provider_payouts WHERE provider='stripe' AND provider_account_id='acct_m7_contract')::text||'|'||
       (SELECT count(*) FROM (SELECT transaction.transaction_id FROM public.financial_ledger_transactions AS transaction JOIN public.financial_ledger_postings AS posting USING(transaction_id) GROUP BY transaction.transaction_id HAVING sum(posting.amount_minor) FILTER (WHERE posting.side='debit') <> sum(posting.amount_minor) FILTER (WHERE posting.side='credit')) AS unbalanced)::text
"@
    $shard = Get-M7Scalar -Service $ShardService -User 'railway_booking' -Database 'railway_booking' -SQL @"
SELECT (SELECT count(*) FROM public.ticket_orders WHERE reservation_id='$($m7Customer.Reservations[3])'::uuid AND status='issued')::text||'|'||
       (SELECT count(*) FROM public.tickets AS ticket JOIN public.ticket_orders AS orders ON orders.id=ticket.ticket_order_id WHERE orders.reservation_id='$($m7Customer.Reservations[3])'::uuid AND ticket.status='active')::text||'|'||
       (SELECT count(*) FROM public.ticket_orders WHERE reservation_id='$($m7Customer.Reservations[4])'::uuid AND status='issued')::text||'|'||
       (SELECT count(*) FROM public.tickets AS ticket JOIN public.ticket_orders AS orders ON orders.id=ticket.ticket_order_id WHERE orders.reservation_id='$($m7Customer.Reservations[4])'::uuid AND ticket.status='active')::text||'|'||
       (SELECT count(*) FROM public.selected_ticket_refund_receipts WHERE refund_request_id IN (SELECT refund_request_id FROM public.ticket_refund_compensation_receipts WHERE ticket_order_id IN ('$($m7Orders[0].OrderID)'::uuid,'$($m7Orders[1].OrderID)'::uuid,'$($m7Orders[2].OrderID)'::uuid)))::text
"@
    if ($control -ne '1|1|2|3|3|1|0' -or $shard -ne '1|2|1|2|3') {
        throw "final financial continuity mismatch: control=$control shard=$shard"
    }
    return [ordered]@{
        failover_payment_completed=1; failback_payment_completed=1; durable_webhooks=2; completed_partial_refunds=3; settlement_balance_transactions=3;
        settlement_payouts=1; unbalanced_ledger_transactions=0; issued_failover_orders=1;
        active_failover_tickets=2; active_failback_tickets=2; selected_refund_receipts=3
    }
}

function Get-M7ReconciliationObservation {
    param([string]$ControlService, [string]$Shard0Service, [string]$Shard1Service)
    $reservationIDs = ($m7Customer.Reservations | ForEach-Object { "'$_'::uuid" }) -join ','
    $orderIDs = ($m7Orders | ForEach-Object { "'$($_.OrderID)'::uuid" }) -join ','
    $control = Get-M7Scalar -Service $ControlService -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT (SELECT count(*) FROM public.reservation_shard_locators WHERE reservation_id IN ($reservationIDs) AND shard_id='physical-shard-0')::text||'|'||
       (SELECT count(*) FROM public.payment_intents WHERE reservation_id IN ($reservationIDs))::text||'|'||
       (SELECT count(*) FROM public.ticket_refund_requests WHERE ticket_order_id IN ($orderIDs) AND state='completed')::text||'|'||
       (SELECT count(*) FROM public.payment_webhook_inbox WHERE provider_event_id IN ('evt_m7_previous_$suffix','evt_m7_current_$suffix'))::text||'|'||
       (SELECT count(*) FROM public.provider_balance_transactions WHERE provider='stripe' AND provider_account_id='acct_m7_contract')::text||'|'||
       (SELECT count(*) FROM public.provider_payouts WHERE provider='stripe' AND provider_account_id='acct_m7_contract')::text||'|'||
       (SELECT count(*) FROM (SELECT transaction.transaction_id FROM public.financial_ledger_transactions AS transaction JOIN public.financial_ledger_postings AS posting USING(transaction_id) GROUP BY transaction.transaction_id HAVING sum(posting.amount_minor) FILTER (WHERE posting.side='debit') <> sum(posting.amount_minor) FILTER (WHERE posting.side='credit')) AS unbalanced)::text
"@
    $shard0 = Get-M7Scalar -Service $Shard0Service -User 'railway_booking' -Database 'railway_booking' -SQL @"
SELECT (SELECT count(*) FROM public.ticket_orders WHERE id IN ($orderIDs) AND status IN ('issued','partially_refunded'))::text||'|'||
       (SELECT count(*) FROM public.tickets WHERE ticket_order_id IN ($orderIDs))::text||'|'||
       (SELECT count(*) FROM public.tickets WHERE ticket_order_id IN ($orderIDs) AND status='refunded')::text||'|'||
       (SELECT count(*) FROM public.selected_ticket_refund_receipts WHERE ticket_id IN (SELECT id FROM public.tickets WHERE ticket_order_id IN ($orderIDs)))::text
"@
    $shard1 = Get-M7Scalar -Service $Shard1Service -User 'railway_booking' -Database 'railway_booking' -SQL "SELECT count(*) FROM public.selected_ticket_refund_receipts WHERE id::text LIKE '00000000-%'"
    $controlParts = $control -split '\|'
    if ($controlParts.Count -ne 7 -or [int]$controlParts[0] -ne 8 -or [int]$controlParts[1] -ne 8 -or [int]$controlParts[2] -ne 2 -or [int]$controlParts[3] -ne 2 -or [int]$controlParts[4] -ne 3 -or [int]$controlParts[5] -ne 1 -or [int]$controlParts[6] -ne 0 -or $shard0 -ne '3|6|2|2' -or $shard1 -ne '0') {
        throw "authoritative reconciliation mismatch: control=$control shard0=$shard0 shard1=$shard1"
    }
    return [ordered]@{
        routed_reservations=8; payment_intents=[int]$controlParts[1]; completed_refunds=2; durable_webhooks=2;
        settlement_transactions=3; settlement_payouts=1; unbalanced_ledger_transactions=0;
        ticket_orders=3; tickets=6; refunded_tickets=2; selected_refund_receipts=2; shard_1_unexpected_receipts=0
    }
}

function Get-M7FailbackReconciliationObservation {
    param([string]$ControlService, [string]$Shard0Service, [string]$Shard1Service)
    $reservationIDs = ($m7Customer.Reservations | ForEach-Object { "'$_'::uuid" }) -join ','
    $orderIDs = ($m7Orders | ForEach-Object { "'$($_.OrderID)'::uuid" }) -join ','
    $control = Get-M7Scalar -Service $ControlService -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT (SELECT count(*) FROM public.reservation_shard_locators WHERE reservation_id IN ($reservationIDs) AND shard_id='physical-shard-0')::text||'|'||
       (SELECT count(*) FROM public.payment_intents WHERE reservation_id IN ($reservationIDs))::text||'|'||
       (SELECT count(*) FROM public.payment_intents WHERE reservation_id='$($m7Customer.Reservations[3])'::uuid AND state='completed')::text||'|'||
       (SELECT count(*) FROM public.ticket_refund_requests WHERE ticket_order_id IN ($orderIDs) AND state='completed')::text||'|'||
       (SELECT count(*) FROM public.payment_webhook_inbox WHERE provider_event_id IN ('evt_m7_previous_$suffix','evt_m7_current_$suffix'))::text||'|'||
       (SELECT count(*) FROM public.provider_balance_transactions WHERE provider='stripe' AND provider_account_id='acct_m7_contract')::text||'|'||
       (SELECT count(*) FROM public.provider_payouts WHERE provider='stripe' AND provider_account_id='acct_m7_contract')::text||'|'||
       (SELECT count(*) FROM (SELECT transaction.transaction_id FROM public.financial_ledger_transactions AS transaction JOIN public.financial_ledger_postings AS posting USING(transaction_id) GROUP BY transaction.transaction_id HAVING sum(posting.amount_minor) FILTER (WHERE posting.side='debit') <> sum(posting.amount_minor) FILTER (WHERE posting.side='credit')) AS unbalanced)::text
"@
    $shard0 = Get-M7Scalar -Service $Shard0Service -User 'railway_booking' -Database 'railway_booking' -SQL @"
SELECT (SELECT count(*) FROM public.ticket_orders WHERE reservation_id IN ($reservationIDs) AND status IN ('issued','partially_refunded'))::text||'|'||
       (SELECT count(*) FROM public.tickets AS ticket JOIN public.ticket_orders AS orders ON orders.id=ticket.ticket_order_id WHERE orders.reservation_id IN ($reservationIDs))::text||'|'||
       (SELECT count(*) FROM public.tickets WHERE ticket_order_id IN ($orderIDs) AND status='refunded')::text||'|'||
       (SELECT count(*) FROM public.selected_ticket_refund_receipts WHERE ticket_id IN (SELECT id FROM public.tickets WHERE ticket_order_id IN ($orderIDs)))::text
"@
    $shard1 = Get-M7Scalar -Service $Shard1Service -User 'railway_booking' -Database 'railway_booking' -SQL "SELECT count(*) FROM public.ticket_orders WHERE reservation_id IN ($reservationIDs)"
    $parts = $control -split '\|'
    if ($parts.Count -ne 8 -or [int]$parts[0] -ne 8 -or [int]$parts[1] -ne 9 -or [int]$parts[2] -ne 1 -or [int]$parts[3] -ne 3 -or [int]$parts[4] -ne 2 -or [int]$parts[5] -ne 3 -or [int]$parts[6] -ne 1 -or [int]$parts[7] -ne 0 -or $shard0 -ne '7|14|3|3' -or $shard1 -ne '0') {
        throw "authoritative failback reconciliation mismatch: control=$control shard0=$shard0 shard1=$shard1"
    }
    return [ordered]@{
        routed_reservations=8; payment_intents=[int]$parts[1]; recovered_payment_intents=1; completed_refunds=3;
        durable_rotation_webhooks=2; settlement_transactions=3; settlement_payouts=1; unbalanced_ledger_transactions=0;
        ticket_orders=7; tickets=14; refunded_tickets=3; selected_refund_receipts=3; shard_1_unexpected_orders=0
    }
}

function Add-M7Phase {
    param([string]$Name, [string]$Status='passed')
    $phaseEvidence.Add([ordered]@{ name=$Name; status=$Status; observed_at=[DateTimeOffset]::UtcNow.ToString('o') })
    Write-Output "m7-dr-phase:${Name}:$Status"
}

function Remove-M7ProjectVolume {
    param([string]$Suffix)
    $name = "${ProjectName}_$Suffix"
    $inspection = Invoke-M7Native -AllowFailure -Command { & docker volume inspect $name }
    if ($inspection.ExitCode -ne 0) {
        throw "project volume ownership is invalid for $Suffix"
    }
    $metadata = @(([string]($inspection.Output -join "`n") | ConvertFrom-Json))
    if ($metadata.Count -ne 1 -or [string]$metadata[0].Name -cne $name -or
        [string]$metadata[0].Labels.'com.docker.compose.project' -cne $ProjectName) {
        throw "project volume ownership is invalid for $Suffix"
    }
    Invoke-M7Native -Command { & docker volume rm $name } | Out-Null
}

function Assert-M7EvidenceSecretSafe {
    foreach ($file in @(Get-ChildItem -LiteralPath $EvidenceDirectory -File)) {
        $text = Get-Content -Raw -LiteralPath $file.FullName
        foreach ($secret in $sensitiveValues) {
            if (-not [string]::IsNullOrEmpty($secret) -and $text.Contains($secret)) { throw "secret value leaked into $($file.Name)" }
        }
        if ($text -match '(?i)postgres(?:ql)?://[^\s"''`]+@' -or $text -match '(?i)(password|cipher_pass|secret)\s*[:=]\s*[^\s,"}]{8,}') {
            throw "secret-shaped content leaked into $($file.Name)"
        }
    }
}

function Write-M7EvidenceIndex {
    $entries = [System.Collections.Generic.List[object]]::new()
    $canonical = [System.Collections.Generic.List[string]]::new()
    foreach ($file in @(Get-ChildItem -LiteralPath $EvidenceDirectory -File | Where-Object { $_.Name -ne 'evidence-index.json' } | Sort-Object Name)) {
        $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $file.FullName).Hash.ToLowerInvariant()
        $entries.Add([ordered]@{ path=$file.Name; bytes=[int64]$file.Length; sha256=$hash })
        $canonical.Add("$($file.Name)|$($file.Length)|$hash")
    }
    Write-M7JSON -Name 'evidence-index.json' -Value ([ordered]@{
        status='complete'; file_count=$entries.Count; bundle_sha256=(Get-M7SHA256 -Text ($canonical -join "`n")); files=$entries
    })
}

$databases = @(
    [pscustomobject]@{ Name='control'; Primary='control-postgres'; Standby='control-postgres-region-b'; Reseed='control-postgres-region-a-reseed'; User='railway_control'; ReplicationUser='railway_control_replicator'; Database='railway_control'; Stanza='railway-control'; Slot='control_region_b'; Volume='control-postgres-data'; StandbyVolume='control-postgres-region-b-data'; ExpectedVersion=11 },
    [pscustomobject]@{ Name='shard-0'; Primary='booking-shard-0-postgres'; Standby='booking-shard-0-postgres-region-b'; Reseed='booking-shard-0-postgres-region-a-reseed'; User='railway_booking'; ReplicationUser='railway_shard_0_replicator'; Database='railway_booking'; Stanza='railway-shard-0'; Slot='shard_0_region_b'; Volume='booking-shard-0-postgres-data'; StandbyVolume='booking-shard-0-postgres-region-b-data'; ExpectedVersion=3 },
    [pscustomobject]@{ Name='shard-1'; Primary='booking-shard-1-postgres'; Standby='booking-shard-1-postgres-region-b'; Reseed='booking-shard-1-postgres-region-a-reseed'; User='railway_booking'; ReplicationUser='railway_shard_1_replicator'; Database='railway_booking'; Stanza='railway-shard-1'; Slot='shard_1_region_b'; Volume='booking-shard-1-postgres-data'; StandbyVolume='booking-shard-1-postgres-region-b-data'; ExpectedVersion=3 }
)
$regionAAppServices = @(
    'api-1','api-2','api-3','payment-worker-1','payment-worker-2','payment-reconciler',
    'settlement-worker-region-a','admission-worker-1','admission-worker-2',
    'read-model-worker-1','read-model-worker-2','hold-expirer','outbox-worker',
    'booking-command-reconciler','redis','payment-sandbox','payment-stripe-contract',
    'proxy-region-a','global-test-ingress'
)
$regionBWriterServices = @(
    'api-region-b-1','api-region-b-2','api-region-b-3','payment-worker-region-b-1',
    'payment-worker-region-b-2','payment-reconciler-region-b','settlement-worker-region-b',
    'admission-worker-region-b-1','admission-worker-region-b-2','read-model-worker-region-b-1','read-model-worker-region-b-2',
    'hold-expirer-region-b','outbox-worker-region-b','booking-command-reconciler-region-b',
    'proxy-region-b','global-test-ingress'
)
$appBuildServices = @(
    'api-1','payment-worker-1','payment-reconciler','settlement-worker-region-a',
    'admission-worker-1','read-model-worker-1','hold-expirer','outbox-worker',
    'booking-command-reconciler','payment-sandbox','payment-stripe-contract',
    'api-region-b-1','payment-worker-region-b-1','payment-reconciler-region-b',
    'admission-worker-region-b-1','read-model-worker-region-b-1','hold-expirer-region-b','outbox-worker-region-b','booking-command-reconciler-region-b',
    'settlement-worker-region-b','settlement-admin','proxy-region-a','proxy-region-b','global-test-ingress','physical-shard-admin'
)

try {
    try {
        $existingContainers = Invoke-M7Native -AllowFailure -Command { & docker ps -aq --filter "label=com.docker.compose.project=$ProjectName" }
        $existingVolumes = Invoke-M7Native -AllowFailure -Command { & docker volume ls -q --filter "label=com.docker.compose.project=$ProjectName" }
        if (@($existingContainers.Output | Where-Object { $_.Trim() -ne '' }).Count -ne 0 -or
            @($existingVolumes.Output | Where-Object { $_.Trim() -ne '' }).Count -ne 0) {
            throw 'ProjectName already owns Docker resources'
        }
        $sourceState = Get-M7SourceState
        $sourceCommit = [string](& git -C $root rev-parse HEAD)
        if ($LASTEXITCODE -ne 0 -or $sourceCommit.Trim() -notmatch '^[0-9a-f]{40}$') { throw 'source commit identity was not observable' }
        $sourceCommit = $sourceCommit.Trim()
        $sourceDirtyAtStart = @(& git -C $root status --porcelain=v1 --untracked-files=all).Count -gt 0
        if ($LASTEXITCODE -ne 0) { throw 'source dirty-state observation failed' }
        $rendered = Invoke-M7Native -Command { & docker @composeArguments --profile '*' config }
        $renderedText = $rendered.Output -join "`n"
        if ([string]::IsNullOrWhiteSpace($renderedText)) { throw 'rendered Compose config is empty' }
        $renderedDigest = Get-M7SHA256 -Text $renderedText
        $composeBindingFiles = @(
            'docker-compose.dr.yml','docker-compose.payment.yml','docker-compose.physical-shards.yml',
            'deploy/compose/payment.override.yml','deploy/compose/dr-app.override.yml','Dockerfile'
        )
        $composeBindingSources = @($composeBindingFiles | ForEach-Object {
            $path = Join-Path $root $_
            if (-not [System.IO.File]::Exists($path)) { throw "Compose binding source is missing: $_" }
            [ordered]@{ path=$_; sha256=(Get-FileHash -Algorithm SHA256 -LiteralPath $path).Hash.ToLowerInvariant() }
        })
        Write-M7JSON -Name 'compose-binding.json' -Value ([ordered]@{
            rendered_compose_config_sha256=$renderedDigest; source_count=$composeBindingSources.Count; sources=$composeBindingSources
        })
        Add-M7Phase 'source-and-compose-bound'

        if (-not $SkipBuild) {
            Invoke-M7Compose -Arguments (@('build') + @($databases.Primary) + @($databases.Standby) + @($databases.Reseed) + @(
                'restore-validation-control','restore-validation-shard-0','restore-validation-shard-1',
                'migrate-control','migrate-booking-shard-0','migrate-booking-shard-1','dr-admin'
            ) + $appBuildServices) | Out-Null
            Add-M7Phase 'runtime-images-built'
        }

        $upArguments = @('up','-d')
        if ($SkipBuild) { $upArguments += '--no-build' }
        $upArguments += @($databases.Primary)
        $started = $true
        Invoke-M7Compose -Arguments $upArguments | Out-Null
        foreach ($database in $databases) { Wait-M7Role -Service $database.Primary -User $database.User -Database $database.Database -Recovery $false }

        Invoke-M7Compose -Arguments @('run','--rm','--no-deps','migrate-control','up-to','7') | Out-Null
        foreach ($fixture in @(
            'populated_v5_fixture.sql',
            'seed_hot_train_v6_fixture.sql',
            'seed_read_model_v7_fixture.sql',
            'seed_milestone4_v7_fixture.sql'
        )) {
            Invoke-M7SQLFile -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -Path "migrations/testdata/$fixture"
        }
        Invoke-M7Compose -Arguments @('run','--rm','--no-deps','migrate-control','up-to','10') | Out-Null
        Invoke-M7SQLFile -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -Path 'migrations/testdata/seed_milestone7_v10_fixture.sql'
        Invoke-M7Compose -Arguments @('run','--rm','--no-deps','migrate-control','up') | Out-Null
        Invoke-M7SQLFile -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -Path 'migrations/testdata/assert_milestone7_v11_data.sql'
        Invoke-M7SQLFile -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -Path 'migrations/testdata/seed_milestone7_v11_financial_evidence.sql'
        foreach ($shardIndex in 0,1) {
            $migration = "migrate-booking-shard-$shardIndex"
            $service = "booking-shard-$shardIndex-postgres"
            Invoke-M7Compose -Arguments @('run','--rm','--no-deps',$migration,'up-to','1') | Out-Null
            Invoke-M7SQLFile -Service $service -User 'railway_booking' -Database 'railway_booking' -Path 'migrations/testdata/seed_booking_shard_v1_payment_fixture.sql'
            Invoke-M7Compose -Arguments @('run','--rm','--no-deps',$migration,'up') | Out-Null
            Invoke-M7SQLFile -Service $service -User 'railway_booking' -Database 'railway_booking' -Path 'migrations/testdata/assert_booking_shard_v3_data.sql'
            Invoke-M7SQLFile -Service $service -User 'railway_booking' -Database 'railway_booking' -Path 'migrations/testdata/seed_booking_shard_v3_refund_evidence.sql'
        }
        foreach ($roleService in @(
            'runtime-control-role','runtime-booking-shard-0-role','runtime-booking-shard-1-role',
            'payment-reconciler-control-role','payment-reconciler-shard-0-role','payment-reconciler-shard-1-role'
        )) {
            Invoke-M7Compose -Arguments @('run','--rm','--no-deps',$roleService) | Out-Null
        }
        $runtimeRoleEvidence = [System.Collections.Generic.List[object]]::new()
        foreach ($database in $databases) {
            $roleProof = Get-M7Scalar -Service $database.Primary -User $database.User -Database $database.Database -SQL @"
SELECT rolsuper::text||'|'||rolcreatedb::text||'|'||rolcreaterole::text||'|'||rolinherit::text||'|'||
       EXISTS(SELECT 1 FROM pg_class WHERE relowner=(SELECT oid FROM pg_roles WHERE rolname='railway_runtime') AND relkind IN ('r','p'))::text||'|'||
       has_table_privilege('railway_runtime','public.regional_write_authority','UPDATE')::text||'|'||
       has_table_privilege('railway_runtime','public.regional_write_authority','SELECT')::text||'|'||
       CASE WHEN to_regclass('public.backup_expiration_operations') IS NULL THEN false ELSE
         has_table_privilege('railway_runtime','public.backup_expiration_operations','INSERT') OR
         has_table_privilege('railway_runtime','public.backup_expiration_operations','UPDATE') OR
         has_table_privilege('railway_runtime','public.backup_expiration_operations','DELETE')
       END::text
FROM pg_roles WHERE rolname='railway_runtime'
"@
            $expectedRoleProof = if ($database.Name -eq 'control') { 'false|false|false|false|false|false|true|false' } else { 'false|false|false|false|false|false|true|false' }
            if ($roleProof -cne $expectedRoleProof) { throw "runtime role boundary invalid for $($database.Name): $roleProof" }
            $runtimeRoleEvidence.Add([ordered]@{database=$database.Name; nosuperuser=$true; non_owner=$true; authority_update=$false; authority_read=$true; backup_expiration_mutation=$false})
        }
        Write-M7JSON -Name 'runtime-database-role-boundary.json' -Value ([ordered]@{role='railway_runtime'; databases=$runtimeRoleEvidence})
        Add-M7Phase 'runtime-database-roles-bounded'
        Add-M7Phase 'schemas-migrated'

        $replicationHBAEvidence = [System.Collections.Generic.List[object]]::new()
        foreach ($database in $databases) {
            Invoke-M7Compose -Arguments @('exec','-T',$database.Primary,'sh','/etc/railway/configure-replication.sh') | Out-Null
            $loadedRules = 0
            for ($attempt = 0; $attempt -lt 20; $attempt++) {
                $loadedRules = [int](Get-M7Scalar -Service $database.Primary -User $database.User -Database $database.Database -SQL @"
SELECT count(*)
FROM pg_hba_file_rules
WHERE type='hostssl'
  AND database @> ARRAY['replication']::text[]
  AND user_name @> ARRAY['$($database.ReplicationUser)']::text[]
  AND auth_method='scram-sha-256'
  AND error IS NULL
"@)
                if ($loadedRules -eq 1) { break }
                Start-Sleep -Milliseconds 250
            }
            if ($loadedRules -ne 1) {
                $hbaDiagnostics = Get-M7Scalar -Service $database.Primary -User $database.User -Database $database.Database -SQL @"
SELECT json_build_object(
  'hba_file', current_setting('hba_file'),
  'managed_rules', COALESCE((
    SELECT json_agg(json_build_object(
      'file_name', file_name,
      'line_number', line_number,
      'type', type,
      'database', database,
      'user_name', user_name,
      'address', address,
      'netmask', netmask,
      'auth_method', auth_method,
      'error', error
    ) ORDER BY rule_number NULLS LAST, line_number)
    FROM pg_hba_file_rules
    WHERE file_name LIKE '%pg_hba.replication.conf' OR error IS NOT NULL
  ), '[]'::json)
)::text
"@
                Write-Warning "managed replication HBA diagnostics for $($database.Name): $hbaDiagnostics"
                throw "managed replication HBA rule count was $loadedRules for $($database.Name), expected 1"
            }
            $replicationHBAEvidence.Add([ordered]@{
                database=$database.Name; replication_identity=$database.ReplicationUser;
                managed_rule_count=$loadedRules; tls_required=$true; auth_method='scram-sha-256'
            })
        }
        Write-M7JSON -Name 'replication-hba-rules.json' -Value ([ordered]@{databases=$replicationHBAEvidence})
        Add-M7Phase 'replication-hba-rules-loaded'

        foreach ($database in $databases) {
            Invoke-M7Compose -Arguments @('exec','-T',$database.Primary,'/etc/railway/pgbackrest-secret.sh',"--stanza=$($database.Stanza)",'stanza-create') | Out-Null
        }
        Add-M7Phase 'pgbackrest-stanzas-created'

        $standbyUp = @('up','-d')
        if ($SkipBuild) { $standbyUp += '--no-build' }
        $standbyUp += @($databases.Standby)
        Invoke-M7Compose -Arguments $standbyUp | Out-Null
        foreach ($database in $databases) { Wait-M7Role -Service $database.Standby -User $database.ReplicationUser -Database $database.Database -Recovery $true }
        Add-M7Phase 'async-streaming-established'

        $topologyPreflight = [System.Collections.Generic.List[object]]::new()
        foreach ($database in $databases) {
            Invoke-M7SQL -Service $database.Primary -User $database.User -Database $database.Database -SQL @"
CREATE TABLE IF NOT EXISTS public.dr_evidence_markers(marker bigint PRIMARY KEY, created_at timestamptz NOT NULL DEFAULT clock_timestamp());
INSERT INTO public.dr_evidence_markers(marker) VALUES (1) ON CONFLICT DO NOTHING;
"@
            $sourceLSN = Get-M7Scalar -Service $database.Primary -User $database.User -Database $database.Database -SQL 'SELECT pg_current_wal_lsn()::text'
            Wait-M7Replay -Service $database.Standby -User $database.ReplicationUser -Database $database.Database -LSN $sourceLSN
            $observationSQL = @"
SELECT pg_is_in_recovery()::text||'|'||
       pg_wal_lsn_diff(CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_lsn() END,'0/0')::bigint::text||'|'||
       ((pg_control_checkpoint()).timeline_id)::text||'|'||authority.region||'|'||authority.epoch::text||'|'||
       authority.state||'|'||authority.writes_enabled::text||'|'||migrations.version::text||'|'||migrations.dirty::text
FROM public.regional_write_authority AS authority
CROSS JOIN public.schema_migrations AS migrations
WHERE authority.singleton
LIMIT 1
"@
            $targetObservation = (Get-M7Scalar -Service $database.Standby -User $database.User -Database $database.Database -SQL $observationSQL) -split '\|'
            $sourceObservation = (Get-M7Scalar -Service $database.Primary -User $database.User -Database $database.Database -SQL $observationSQL) -split '\|'
            if ($sourceObservation.Count -ne 9 -or $targetObservation.Count -ne 9 -or
                $sourceObservation[0] -ne 'false' -or $targetObservation[0] -ne 'true' -or
                [int64]$sourceObservation[1] -le 0 -or [int64]$targetObservation[1] -le 0 -or
                [int64]$targetObservation[1] -gt [int64]$sourceObservation[1] -or
                $targetObservation[2] -ne $sourceObservation[2] -or
                $sourceObservation[3] -ne 'region-a' -or $targetObservation[3] -ne 'region-a' -or
                $sourceObservation[4] -ne '1' -or $targetObservation[4] -ne '1' -or
                $sourceObservation[5] -ne 'active' -or $targetObservation[5] -ne 'active' -or
                $sourceObservation[6] -ne 'true' -or $targetObservation[6] -ne 'true' -or
                [int]$sourceObservation[7] -ne $database.ExpectedVersion -or $targetObservation[7] -ne $sourceObservation[7] -or
                $sourceObservation[8] -ne 'false' -or $targetObservation[8] -ne 'false') {
                throw "typed failover topology preflight failed for $($database.Name): source=$($sourceObservation -join '|') target=$($targetObservation -join '|')"
            }
            $topologyPreflight.Add([ordered]@{
                database=$database.Name; source_role='primary'; target_role='standby'; source_wal=[int64]$sourceObservation[1];
                target_wal=[int64]$targetObservation[1]; timeline=[int]$sourceObservation[2]; authority_region='region-a';
                authority_epoch=1; authority_state='active'; writes_enabled=$true; schema_version=[int]$sourceObservation[7]; schema_dirty=$false
            })
        }
        Write-M7JSON -Name 'topology-preflight.json' -Value ([ordered]@{databases=$topologyPreflight})
        Add-M7Phase 'topology-preflight-synchronized'

        $env:DR_RECOVERY_EPOCH = '1'
        $env:DR_JOURNAL_REGION = 'region-a'
        $env:DR_JOURNAL_DATABASE_URL = 'postgresql://railway_control:control-local-only@control-postgres:5432/railway_control?sslmode=disable&connect_timeout=3'
        $preflight = Invoke-M7DRAdmin -Arguments @(
            'failover','--operation-id',$failoverOperationID,'--incident-id',$failoverIncidentID,
            '--from','region-a','--to','region-b','--source-epoch','1','--operator','operator:local-dr',
            '--reason','region_failure','--dry-run','--timeout','2m'
        )
        if ([string]$preflight.result.stage -ne 'planned' -or [string]$preflight.status -ne 'dry-run') { throw 'typed failover dry-run preflight did not validate the synchronized topology' }
        $planned = Invoke-M7DRAdmin -Arguments @(
            'failover','--operation-id',$failoverOperationID,'--incident-id',$failoverIncidentID,
            '--from','region-a','--to','region-b','--source-epoch','1','--operator','operator:local-dr',
            '--reason','region_failure','--confirm','--timeout','2m'
        )
        if ([string]$planned.result.stage -ne 'planned') { throw 'typed failover operation was not durably planned' }
        Add-M7Phase 'typed-failover-planned'

        foreach ($database in $databases) {
            $replicationCount = [int](Get-M7Scalar -Service $database.Primary -User $database.User -Database $database.Database -SQL "SELECT count(*) FROM pg_stat_replication WHERE application_name<>'' AND state='streaming'")
            if ($replicationCount -lt 1) { throw "streaming replication was not observed for $($database.Name)" }
            $tlsReplicationCount = [int](Get-M7Scalar -Service $database.Primary -User $database.User -Database $database.Database -SQL "SELECT count(*) FROM pg_stat_replication AS replication JOIN pg_stat_ssl AS tls USING(pid) WHERE replication.application_name='$($database.Slot)' AND replication.state='streaming' AND tls.ssl")
            if ($tlsReplicationCount -ne 1) { throw "TLS-authenticated streaming replication was not observed for $($database.Name)" }
            $checksumState = Get-M7Scalar -Service $database.Primary -User $database.User -Database $database.Database -SQL "SELECT current_setting('data_checksums')||'|'||current_setting('ssl')"
            if ($checksumState -ne 'on|on') { throw "data checksums or PostgreSQL TLS was not enabled for $($database.Name)" }
            Invoke-M7Compose -Arguments @('exec','-T',$database.Primary,'/etc/railway/pgbackrest-secret.sh',"--stanza=$($database.Stanza)",'check') | Out-Null
            Invoke-M7Compose -Arguments @('exec','-T',$database.Primary,'/etc/railway/pgbackrest-secret.sh',"--stanza=$($database.Stanza)",'--type=full','backup') | Out-Null
            $archivedBefore = [int64](Get-M7Scalar -Service $database.Primary -User $database.User -Database $database.Database -SQL 'SELECT archived_count FROM pg_stat_archiver')
            Invoke-M7Compose -Arguments @('exec','-T',$database.Primary,'/etc/railway/pgbackrest-secret.sh',"--stanza=$($database.Stanza)",'check') | Out-Null
            $archiveHealthy = $false
            for ($archiveAttempt=1; $archiveAttempt -le 60; $archiveAttempt++) {
                $archivedNow = [int64](Get-M7Scalar -Service $database.Primary -User $database.User -Database $database.Database -SQL 'SELECT archived_count FROM pg_stat_archiver')
                if ($archivedNow -gt $archivedBefore) { $archiveHealthy = $true; break }
                Start-Sleep -Seconds 1
            }
            if (-not $archiveHealthy) { throw "pg_stat_archiver did not prove WAL continuity for $($database.Name)" }
            $slotObservation = (Get-M7Scalar -Service $database.Primary -User $database.User -Database $database.Database -SQL @"
SELECT slot_name||'|'||active::text||'|'||coalesce(wal_status,'')||'|'||coalesce(safe_wal_size,-1)::text||'|'||
       coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn),-1)::bigint::text
FROM pg_replication_slots WHERE slot_name='$($database.Slot)' AND slot_type='physical'
"@) -split '\|'
            if ($slotObservation.Count -ne 5 -or $slotObservation[0] -cne $database.Slot -or $slotObservation[1] -ne 'true' -or $slotObservation[2] -eq 'lost' -or [int64]$slotObservation[3] -lt 0 -or [int64]$slotObservation[4] -lt 0) {
                throw "physical replication slot health was not bounded for $($database.Name)"
            }
            $archiveObservation = (Get-M7Scalar -Service $database.Primary -User $database.User -Database $database.Database -SQL "SELECT archived_count::text||'|'||failed_count::text||'|'||coalesce(extract(epoch FROM last_archived_time)::bigint,0)::text||'|'||coalesce(extract(epoch FROM last_failed_time)::bigint,0)::text FROM pg_stat_archiver") -split '\|'
            if ($archiveObservation.Count -ne 4 -or [int64]$archiveObservation[0] -lt 1 -or ([int64]$archiveObservation[3] -gt [int64]$archiveObservation[2])) {
                throw "WAL archive freshness was not healthy for $($database.Name)"
            }
            $standbyObservation = (Get-M7Scalar -Service $database.Standby -User $database.ReplicationUser -Database $database.Database -SQL "SELECT coalesce((SELECT status FROM pg_stat_wal_receiver LIMIT 1),'')||'|'||coalesce(pg_last_wal_replay_lsn()::text,'')||'|'||coalesce(extract(epoch FROM pg_last_xact_replay_timestamp())::bigint,0)::text||'|'||((pg_control_checkpoint()).timeline_id)::text") -split '\|'
            if ($standbyObservation.Count -ne 4 -or $standbyObservation[0] -ne 'streaming' -or [string]::IsNullOrWhiteSpace($standbyObservation[1]) -or [int64]$standbyObservation[2] -le 0 -or [int]$standbyObservation[3] -le 0) {
                throw "standby replay freshness or timeline was not observable for $($database.Name)"
            }
            $replicationEvidence.Add([ordered]@{
                database=$database.Name; replication_identity=$database.ReplicationUser; slot=$database.Slot;
                tls_authenticated=$true; data_checksums='on'; slot_active=$true; slot_wal_status=$slotObservation[2];
                slot_safe_wal_bytes=[int64]$slotObservation[3]; retained_wal_bytes=[int64]$slotObservation[4];
                archived_wal_count=[int64]$archiveObservation[0]; archive_failed_count=[int64]$archiveObservation[1];
                last_archived_at_epoch=[int64]$archiveObservation[2]; last_archive_failure_at_epoch=[int64]$archiveObservation[3];
                wal_receiver_state=$standbyObservation[0]; replay_lsn=$standbyObservation[1]; last_replay_at_epoch=[int64]$standbyObservation[2]; timeline=[int]$standbyObservation[3]
            })
            $infoResult = Invoke-M7Compose -Arguments @('exec','-T',$database.Primary,'/etc/railway/pgbackrest-secret.sh',"--stanza=$($database.Stanza)",'--log-level-console=off','--output=json','info')
            $infoText = $infoResult.Output -join "`n"
            $info = $infoText | ConvertFrom-Json
            $backups = @($info[0].backup)
            if ($backups.Count -lt 1 -or [string]::IsNullOrWhiteSpace([string]$backups[-1].label) -or [string]$info[0].cipher -ne 'aes-256-cbc') {
                throw "encrypted pgBackRest backup metadata is invalid for $($database.Name)"
            }
            $backupSet = [string]$backups[-1].label
            Invoke-M7Compose -Arguments @('exec','-T',$database.Primary,'/etc/railway/pgbackrest-secret.sh',"--stanza=$($database.Stanza)","--set=$backupSet",'verify') | Out-Null
            $backupSource = (Get-M7Scalar -Service $database.Primary -User $database.User -Database $database.Database -SQL "SELECT ((pg_control_checkpoint()).timeline_id)::text||'|'||pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0')::bigint::text") -split '\|'
            if ($backupSource.Count -ne 2 -or [int]$backupSource[0] -lt 1 -or [uint64]$backupSource[1] -lt 1) { throw "backup source position was malformed for $($database.Name)" }
            $backupEvidence.Add([ordered]@{
                database=$database.Name; stanza=$database.Stanza; backup_set=$backupSet;
                metadata_sha256=(Get-M7SHA256 -Text ($infoText.Trim())); encrypted=$true;
                repository_check='passed'; repository_verify='passed';
                source_timeline=[int]$backupSource[0]; source_wal=[uint64]$backupSource[1]
            })
        }
        Add-M7Phase 'encrypted-backups-verified'

        $pitrTarget = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
        $env:PGBACKREST_PITR_TARGET = $pitrTarget
        foreach ($database in $databases) {
            $restoreService = "restore-validation-$($database.Name)"
            $restoreUp = @('--profile','dr-restore','up','-d','--no-deps')
            if ($SkipBuild) { $restoreUp += '--no-build' }
            $restoreUp += $restoreService
            Invoke-M7Compose -Arguments $restoreUp | Out-Null
            Wait-M7Role -Service $restoreService -User $database.User -Database $database.Database -Recovery $false
            $schema = Get-M7Scalar -Service $restoreService -User $database.User -Database $database.Database -SQL "SELECT version::text||'|'||dirty::text FROM public.schema_migrations"
            $markerCount = [int](Get-M7Scalar -Service $restoreService -User $database.User -Database $database.Database -SQL 'SELECT count(*) FROM public.dr_evidence_markers WHERE marker=1')
            if ($schema -ne "$($database.ExpectedVersion)|false" -or $markerCount -ne 1) { throw "isolated restore validation failed for $($database.Name)" }
            $timeline = [int](Get-M7Scalar -Service $restoreService -User $database.User -Database $database.Database -SQL 'SELECT ((pg_control_checkpoint()).timeline_id)::text')
            if ($database.Name -eq 'control') {
                Invoke-M7SQLFile -Service $restoreService -User $database.User -Database $database.Database -Path 'migrations/testdata/assert_milestone7_v11_data.sql'
            } else {
                Invoke-M7SQLFile -Service $restoreService -User $database.User -Database $database.Database -Path 'migrations/testdata/assert_booking_shard_v3_data.sql'
            }
            $restoreEvidence.Add([ordered]@{ database=$database.Name; schema_version=$database.ExpectedVersion; timeline=$timeline; pitr_target=$pitrTarget; marker_count=$markerCount; financial_fixture_assertion='passed' })
        }
        foreach ($database in $databases) {
            $backup = @($backupEvidence | Where-Object { $_.database -eq $database.Name })
            $restore = @($restoreEvidence | Where-Object { $_.database -eq $database.Name })
            if ($backup.Count -ne 1 -or $restore.Count -ne 1 -or [string]$backup[0].backup_set -notmatch '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$') { throw "backup/restore evidence record was malformed for $($database.Name)" }
            $backupID = [guid]::NewGuid().ToString()
            $verificationID = [guid]::NewGuid().ToString()
            $restoreID = [guid]::NewGuid().ToString()
            Invoke-M7SQL -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL @"
BEGIN;
SELECT set_config('railway.deployment_region','region-a',true),set_config('railway.deployment_role','active',true),set_config('railway.region_epoch','1',true),set_config('railway.regional_writes_enabled','true',true);
INSERT INTO public.backup_artifacts(backup_id,database_id,repository_id,backup_set,checksum,encrypted,source_timeline,source_wal,created_at)
VALUES ('$backupID'::uuid,'$($database.Name)','pgbackrest-local','$($backup[0].backup_set)',decode('$($backup[0].metadata_sha256)','hex'),true,$($backup[0].source_timeline),$($backup[0].source_wal),clock_timestamp());
INSERT INTO public.backup_verifications(verification_id,backup_id,state,checksum,verifier_kind,verified_at)
VALUES ('$verificationID'::uuid,'$backupID'::uuid,'passed',decode('$($backup[0].metadata_sha256)','hex'),'pgbackrest_verify',clock_timestamp());
INSERT INTO public.restore_validations(restore_validation_id,backup_id,target_id,database_id,state,point_in_time,schema_version,timeline,reconciled,started_at,completed_at)
VALUES ('$restoreID'::uuid,'$backupID'::uuid,'isolated-$($database.Name)','$($database.Name)','passed','$($restore[0].pitr_target)'::timestamptz,$($restore[0].schema_version),$($restore[0].timeline),true,clock_timestamp()-interval '1 second',clock_timestamp());
COMMIT;
"@
        }
        Add-M7Phase 'isolated-restores-validated'

        $env:ACTIVE_REGION_UPSTREAM = 'proxy-region-a'
        $activeAppUp = @('--profile','dr-app','up','-d','--no-deps')
        if ($SkipBuild) { $activeAppUp += '--no-build' }
        $activeAppUp += $regionAAppServices
        Invoke-M7Compose -Arguments $activeAppUp | Out-Null
        foreach ($api in @('api-1','api-2','api-3')) { Wait-M7ServiceHTTP -Service $api -URL 'http://127.0.0.1:8080/readyz' }
        Wait-M7ServiceHTTP -Service 'global-test-ingress' -URL 'http://127.0.0.1:8080/readyz'
        Wait-M7ServiceHTTP -Service 'settlement-worker-region-a' -URL 'http://127.0.0.1:9090/metrics'

        Initialize-Milestone5DriverFixture -Context $driverContext
        $m7Train = '21000000-0000-4000-8000-000000000401'
        $m7Migration = '67000000-0000-4000-8000-000000000701'
        New-Milestone5Migration -Context $driverContext -TrainRunID $m7Train -TargetShard 'physical-shard-0' -MigrationID $m7Migration -Prefix 'm7-dr-train'
        Move-Milestone5Migration -Context $driverContext -MigrationID $m7Migration -Target validating_online -Prefix 'm7-dr-train'
        if ((Get-Milestone5MigrationState -Context $driverContext -MigrationID $m7Migration -Artifact 'm7-refund-migration-barrier-open.log') -ne 'validating_online') {
            throw 'refund/migration barrier did not open in validating_online'
        }
        $m7HealthyTrainSeed = '21000000-0000-4000-8000-000000000402'
        $m7HealthyMigration = '67000000-0000-4000-8000-000000000702'
        New-Milestone5Migration -Context $driverContext -TrainRunID $m7HealthyTrainSeed -TargetShard 'physical-shard-1' -MigrationID $m7HealthyMigration -Prefix 'm7-dr-healthy-train'
        Move-Milestone5Migration -Context $driverContext -MigrationID $m7HealthyMigration -Target rollback_window -Prefix 'm7-dr-healthy-train'
        $m7Customer = New-M7CustomerFixtures -BaseURL (Get-M7PublishedURL) -TrainRunID $m7Train -Count 10
        $commonK6 = @{
            VUS='1'; ITERATIONS_PER_VU='1'; DURATION='3m'; POLL_ATTEMPTS='100';
            PAYMENT_POLL_ATTEMPTS='120'; REFUND_POLL_ATTEMPTS='120';
            BASE_URL='http://api-1:8080'; SANDBOX_URL='http://payment-sandbox:8099';
            SANDBOX_CONTROL_TOKEN='synthetic-disposable-fault-token'
        }
        $sensitiveValues += [string]$commonK6.SANDBOX_CONTROL_TOKEN
        if ($env:GITHUB_ACTIONS -eq 'true') { Write-Output "::add-mask::$($commonK6.SANDBOX_CONTROL_TOKEN)" }
        foreach ($reservationID in $m7Customer.Reservations[0..2]) {
            $intent = Invoke-Milestone5DriverAPI -BaseURL (Get-M7PublishedURL) -Method POST `
                -Path "/api/v1/reservations/$reservationID/payment-intents" -Token $m7Customer.Token `
                -IdempotencyKey "m7-initial-ticket-$reservationID" -Body @{} -ExpectedStatus @(202)
            if ([string]$intent.Body.id -notmatch '^[0-9a-f-]{36}$') { throw 'initial ticket payment omitted its durable identity' }
            Complete-M7SandboxPayment -DatabaseService 'control-postgres' -ReservationID $reservationID -WebhookBaseURL (Get-M7PublishedURL)
        }
        $orderFixtures = [System.Collections.Generic.List[object]]::new()
        foreach ($reservationID in $m7Customer.Reservations[0..2]) {
            if ($reservationID -notmatch '^[0-9a-f-]{36}$') { throw 'M7 reservation fixture identity is malformed' }
            $orderID = Get-M7Scalar -Service 'booking-shard-0-postgres' -User 'railway_booking' -Database 'railway_booking' -SQL "SELECT id::text FROM public.ticket_orders WHERE reservation_id='$reservationID'::uuid AND status='issued'"
            $ticketIDs = Get-M7Scalar -Service 'booking-shard-0-postgres' -User 'railway_booking' -Database 'railway_booking' -SQL "SELECT string_agg(id::text,',' ORDER BY id) FROM public.tickets WHERE ticket_order_id='$orderID'::uuid AND status='active'"
            if (($ticketIDs -split ',').Count -ne 2) { throw 'M7 issued order did not contain exactly two active tickets' }
            $orderFixtures.Add([pscustomobject]@{ OrderID=$orderID; TicketIDs=$ticketIDs })
        }
        $m7Orders = [object[]]$orderFixtures
        $m7HealthyTrain = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT train_run_id::text FROM public.train_run_shard_assignments
WHERE is_current AND shard_id='physical-shard-1' AND train_run_id IN (
 '21000000-0000-4000-8000-000000000402'::uuid,'21000000-0000-4000-8000-000000000403'::uuid,
 '21000000-0000-4000-8000-000000000404'::uuid,'21000000-0000-4000-8000-000000000405'::uuid)
ORDER BY train_run_id LIMIT 1
"@
        if ($m7HealthyTrain -notmatch '^[0-9a-f-]{36}$') { throw 'healthy shard train fixture was not routed to physical-shard-1' }
        $m7HealthyCustomer = New-M7CustomerFixtures -BaseURL (Get-M7PublishedURL) -TrainRunID $m7HealthyTrain -Count 1 -Label 'healthy-shard'
        $healthyIntent = Invoke-Milestone5DriverAPI -BaseURL (Get-M7PublishedURL) -Method POST `
            -Path "/api/v1/reservations/$($m7HealthyCustomer.Reservations[0])/payment-intents" -Token $m7HealthyCustomer.Token `
            -IdempotencyKey "m7-healthy-shard-ticket-$suffix" -Body @{} -ExpectedStatus @(202)
        if ([string]$healthyIntent.Body.id -notmatch '^[0-9a-f-]{36}$') { throw 'healthy shard payment omitted its durable identity' }
        Complete-M7SandboxPayment -DatabaseService 'control-postgres' -ReservationID $m7HealthyCustomer.Reservations[0] -WebhookBaseURL (Get-M7PublishedURL)
        $m7HealthyOrderID = Get-M7Scalar -Service 'booking-shard-1-postgres' -User 'railway_booking' -Database 'railway_booking' -SQL "SELECT id::text FROM public.ticket_orders WHERE reservation_id='$($m7HealthyCustomer.Reservations[0])'::uuid AND status='issued'"
        if ($m7HealthyOrderID -notmatch '^[0-9a-f-]{36}$') { throw 'healthy shard ticket order was not durably issued' }
        $refundCrashIntent = Invoke-Milestone5DriverAPI -BaseURL (Get-M7PublishedURL) -Method POST `
            -Path "/api/v1/reservations/$($m7Customer.Reservations[6])/payment-intents" -Token $m7Customer.Token `
            -IdempotencyKey "m7-refund-crash-order-$suffix" -Body @{} -ExpectedStatus @(202)
        if ([string]$refundCrashIntent.Body.id -notmatch '^[0-9a-f-]{36}$') { throw 'refund crash fixture omitted its payment identity' }
        Complete-M7SandboxPayment -DatabaseService 'control-postgres' -ReservationID $m7Customer.Reservations[6] -WebhookBaseURL (Get-M7PublishedURL)
        $refundCrashOrderID = Get-M7Scalar -Service 'booking-shard-0-postgres' -User 'railway_booking' -Database 'railway_booking' -SQL "SELECT id::text FROM public.ticket_orders WHERE reservation_id='$($m7Customer.Reservations[6])'::uuid AND status='issued'"
        $refundCrashTicketIDs = Get-M7Scalar -Service 'booking-shard-0-postgres' -User 'railway_booking' -Database 'railway_booking' -SQL "SELECT string_agg(id::text,',' ORDER BY id) FROM public.tickets WHERE ticket_order_id='$refundCrashOrderID'::uuid AND status='active'"
        if (($refundCrashTicketIDs -split ',').Count -ne 2) { throw 'refund crash fixture did not issue exactly two tickets' }
        $refundShardCrashIntent = Invoke-Milestone5DriverAPI -BaseURL (Get-M7PublishedURL) -Method POST `
            -Path "/api/v1/reservations/$($m7Customer.Reservations[7])/payment-intents" -Token $m7Customer.Token `
            -IdempotencyKey "m7-refund-shard-crash-order-$suffix" -Body @{} -ExpectedStatus @(202)
        if ([string]$refundShardCrashIntent.Body.id -notmatch '^[0-9a-f-]{36}$') { throw 'refund shard crash fixture omitted its payment identity' }
        Complete-M7SandboxPayment -DatabaseService 'control-postgres' -ReservationID $m7Customer.Reservations[7] -WebhookBaseURL (Get-M7PublishedURL)
        $refundShardCrashOrderID = Get-M7Scalar -Service 'booking-shard-0-postgres' -User 'railway_booking' -Database 'railway_booking' -SQL "SELECT id::text FROM public.ticket_orders WHERE reservation_id='$($m7Customer.Reservations[7])'::uuid AND status='issued'"
        $refundShardCrashTicketIDs = Get-M7Scalar -Service 'booking-shard-0-postgres' -User 'railway_booking' -Database 'railway_booking' -SQL "SELECT string_agg(id::text,',' ORDER BY id) FROM public.tickets WHERE ticket_order_id='$refundShardCrashOrderID'::uuid AND status='active'"
        if (($refundShardCrashTicketIDs -split ',').Count -ne 2) { throw 'refund shard crash fixture did not issue exactly two tickets' }
        $fullRefundProviderIntent = Invoke-Milestone5DriverAPI -BaseURL (Get-M7PublishedURL) -Method POST `
            -Path "/api/v1/reservations/$($m7Customer.Reservations[8])/payment-intents" -Token $m7Customer.Token `
            -IdempotencyKey "m7-full-refund-provider-crash-$suffix" -Body @{} -ExpectedStatus @(202)
        $fullRefundProviderHosted = Get-M7HostedPayment -DatabaseService 'control-postgres' -ReservationID $m7Customer.Reservations[8]
        if ([string]$fullRefundProviderHosted.IntentID -cne [string]$fullRefundProviderIntent.Body.id) { throw 'full-refund provider crash fixture identity was inconsistent' }
        $fullRefundShardIntent = Invoke-Milestone5DriverAPI -BaseURL (Get-M7PublishedURL) -Method POST `
            -Path "/api/v1/reservations/$($m7Customer.Reservations[9])/payment-intents" -Token $m7Customer.Token `
            -IdempotencyKey "m7-full-refund-shard-crash-$suffix" -Body @{} -ExpectedStatus @(202)
        $fullRefundShardHosted = Get-M7HostedPayment -DatabaseService 'control-postgres' -ReservationID $m7Customer.Reservations[9]
        if ([string]$fullRefundShardHosted.IntentID -cne [string]$fullRefundShardIntent.Body.id) { throw 'full-refund shard crash fixture identity was inconsistent' }
        Invoke-M7K6 -Script 'production-provider-contract.js' -Environment @{
            VUS='1'; ITERATIONS_PER_VU='1'; DURATION='1m'; PROVIDER_CONTRACT_URL='http://payment-stripe-contract:8100';
            PROVIDER_CONTRACT_API_KEY=$env:PAYMENT_CONTRACT_API_KEY; PROVIDER_CONTRACT_ACCOUNT_ID='acct_m7_contract'; PROVIDER_CONTRACT_API_VERSION='2026-07-29.dahlia'
        }
        $env:REGION_A_SETTLEMENT_ENABLED = 'true'
        $settlementUp = @('--profile','dr-app','up','-d','--no-deps','--force-recreate')
        if ($SkipBuild) { $settlementUp += '--no-build' }
        $settlementUp += 'settlement-worker-region-a'
        Invoke-M7Compose -Arguments $settlementUp | Out-Null
        Wait-M7ServiceHTTP -Service 'settlement-worker-region-a' -URL 'http://127.0.0.1:9090/metrics'
        $settlementObservation = Assert-M7SettlementImport -DatabaseService 'control-postgres' -WorkerService 'settlement-worker-region-a' -Phase 'region-a-before-failover' -CrashAfterFirstPage
        Wait-M7ServiceHTTP -Service 'payment-reconciler' -URL 'http://127.0.0.1:9090/metrics'
        Invoke-M7K6 -Script 'settlement-import.js' -Environment @{
            VUS='1'; ITERATIONS_PER_VU='1'; DURATION='1m';
            SETTLEMENT_WORKER_URL='http://settlement-worker-region-a:9090';
            DURABLE_METRICS_URL='http://payment-reconciler:9090';
            SETTLEMENT_OBSERVATION_SECONDS=([string]::Format([System.Globalization.CultureInfo]::InvariantCulture,'{0:F6}',[double]$settlementObservation.observation_seconds))
        }
        $settlementReconciliationEvidence = Invoke-M7SettlementMismatchEvidence
        Add-M7Phase 'detect-only-settlement-mismatch-review-recorded'
        Assert-M7DurableMetrics -Service 'payment-reconciler' -Phase 'region-a-before-failover' -Families @(
            'financial_ledger_transaction_total','settlement_import_total','settlement_reconciliation_total','settlement_reconciliation_mismatch_total','regional_active_epoch',
            'backup_total','backup_restore_duration_seconds'
        )
        Invoke-M7K6 -Script 'partial-ticket-refund.js' -Environment (@{} + $commonK6 + @{
            CUSTOMER_TOKEN=$m7Customer.Token; TICKET_ORDER_ID=$m7Orders[0].OrderID; TICKET_IDS=($m7Orders[0].TicketIDs -split ',')[0]
        })
        Invoke-M7K6 -Script 'partial-refund-idempotency.js' -Environment (@{} + $commonK6 + @{
            BASE_URLS='http://api-1:8080,http://api-2:8080,http://api-3:8080';
            CUSTOMER_TOKEN=$m7Customer.Token; TICKET_ORDER_ID=$m7Orders[1].OrderID;
            TICKET_IDS=($m7Orders[1].TicketIDs -split ',')[0]; CONFLICT_TICKET_ID=($m7Orders[1].TicketIDs -split ',')[1]
        })
        Assert-M7PartialRefund -OrderID $m7Orders[0].OrderID -SelectedTicketID (($m7Orders[0].TicketIDs -split ',')[0]) -Scenario 'partial-ticket-refund'
        Assert-M7PartialRefund -OrderID $m7Orders[1].OrderID -SelectedTicketID (($m7Orders[1].TicketIDs -split ',')[0]) -Scenario 'partial-refund-idempotency'
        if ((Get-Milestone5MigrationState -Context $driverContext -MigrationID $m7Migration -Artifact 'm7-refund-migration-barrier-held.log') -ne 'validating_online') {
            throw 'physical migration advanced before both refund effects were durably applied'
        }
        Move-Milestone5Migration -Context $driverContext -MigrationID $m7Migration -Target rollback_window -Prefix 'm7-dr-train-after-refund'
        $postCutoverRefundRoute = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT (SELECT shard_id FROM public.train_run_shard_assignments WHERE train_run_id='$m7Train'::uuid AND is_current)::text||'|'||
       (SELECT count(*) FROM public.reservation_shard_locators WHERE reservation_id IN ('$($m7Customer.Reservations[0])'::uuid,'$($m7Customer.Reservations[1])'::uuid) AND shard_id='physical-shard-0')::text
"@
        if ($postCutoverRefundRoute -ne 'physical-shard-0|2') { throw 'post-cutover refund locators did not follow the current physical route' }
        Add-M7Phase 'partial-refunds-applied-during-physical-migration'

        $m7ReverseMigration = '67000000-0000-4000-8000-000000000703'
        $m7ReturnMigration = '67000000-0000-4000-8000-000000000704'
        Invoke-Milestone5DriverAdmin -Context $driverContext -Arguments @(
            'plan-reverse-migration','--migration-id',$m7Migration,'--reverse-migration-id',$m7ReverseMigration,
            '--generation','3','--confirm','--timeout','2m'
        ) -Artifact 'm7-refund-reverse-plan.log' | Out-Null
        Move-Milestone5Migration -Context $driverContext -MigrationID $m7ReverseMigration -Target rollback_window -Prefix 'm7-refund-reverse' -ReverseStart
        $reverseRefundEvidence = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT assignment.shard_id||'|'||assignment.assignment_generation::text||'|'||
       count(receipt.*)::text
FROM public.train_run_shard_assignments AS assignment
LEFT JOIN public.physical_source_selected_ticket_refund_receipt_rows AS receipt
  ON receipt.source_shard_id=assignment.shard_id
 AND receipt.ticket_id IN ('$((($m7Orders[0].TicketIDs -split ',')[0]))'::uuid,'$((($m7Orders[1].TicketIDs -split ',')[0]))'::uuid)
WHERE assignment.train_run_id='$m7Train'::uuid AND assignment.is_current
GROUP BY assignment.shard_id,assignment.assignment_generation
"@
        if ($reverseRefundEvidence -notmatch '^(legacy|shard-0|shard-1)\|3\|2$') { throw "reverse migration did not preserve both applied partial refunds: $reverseRefundEvidence" }
        New-Milestone5Migration -Context $driverContext -TrainRunID $m7Train -TargetShard 'physical-shard-0' -MigrationID $m7ReturnMigration -Prefix 'm7-refund-return'
        Move-Milestone5Migration -Context $driverContext -MigrationID $m7ReturnMigration -Target rollback_window -Prefix 'm7-refund-return'
        $returnRefundEvidence = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL "SELECT shard_id||'|'||assignment_generation::text FROM public.train_run_shard_assignments WHERE train_run_id='$m7Train'::uuid AND is_current"
        $returnRefundReceipts = Get-M7Scalar -Service 'booking-shard-0-postgres' -User 'railway_booking' -Database 'railway_booking' -SQL "SELECT count(*) FROM public.selected_ticket_refund_receipts WHERE ticket_id IN ('$((($m7Orders[0].TicketIDs -split ',')[0]))'::uuid,'$((($m7Orders[1].TicketIDs -split ',')[0]))'::uuid)"
        if ($returnRefundEvidence -ne 'physical-shard-0|4' -or $returnRefundReceipts -ne '2') { throw 'post-reverse return migration did not preserve both applied partial refunds on the active physical shard' }
        $physicalMigrationInteractionEvidence = [ordered]@{
            forward_migration_id_sha256=(Get-M7SHA256 -Text $m7Migration); reverse_migration_id_sha256=(Get-M7SHA256 -Text $m7ReverseMigration);
            return_migration_id_sha256=(Get-M7SHA256 -Text $m7ReturnMigration); reverse_completed=$true; return_completed=$true;
            reverse_assignment=$reverseRefundEvidence; final_assignment=$returnRefundEvidence; refunds_after_return=2
        }
        Add-M7Phase 'partial-refunds-survived-complete-reverse-and-return-migration'

        $ticketCrashCustomer = New-M7CustomerFixtures -BaseURL (Get-M7PublishedURL) -TrainRunID $m7Train -Count 1 -Label 'ticket-crash'
        $ticketCrashIntent = Invoke-Milestone5DriverAPI -BaseURL (Get-M7PublishedURL) -Method POST `
            -Path "/api/v1/reservations/$($ticketCrashCustomer.Reservations[0])/payment-intents" -Token $ticketCrashCustomer.Token `
            -IdempotencyKey "m7-ticket-crash-$suffix" -Body @{} -ExpectedStatus @(202)
        $ticketCrashHosted = Get-M7HostedPayment -DatabaseService 'control-postgres' -ReservationID $ticketCrashCustomer.Reservations[0]
        if ([string]$ticketCrashHosted.IntentID -cne [string]$ticketCrashIntent.Body.id) { throw 'ticket crash fixture identity was inconsistent' }

        $paymentCrashIntent = Invoke-Milestone5DriverAPI -BaseURL (Get-M7PublishedURL) -Method POST `
            -Path "/api/v1/reservations/$($m7Customer.Reservations[5])/payment-intents" -Token $m7Customer.Token `
            -IdempotencyKey "m7-capture-crash-$suffix" -Body @{} -ExpectedStatus @(202)
        if ([string]$paymentCrashIntent.Body.id -notmatch '^[0-9a-f-]{36}$') { throw 'capture crash fixture omitted its payment identity' }
        $paymentCrashHosted = Get-M7HostedPayment -DatabaseService 'control-postgres' -ReservationID $m7Customer.Reservations[5]
        if ([string]$paymentCrashHosted.IntentID -cne [string]$paymentCrashIntent.Body.id) { throw 'capture crash hosted identity was inconsistent' }
        Invoke-M7Compose -Arguments @('stop','-t','15','payment-worker-1','payment-worker-2','payment-reconciler') | Out-Null
        Invoke-M7Compose -Arguments @('exec','-T','payment-sandbox','wget','-q','-O','/dev/null','--post-data=',"http://127.0.0.1:8099/hosted/checkouts/$($paymentCrashHosted.ProviderPaymentID)/authorize") | Out-Null
        [void](Send-M7SandboxWebhooks -WebhookBaseURL (Get-M7PublishedURL))
        $paymentCrashAmount = [int64](Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL "SELECT amount_minor FROM public.payment_intents WHERE payment_intent_id='$($paymentCrashHosted.IntentID)'::uuid")
        Invoke-M7CrashWorker -Service 'payment-worker-1' -Point 'capture_provider_committed' -TargetID $paymentCrashHosted.IntentID -Scenario 'capture-provider-before-control'
        $captureInterrupted = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT intent.state||'|'||operation.state
FROM public.payment_intents AS intent JOIN public.payment_operations AS operation USING(payment_intent_id)
WHERE intent.payment_intent_id='$($paymentCrashHosted.IntentID)'::uuid AND operation.operation_type='capture'
"@
        if ($captureInterrupted -ne 'capture_pending|in_flight') { throw "capture crash window did not preserve the expected control state: $captureInterrupted" }
        $captureProviderEffect = Get-M7SandboxEffectEvidence -IntentID $paymentCrashHosted.IntentID -ExpectedStatus 'captured' -CapturedMinor $paymentCrashAmount -RefundedMinor 0
        Set-M7CrashLeaseBarrier -Kind 'payment-operation' -Service 'control-postgres' -TargetID $paymentCrashHosted.IntentID -ExpectedState 'in_flight'

        Invoke-M7Compose -Arguments @('exec','-T','payment-sandbox','wget','-q','-O','/dev/null','--post-data=',"http://127.0.0.1:8099/hosted/checkouts/$($ticketCrashHosted.ProviderPaymentID)/authorize") | Out-Null
        [void](Send-M7SandboxWebhooks -WebhookBaseURL (Get-M7PublishedURL))
        Invoke-M7CrashWorker -Service 'payment-worker-1' -Point 'ticket_issue_shard_committed' -TargetID $ticketCrashHosted.IntentID -Scenario 'ticket-shard-before-control'
        $ticketInterrupted = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT intent.state||'|'||saga.state||'|'||saga.current_step||'|'||action.state
FROM public.payment_intents AS intent JOIN public.payment_sagas AS saga USING(payment_intent_id)
JOIN public.payment_saga_actions AS action USING(saga_id)
WHERE intent.payment_intent_id='$($ticketCrashHosted.IntentID)'::uuid
  AND action.action_type='issue_tickets'
"@
        $ticketShardInterrupted = Get-M7Scalar -Service 'booking-shard-0-postgres' -User 'railway_booking' -Database 'railway_booking' -SQL @"
SELECT (SELECT count(*) FROM public.ticket_issuance_receipts WHERE payment_intent_id='$($ticketCrashHosted.IntentID)'::uuid)::text||'|'||
       (SELECT count(*) FROM public.ticket_orders WHERE payment_intent_id='$($ticketCrashHosted.IntentID)'::uuid AND status='issued')::text||'|'||
       (SELECT count(*) FROM public.tickets AS ticket JOIN public.ticket_orders AS orders ON orders.id=ticket.ticket_order_id WHERE orders.payment_intent_id='$($ticketCrashHosted.IntentID)'::uuid AND ticket.status='active')::text
"@
        if ($ticketInterrupted -ne 'ticket_issue_pending|issuing_tickets|issue_tickets|processing' -or $ticketShardInterrupted -ne '1|1|2') { throw 'ticket shard/control crash window was not durably split' }
        Set-M7CrashLeaseBarrier -Kind 'payment-action' -Service 'control-postgres' -TargetID $ticketCrashHosted.IntentID -ExpectedState 'processing'
        $interruptedPaymentEvidence = [ordered]@{
            capture_intent_id_sha256=(Get-M7SHA256 -Text $paymentCrashHosted.IntentID); ticket_intent_id_sha256=(Get-M7SHA256 -Text $ticketCrashHosted.IntentID);
            capture_pre_fence_control_state=$captureInterrupted; ticket_pre_fence_control_state=$ticketInterrupted;
            provider_effect=$captureProviderEffect; shard_issuance_receipts=1; issued_orders=1; issued_tickets=2; recovered_after_failover=$false
        }

        Invoke-M7Compose -Arguments @('exec','-T','payment-sandbox','wget','-q','-O','/dev/null','--post-data=',"http://127.0.0.1:8099/hosted/checkouts/$($fullRefundProviderHosted.ProviderPaymentID)/authorize") | Out-Null
        [void](Send-M7SandboxWebhooks -WebhookBaseURL (Get-M7PublishedURL))
        Invoke-M7TicketConflictWorker -Service 'payment-worker-1' -TargetID $fullRefundProviderHosted.IntentID -Scenario 'full-refund-provider-prepare'
        $fullRefundProviderAmount = [int64](Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL "SELECT amount_minor FROM public.payment_intents WHERE payment_intent_id='$($fullRefundProviderHosted.IntentID)'::uuid")
        Invoke-M7CrashWorker -Service 'payment-worker-1' -Point 'refund_provider_committed' -TargetID $fullRefundProviderHosted.IntentID -Scenario 'full-refund-provider-before-control'
        $fullRefundProviderInterrupted = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL "SELECT intent.state||'|'||operation.state||'|'||saga.state||'|'||saga.current_step FROM public.payment_intents AS intent JOIN public.payment_operations AS operation USING(payment_intent_id) JOIN public.payment_sagas AS saga USING(payment_intent_id) WHERE intent.payment_intent_id='$($fullRefundProviderHosted.IntentID)'::uuid AND operation.operation_type='refund'"
        if ($fullRefundProviderInterrupted -ne 'refund_pending|in_flight|refunding|refund') { throw "full-refund provider/control crash window was not durably split: $fullRefundProviderInterrupted" }
        $fullRefundProviderEffect = Get-M7SandboxEffectEvidence -IntentID $fullRefundProviderHosted.IntentID -ExpectedStatus 'refunded' -CapturedMinor $fullRefundProviderAmount -RefundedMinor $fullRefundProviderAmount
        Set-M7CrashLeaseBarrier -Kind 'payment-operation' -Service 'control-postgres' -TargetID $fullRefundProviderHosted.IntentID -ExpectedState 'in_flight'

        Invoke-M7Compose -Arguments @('exec','-T','payment-sandbox','wget','-q','-O','/dev/null','--post-data=',"http://127.0.0.1:8099/hosted/checkouts/$($fullRefundShardHosted.ProviderPaymentID)/authorize") | Out-Null
        [void](Send-M7SandboxWebhooks -WebhookBaseURL (Get-M7PublishedURL))
        Invoke-M7TicketConflictWorker -Service 'payment-worker-1' -TargetID $fullRefundShardHosted.IntentID -Scenario 'full-refund-shard-prepare'
        $fullRefundShardAmount = [int64](Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL "SELECT amount_minor FROM public.payment_intents WHERE payment_intent_id='$($fullRefundShardHosted.IntentID)'::uuid")
        Invoke-M7CrashWorker -Service 'payment-worker-1' -Point 'refund_compensation_shard_committed' -TargetID $fullRefundShardHosted.IntentID -Scenario 'full-refund-shard-before-control'
        $fullRefundShardInterrupted = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL "SELECT intent.state||'|'||saga.state||'|'||saga.current_step||'|'||action.state FROM public.payment_intents AS intent JOIN public.payment_sagas AS saga USING(payment_intent_id) JOIN public.payment_saga_actions AS action USING(saga_id) WHERE intent.payment_intent_id='$($fullRefundShardHosted.IntentID)'::uuid AND action.action_type='compensate'"
        $fullRefundShardReceipt = Get-M7Scalar -Service 'booking-shard-0-postgres' -User 'railway_booking' -Database 'railway_booking' -SQL "SELECT count(*) FROM public.payment_compensation_receipts WHERE payment_intent_id='$($fullRefundShardHosted.IntentID)'::uuid"
        if ($fullRefundShardInterrupted -ne 'refunded|refunding|compensate|processing' -or $fullRefundShardReceipt -ne '1') { throw 'full-refund shard/control crash window was not durably split' }
        $fullRefundShardEffect = Get-M7SandboxEffectEvidence -IntentID $fullRefundShardHosted.IntentID -ExpectedStatus 'refunded' -CapturedMinor $fullRefundShardAmount -RefundedMinor $fullRefundShardAmount
        Set-M7CrashLeaseBarrier -Kind 'payment-action' -Service 'control-postgres' -TargetID $fullRefundShardHosted.IntentID -ExpectedState 'processing'
        $interruptedFullRefundEvidence = [ordered]@{
            provider_pending=[ordered]@{ intent_id_sha256=(Get-M7SHA256 -Text $fullRefundProviderHosted.IntentID); pre_fence_state=$fullRefundProviderInterrupted; provider_effect=$fullRefundProviderEffect; recovered_after_failover=$false };
            shard_committed=[ordered]@{ intent_id_sha256=(Get-M7SHA256 -Text $fullRefundShardHosted.IntentID); pre_fence_state=$fullRefundShardInterrupted; provider_effect=$fullRefundShardEffect; compensation_receipts=1; recovered_after_failover=$false }
        }

        $providerRefundSelectedTicket = ($refundCrashTicketIDs -split ',')[0]
        $providerRefundResponse = Invoke-Milestone5DriverAPI -BaseURL (Get-M7PublishedURL) -Method POST `
            -Path "/api/v1/ticket-orders/$refundCrashOrderID/refunds" -Token $m7Customer.Token `
            -IdempotencyKey "m7-refund-provider-crash-$suffix" -Body @{ticket_ids=@($providerRefundSelectedTicket)} -ExpectedStatus @(202)
        $providerRefundRequestID = [string]$providerRefundResponse.Body.id
        $providerRefundOperationID = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL "SELECT refund_operation_id::text FROM public.ticket_refund_operations WHERE refund_request_id='$providerRefundRequestID'::uuid"
        $providerRefundFinancial = (Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL "SELECT intent.payment_intent_id::text||'|'||intent.amount_minor::text||'|'||request.amount_minor::text FROM public.ticket_refund_requests AS request JOIN public.payment_intents AS intent USING(payment_intent_id) WHERE request.refund_request_id='$providerRefundRequestID'::uuid") -split '\|'
        Invoke-M7CrashWorker -Service 'payment-worker-1' -Point 'partial_refund_provider_committed' -TargetID $providerRefundOperationID -Scenario 'partial-refund-provider-before-control'
        $providerRefundInterrupted = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT request.state||'|'||operation.state||'|'||saga.state||'|'||saga.current_step
FROM public.ticket_refund_requests AS request JOIN public.ticket_refund_operations AS operation USING(refund_request_id)
JOIN public.ticket_refund_sagas AS saga USING(refund_request_id)
WHERE request.refund_request_id='$providerRefundRequestID'::uuid
"@
        if ($providerRefundInterrupted -ne 'refund_pending|processing|refund_pending|refund_provider') { throw "provider refund crash window did not remain pending: $providerRefundInterrupted" }
        $providerRefundEffect = Get-M7SandboxEffectEvidence -IntentID $providerRefundFinancial[0] -ExpectedStatus 'refunded' -CapturedMinor ([int64]$providerRefundFinancial[1]) -RefundedMinor ([int64]$providerRefundFinancial[2])
        Set-M7CrashLeaseBarrier -Kind 'partial-refund-operation' -Service 'control-postgres' -TargetID $providerRefundOperationID -ExpectedState 'processing'

        $shardRefundSelectedTicket = ($refundShardCrashTicketIDs -split ',')[0]
        $shardRefundResponse = Invoke-Milestone5DriverAPI -BaseURL (Get-M7PublishedURL) -Method POST `
            -Path "/api/v1/ticket-orders/$refundShardCrashOrderID/refunds" -Token $m7Customer.Token `
            -IdempotencyKey "m7-refund-shard-crash-$suffix" -Body @{ticket_ids=@($shardRefundSelectedTicket)} -ExpectedStatus @(202)
        $shardRefundRequestID = [string]$shardRefundResponse.Body.id
        $shardRefundOperationID = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL "SELECT refund_operation_id::text FROM public.ticket_refund_operations WHERE refund_request_id='$shardRefundRequestID'::uuid"
        $shardRefundFinancial = (Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL "SELECT intent.payment_intent_id::text||'|'||intent.amount_minor::text||'|'||request.amount_minor::text FROM public.ticket_refund_requests AS request JOIN public.payment_intents AS intent USING(payment_intent_id) WHERE request.refund_request_id='$shardRefundRequestID'::uuid") -split '\|'
        Invoke-M7CrashWorker -Service 'payment-worker-1' -Point 'partial_refund_shard_committed' -TargetID $shardRefundOperationID -Scenario 'partial-refund-shard-before-control'
        $shardRefundInterrupted = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT request.state||'|'||operation.state||'|'||saga.state||'|'||saga.current_step
FROM public.ticket_refund_requests AS request JOIN public.ticket_refund_operations AS operation USING(refund_request_id)
JOIN public.ticket_refund_sagas AS saga USING(refund_request_id)
WHERE request.refund_request_id='$shardRefundRequestID'::uuid
"@
        $shardRefundReceipt = Get-M7Scalar -Service 'booking-shard-0-postgres' -User 'railway_booking' -Database 'railway_booking' -SQL @"
SELECT (SELECT count(*) FROM public.ticket_refund_compensation_receipts WHERE refund_request_id='$shardRefundRequestID'::uuid)::text||'|'||
       (SELECT count(*) FROM public.selected_ticket_refund_receipts WHERE refund_request_id='$shardRefundRequestID'::uuid)::text||'|'||
       (SELECT count(*) FROM public.tickets WHERE ticket_order_id='$refundShardCrashOrderID'::uuid AND status='refunded')::text||'|'||
       (SELECT count(*) FROM public.tickets WHERE ticket_order_id='$refundShardCrashOrderID'::uuid AND status='active')::text
"@
        if ($shardRefundInterrupted -ne 'refund_succeeded|succeeded|shard_compensating|compensate_shard' -or $shardRefundReceipt -ne '1|1|1|1') { throw 'refund shard/control crash window was not durably split' }
        $shardRefundEffect = Get-M7SandboxEffectEvidence -IntentID $shardRefundFinancial[0] -ExpectedStatus 'refunded' -CapturedMinor ([int64]$shardRefundFinancial[1]) -RefundedMinor ([int64]$shardRefundFinancial[2])
        Set-M7CrashLeaseBarrier -Kind 'partial-refund-saga' -Service 'control-postgres' -TargetID $shardRefundRequestID -ExpectedState 'shard_compensating'
        $interruptedRefundEvidence = [ordered]@{
            provider_pending=[ordered]@{ request_id_sha256=(Get-M7SHA256 -Text $providerRefundRequestID); pre_fence_state=$providerRefundInterrupted; provider_effect=$providerRefundEffect; recovered_after_failover=$false };
            shard_committed=[ordered]@{ request_id_sha256=(Get-M7SHA256 -Text $shardRefundRequestID); pre_fence_state=$shardRefundInterrupted; provider_effect=$shardRefundEffect; compensation_receipts=1; selected_receipts=1; refunded_tickets=1; active_tickets=1; recovered_after_failover=$false }
        }
        Add-M7Phase 'pre-fence-payment-and-refund-crash-windows-persisted'
        $webhookTimestamp = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds().ToString()
        $webhookOccurredAt = [DateTimeOffset]::UtcNow.ToString('o')
        $webhookAckBody = [ordered]@{
            provider_event_id="evt_m7_ack_$suffix"; type='payment.captured'; provider_payment_id="pay_m7_ack_$suffix"
            status='captured'; amount_minor=1000; currency='TWD'; occurred_at=$webhookOccurredAt
        } | ConvertTo-Json -Compress
        $webhookPreviousBody = [ordered]@{
            provider_event_id="evt_m7_previous_$suffix"; type='payment.captured'; provider_payment_id="pay_m7_previous_$suffix"
            status='captured'; amount_minor=1000; currency='TWD'; occurred_at=$webhookOccurredAt
        } | ConvertTo-Json -Compress
        $webhookCurrentBody = [ordered]@{
            provider_event_id="evt_m7_current_$suffix"; type='payment.captured'; provider_payment_id="pay_m7_current_$suffix"
            status='captured'; amount_minor=1000; currency='TWD'; occurred_at=$webhookOccurredAt
        } | ConvertTo-Json -Compress
        $webhookOutageStartedAt = [DateTimeOffset]::UtcNow
        Invoke-M7Compose -Arguments @('stop','-t','15','control-postgres') | Out-Null
        Invoke-M7K6 -Script 'webhook-ack-failure.js' -Environment @{
            VUS='1'; ITERATIONS_PER_VU='1'; DURATION='1m'; WEBHOOK_URL='http://api-1:8080/webhooks/payments/sandbox';
            WEBHOOK_BODY=$webhookAckBody; WEBHOOK_SIGNATURE_HEADER='X-Payment-Signature'; WEBHOOK_KEY_ID='current';
            WEBHOOK_TIMESTAMP=$webhookTimestamp; WEBHOOK_SIGNATURE=(Get-M7WebhookSignature -Key $webhookCurrentKey -Timestamp $webhookTimestamp -Body $webhookAckBody);
            EXPECTED_WEBHOOK_STATUS='500'
        }
        Invoke-M7Compose -Arguments @('start','control-postgres') | Out-Null
        Wait-M7Role -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -Recovery $false
        Wait-M7ServiceHTTP -Service 'api-1' -URL 'http://127.0.0.1:8080/readyz'
        Invoke-M7K6 -Script 'webhook-key-rotation.js' -Environment @{
            VUS='1'; ITERATIONS_PER_VU='1'; DURATION='1m'; WEBHOOK_URL='http://api-1:8080/webhooks/payments/sandbox';
            WEBHOOK_SIGNATURE_HEADER='X-Payment-Signature';
            WEBHOOK_PREVIOUS_BODY=$webhookPreviousBody; WEBHOOK_PREVIOUS_KEY_ID='previous'; WEBHOOK_PREVIOUS_TIMESTAMP=$webhookTimestamp;
            WEBHOOK_PREVIOUS_SIGNATURE=(Get-M7WebhookSignature -Key $webhookPreviousKey -Timestamp $webhookTimestamp -Body $webhookPreviousBody);
            WEBHOOK_CURRENT_BODY=$webhookCurrentBody; WEBHOOK_CURRENT_KEY_ID='current'; WEBHOOK_CURRENT_TIMESTAMP=$webhookTimestamp;
            WEBHOOK_CURRENT_SIGNATURE=(Get-M7WebhookSignature -Key $webhookCurrentKey -Timestamp $webhookTimestamp -Body $webhookCurrentBody)
        }
        $webhookDurability = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL "SELECT count(*) FROM public.payment_webhook_inbox WHERE provider_event_id IN ('evt_m7_previous_$suffix','evt_m7_current_$suffix')"
        if ($webhookDurability -ne '2') { throw 'webhook overlap did not durably record exactly two synthetic events' }

        $overlapLSN = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL 'SELECT pg_current_wal_lsn()::text'
        Wait-M7Replay -Service 'control-postgres-region-b' -User 'railway_control_replicator' -Database 'railway_control' -LSN $overlapLSN
        $env:REGION_B_DEPLOYMENT_ROLE = 'passive'
        $env:REGION_B_EPOCH = '1'
        $env:REGION_B_WRITES_ENABLED = 'false'
        $passiveAPIUp = @('--profile','dr-app','up','-d','--no-deps','--force-recreate')
        if ($SkipBuild) { $passiveAPIUp += '--no-build' }
        $passiveAPIUp += @(
            'redis-region-b','api-region-b-1','api-region-b-2','api-region-b-3','payment-reconciler-region-b',
            'admission-worker-region-b-1','admission-worker-region-b-2','read-model-worker-region-b-1','read-model-worker-region-b-2',
            'hold-expirer-region-b','outbox-worker-region-b','booking-command-reconciler-region-b'
        )
        Invoke-M7Compose -Arguments $passiveAPIUp | Out-Null
        foreach ($api in @('api-region-b-1','api-region-b-2','api-region-b-3')) { Wait-M7ServiceHTTP -Service $api -URL 'http://127.0.0.1:8080/livez' }
        foreach ($worker in @('admission-worker-region-b-1','admission-worker-region-b-2','read-model-worker-region-b-1','read-model-worker-region-b-2','hold-expirer-region-b','outbox-worker-region-b','booking-command-reconciler-region-b')) {
            Wait-M7ServiceHTTP -Service $worker -URL 'http://127.0.0.1:9090/livez'
        }
        Wait-M7ServiceHTTP -Service 'payment-reconciler-region-b' -URL 'http://127.0.0.1:9090/metrics'
        Assert-M7DurableMetrics -Service 'payment-reconciler-region-b' -Phase 'region-b-passive-before-failover' -Families @(
            'regional_replication_lag_bytes','regional_replication_lag_seconds','regional_last_replay_timestamp_seconds'
        ) -AllowZeroFamilies @('regional_replication_lag_bytes','regional_replication_lag_seconds') -ExpectedDatabaseTuples @(
            'region-b|control|none','region-b|booking_shard|shard-0','region-b|booking_shard|shard-1'
        )
        $previousSignature = Get-M7WebhookSignature -Key $webhookPreviousKey -Timestamp $webhookTimestamp -Body $webhookPreviousBody
        $currentSignature = Get-M7WebhookSignature -Key $webhookCurrentKey -Timestamp $webhookTimestamp -Body $webhookCurrentBody
        Invoke-M7ContainerWebhookStatus -Service 'api-region-b-1' -Body $webhookPreviousBody -KeyID 'previous' -Timestamp $webhookTimestamp -Signature $previousSignature -ExpectedStatus @(500) | Out-Null
        Invoke-M7ContainerWebhookStatus -Service 'api-region-b-2' -Body $webhookCurrentBody -KeyID 'current' -Timestamp $webhookTimestamp -Signature $currentSignature -ExpectedStatus @(500) | Out-Null
        Add-M7Phase 'passive-webhook-keys-verified-and-writes-rejected'

        $retiredBody = [ordered]@{
            provider_event_id="evt_m7_retired_$suffix"; type='payment.captured'; provider_payment_id="pay_m7_retired_$suffix";
            status='captured'; amount_minor=1000; currency='TWD'; occurred_at=$webhookOccurredAt
        } | ConvertTo-Json -Compress
        $postRotationBody = [ordered]@{
            provider_event_id="evt_m7_post_rotation_$suffix"; type='payment.captured'; provider_payment_id="pay_m7_post_rotation_$suffix";
            status='captured'; amount_minor=1000; currency='TWD'; occurred_at=$webhookOccurredAt
        } | ConvertTo-Json -Compress
        $retiredSignature = Get-M7WebhookSignature -Key $webhookPreviousKey -Timestamp $webhookTimestamp -Body $retiredBody
        $postRotationSignature = Get-M7WebhookSignature -Key $webhookCurrentKey -Timestamp $webhookTimestamp -Body $postRotationBody
        $env:M7_WEBHOOK_KEYRING = "current=$webhookCurrentB64"
        $env:M7_WEBHOOK_ACCEPT_KEY_IDS = 'current'
        $rotatedAPIUp = @('--profile','dr-app','up','-d','--no-deps','--force-recreate')
        if ($SkipBuild) { $rotatedAPIUp += '--no-build' }
        $rotatedAPIUp += @('api-1','api-2','api-3','api-region-b-1','api-region-b-2','api-region-b-3')
        Invoke-M7Compose -Arguments $rotatedAPIUp | Out-Null
        foreach ($api in @('api-1','api-2','api-3')) { Wait-M7ServiceHTTP -Service $api -URL 'http://127.0.0.1:8080/readyz' }
        foreach ($api in @('api-region-b-1','api-region-b-2','api-region-b-3')) { Wait-M7ServiceHTTP -Service $api -URL 'http://127.0.0.1:8080/livez' }
        Invoke-M7ContainerWebhookStatus -Service 'api-1' -Body $retiredBody -KeyID 'previous' -Timestamp $webhookTimestamp -Signature $retiredSignature -ExpectedStatus @(401) | Out-Null
        Invoke-M7ContainerWebhookStatus -Service 'api-2' -Body $postRotationBody -KeyID 'current' -Timestamp $webhookTimestamp -Signature $postRotationSignature -ExpectedStatus @(202) | Out-Null
        $rotationPersistence = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL "SELECT count(*) FILTER (WHERE provider_event_id='evt_m7_retired_$suffix')::text||'|'||count(*) FILTER (WHERE provider_event_id='evt_m7_post_rotation_$suffix')::text FROM public.payment_webhook_inbox"
        if ($rotationPersistence -ne '0|1') { throw 'retired webhook key was persisted or current generation was not durable' }
        $rotationLSN = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL 'SELECT pg_current_wal_lsn()::text'
        Wait-M7Replay -Service 'control-postgres-region-b' -User 'railway_control_replicator' -Database 'railway_control' -LSN $rotationLSN
        Invoke-M7ContainerWebhookStatus -Service 'api-region-b-1' -Body $retiredBody -KeyID 'previous' -Timestamp $webhookTimestamp -Signature $retiredSignature -ExpectedStatus @(401) | Out-Null
        Invoke-M7ContainerWebhookStatus -Service 'api-region-b-2' -Body $postRotationBody -KeyID 'current' -Timestamp $webhookTimestamp -Signature $postRotationSignature -ExpectedStatus @(500) | Out-Null
        Add-M7Phase 'webhook-previous-generation-retired'

        # Exercise the production Stripe verifier and durable key lifecycle on
        # dedicated ingress replicas. The sandbox overlap above remains a
        # durability probe and is not represented as Stripe grace evidence.
        $stripeTimestamp = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds().ToString()
        $newStripeBody = {
            param([string]$EventID,[string]$PaymentID)
            return ([ordered]@{
                id=$EventID; type='payment_intent.succeeded'; api_version='2026-07-29.dahlia';
                account='acct_m7_contract'; livemode=$false; created=[int64]$stripeTimestamp;
                data=[ordered]@{object=[ordered]@{
                    id=$PaymentID; amount=1000; amount_received=1000; currency='twd';
                    status='succeeded'; created=[int64]$stripeTimestamp
                }}
            } | ConvertTo-Json -Depth 6 -Compress)
        }
        $stripeUp = @('--profile','dr-app','up','-d','--no-deps','--force-recreate')
        if ($SkipBuild) { $stripeUp += '--no-build' }

        $env:M7_STRIPE_WEBHOOK_KEYRING = "previous=$stripeWebhookPrevious"
        $env:M7_STRIPE_WEBHOOK_PRIMARY_KEY_ID = 'previous'
        $env:M7_STRIPE_WEBHOOK_ACCEPT_KEY_IDS = 'previous'
        Invoke-M7Compose -Arguments ($stripeUp + @('stripe-webhook-api-region-a')) | Out-Null
        Wait-M7ServiceHTTP -Service 'stripe-webhook-api-region-a' -URL 'http://127.0.0.1:8080/readyz'

        $env:M7_STRIPE_WEBHOOK_KEYRING = "previous=$stripeWebhookPrevious,current=$stripeWebhookCurrent"
        $env:M7_STRIPE_WEBHOOK_PRIMARY_KEY_ID = 'previous'
        $env:M7_STRIPE_WEBHOOK_ACCEPT_KEY_IDS = 'previous,current'
        Invoke-M7Compose -Arguments ($stripeUp + @('stripe-webhook-api-region-a')) | Out-Null
        Wait-M7ServiceHTTP -Service 'stripe-webhook-api-region-a' -URL 'http://127.0.0.1:8080/readyz'
        $stripeStaged = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT string_agg(key_id||':'||state||':'||COALESCE(retirement_not_before::text,'none'),';' ORDER BY key_id)
FROM public.payment_webhook_key_versions WHERE provider='stripe' AND provider_account_id='acct_m7_contract'
"@
        if ($stripeStaged -ne 'current:accepted:none;previous:primary:none') { throw "Stripe staged keyring started grace or had wrong state: $stripeStaged" }
        $stripePreviousOverlapBody = & $newStripeBody "evt_m7_stripe_previous_overlap_$suffix" "pi_m7_stripe_previous_overlap_$suffix"
        $stripeCurrentOverlapBody = & $newStripeBody "evt_m7_stripe_current_overlap_$suffix" "pi_m7_stripe_current_overlap_$suffix"
        Invoke-M7StripeWebhookStatus -Service 'stripe-webhook-api-region-a' -Body $stripePreviousOverlapBody -Timestamp $stripeTimestamp -Signature (Get-M7WebhookSignature -Key ([Text.Encoding]::UTF8.GetBytes($stripeWebhookPrevious)) -Timestamp $stripeTimestamp -Body $stripePreviousOverlapBody) -ExpectedStatus @(202) | Out-Null
        Invoke-M7StripeWebhookStatus -Service 'stripe-webhook-api-region-a' -Body $stripeCurrentOverlapBody -Timestamp $stripeTimestamp -Signature (Get-M7WebhookSignature -Key ([Text.Encoding]::UTF8.GetBytes($stripeWebhookCurrent)) -Timestamp $stripeTimestamp -Body $stripeCurrentOverlapBody) -ExpectedStatus @(202) | Out-Null

        $env:M7_STRIPE_WEBHOOK_PRIMARY_KEY_ID = 'current'
        $env:M7_STRIPE_WEBHOOK_ACCEPT_KEY_IDS = 'current,previous'
        Invoke-M7Compose -Arguments ($stripeUp + @('stripe-webhook-api-region-a')) | Out-Null
        Wait-M7ServiceHTTP -Service 'stripe-webhook-api-region-a' -URL 'http://127.0.0.1:8080/readyz'
        $stripeDemoted = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT (state='accepted' AND retirement_not_before>clock_timestamp())::text
FROM public.payment_webhook_key_versions
WHERE provider='stripe' AND provider_account_id='acct_m7_contract' AND key_id='previous'
"@
        if ($stripeDemoted -ne 'true') { throw 'Stripe previous key grace did not begin on primary demotion' }
        $stripeGraceBody = & $newStripeBody "evt_m7_stripe_grace_$suffix" "pi_m7_stripe_grace_$suffix"
        Invoke-M7StripeWebhookStatus -Service 'stripe-webhook-api-region-a' -Body $stripeGraceBody -Timestamp $stripeTimestamp -Signature (Get-M7WebhookSignature -Key ([Text.Encoding]::UTF8.GetBytes($stripeWebhookPrevious)) -Timestamp $stripeTimestamp -Body $stripeGraceBody) -ExpectedStatus @(202) | Out-Null

        $stripeGraceExpired = $false
        for ($attempt = 0; $attempt -lt 20; $attempt++) {
            $expired = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL "SELECT (clock_timestamp()>=retirement_not_before)::text FROM public.payment_webhook_key_versions WHERE provider='stripe' AND provider_account_id='acct_m7_contract' AND key_id='previous'"
            if ($expired -eq 'true') { $stripeGraceExpired = $true; break }
            Start-Sleep -Seconds 1
        }
        if (-not $stripeGraceExpired) { throw 'Stripe webhook retirement grace did not expire within the bounded probe' }
        $stripeExpiredBody = & $newStripeBody "evt_m7_stripe_expired_$suffix" "pi_m7_stripe_expired_$suffix"
        Invoke-M7StripeWebhookStatus -Service 'stripe-webhook-api-region-a' -Body $stripeExpiredBody -Timestamp $stripeTimestamp -Signature (Get-M7WebhookSignature -Key ([Text.Encoding]::UTF8.GetBytes($stripeWebhookPrevious)) -Timestamp $stripeTimestamp -Body $stripeExpiredBody) -ExpectedStatus @(401) | Out-Null

        $env:M7_STRIPE_WEBHOOK_KEYRING = "current=$stripeWebhookCurrent"
        $env:M7_STRIPE_WEBHOOK_ACCEPT_KEY_IDS = 'current'
        Invoke-M7Compose -Arguments ($stripeUp + @('stripe-webhook-api-region-a')) | Out-Null
        Wait-M7ServiceHTTP -Service 'stripe-webhook-api-region-a' -URL 'http://127.0.0.1:8080/readyz'
        $stripeRetirement = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT version.state||'|'||
       (SELECT count(*)::text FROM public.payment_webhook_key_rotation_audit WHERE provider='stripe' AND provider_account_id='acct_m7_contract')||'|'||
       (SELECT count(*)::text FROM public.payment_webhook_key_version_archive WHERE provider='stripe' AND provider_account_id='acct_m7_contract')
FROM public.payment_webhook_key_versions AS version
WHERE version.provider='stripe' AND version.provider_account_id='acct_m7_contract' AND version.key_id='previous'
"@
        $stripeRetirementParts = $stripeRetirement -split '\|'
        if ($stripeRetirementParts.Count -ne 3 -or $stripeRetirementParts[0] -ne 'retired' -or
            [int]$stripeRetirementParts[1] -lt 5 -or [int]$stripeRetirementParts[2] -ne 0) {
            throw "Stripe retirement/audit evidence was incomplete: $stripeRetirement"
        }
        $stripeCurrentAfterBody = & $newStripeBody "evt_m7_stripe_current_after_$suffix" "pi_m7_stripe_current_after_$suffix"
        Invoke-M7StripeWebhookStatus -Service 'stripe-webhook-api-region-a' -Body $stripeCurrentAfterBody -Timestamp $stripeTimestamp -Signature (Get-M7WebhookSignature -Key ([Text.Encoding]::UTF8.GetBytes($stripeWebhookCurrent)) -Timestamp $stripeTimestamp -Body $stripeCurrentAfterBody) -ExpectedStatus @(202) | Out-Null

        $stripeLSN = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL 'SELECT pg_current_wal_lsn()::text'
        Wait-M7Replay -Service 'control-postgres-region-b' -User 'railway_control_replicator' -Database 'railway_control' -LSN $stripeLSN
        Invoke-M7Compose -Arguments ($stripeUp + @('stripe-webhook-api-region-b')) | Out-Null
        Wait-M7ServiceHTTP -Service 'stripe-webhook-api-region-b' -URL 'http://127.0.0.1:8080/livez'
        $stripePassiveCurrentBody = & $newStripeBody "evt_m7_stripe_passive_current_$suffix" "pi_m7_stripe_passive_current_$suffix"
        Invoke-M7StripeWebhookStatus -Service 'stripe-webhook-api-region-b' -Body $stripeExpiredBody -Timestamp $stripeTimestamp -Signature (Get-M7WebhookSignature -Key ([Text.Encoding]::UTF8.GetBytes($stripeWebhookPrevious)) -Timestamp $stripeTimestamp -Body $stripeExpiredBody) -ExpectedStatus @(401) | Out-Null
        Invoke-M7StripeWebhookStatus -Service 'stripe-webhook-api-region-b' -Body $stripePassiveCurrentBody -Timestamp $stripeTimestamp -Signature (Get-M7WebhookSignature -Key ([Text.Encoding]::UTF8.GetBytes($stripeWebhookCurrent)) -Timestamp $stripeTimestamp -Body $stripePassiveCurrentBody) -ExpectedStatus @(500) | Out-Null
        $stripeRotationPersistence = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL "SELECT count(*) FROM public.payment_webhook_inbox WHERE provider='stripe' AND provider_event_id LIKE 'evt_m7_stripe_%_$suffix'"
        if ($stripeRotationPersistence -ne '4') { throw "Stripe rotation persisted an unexpected event count: $stripeRotationPersistence" }
        $stripeRotationEvidence = [ordered]@{
            provider='stripe'; staged_without_grace=$true; grace_started_on_demotion=$true;
            previous_accepted_during_grace=$true; previous_rejected_after_grace=$true;
            current_accepted_after_retirement=$true; passive_current_verified_then_fenced=$true;
            immutable_transition_audit=$true; durable_event_count=4
        }
        Invoke-M7Compose -Arguments @('stop','-t','15','stripe-webhook-api-region-a','stripe-webhook-api-region-b') | Out-Null
        Add-M7Phase 'stripe-webhook-durable-grace-lifecycle-proven'
        $preFenceCrashWindows = Get-M7Scalar -Service 'control-postgres' -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT
 (SELECT intent.state||'|'||operation.state||'|'||(operation.lease_until>clock_timestamp())::text FROM public.payment_intents AS intent JOIN public.payment_operations AS operation USING(payment_intent_id) WHERE intent.payment_intent_id='$($paymentCrashHosted.IntentID)'::uuid AND operation.operation_type='capture')||';'||
 (SELECT intent.state||'|'||saga.state||'|'||saga.current_step||'|'||action.state||'|'||(action.lease_until>clock_timestamp())::text FROM public.payment_intents AS intent JOIN public.payment_sagas AS saga USING(payment_intent_id) JOIN public.payment_saga_actions AS action USING(saga_id) WHERE intent.payment_intent_id='$($ticketCrashHosted.IntentID)'::uuid AND action.action_type='issue_tickets')||';'||
 (SELECT intent.state||'|'||operation.state||'|'||saga.state||'|'||saga.current_step||'|'||(operation.lease_until>clock_timestamp())::text FROM public.payment_intents AS intent JOIN public.payment_operations AS operation USING(payment_intent_id) JOIN public.payment_sagas AS saga USING(payment_intent_id) WHERE intent.payment_intent_id='$($fullRefundProviderHosted.IntentID)'::uuid AND operation.operation_type='refund')||';'||
 (SELECT intent.state||'|'||saga.state||'|'||saga.current_step||'|'||action.state||'|'||(action.lease_until>clock_timestamp())::text FROM public.payment_intents AS intent JOIN public.payment_sagas AS saga USING(payment_intent_id) JOIN public.payment_saga_actions AS action USING(saga_id) WHERE intent.payment_intent_id='$($fullRefundShardHosted.IntentID)'::uuid AND action.action_type='compensate')||';'||
 (SELECT request.state||'|'||operation.state||'|'||saga.state||'|'||saga.current_step||'|'||(operation.lease_until>clock_timestamp())::text FROM public.ticket_refund_requests AS request JOIN public.ticket_refund_operations AS operation USING(refund_request_id) JOIN public.ticket_refund_sagas AS saga USING(refund_request_id) WHERE operation.refund_operation_id='$providerRefundOperationID'::uuid)||';'||
 (SELECT request.state||'|'||operation.state||'|'||saga.state||'|'||saga.current_step||'|'||(saga.lease_until>clock_timestamp())::text FROM public.ticket_refund_requests AS request JOIN public.ticket_refund_operations AS operation USING(refund_request_id) JOIN public.ticket_refund_sagas AS saga USING(refund_request_id) WHERE request.refund_request_id='$shardRefundRequestID'::uuid)
"@
        $expectedPreFenceCrashWindows = 'capture_pending|in_flight|true;ticket_issue_pending|issuing_tickets|issue_tickets|processing|true;refund_pending|in_flight|refunding|refund|true;refunded|refunding|compensate|processing|true;refund_pending|processing|refund_pending|refund_provider|true;refund_succeeded|succeeded|shard_compensating|compensate_shard|true'
        if ($preFenceCrashWindows -cne $expectedPreFenceCrashWindows) { throw "application crash windows changed before the external fence: $preFenceCrashWindows" }
        Add-M7Phase 'six-application-crash-windows-parked-before-source-fence'
        foreach ($database in $databases) {
            $applicationLSN = Get-M7Scalar -Service $database.Primary -User $database.User -Database $database.Database -SQL 'SELECT pg_current_wal_lsn()::text'
            Wait-M7Replay -Service $database.Standby -User $database.ReplicationUser -Database $database.Database -LSN $applicationLSN
        }
        Add-M7Phase 'region-a-active-application-ready'

        $failoverStart = [DateTimeOffset]::UtcNow
        Ensure-M7ServicesStopped -Services $regionAAppServices -OperationKind 'failover' -Boundary 'region-a-applications' -CrashReobserve | Out-Null
        foreach ($database in $databases) {
            $sourceObservedAt = [DateTimeOffset]::UtcNow
            $position = Get-M7Scalar -Service $database.Primary -User $database.User -Database $database.Database -SQL "SELECT ((pg_control_checkpoint()).timeline_id)::text||'|'||pg_current_wal_lsn()::text||'|'||pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0')::bigint::text"
            $parts = $position -split '\|'
            if ($parts.Count -ne 3) { throw "preliminary source WAL position was malformed for $($database.Name)" }
            $failoverPositions[$database.Name] = [ordered]@{ timeline=[uint32]$parts[0]; lsn=$parts[1]; wal=[uint64]$parts[2]; observed_at=$sourceObservedAt.ToString('o') }
        }
        $prePromotionFenceObservation = [ordered]@{
            region='region-a'; stopped_writer_processes=$regionAAppServices.Count; source_primaries_running_for_journal=$true;
            ingress_stopped=$true; credential_consumers_stopped=$true; application_database_clients_stopped=$true;
            promotion_permitted_only_after_durable_markers=$true; observed_at=[DateTimeOffset]::UtcNow.ToString('o')
        }
        Write-M7JSON -Name 'region-a-pre-promotion-fence-observation.json' -Value $prePromotionFenceObservation
        $prePromotionFenceText = $prePromotionFenceObservation | ConvertTo-Json -Compress
        $signedFenceEvidence = New-M7SignedFenceEvidence -OperationID $failoverOperationID -SourceRegion 'region-a' -SourceEpoch 1 -Purpose 'initial_fence' `
            -IncidentID $failoverIncidentID -OperatorID 'operator:local-dr' -ObservationText $prePromotionFenceText
        Advance-M7DRPhase -Stage 'external_fencing_verified' -CrashOnce -Evidence $signedFenceEvidence
        Advance-M7DRPhase -Stage 'positions_recorded' -Evidence @{
            control=@{timeline=$failoverPositions['control'].timeline;wal=$failoverPositions['control'].wal}
            shard_0=@{timeline=$failoverPositions['shard-0'].timeline;wal=$failoverPositions['shard-0'].wal}
            shard_1=@{timeline=$failoverPositions['shard-1'].timeline;wal=$failoverPositions['shard-1'].wal}
        }
        Advance-M7DRPhase -Stage 'passive_readiness_removed' -Evidence @{ artifact_sha256=(Get-M7SHA256 "$prePromotionFenceText|readiness-removed") }
        foreach ($database in $databases) {
            $journalLSN = Get-M7Scalar -Service $database.Primary -User $database.User -Database $database.Database -SQL 'SELECT pg_current_wal_lsn()::text'
            Wait-M7Replay -Service $database.Standby -User $database.ReplicationUser -Database $database.Database -LSN $journalLSN
        }
        Add-M7Phase 'failover-journal-durable-before-final-rpo-marker'
        foreach ($database in $databases) {
            $sourceObservedAt = [DateTimeOffset]::Parse((Get-M7Scalar -Service $database.Primary -User $database.User -Database $database.Database -SQL 'INSERT INTO public.dr_evidence_markers(marker) VALUES (2) RETURNING created_at::text'))
            $position = Get-M7Scalar -Service $database.Primary -User $database.User -Database $database.Database -SQL "SELECT ((pg_control_checkpoint()).timeline_id)::text||'|'||pg_current_wal_lsn()::text||'|'||pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0')::bigint::text"
            $parts = $position -split '\|'
            if ($parts.Count -ne 3) { throw "final source WAL position was malformed for $($database.Name)" }
            $failoverPositions[$database.Name] = [ordered]@{ timeline=[uint32]$parts[0]; lsn=$parts[1]; wal=[uint64]$parts[2]; source_marker=2; observed_at=$sourceObservedAt.ToString('o') }
        }
        Add-M7Phase 'failover-final-rpo-markers-committed-without-catchup-wait'
        Ensure-M7ServicesStopped -Services @($databases.Primary) -OperationKind 'failover' -Boundary 'region-a-databases' -CrashReobserve | Out-Null
        foreach ($service in $regionAAppServices) {
            $running = Invoke-M7Compose -AllowFailure -Arguments @('ps','--status','running','-q',$service)
            if (@($running.Output | Where-Object { $_.Trim() -ne '' }).Count -ne 0) { throw "external application fence failed for $service" }
        }
        foreach ($database in $databases) {
            $running = Invoke-M7Compose -AllowFailure -Arguments @('ps','--status','running','-q',$database.Primary)
            if (@($running.Output | Where-Object { $_.Trim() -ne '' }).Count -ne 0) { throw "external fence failed for $($database.Primary)" }
            $replay = Get-M7Scalar -Service $database.Standby -User $database.ReplicationUser -Database $database.Database -SQL "SELECT pg_last_wal_replay_lsn()::text||'|'||pg_wal_lsn_diff(pg_last_wal_replay_lsn(),'0/0')::bigint::text"
            $replayParts = $replay -split '\|'
            if ($replayParts.Count -ne 2) { throw "standby replay position was malformed for $($database.Name)" }
            $standbyReplayPositions[$database.Name] = [ordered]@{ timeline=[uint32]$failoverPositions[$database.Name].timeline; lsn=$replayParts[0]; wal=[uint64]$replayParts[1]; observed_at=[DateTimeOffset]::UtcNow.ToString('o') }
        }
        Invoke-M7Compose -Arguments @('rm','-sf','redis') | Out-Null
        Remove-M7ProjectVolume -Suffix 'redis-data'
        Add-M7Phase 'region-a-redis-loss-boundary-enforced'
        Add-M7Phase 'region-a-externally-fenced'
        $fenceObservation = [ordered]@{
            region='region-a'; stopped_writer_processes=$regionAAppServices.Count; stopped_databases=$databases.Count;
            ingress_stopped=$true; credential_consumers_stopped=$true; database_network_endpoints_stopped=$true;
            redis_volume_destroyed=$true; observed_at=[DateTimeOffset]::UtcNow.ToString('o')
        }
        Write-M7JSON -Name 'region-a-fence-observation.json' -Value $fenceObservation
        $fenceObservationText = $fenceObservation | ConvertTo-Json -Compress
        $promotedPositions = @{}
        foreach ($database in $databases) {
            Refresh-M7DRFence -OperationID $failoverOperationID -Prefix 'dr-phase' -Boundary "pre-$($database.Name)-promotion"
            $promotionReobservation = Ensure-M7PromotedPrimary -Service $database.Standby -User $database.User -Database $database.Database `
                -OperationKind 'failover' -DatabaseName $database.Name -CrashReobserve:($database.Name -in @('control','shard-0'))
            $position = Get-M7Scalar -Service $database.Standby -User $database.User -Database $database.Database -SQL "SELECT ((pg_control_checkpoint()).timeline_id)::text||'|'||pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0')::bigint::text"
            $parts = $position -split '\|'
            $promotedPositions[$database.Name] = [ordered]@{ timeline=[uint32]$parts[0]; wal=[uint64]$parts[1] }
            if ($database.Name -eq 'control') {
                $env:DR_RECOVERY_EPOCH = '2'
                $env:DR_JOURNAL_REGION = 'region-b'
                $env:DR_JOURNAL_DATABASE_URL = 'postgresql://railway_control:control-local-only@control-postgres-region-b:5432/railway_control?sslmode=disable&connect_timeout=3'
            }
            $stage = @{'control'='control_promoted';'shard-0'='shard_0_promoted';'shard-1'='shard_1_promoted'}[$database.Name]
            Advance-M7DRPhase -Stage $stage -CrashOnce:($stage -in @('control_promoted','shard_0_promoted')) -Evidence @{
                database=$database.Name; position=$promotedPositions[$database.Name]
            }
        }
        Advance-M7DRPhase -Stage 'roles_and_timelines_verified' -Evidence @{
            control=@{role='primary';timeline=$promotedPositions['control'].timeline};
            shard_0=@{role='primary';timeline=$promotedPositions['shard-0'].timeline};
            shard_1=@{role='primary';timeline=$promotedPositions['shard-1'].timeline}
        }
        Advance-M7DRPhase -Stage 'epoch_allocated' -CrashOnce -Evidence @{ target_epoch=2 }
        $updated = Invoke-M7AuthorityTransition -Service 'control-postgres-region-b' -User 'railway_control' -Database 'railway_control' -Region 'region-b' -Epoch 2 -State 'recovery' -Writes $false
        if ($updated -ne '1') { throw 'region-b recovery authority update failed for control' }
        Advance-M7DRPhase -Stage 'control_recovery_installed' -CrashOnce -Evidence @{}
        foreach ($database in @($databases | Where-Object { $_.Name -ne 'control' })) {
            $updated = Invoke-M7AuthorityTransition -Service $database.Standby -User $database.User -Database $database.Database -Region 'region-b' -Epoch 2 -State 'recovery' -Writes $false
            if ($updated -ne '1') { throw "region-b recovery authority update failed for $($database.Name)" }
        }
        Advance-M7DRPhase -Stage 'shard_authorities_installed' -Evidence @{}
        Add-M7Phase 'region-b-promoted'
        foreach ($database in $databases) {
            Assert-M7StaleWriterRejected -Service $database.Standby -User $database.User -Database $database.Database -Region 'region-a' -Epoch 1
        }
        Add-M7Phase 'region-a-stale-writers-rejected'

        $env:REGION_B_DEPLOYMENT_ROLE = 'recovery'
        $env:REGION_B_EPOCH = '2'
        $env:REGION_B_WRITES_ENABLED = 'false'
        $env:ACTIVE_REGION_UPSTREAM = 'proxy-region-b'
        # Recovery starts only the dependency and API surfaces required for
        # reconciliation. Mutating workers and the regional proxy remain
        # stopped until all three database authorities are active.
        $recoveryAppUp = @('--profile','dr-app','up','-d','--no-deps','--force-recreate')
        if ($SkipBuild) { $recoveryAppUp += '--no-build' }
        $recoveryAppUp += @(
            'payment-stripe-contract','payment-sandbox','redis-region-b',
            'api-region-b-1','api-region-b-2','api-region-b-3'
        )
        Invoke-M7Compose -Arguments $recoveryAppUp | Out-Null
        Wait-M7ServiceHTTP -Service 'payment-stripe-contract' -URL 'http://127.0.0.1:8100/readyz'
        foreach ($api in @('api-region-b-1','api-region-b-2','api-region-b-3')) { Wait-M7ServiceHTTP -Service $api -URL 'http://127.0.0.1:8080/livez' }
        $recoveryAPIHash = Get-M7RunningServiceHash -Services @('api-region-b-1','api-region-b-2','api-region-b-3')
        Advance-M7DRPhase -Stage 'recovery_apis_started' -Evidence @{ artifact_sha256=$recoveryAPIHash }

        $prematureRegionBServices = @(
            'payment-worker-region-b-1','payment-worker-region-b-2','payment-reconciler-region-b',
            'admission-worker-region-b-1','admission-worker-region-b-2','read-model-worker-region-b-1','read-model-worker-region-b-2',
            'hold-expirer-region-b','outbox-worker-region-b','booking-command-reconciler-region-b',
            'settlement-worker-region-b','proxy-region-b'
        )
        foreach ($service in $prematureRegionBServices) {
            $running = Invoke-M7Compose -AllowFailure -Arguments @('ps','--status','running','-q',$service)
            if (@($running.Output | Where-Object { $_.Trim() -ne '' }).Count -ne 0) { throw "region-b worker or proxy started before complete database activation: $service" }
        }
        Invoke-M7SQLFile -Service 'control-postgres-region-b' -User 'railway_control' -Database 'railway_control' -Path 'migrations/testdata/assert_milestone7_v11_data.sql'
        Invoke-M7SQLFile -Service 'booking-shard-0-postgres-region-b' -User 'railway_booking' -Database 'railway_booking' -Path 'migrations/testdata/assert_booking_shard_v3_data.sql'
        Invoke-M7SQLFile -Service 'booking-shard-1-postgres-region-b' -User 'railway_booking' -Database 'railway_booking' -Path 'migrations/testdata/assert_booking_shard_v3_data.sql'
        $reconciliationObservation = Get-M7ReconciliationObservation -ControlService 'control-postgres-region-b' `
            -Shard0Service 'booking-shard-0-postgres-region-b' -Shard1Service 'booking-shard-1-postgres-region-b'
        Write-M7JSON -Name 'region-b-reconciliation-observation.json' -Value $reconciliationObservation
        Advance-M7DRPhase -Stage 'reconciled' -Evidence @{
            control=$true; shards=$true; payments=$true; tickets=$true; refunds=$true; ledger=$true; routing=$true;
            artifact_sha256=(Get-M7SHA256 -Text ($reconciliationObservation | ConvertTo-Json -Compress))
        }
        $preSwitchIngress = Invoke-M7Compose -AllowFailure -Arguments @('ps','--status','running','-q','global-test-ingress')
        if (@($preSwitchIngress.Output | Where-Object { $_.Trim() -ne '' }).Count -ne 0) { throw 'global ingress was running before complete database activation' }

        # Activate both shard authorities first. Control is the final durable
        # commit that declares the three-database authority set complete.
        foreach ($database in @($databases | Where-Object { $_.Name -ne 'control' })) {
            $updated = Invoke-M7AuthorityTransition -Service $database.Standby -User $database.User -Database $database.Database -Region 'region-b' -Epoch 2 -State 'active' -Writes $true
            if ($updated -ne '1') { throw "region-b activation failed for $($database.Name)" }
        }
        $updated = Invoke-M7AuthorityTransition -Service 'control-postgres-region-b' -User 'railway_control' -Database 'railway_control' -Region 'region-b' -Epoch 2 -State 'active' -Writes $true
        if ($updated -ne '1') { throw 'region-b final control activation failed' }
        $completeAuthority = foreach ($database in $databases) {
            Get-M7Scalar -Service $database.Standby -User $database.User -Database $database.Database -SQL "SELECT region||'|'||epoch::text||'|'||state||'|'||writes_enabled::text||'|'||pg_is_in_recovery()::text FROM public.regional_write_authority WHERE singleton"
        }
        if (@($completeAuthority | Where-Object { $_ -cne 'region-b|2|active|true|false' }).Count -ne 0) { throw 'region-b authority set was not completely active' }
        Write-M7JSON -Name 'region-b-complete-authority-set.json' -Value ([ordered]@{control_last=$true; databases=@($completeAuthority); complete=$true})

        $env:REGION_B_DEPLOYMENT_ROLE = 'active'
        $env:REGION_B_WRITES_ENABLED = 'true'
        $activeAppUp = @('--profile','dr-app','up','-d','--no-deps','--force-recreate')
        if ($SkipBuild) { $activeAppUp += '--no-build' }
        $activeAppUp += @(
            'payment-stripe-contract','payment-sandbox','redis-region-b',
            'api-region-b-1','api-region-b-2','api-region-b-3',
            'payment-worker-region-b-1','payment-worker-region-b-2','payment-reconciler-region-b',
            'admission-worker-region-b-1','admission-worker-region-b-2','read-model-worker-region-b-1','read-model-worker-region-b-2',
            'hold-expirer-region-b','outbox-worker-region-b','booking-command-reconciler-region-b',
            'settlement-worker-region-b','proxy-region-b'
        )
        Invoke-M7Compose -Arguments $activeAppUp | Out-Null
        foreach ($api in @('api-region-b-1','api-region-b-2','api-region-b-3')) { Wait-M7ServiceHTTP -Service $api -URL 'http://127.0.0.1:8080/readyz' }
        foreach ($worker in @('payment-worker-region-b-1','payment-worker-region-b-2')) { Wait-M7ServiceHTTP -Service $worker -URL 'http://127.0.0.1:9090/readyz' }
        Wait-M7ServiceHTTP -Service 'settlement-worker-region-b' -URL 'http://127.0.0.1:9090/readyz'
        Advance-M7DRPhase -Stage 'payment_workers_enabled' -Evidence @{ authority_set_complete=$true; artifact_sha256=(Get-M7RunningServiceHash -Services @('payment-worker-region-b-1','payment-worker-region-b-2')) }
        Advance-M7DRPhase -Stage 'settlement_workers_enabled' -Evidence @{ authority_set_complete=$true; artifact_sha256=(Get-M7RunningServiceHash -Services @('settlement-worker-region-b')) }

        $globalIngressUp = @('--profile','dr-app','up','-d','--no-deps','--force-recreate')
        if ($SkipBuild) { $globalIngressUp += '--no-build' }
        $globalIngressUp += 'global-test-ingress'
        Invoke-M7Compose -Arguments $globalIngressUp | Out-Null
        Wait-M7ServiceHTTP -Service 'global-test-ingress' -URL 'http://127.0.0.1:8080/readyz'
        Advance-M7DRPhase -Stage 'ingress_switched' -CrashOnce -Evidence @{
            webhook=$true; global=$true; external_action_reobserved=$true;
            artifact_sha256=(Get-M7RunningServiceHash -Services @('proxy-region-b','global-test-ingress'))
        }
        Add-M7Phase 'global-ingress-switched-to-region-b-after-complete-authority'
        Advance-M7DRPhase -Stage 'customer_writes_configured' -CrashOnce -Evidence @{ enabled=$true; readiness_gated=$true; all_database_authorities_active=$true; artifact_sha256=(Get-M7RunningServiceHash -Services @('api-region-b-1','api-region-b-2','api-region-b-3')) }
        $rpoWindows = @{}
        $rpoMissingRecords = @{}
        foreach ($database in $databases) {
            $sent = [uint64]$failoverPositions[$database.Name].wal
            $replayed = [uint64]$standbyReplayPositions[$database.Name].wal
            $missingWalBytes = if ($sent -gt $replayed) { $sent - $replayed } else { [uint64]0 }
            $targetMarkerCount = [int](Get-M7Scalar -Service $database.Standby -User $database.User -Database $database.Database -SQL 'SELECT count(*) FROM public.dr_evidence_markers WHERE marker=2')
            if ($targetMarkerCount -lt 0 -or $targetMarkerCount -gt 1) { throw "promoted marker observation was invalid for $($database.Name)" }
            $missingRecords = 1 - $targetMarkerCount
            $sourceObservedAt = [DateTimeOffset]::Parse([string]$failoverPositions[$database.Name].observed_at)
            $replayObservedAt = [DateTimeOffset]::Parse([string]$standbyReplayPositions[$database.Name].observed_at)
            $rpoWindowMS = [Math]::Max(0,[int64]($replayObservedAt.Subtract($sourceObservedAt).TotalMilliseconds))
            $rpoWindows[$database.Name] = $rpoWindowMS
            $rpoMissingRecords[$database.Name] = $missingRecords
            $rpoEvidence.Add([ordered]@{
                database=$database.Name; source_timeline=[uint32]$failoverPositions[$database.Name].timeline;
                standby_replay_timeline=[uint32]$standbyReplayPositions[$database.Name].timeline;
                promoted_timeline=[uint32]$promotedPositions[$database.Name].timeline; source_end_lsn=$failoverPositions[$database.Name].lsn;
                standby_replay_lsn=$standbyReplayPositions[$database.Name].lsn; sent_wal_bytes=$sent;
                replayed_wal_bytes=$replayed; missing_wal_bytes=$missingWalBytes; source_marker=2; target_marker_count=$targetMarkerCount; missing_records=$missingRecords;
                source_observed_at=$sourceObservedAt.ToString('o'); replay_observed_at=$replayObservedAt.ToString('o'); observation_window_ms=$rpoWindowMS;
                window_definition='acknowledged source marker observation to promoted target replay observation'
            })
        }
        $missingMarkers = [int](($rpoEvidence | Measure-Object -Property missing_records -Sum).Sum)
        $rtoMS = [Math]::Max(1,[int64]([DateTimeOffset]::UtcNow.Subtract($failoverStart).TotalMilliseconds))
        $failoverRTOEvidence = [ordered]@{ duration_ms=$rtoMS; authority_active=$true; customer_readiness=$true; ingress_switched_after_resume=$true }
        Advance-M7DRPhase -Stage 'rto_recorded' -Evidence @{ duration_ms=$rtoMS }
        Advance-M7DRPhase -Stage 'rpo_recorded' -Evidence @{
            control=@{missing_records=$rpoMissingRecords['control'];window_ms=$rpoWindows['control']}; shard_0=@{missing_records=$rpoMissingRecords['shard-0'];window_ms=$rpoWindows['shard-0']}; shard_1=@{missing_records=$rpoMissingRecords['shard-1'];window_ms=$rpoWindows['shard-1']}
        }
        Advance-M7DRPhase -Stage 'target_active' -Evidence @{
            observed_at=[DateTimeOffset]::UtcNow.ToString('o')
            control=@{region='region-b';epoch=2;state='active';writes_enabled=$true}
            shard_0=@{region='region-b';epoch=2;state='active';writes_enabled=$true}
            shard_1=@{region='region-b';epoch=2;state='active';writes_enabled=$true}
        }
        Set-M7CrashLeaseBarrier -Kind 'payment-operation' -Service 'control-postgres-region-b' -TargetID $paymentCrashHosted.IntentID -ExpectedState 'in_flight' -Region 'region-b' -Epoch 2 -Release
        Set-M7CrashLeaseBarrier -Kind 'payment-action' -Service 'control-postgres-region-b' -TargetID $ticketCrashHosted.IntentID -ExpectedState 'processing' -Region 'region-b' -Epoch 2 -Release
        Set-M7CrashLeaseBarrier -Kind 'payment-operation' -Service 'control-postgres-region-b' -TargetID $fullRefundProviderHosted.IntentID -ExpectedState 'in_flight' -Region 'region-b' -Epoch 2 -Release
        Set-M7CrashLeaseBarrier -Kind 'payment-action' -Service 'control-postgres-region-b' -TargetID $fullRefundShardHosted.IntentID -ExpectedState 'processing' -Region 'region-b' -Epoch 2 -Release
        Set-M7CrashLeaseBarrier -Kind 'partial-refund-operation' -Service 'control-postgres-region-b' -TargetID $providerRefundOperationID -ExpectedState 'processing' -Region 'region-b' -Epoch 2 -Release
        Set-M7CrashLeaseBarrier -Kind 'partial-refund-saga' -Service 'control-postgres-region-b' -TargetID $shardRefundRequestID -ExpectedState 'shard_compensating' -Region 'region-b' -Epoch 2 -Release
        Add-M7Phase 'six-application-crash-leases-released-only-after-region-b-active'
        $recovered = $false
        for ($attempt=1; $attempt -le 180; $attempt++) {
            $paymentRecovery = Get-M7Scalar -Service 'control-postgres-region-b' -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT intent.state||'|'||saga.state||'|'||saga.current_step||'|'||action.state
FROM public.payment_intents AS intent JOIN public.payment_sagas AS saga USING(payment_intent_id)
JOIN public.payment_saga_actions AS action USING(saga_id)
WHERE intent.payment_intent_id='$($paymentCrashHosted.IntentID)'::uuid
  AND action.action_type='issue_tickets'
"@
            $ticketRecovery = Get-M7Scalar -Service 'control-postgres-region-b' -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT intent.state||'|'||saga.state||'|'||saga.current_step||'|'||action.state
FROM public.payment_intents AS intent JOIN public.payment_sagas AS saga USING(payment_intent_id)
JOIN public.payment_saga_actions AS action USING(saga_id)
WHERE intent.payment_intent_id='$($ticketCrashHosted.IntentID)'::uuid
  AND action.action_type='issue_tickets'
"@
            $fullRefundRecovery = Get-M7Scalar -Service 'control-postgres-region-b' -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT string_agg(intent.state||'|'||saga.state||'|'||saga.current_step||'|'||action.state,';' ORDER BY intent.payment_intent_id)
FROM public.payment_intents AS intent JOIN public.payment_sagas AS saga USING(payment_intent_id)
JOIN public.payment_saga_actions AS action USING(saga_id)
WHERE intent.payment_intent_id IN ('$($fullRefundProviderHosted.IntentID)'::uuid,'$($fullRefundShardHosted.IntentID)'::uuid)
  AND action.action_type='compensate'
"@
            $refundRecovery = Get-M7Scalar -Service 'control-postgres-region-b' -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT string_agg(request.state||'|'||operation.state||'|'||saga.state||'|'||saga.current_step,';' ORDER BY request.refund_request_id)
FROM public.ticket_refund_requests AS request JOIN public.ticket_refund_operations AS operation USING(refund_request_id)
JOIN public.ticket_refund_sagas AS saga USING(refund_request_id)
WHERE request.refund_request_id IN ('$providerRefundRequestID'::uuid,'$shardRefundRequestID'::uuid)
"@
            if ($paymentRecovery -eq 'completed|completed|complete|completed' -and
                $ticketRecovery -eq 'completed|completed|complete|completed' -and
                ($fullRefundRecovery -split ';' | Where-Object { $_ -ne 'cancelled|compensated|complete|completed' }).Count -eq 0 -and ($fullRefundRecovery -split ';').Count -eq 2 -and
                ($refundRecovery -split ';' | Where-Object { $_ -ne 'completed|succeeded|completed|complete' }).Count -eq 0 -and ($refundRecovery -split ';').Count -eq 2) { $recovered=$true; break }
            Start-Sleep -Seconds 1
        }
        if (-not $recovered) { throw 'interrupted payment/refund sagas did not converge after region-b activation' }
        $recoveredPaymentShard = Get-M7Scalar -Service 'booking-shard-0-postgres-region-b' -User 'railway_booking' -Database 'railway_booking' -SQL @"
SELECT (SELECT count(*) FROM public.ticket_issuance_receipts WHERE payment_intent_id IN ('$($paymentCrashHosted.IntentID)'::uuid,'$($ticketCrashHosted.IntentID)'::uuid))::text||'|'||
       (SELECT count(*) FROM public.ticket_orders WHERE payment_intent_id IN ('$($paymentCrashHosted.IntentID)'::uuid,'$($ticketCrashHosted.IntentID)'::uuid) AND status='issued')::text||'|'||
       (SELECT count(*) FROM public.tickets AS ticket JOIN public.ticket_orders AS orders ON orders.id=ticket.ticket_order_id WHERE orders.payment_intent_id IN ('$($paymentCrashHosted.IntentID)'::uuid,'$($ticketCrashHosted.IntentID)'::uuid) AND ticket.status='active')::text||'|'||
       (SELECT count(*) FROM public.payment_compensation_receipts WHERE payment_intent_id IN ('$($fullRefundProviderHosted.IntentID)'::uuid,'$($fullRefundShardHosted.IntentID)'::uuid))::text
"@
        $recoveredRefundShard = Get-M7Scalar -Service 'booking-shard-0-postgres-region-b' -User 'railway_booking' -Database 'railway_booking' -SQL @"
SELECT (SELECT count(*) FROM public.ticket_refund_compensation_receipts WHERE refund_request_id IN ('$providerRefundRequestID'::uuid,'$shardRefundRequestID'::uuid))::text||'|'||
       (SELECT count(*) FROM public.selected_ticket_refund_receipts WHERE refund_request_id IN ('$providerRefundRequestID'::uuid,'$shardRefundRequestID'::uuid))::text||'|'||
       (SELECT count(*) FROM public.tickets WHERE ticket_order_id IN ('$refundCrashOrderID'::uuid,'$refundShardCrashOrderID'::uuid) AND status='refunded')::text||'|'||
       (SELECT count(*) FROM public.tickets WHERE ticket_order_id IN ('$refundCrashOrderID'::uuid,'$refundShardCrashOrderID'::uuid) AND status='active')::text
"@
        if ($recoveredPaymentShard -ne '2|2|4|2' -or $recoveredRefundShard -ne '2|2|2|2') { throw 'recovered shard effects were not exact-once' }
        $refundLedger = (Get-M7Scalar -Service 'control-postgres-region-b' -User 'railway_control' -Database 'railway_control' -SQL @"
SELECT count(DISTINCT ledger.transaction_id)::text||'|'||count(posting.*)::text||'|'||
       coalesce(sum(posting.amount_minor) FILTER (WHERE posting.side='debit'),0)::text||'|'||
       coalesce(sum(posting.amount_minor) FILTER (WHERE posting.side='credit'),0)::text
FROM public.financial_ledger_transactions AS ledger JOIN public.financial_ledger_postings AS posting USING(transaction_id)
WHERE ledger.event_id IN ('partial_refund:$providerRefundOperationID','partial_refund:$shardRefundOperationID')
"@) -split '\|'
        if ($refundLedger.Count -ne 4 -or $refundLedger[0] -ne '2' -or $refundLedger[1] -ne '4' -or [int64]$refundLedger[2] -ne [int64]$refundLedger[3]) { throw 'recovered partial-refund ledger was not exact and balanced' }
        $interruptedPaymentEvidence.recovered_after_failover = $true
        $interruptedPaymentEvidence.capture_terminal_control_state = $paymentRecovery
        $interruptedPaymentEvidence.ticket_terminal_control_state = $ticketRecovery
        $interruptedPaymentEvidence.shard_issuance_receipts = 2
        $interruptedPaymentEvidence.issued_orders = 2
        $interruptedPaymentEvidence.issued_tickets = 4
        $interruptedFullRefundEvidence.provider_pending.recovered_after_failover = $true
        $interruptedFullRefundEvidence.shard_committed.recovered_after_failover = $true
        $interruptedFullRefundEvidence.terminal_control_states = [string[]]($fullRefundRecovery -split ';')
        $interruptedFullRefundEvidence.terminal_compensation_receipts = 2
        $interruptedRefundEvidence.provider_pending.recovered_after_failover = $true
        $interruptedRefundEvidence.shard_committed.recovered_after_failover = $true
        $interruptedRefundEvidence.terminal_control_states = [string[]]($refundRecovery -split ';')
        $interruptedRefundEvidence.terminal_receipts = [ordered]@{ compensation=2; selected=2; refunded_tickets=2; unselected_active_tickets=2; ledger_transactions=2; ledger_postings=4; ledger_balanced=$true }
        foreach ($entry in $applicationCrashEvidence) { $entry['resumed']=$true }
        Add-M7Phase 'pre-fence-payment-and-refund-sagas-recovered-on-region-b'

        $redisBeforeResult = Invoke-M7Compose -Arguments @('exec','-T','redis-region-b','redis-cli','--raw','DBSIZE')
        $redisBeforeText = [string](@($redisBeforeResult.Output | Where-Object { $_ -match '^\d+$' }) | Select-Object -Last 1)
        if ($redisBeforeText -notmatch '^\d+$') { throw 'region-b Redis pre-rebuild key count was not observable' }
        $redisKeysBefore = [int]$redisBeforeText
        if ($redisKeysBefore -ne 0) { throw "region-b Redis was not fresh before rebuild; observed $redisKeysBefore keys" }
        $ackSignature = Get-M7WebhookSignature -Key $webhookCurrentKey -Timestamp $webhookTimestamp -Body $webhookAckBody
        Invoke-M7ContainerWebhookStatus -Service 'api-region-b-1' -Body $webhookAckBody -KeyID 'current' -Timestamp $webhookTimestamp -Signature $ackSignature -ExpectedStatus @(202) | Out-Null
        $ackDurability = Get-M7Scalar -Service 'control-postgres-region-b' -User 'railway_control' -Database 'railway_control' -SQL "SELECT count(*) FROM public.payment_webhook_inbox WHERE provider_event_id='evt_m7_ack_$suffix'"
        if ($ackDurability -ne '1') { throw 'exact pre-failover webhook retry was not durably applied once after ingress switch' }
        $webhookOutageEndedAt = [DateTimeOffset]::UtcNow
        Add-M7Phase 'webhook-retried-on-region-b-after-switch'

        $redisOutagePassenger = Invoke-Milestone5DriverAPI -BaseURL (Get-M7PublishedURL) -Method POST -Path '/api/v1/passengers' -Token $m7Customer.Token `
            -Body @{ display_name='M7 Redis outage passenger' } -ExpectedStatus @(201)
        $redisOutageBeforeControl = Get-M7Scalar -Service 'control-postgres-region-b' -User 'railway_control' -Database 'railway_control' -SQL "SELECT count(*) FROM public.reservation_shard_locators WHERE train_run_id='$m7HealthyTrain'::uuid"
        $redisOutageBeforeShard = Get-M7Scalar -Service 'booking-shard-1-postgres-region-b' -User 'railway_booking' -Database 'railway_booking' -SQL "SELECT count(*) FROM public.reservations WHERE train_run_id='$m7HealthyTrain'::uuid"
        Invoke-M7Compose -Arguments @('stop','-t','15','redis-region-b') | Out-Null
        Invoke-Milestone5DriverAPI -BaseURL (Get-M7PublishedURL) -Method POST -Path '/api/v1/reservations' -Token $m7Customer.Token `
            -IdempotencyKey "m7-redis-outage-$suffix" -Body @{
                train_run_id=$m7HealthyTrain; origin_station_code='M2A'; destination_station_code='M2B';
                seat_class='standard'; passenger_ids=@([string]$redisOutagePassenger.Body.id)
            } -ExpectedStatus @(503) | Out-Null
        $redisOutageAfterControl = Get-M7Scalar -Service 'control-postgres-region-b' -User 'railway_control' -Database 'railway_control' -SQL "SELECT count(*) FROM public.reservation_shard_locators WHERE train_run_id='$m7HealthyTrain'::uuid"
        $redisOutageAfterShard = Get-M7Scalar -Service 'booking-shard-1-postgres-region-b' -User 'railway_booking' -Database 'railway_booking' -SQL "SELECT count(*) FROM public.reservations WHERE train_run_id='$m7HealthyTrain'::uuid"
        if ($redisOutageAfterControl -ne $redisOutageBeforeControl -or $redisOutageAfterShard -ne $redisOutageBeforeShard) { throw 'active Redis outage bypassed fail-closed admission and wrote booking state' }
        Invoke-M7Compose -Arguments @('start','redis-region-b') | Out-Null
        $redisPing = Invoke-M7Compose -Arguments @('exec','-T','redis-region-b','redis-cli','--raw','PING')
        if ([string](@($redisPing.Output | Select-Object -Last 1)).Trim() -cne 'PONG') { throw 'region-b Redis did not recover after the bounded active outage' }
        Start-Sleep -Seconds 2
        $redisOutageRecoveredControl = Get-M7Scalar -Service 'control-postgres-region-b' -User 'railway_control' -Database 'railway_control' -SQL "SELECT count(*) FROM public.reservation_shard_locators WHERE train_run_id='$m7HealthyTrain'::uuid"
        $redisOutageRecoveredShard = Get-M7Scalar -Service 'booking-shard-1-postgres-region-b' -User 'railway_booking' -Database 'railway_booking' -SQL "SELECT count(*) FROM public.reservations WHERE train_run_id='$m7HealthyTrain'::uuid"
        if ($redisOutageRecoveredControl -ne $redisOutageBeforeControl -or $redisOutageRecoveredShard -ne $redisOutageBeforeShard) { throw 'rejected Redis-outage admission replayed into booking state after Redis recovery' }
        Add-M7Phase 'active-redis-outage-admission-failed-closed-without-booking-bypass'

        $oldDurableRead = Invoke-Milestone5DriverAPI -BaseURL (Get-M7PublishedURL) -Method GET -Path "/api/v1/ticket-orders/$($m7Orders[0].OrderID)" `
            -Token $m7Customer.Token -ExpectedStatus @(200)
        if ([string]$oldDurableRead.Body.id -ne [string]$m7Orders[0].OrderID) { throw 'region-b did not rebuild an old durable read after region-a Redis loss' }
        $redisAdmissionCustomer = New-M7CustomerFixtures -BaseURL (Get-M7PublishedURL) -TrainRunID $m7HealthyTrain -Count 1 -Label 'redis-rebuild'
        $redisAdmissionLocator = Get-M7Scalar -Service 'control-postgres-region-b' -User 'railway_control' -Database 'railway_control' -SQL "SELECT count(*) FROM public.reservation_shard_locators WHERE reservation_id='$($redisAdmissionCustomer.Reservations[0])'::uuid AND shard_id='physical-shard-1'"
        $redisAfterResult = Invoke-M7Compose -Arguments @('exec','-T','redis-region-b','redis-cli','--raw','DBSIZE')
        $redisAfterText = [string](@($redisAfterResult.Output | Where-Object { $_ -match '^\d+$' }) | Select-Object -Last 1)
        if ($redisAdmissionLocator -ne '1' -or $redisAfterText -notmatch '^\d+$' -or [int]$redisAfterText -lt 1) { throw 'region-b Redis admission/cache rebuild did not preserve durable authority' }
        $redisRecoveryEvidence = [ordered]@{
            region_a_volume_destroyed=$true; region_b_fresh_ready=$true; keys_before=$redisKeysBefore; keys_after=[int]$redisAfterText;
            active_outage_admission_rejected=$true; booking_bypass_rows=0; old_durable_ticket_read=$true;
            new_admission_reservation_durable=$true; reservation_locator_count=1; redis_not_source_of_truth=$true
        }
        Add-M7Phase 'redis-admission-and-durable-read-rebuilt-on-region-b'

        Invoke-M7Compose -Arguments @('stop','-t','15','booking-shard-0-postgres-region-b') | Out-Null
        Invoke-Milestone5DriverAPI -BaseURL (Get-M7PublishedURL) -Method GET -Path "/api/v1/ticket-orders/$($m7Orders[0].OrderID)" `
            -Token $m7Customer.Token -ExpectedStatus @(500,503,504) | Out-Null
        $outageHealthyCustomer = New-M7CustomerFixtures -BaseURL (Get-M7PublishedURL) -TrainRunID $m7HealthyTrain -Count 1 -Label 'shard-outage'
        $outageHealthyControl = Get-M7Scalar -Service 'control-postgres-region-b' -User 'railway_control' -Database 'railway_control' -SQL "SELECT count(*) FROM public.reservation_shard_locators WHERE reservation_id='$($outageHealthyCustomer.Reservations[0])'::uuid AND shard_id='physical-shard-1'"
        $outageHealthyShard = Get-M7Scalar -Service 'booking-shard-1-postgres-region-b' -User 'railway_booking' -Database 'railway_booking' -SQL "SELECT count(*) FROM public.reservations WHERE id='$($outageHealthyCustomer.Reservations[0])'::uuid AND status='held'"
        if ($outageHealthyControl -ne '1' -or $outageHealthyShard -ne '1') { throw 'new healthy-shard booking did not complete during peer outage' }
        Invoke-M7Compose -Arguments @('start','booking-shard-0-postgres-region-b') | Out-Null
        Wait-M7Role -Service 'booking-shard-0-postgres-region-b' -User 'railway_shard_0_replicator' -Database 'railway_booking' -Recovery $false
        $restoredAffected = Invoke-Milestone5DriverAPI -BaseURL (Get-M7PublishedURL) -Method GET -Path "/api/v1/ticket-orders/$($m7Orders[0].OrderID)" `
            -Token $m7Customer.Token -ExpectedStatus @(200)
        if ([string]$restoredAffected.Body.id -ne [string]$m7Orders[0].OrderID) { throw 'restored shard did not recover its assigned ticket order' }
        $singleShardOutageEvidence = [ordered]@{ stopped_shard='physical-shard-0'; affected_route_rejected=$true; fallback_forbidden=$true; healthy_shard_completed=$true; healthy_shard_new_operation=$true; restored_and_reconciled=$true }
        Add-M7Phase 'promoted-single-shard-outage-contained-and-recovered'
        foreach ($service in @($regionAAppServices + @($databases.Primary))) {
            $running = Invoke-M7Compose -AllowFailure -Arguments @('ps','--status','running','-q',$service)
            if (@($running.Output | Where-Object { $_.Trim() -ne '' }).Count -ne 0) { throw "retained source fence was lost for $service" }
        }
        $retainedFenceObservation = [ordered]@{
            region='region-a'; stopped_writer_processes=$regionAAppServices.Count; stopped_databases=$databases.Count;
            ingress_stopped=$true; credential_consumers_stopped=$true; database_network_endpoints_stopped=$true;
            observed_at=[DateTimeOffset]::UtcNow.ToString('o')
        }
        Write-M7JSON -Name 'region-a-retained-fence-observation.json' -Value $retainedFenceObservation
        $retainedFenceText = $retainedFenceObservation | ConvertTo-Json -Compress
        $signedRetainedFence = New-M7SignedFenceEvidence -OperationID $failoverOperationID -SourceRegion 'region-a' -SourceEpoch 1 -Purpose 'retained_source_fence' `
            -IncidentID $failoverIncidentID -OperatorID 'operator:local-dr' -ObservationText $retainedFenceText
        Advance-M7DRPhase -Stage 'source_retained_fenced' -Evidence $signedRetainedFence
        Add-M7Phase 'typed-failover-completed'
        Wait-M7ServiceHTTP -Service 'payment-reconciler-region-b' -URL 'http://127.0.0.1:9090/metrics'
        Assert-M7DurableMetrics -Service 'payment-reconciler-region-b' -Phase 'region-b-after-failover' -Families @(
            'financial_ledger_transaction_total','settlement_import_total','settlement_reconciliation_total','settlement_reconciliation_mismatch_total',
            'regional_active_epoch','regional_failover_total','regional_rpo_observed_seconds','regional_rto_observed_seconds',
            'backup_total','backup_restore_duration_seconds'
        )
        Invoke-M7K6 -Script 'regional-failover.js' -Environment @{
            VUS='1'; ITERATIONS_PER_VU='1'; DURATION='1m'; ACTIVE_REGION_URL='http://proxy-region-b:8080'; STALE_REGION_URL='http://proxy-region-a:8080';
            CUSTOMER_TOKENS=$m7Customer.Token; RESERVATION_IDS=$m7Customer.Reservations[4]
        }
        $commonB = @{
            VUS='1'; ITERATIONS_PER_VU='1'; DURATION='3m'; POLL_ATTEMPTS='100'; PAYMENT_POLL_ATTEMPTS='120'; REFUND_POLL_ATTEMPTS='120';
            BASE_URL='http://api-region-b-1:8080'; SANDBOX_URL='http://payment-sandbox:8099'; SANDBOX_CONTROL_TOKEN='synthetic-disposable-fault-token'
        }
        Invoke-M7K6 -Script 'payment-during-failover.js' -Environment (@{} + $commonB + @{
            CUSTOMER_TOKENS=$m7Customer.Token; RESERVATION_IDS=$m7Customer.Reservations[3]
        })
        Invoke-M7K6 -Script 'refund-during-failover.js' -Environment (@{} + $commonB + @{
            CUSTOMER_TOKEN=$m7Customer.Token; TICKET_ORDER_ID=$m7Orders[2].OrderID; TICKET_IDS=($m7Orders[2].TicketIDs -split ',')[0]
        })
        Complete-M7SandboxPayment -DatabaseService 'control-postgres-region-b' -ReservationID $m7Customer.Reservations[3] -WebhookBaseURL (Get-M7PublishedURL)
        Assert-M7PartialRefund -OrderID $m7Orders[2].OrderID -SelectedTicketID (($m7Orders[2].TicketIDs -split ',')[0]) -Scenario 'refund-during-failover' `
            -ControlService 'control-postgres-region-b' -ShardService 'booking-shard-0-postgres-region-b'
        $null = Assert-M7SettlementImport -DatabaseService 'control-postgres-region-b' -WorkerService 'settlement-worker-region-b' -Phase 'region-b-failover'
        Add-M7Phase 'region-b-settlement-import-replayed'

        foreach ($database in $databases) {
            $count = [int](Get-M7Scalar -Service $database.Standby -User $database.User -Database $database.Database -SQL 'SELECT count(*) FROM public.dr_evidence_markers WHERE marker=1')
            if ($count -ne 1) { throw "replicated baseline marker was missing for $($database.Name)" }
        }

        $env:DR_RECOVERY_EPOCH = '2'
        $env:DR_JOURNAL_REGION = 'region-b'
        $env:DR_JOURNAL_DATABASE_URL = 'postgresql://railway_control:control-local-only@control-postgres-region-b:5432/railway_control?sslmode=disable&connect_timeout=3'
        $plannedFailback = Invoke-M7DRAdmin -Arguments @(
            'prepare-failback','--operation-id',$failbackOperationID,'--incident-id',$failbackIncidentID,
            '--from','region-b','--to','region-a','--source-epoch','2','--target-epoch','3',
            '--operator','operator:local-dr','--reason','planned_failback','--confirm','--timeout','2m'
        )
        if ([string]$plannedFailback.result.stage -ne 'planned') { throw 'typed failback operation was not durably planned' }
        Add-M7Phase 'typed-failback-planned-before-reseed'

        $failbackStart = [DateTimeOffset]::UtcNow
        $failbackReseedStartedAt = [DateTimeOffset]::UtcNow
        Invoke-M7Compose -Arguments (@('rm','-sf') + @($databases.Primary)) | Out-Null
        foreach ($database in $databases) { Remove-M7ProjectVolume -Suffix $database.Volume }
        $reseedUp = @('--profile','dr-failback','up','-d','--no-deps')
        if ($SkipBuild) { $reseedUp += '--no-build' }
        $reseedUp += @($databases.Reseed)
        Invoke-M7Compose -Arguments $reseedUp | Out-Null
        foreach ($database in $databases) { Wait-M7Role -Service $database.Reseed -User $database.ReplicationUser -Database $database.Database -Recovery $true }
        Add-M7Phase 'region-a-reseeded-from-region-b'

        Ensure-M7ServicesStopped -Services $regionBWriterServices -OperationKind 'failback' -Boundary 'region-b-applications' -CrashReobserve | Out-Null
        foreach ($database in $databases) {
            $sourceObservedAt = [DateTimeOffset]::UtcNow
            $position = Get-M7Scalar -Service $database.Standby -User $database.User -Database $database.Database -SQL "SELECT ((pg_control_checkpoint()).timeline_id)::text||'|'||pg_current_wal_lsn()::text||'|'||pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0')::bigint::text"
            $parts = $position -split '\|'
            if ($parts.Count -ne 3) { throw "preliminary failback source WAL was malformed for $($database.Name)" }
            $failbackPositions[$database.Name] = [ordered]@{ timeline=[uint32]$parts[0]; lsn=$parts[1]; wal=[uint64]$parts[2]; observed_at=$sourceObservedAt.ToString('o') }
        }
        $preFailbackFenceObservation = [ordered]@{
            region='region-b'; stopped_writer_processes=$regionBWriterServices.Count; source_primaries_running_for_journal=$true;
            ingress_stopped=$true; credential_consumers_stopped=$true; application_database_clients_stopped=$true;
            promotion_permitted_only_after_durable_markers=$true; observed_at=[DateTimeOffset]::UtcNow.ToString('o')
        }
        Write-M7JSON -Name 'region-b-pre-failback-promotion-fence-observation.json' -Value $preFailbackFenceObservation
        $preFailbackFenceText = $preFailbackFenceObservation | ConvertTo-Json -Compress
        $signedFailbackFence = New-M7SignedFenceEvidence -OperationID $failbackOperationID -SourceRegion 'region-b' -SourceEpoch 2 -Purpose 'initial_fence' `
            -IncidentID $failbackIncidentID -OperatorID 'operator:local-dr' -ObservationText $preFailbackFenceText
        Advance-M7DRPhase -OperationID $failbackOperationID -Prefix 'dr-failback-phase' -Stage 'external_fencing_verified' -CrashOnce -Evidence (@{} + $signedFailbackFence)
        Advance-M7DRPhase -OperationID $failbackOperationID -Prefix 'dr-failback-phase' -Stage 'positions_recorded' -Evidence @{
            control=@{timeline=$failbackPositions['control'].timeline;wal=$failbackPositions['control'].wal}
            shard_0=@{timeline=$failbackPositions['shard-0'].timeline;wal=$failbackPositions['shard-0'].wal}
            shard_1=@{timeline=$failbackPositions['shard-1'].timeline;wal=$failbackPositions['shard-1'].wal}
        }
        Advance-M7DRPhase -OperationID $failbackOperationID -Prefix 'dr-failback-phase' -Stage 'passive_readiness_removed' -Evidence @{ artifact_sha256=(Get-M7SHA256 "$preFailbackFenceText|readiness-removed") }
        foreach ($database in $databases) {
            $journalLSN = Get-M7Scalar -Service $database.Standby -User $database.User -Database $database.Database -SQL 'SELECT pg_current_wal_lsn()::text'
            Wait-M7Replay -Service $database.Reseed -User $database.ReplicationUser -Database $database.Database -LSN $journalLSN
            $replay = Get-M7Scalar -Service $database.Reseed -User $database.ReplicationUser -Database $database.Database -SQL "SELECT pg_last_wal_replay_lsn()::text||'|'||pg_wal_lsn_diff(pg_last_wal_replay_lsn(),'0/0')::bigint::text"
            $replayParts = $replay -split '\|'
            if ($replayParts.Count -ne 2 -or [uint64]$replayParts[1] -lt [uint64]$failbackPositions[$database.Name].wal) { throw "failback reseed provenance did not reach the fenced source position for $($database.Name)" }
            $failbackReplayPositions[$database.Name] = [ordered]@{ timeline=[uint32]$failbackPositions[$database.Name].timeline; lsn=$replayParts[0]; wal=[uint64]$replayParts[1]; observed_at=[DateTimeOffset]::UtcNow.ToString('o') }
        }
        $prePromotionFailbackReconciliation = Get-M7FailbackReconciliationObservation -ControlService 'control-postgres-region-a-reseed' `
            -Shard0Service 'booking-shard-0-postgres-region-a-reseed' -Shard1Service 'booking-shard-1-postgres-region-a-reseed'
        Write-M7JSON -Name 'region-a-reseed-pre-promotion-reconciliation.json' -Value $prePromotionFailbackReconciliation
        $failbackReseedCompletedAt = [DateTimeOffset]::UtcNow.ToString('o')
        $signedFailbackValidationFence = New-M7SignedFenceEvidence -OperationID $failbackOperationID -SourceRegion 'region-b' -SourceEpoch 2 -Purpose 'failback_validation' `
            -IncidentID $failbackIncidentID -OperatorID 'operator:local-dr' -ObservationText "$preFailbackFenceText|reseed-reconciled"
        $failbackValidationEvidence = [ordered]@{
            reseed_after=$failbackReseedStartedAt.ToString('o')
            control=@{source_region='region-b';source_epoch=2;started_at=$failbackReseedStartedAt.ToString('o');completed_at=$failbackReseedCompletedAt;source_position=@{timeline=$failbackPositions['control'].timeline;wal=$failbackPositions['control'].wal};replayed_position=@{timeline=$failbackReplayPositions['control'].timeline;wal=$failbackReplayPositions['control'].wal};reconciled=$true}
            shard_0=@{source_region='region-b';source_epoch=2;started_at=$failbackReseedStartedAt.ToString('o');completed_at=$failbackReseedCompletedAt;source_position=@{timeline=$failbackPositions['shard-0'].timeline;wal=$failbackPositions['shard-0'].wal};replayed_position=@{timeline=$failbackReplayPositions['shard-0'].timeline;wal=$failbackReplayPositions['shard-0'].wal};reconciled=$true}
            shard_1=@{source_region='region-b';source_epoch=2;started_at=$failbackReseedStartedAt.ToString('o');completed_at=$failbackReseedCompletedAt;source_position=@{timeline=$failbackPositions['shard-1'].timeline;wal=$failbackPositions['shard-1'].wal};replayed_position=@{timeline=$failbackReplayPositions['shard-1'].timeline;wal=$failbackReplayPositions['shard-1'].wal};reconciled=$true}
            current_fence=([ordered]@{} + $signedFailbackValidationFence)
        }
        Write-M7JSON -Name 'failback-reseed-validation.json' -Value $failbackValidationEvidence
        $validatedFailback = Invoke-M7DRAdmin -Arguments @('validate-failback','--operation-id',$failbackOperationID,'--evidence-file','/evidence/failback-reseed-validation.json','--timeout','2m')
        if ([string]$validatedFailback.result.stage -ne 'passive_readiness_removed') { throw 'dr-admin did not validate failback reseed provenance at the durable pre-promotion boundary' }
        Add-M7Phase 'failback-reseed-provenance-validated-before-promotion'
        Add-M7Phase 'failback-journal-and-reseed-provenance-durable-before-final-rpo-marker'
        foreach ($database in $databases) {
            $sourceObservedAt = [DateTimeOffset]::Parse((Get-M7Scalar -Service $database.Standby -User $database.User -Database $database.Database -SQL 'INSERT INTO public.dr_evidence_markers(marker) VALUES (3) RETURNING created_at::text'))
            $position = Get-M7Scalar -Service $database.Standby -User $database.User -Database $database.Database -SQL "SELECT ((pg_control_checkpoint()).timeline_id)::text||'|'||pg_current_wal_lsn()::text||'|'||pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0')::bigint::text"
            $parts = $position -split '\|'
            if ($parts.Count -ne 3) { throw "final failback source WAL was malformed for $($database.Name)" }
            $failbackPositions[$database.Name] = [ordered]@{ timeline=[uint32]$parts[0]; lsn=$parts[1]; wal=[uint64]$parts[2]; source_marker=3; observed_at=$sourceObservedAt.ToString('o') }
        }
        Add-M7Phase 'failback-final-rpo-markers-committed-without-catchup-wait'
        Ensure-M7ServicesStopped -Services @($databases.Standby) -OperationKind 'failback' -Boundary 'region-b-databases' -CrashReobserve | Out-Null
        foreach ($database in $databases) {
            $replay = Get-M7Scalar -Service $database.Reseed -User $database.ReplicationUser -Database $database.Database -SQL "SELECT pg_last_wal_replay_lsn()::text||'|'||pg_wal_lsn_diff(pg_last_wal_replay_lsn(),'0/0')::bigint::text"
            $parts = $replay -split '\|'
            if ($parts.Count -ne 2) { throw "failback standby replay position was malformed for $($database.Name)" }
            $failbackReplayPositions[$database.Name] = [ordered]@{ timeline=[uint32]$failbackPositions[$database.Name].timeline; lsn=$parts[0]; wal=[uint64]$parts[1]; observed_at=[DateTimeOffset]::UtcNow.ToString('o') }
        }
        Add-M7Phase 'region-b-externally-fenced-for-failback'
        foreach ($service in @($regionBWriterServices + @($databases.Standby))) {
            $running = Invoke-M7Compose -AllowFailure -Arguments @('ps','--status','running','-q',$service)
            if (@($running.Output | Where-Object { $_.Trim() -ne '' }).Count -ne 0) { throw "region-b failback fence failed for $service" }
        }
        $failbackFenceObservation = [ordered]@{
            region='region-b'; stopped_writer_processes=$regionBWriterServices.Count; stopped_databases=$databases.Count;
            ingress_stopped=$true; credential_consumers_stopped=$true; database_network_endpoints_stopped=$true;
            observed_at=[DateTimeOffset]::UtcNow.ToString('o')
        }
        Write-M7JSON -Name 'region-b-failback-fence-observation.json' -Value $failbackFenceObservation
        $failbackFenceText = $failbackFenceObservation | ConvertTo-Json -Compress
        $failbackPromotedPositions = @{}
        foreach ($database in $databases) {
            Refresh-M7DRFence -OperationID $failbackOperationID -Prefix 'dr-failback-phase' -Boundary "pre-$($database.Name)-promotion"
            $promotionReobservation = Ensure-M7PromotedPrimary -Service $database.Reseed -User $database.User -Database $database.Database `
                -OperationKind 'failback' -DatabaseName $database.Name -CrashReobserve:($database.Name -in @('control','shard-0'))
            $position = Get-M7Scalar -Service $database.Reseed -User $database.User -Database $database.Database -SQL "SELECT ((pg_control_checkpoint()).timeline_id)::text||'|'||pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0')::bigint::text"
            $parts = $position -split '\|'
            if ($parts.Count -ne 2) { throw "failback promoted position was malformed for $($database.Name)" }
            $failbackPromotedPositions[$database.Name] = [ordered]@{ timeline=[uint32]$parts[0]; wal=[uint64]$parts[1] }
            if ($database.Name -eq 'control') {
                $env:DR_RECOVERY_EPOCH = '3'
                $env:DR_JOURNAL_REGION = 'region-a'
                $env:DR_JOURNAL_DATABASE_URL = 'postgresql://railway_control:control-local-only@control-postgres-region-a-reseed:5432/railway_control?sslmode=disable&connect_timeout=3'
            }
            $stage = @{'control'='control_promoted';'shard-0'='shard_0_promoted';'shard-1'='shard_1_promoted'}[$database.Name]
            Advance-M7DRPhase -OperationID $failbackOperationID -Prefix 'dr-failback-phase' -Stage $stage -CrashOnce:($stage -in @('control_promoted','shard_0_promoted')) -Evidence @{
                database=$database.Name; position=$failbackPromotedPositions[$database.Name]
            }
        }
        Advance-M7DRPhase -OperationID $failbackOperationID -Prefix 'dr-failback-phase' -Stage 'roles_and_timelines_verified' -Evidence @{
            control=@{role='primary';timeline=$failbackPromotedPositions['control'].timeline};
            shard_0=@{role='primary';timeline=$failbackPromotedPositions['shard-0'].timeline};
            shard_1=@{role='primary';timeline=$failbackPromotedPositions['shard-1'].timeline}
        }
        Advance-M7DRPhase -OperationID $failbackOperationID -Prefix 'dr-failback-phase' -Stage 'epoch_allocated' -CrashOnce -Evidence @{ target_epoch=3 }
        $updated = Invoke-M7AuthorityTransition -Service 'control-postgres-region-a-reseed' -User 'railway_control' -Database 'railway_control' -Region 'region-a' -Epoch 3 -State 'recovery' -Writes $false
        if ($updated -ne '1') { throw 'region-a failback recovery authority update failed for control' }
        Advance-M7DRPhase -OperationID $failbackOperationID -Prefix 'dr-failback-phase' -Stage 'control_recovery_installed' -CrashOnce -Evidence @{}
        foreach ($database in @($databases | Where-Object { $_.Name -ne 'control' })) {
            $updated = Invoke-M7AuthorityTransition -Service $database.Reseed -User $database.User -Database $database.Database -Region 'region-a' -Epoch 3 -State 'recovery' -Writes $false
            if ($updated -ne '1') { throw "region-a failback recovery authority update failed for $($database.Name)" }
        }
        Advance-M7DRPhase -OperationID $failbackOperationID -Prefix 'dr-failback-phase' -Stage 'shard_authorities_installed' -Evidence @{}
        Add-M7Phase 'region-a-failback-promoted'

        $env:REGION_A_DEPLOYMENT_ROLE = 'recovery'
        $env:REGION_A_EPOCH = '3'
        $env:REGION_A_WRITES_ENABLED = 'false'
        $env:REGION_A_CONTROL_DATABASE_URL = 'postgresql://railway_runtime:runtime-local-only@control-postgres-region-a-reseed:5432/railway_control?sslmode=disable&connect_timeout=3'
        $env:CONTROL_DATABASE_URL = $env:REGION_A_CONTROL_DATABASE_URL
        $env:BOOKING_SHARD_0_DATABASE_URL = 'postgresql://railway_runtime:runtime-local-only@booking-shard-0-postgres-region-a-reseed:5432/railway_booking?sslmode=disable&connect_timeout=3'
        $env:BOOKING_SHARD_1_DATABASE_URL = 'postgresql://railway_runtime:runtime-local-only@booking-shard-1-postgres-region-a-reseed:5432/railway_booking?sslmode=disable&connect_timeout=3'
        $env:REGION_A_RECONCILER_CONTROL_DATABASE_URL = 'postgresql://payment_reconciler:reconciler-local-only@control-postgres-region-a-reseed:5432/railway_control?sslmode=disable&connect_timeout=3'
        $env:REGION_A_RECONCILER_SHARD_0_DATABASE_URL = 'postgresql://payment_reconciler:reconciler-local-only@booking-shard-0-postgres-region-a-reseed:5432/railway_booking?sslmode=disable&connect_timeout=3&options=-c%20default_transaction_read_only=on'
        $env:REGION_A_RECONCILER_SHARD_1_DATABASE_URL = 'postgresql://payment_reconciler:reconciler-local-only@booking-shard-1-postgres-region-a-reseed:5432/railway_booking?sslmode=disable&connect_timeout=3&options=-c%20default_transaction_read_only=on'
        $env:ACTIVE_REGION_UPSTREAM = 'proxy-region-a'
        $regionARecoveryUp = @('--profile','dr-app','up','-d','--no-deps','--force-recreate')
        if ($SkipBuild) { $regionARecoveryUp += '--no-build' }
        $regionARecoveryUp += @('redis','api-1','api-2','api-3')
        Invoke-M7Compose -Arguments $regionARecoveryUp | Out-Null
        foreach ($api in @('api-1','api-2','api-3')) { Wait-M7ServiceHTTP -Service $api -URL 'http://127.0.0.1:8080/livez' }
        Advance-M7DRPhase -OperationID $failbackOperationID -Prefix 'dr-failback-phase' -Stage 'recovery_apis_started' -Evidence @{ artifact_sha256=(Get-M7RunningServiceHash -Services @('api-1','api-2','api-3')) }
        $prematureRegionAServices = @(
            'payment-worker-1','payment-worker-2','payment-reconciler','admission-worker-1','admission-worker-2',
            'read-model-worker-1','read-model-worker-2','hold-expirer','outbox-worker','booking-command-reconciler',
            'settlement-worker-region-a','proxy-region-a'
        )
        foreach ($service in $prematureRegionAServices) {
            $running = Invoke-M7Compose -AllowFailure -Arguments @('ps','--status','running','-q',$service)
            if (@($running.Output | Where-Object { $_.Trim() -ne '' }).Count -ne 0) { throw "region-a worker or proxy started before complete failback database activation: $service" }
        }
        $failbackReconciliation = Get-M7FailbackReconciliationObservation -ControlService 'control-postgres-region-a-reseed' -Shard0Service 'booking-shard-0-postgres-region-a-reseed' -Shard1Service 'booking-shard-1-postgres-region-a-reseed'
        Write-M7JSON -Name 'region-a-failback-reconciliation-observation.json' -Value $failbackReconciliation
        Advance-M7DRPhase -OperationID $failbackOperationID -Prefix 'dr-failback-phase' -Stage 'reconciled' -Evidence @{
            control=$true; shards=$true; payments=$true; tickets=$true; refunds=$true; ledger=$true; routing=$true;
            artifact_sha256=(Get-M7SHA256 -Text ($failbackReconciliation | ConvertTo-Json -Compress))
        }
        $preFailbackSwitchIngress = Invoke-M7Compose -AllowFailure -Arguments @('ps','--status','running','-q','global-test-ingress')
        if (@($preFailbackSwitchIngress.Output | Where-Object { $_.Trim() -ne '' }).Count -ne 0) { throw 'global ingress was running before complete failback database activation' }

        foreach ($database in @($databases | Where-Object { $_.Name -ne 'control' })) {
            $updated = Invoke-M7AuthorityTransition -Service $database.Reseed -User $database.User -Database $database.Database -Region 'region-a' -Epoch 3 -State 'active' -Writes $true
            if ($updated -ne '1') { throw "region-a failback activation failed for $($database.Name)" }
        }
        $updated = Invoke-M7AuthorityTransition -Service 'control-postgres-region-a-reseed' -User 'railway_control' -Database 'railway_control' -Region 'region-a' -Epoch 3 -State 'active' -Writes $true
        if ($updated -ne '1') { throw 'region-a final control failback activation failed' }
        $completeFailbackAuthority = foreach ($database in $databases) {
            Get-M7Scalar -Service $database.Reseed -User $database.User -Database $database.Database -SQL "SELECT region||'|'||epoch::text||'|'||state||'|'||writes_enabled::text||'|'||pg_is_in_recovery()::text FROM public.regional_write_authority WHERE singleton"
        }
        if (@($completeFailbackAuthority | Where-Object { $_ -cne 'region-a|3|active|true|false' }).Count -ne 0) { throw 'region-a failback authority set was not completely active' }
        Write-M7JSON -Name 'region-a-complete-authority-set.json' -Value ([ordered]@{control_last=$true; databases=@($completeFailbackAuthority); complete=$true})

        $env:REGION_A_DEPLOYMENT_ROLE = 'active'
        $env:REGION_A_WRITES_ENABLED = 'true'
        $regionAIngressUp = @('--profile','dr-app','up','-d','--no-deps','--force-recreate')
        if ($SkipBuild) { $regionAIngressUp += '--no-build' }
        $regionAIngressUp += @(
            'redis','api-1','api-2','api-3','payment-worker-1','payment-worker-2','payment-reconciler',
            'admission-worker-1','admission-worker-2','read-model-worker-1','read-model-worker-2',
            'hold-expirer','outbox-worker','booking-command-reconciler','settlement-worker-region-a','proxy-region-a'
        )
        Invoke-M7Compose -Arguments $regionAIngressUp | Out-Null
        foreach ($api in @('api-1','api-2','api-3')) { Wait-M7ServiceHTTP -Service $api -URL 'http://127.0.0.1:8080/readyz' }
        foreach ($worker in @('payment-worker-1','payment-worker-2')) { Wait-M7ServiceHTTP -Service $worker -URL 'http://127.0.0.1:9090/readyz' }
        Wait-M7ServiceHTTP -Service 'settlement-worker-region-a' -URL 'http://127.0.0.1:9090/readyz'
        Advance-M7DRPhase -OperationID $failbackOperationID -Prefix 'dr-failback-phase' -Stage 'payment_workers_enabled' -Evidence @{ authority_set_complete=$true; artifact_sha256=(Get-M7RunningServiceHash -Services @('payment-worker-1','payment-worker-2')) }
        Advance-M7DRPhase -OperationID $failbackOperationID -Prefix 'dr-failback-phase' -Stage 'settlement_workers_enabled' -Evidence @{ authority_set_complete=$true; artifact_sha256=(Get-M7RunningServiceHash -Services @('settlement-worker-region-a')) }
        Invoke-M7Compose -Arguments $globalIngressUp | Out-Null
        Wait-M7ServiceHTTP -Service 'global-test-ingress' -URL 'http://127.0.0.1:8080/readyz'
        Advance-M7DRPhase -OperationID $failbackOperationID -Prefix 'dr-failback-phase' -Stage 'ingress_switched' -CrashOnce -Evidence @{
            webhook=$true; global=$true; external_action_reobserved=$true;
            artifact_sha256=(Get-M7RunningServiceHash -Services @('proxy-region-a','global-test-ingress'))
        }
        Add-M7Phase 'global-ingress-switched-to-region-a-after-complete-authority'
        Advance-M7DRPhase -OperationID $failbackOperationID -Prefix 'dr-failback-phase' -Stage 'customer_writes_configured' -CrashOnce -Evidence @{ enabled=$true; readiness_gated=$true; all_database_authorities_active=$true; artifact_sha256=(Get-M7RunningServiceHash -Services @('api-1','api-2','api-3')) }
        $failbackRPOWindows = @{}
        $failbackMissingRecords = @{}
        foreach ($database in $databases) {
            $sent = [uint64]$failbackPositions[$database.Name].wal
            $replayed = [uint64]$failbackReplayPositions[$database.Name].wal
            $missingWalBytes = if ($sent -gt $replayed) { $sent - $replayed } else { [uint64]0 }
            $targetMarkerCount = [int](Get-M7Scalar -Service $database.Reseed -User $database.User -Database $database.Database -SQL 'SELECT count(*) FROM public.dr_evidence_markers WHERE marker=3')
            if ($targetMarkerCount -lt 0 -or $targetMarkerCount -gt 1) { throw "failback marker observation was invalid for $($database.Name)" }
            $missingRecords = 1 - $targetMarkerCount
            $sourceObservedAt = [DateTimeOffset]::Parse([string]$failbackPositions[$database.Name].observed_at)
            $replayObservedAt = [DateTimeOffset]::Parse([string]$failbackReplayPositions[$database.Name].observed_at)
            $failbackRPOWindowMS = [Math]::Max(0,[int64]($replayObservedAt.Subtract($sourceObservedAt).TotalMilliseconds))
            $failbackRPOWindows[$database.Name] = $failbackRPOWindowMS
            $failbackMissingRecords[$database.Name] = $missingRecords
            $failbackRPOEvidence.Add([ordered]@{
                database=$database.Name; source_timeline=[uint32]$failbackPositions[$database.Name].timeline;
                standby_replay_timeline=[uint32]$failbackReplayPositions[$database.Name].timeline;
                promoted_timeline=[uint32]$failbackPromotedPositions[$database.Name].timeline; source_end_lsn=$failbackPositions[$database.Name].lsn;
                standby_replay_lsn=$failbackReplayPositions[$database.Name].lsn; sent_wal_bytes=$sent; replayed_wal_bytes=$replayed;
                missing_wal_bytes=$missingWalBytes; source_marker=3; target_marker_count=$targetMarkerCount; missing_records=$missingRecords;
                source_observed_at=$sourceObservedAt.ToString('o'); replay_observed_at=$replayObservedAt.ToString('o'); observation_window_ms=$failbackRPOWindowMS;
                window_definition='acknowledged source marker observation to promoted target replay observation'
            })
        }
        $failbackRTOMS = [Math]::Max(1,[int64]([DateTimeOffset]::UtcNow.Subtract($failbackStart).TotalMilliseconds))
        $failbackRTOEvidence = [ordered]@{ duration_ms=$failbackRTOMS; authority_active=$true; customer_readiness=$true; ingress_switched_after_resume=$true }
        Advance-M7DRPhase -OperationID $failbackOperationID -Prefix 'dr-failback-phase' -Stage 'rto_recorded' -Evidence @{ duration_ms=$failbackRTOMS }
        Advance-M7DRPhase -OperationID $failbackOperationID -Prefix 'dr-failback-phase' -Stage 'rpo_recorded' -Evidence @{
            control=@{missing_records=$failbackMissingRecords['control'];window_ms=$failbackRPOWindows['control']}; shard_0=@{missing_records=$failbackMissingRecords['shard-0'];window_ms=$failbackRPOWindows['shard-0']}; shard_1=@{missing_records=$failbackMissingRecords['shard-1'];window_ms=$failbackRPOWindows['shard-1']}
        }
        Advance-M7DRPhase -OperationID $failbackOperationID -Prefix 'dr-failback-phase' -Stage 'target_active' -Evidence @{
            observed_at=[DateTimeOffset]::UtcNow.ToString('o')
            control=@{region='region-a';epoch=3;state='active';writes_enabled=$true}
            shard_0=@{region='region-a';epoch=3;state='active';writes_enabled=$true}
            shard_1=@{region='region-a';epoch=3;state='active';writes_enabled=$true}
        }
        $null = Assert-M7SettlementImport -DatabaseService 'control-postgres-region-a-reseed' -WorkerService 'settlement-worker-region-a' -Phase 'region-a-failback-epoch-3'
        Invoke-M7K6 -Script 'regional-failback.js' -Environment @{
            VUS='1'; ITERATIONS_PER_VU='1'; DURATION='1m'; FAILBACK_ACTIVE_URL='http://proxy-region-a:8080'; RETAINED_REGION_URL='http://proxy-region-b:8080'
        }
        $failbackIntent = Invoke-Milestone5DriverAPI -BaseURL (Get-M7PublishedURL) -Method POST `
            -Path "/api/v1/reservations/$($m7Customer.Reservations[4])/payment-intents" -Token $m7Customer.Token `
            -IdempotencyKey "m7-failback-payment-$suffix" -Body @{} -ExpectedStatus @(202)
        if ([string]::IsNullOrWhiteSpace([string]$failbackIntent.Body.id)) { throw 'failback payment omitted its durable identity' }
        Complete-M7SandboxPayment -DatabaseService 'control-postgres-region-a-reseed' -ReservationID $m7Customer.Reservations[4] -WebhookBaseURL (Get-M7PublishedURL)
        $failbackTickets = Get-M7Scalar -Service 'booking-shard-0-postgres-region-a-reseed' -User 'railway_booking' -Database 'railway_booking' -SQL "SELECT count(*) FROM public.tickets AS ticket JOIN public.ticket_orders AS orders ON orders.id=ticket.ticket_order_id WHERE orders.reservation_id='$($m7Customer.Reservations[4])'::uuid AND orders.status='issued' AND ticket.status='active'"
        if ($failbackTickets -ne '2') { throw 'failback payment did not issue exactly two active tickets' }
        Add-M7Phase 'region-a-epoch-3-app-connectivity-verified'

        $regionBRetainedServices = @($regionBWriterServices | Where-Object { $_ -ne 'global-test-ingress' }) + @($databases.Standby)
        foreach ($service in $regionBRetainedServices) {
            $running = Invoke-M7Compose -AllowFailure -Arguments @('ps','--status','running','-q',$service)
            if (@($running.Output | Where-Object { $_.Trim() -ne '' }).Count -ne 0) { throw "retained region-b source fence was lost for $service" }
        }
        $regionBRetainedFence = [ordered]@{
            region='region-b'; stopped_writer_processes=($regionBRetainedServices.Count-$databases.Count); stopped_databases=$databases.Count;
            ingress_stopped=$true; credential_consumers_stopped=$true; database_network_endpoints_stopped=$true;
            observed_at=[DateTimeOffset]::UtcNow.ToString('o'); failback_payment_completed=$true; failback_ticket_count=2
        }
        Write-M7JSON -Name 'region-b-retained-fence-observation.json' -Value $regionBRetainedFence
        $regionBRetainedText = $regionBRetainedFence | ConvertTo-Json -Compress
        $signedRegionBRetainedFence = New-M7SignedFenceEvidence -OperationID $failbackOperationID -SourceRegion 'region-b' -SourceEpoch 2 -Purpose 'retained_source_fence' `
            -IncidentID $failbackIncidentID -OperatorID 'operator:local-dr' -ObservationText $regionBRetainedText
        Advance-M7DRPhase -OperationID $failbackOperationID -Prefix 'dr-failback-phase' -Stage 'source_retained_fenced' -Evidence $signedRegionBRetainedFence
        Add-M7Phase 'typed-failback-completed'
        Wait-M7ServiceHTTP -Service 'payment-reconciler' -URL 'http://127.0.0.1:9090/metrics'
        Assert-M7DurableMetrics -Service 'payment-reconciler' -Phase 'region-a-after-failback' -Families @(
            'financial_ledger_transaction_total','settlement_import_total','settlement_reconciliation_total','settlement_reconciliation_mismatch_total',
            'regional_active_epoch','regional_failover_total','regional_failback_total','regional_rpo_observed_seconds','regional_rto_observed_seconds',
            'backup_total','backup_restore_duration_seconds'
        )

        Invoke-M7Compose -Arguments (@('rm','-sf') + @($databases.Standby)) | Out-Null
        foreach ($database in $databases) { Remove-M7ProjectVolume -Suffix $database.StandbyVolume }
        $env:CONTROL_PRIMARY_HOST = 'control-postgres-region-a-reseed-replication'
        $env:SHARD_0_PRIMARY_HOST = 'booking-shard-0-postgres-region-a-reseed-replication'
        $env:SHARD_1_PRIMARY_HOST = 'booking-shard-1-postgres-region-a-reseed-replication'
        $finalStandbyUp = @('up','-d','--no-deps')
        if ($SkipBuild) { $finalStandbyUp += '--no-build' }
        $finalStandbyUp += @($databases.Standby)
        Invoke-M7Compose -Arguments $finalStandbyUp | Out-Null
        foreach ($database in $databases) {
            Wait-M7Role -Service $database.Standby -User $database.ReplicationUser -Database $database.Database -Recovery $true
            $authority = Get-M7Scalar -Service $database.Standby -User $database.User -Database $database.Database -SQL "SELECT region||'|'||epoch::text||'|'||state||'|'||writes_enabled::text FROM public.regional_write_authority WHERE singleton"
            if ($authority -ne 'region-a|3|active|true') { throw "final passive authority is inconsistent for $($database.Name)" }
        }
        $env:REGION_B_DEPLOYMENT_ROLE = 'passive'
        $env:REGION_B_EPOCH = '3'
        $env:REGION_B_WRITES_ENABLED = 'false'
        $passiveReconcilerUp = @('--profile','dr-app','up','-d','--no-deps','--force-recreate')
        if ($SkipBuild) { $passiveReconcilerUp += '--no-build' }
        $passiveReconcilerUp += 'payment-reconciler-region-b'
        Invoke-M7Compose -Arguments $passiveReconcilerUp | Out-Null
        Wait-M7ServiceHTTP -Service 'payment-reconciler-region-b' -URL 'http://127.0.0.1:9090/metrics'
        Assert-M7DurableMetrics -Service 'payment-reconciler-region-b' -Phase 'region-b-final-passive-after-failback' -Families @(
            'regional_replication_lag_bytes','regional_replication_lag_seconds','regional_last_replay_timestamp_seconds'
        ) -AllowZeroFamilies @('regional_replication_lag_bytes','regional_replication_lag_seconds') -ExpectedDatabaseTuples @(
            'region-b|control|none','region-b|booking_shard|shard-0','region-b|booking_shard|shard-1'
        )
        Add-M7Phase 'region-b-reseeded-as-final-passive'

        $finalFinancial = Get-M7FinancialContinuity -ControlService 'control-postgres-region-a-reseed' -ShardService 'booking-shard-0-postgres-region-a-reseed'
        Add-M7Phase 'final-financial-reconciliation-passed'

        $failoverJournal = Get-M7Scalar -Service 'control-postgres-region-a-reseed' -User 'railway_control' -Database 'railway_control' -SQL "SELECT operation_kind||'|'||stage||'|'||checkpoint_version::text FROM public.regional_failover_operations WHERE operation_id='$failoverOperationID'::uuid"
        $failbackJournal = Get-M7Scalar -Service 'control-postgres-region-a-reseed' -User 'railway_control' -Database 'railway_control' -SQL "SELECT operation_kind||'|'||stage||'|'||checkpoint_version::text FROM public.regional_failover_operations WHERE operation_id='$failbackOperationID'::uuid"
        if ($failoverJournal -ne 'failover|source_retained_fenced|21' -or $failbackJournal -ne 'failback|source_retained_fenced|21') {
            throw "typed recovery journals were not terminal: failover=$failoverJournal failback=$failbackJournal"
        }
        $postgresVersions = [System.Collections.Generic.List[object]]::new()
        foreach ($database in $databases) {
            $schemaDirty = Get-M7Scalar -Service $database.Reseed -User $database.User -Database $database.Database -SQL 'SELECT dirty FROM public.schema_migrations'
            if ($schemaDirty -notin @('f','false')) { throw "final schema remained dirty for $($database.Name)" }
            $postgresVersions.Add([ordered]@{
                database=$database.Name; server_version=(Get-M7Scalar -Service $database.Reseed -User $database.User -Database $database.Database -SQL 'SHOW server_version');
                schema_version=[int](Get-M7Scalar -Service $database.Reseed -User $database.User -Database $database.Database -SQL 'SELECT version FROM public.schema_migrations');
                schema_dirty=$false
            })
        }
        $pgbackrestResult = Invoke-M7Compose -Arguments @('exec','-T','control-postgres-region-a-reseed','pgbackrest','version')
        $pgbackrestVersion = [string](@($pgbackrestResult.Output | Where-Object { $_ -match '^pgBackRest [0-9]' }) | Select-Object -Last 1)
        if ($pgbackrestVersion -cne 'pgBackRest 2.59.0') { throw "pgBackRest runtime version was not the selected 2.59.0: $pgbackrestVersion" }
        $paymentIDs = Get-M7Scalar -Service 'control-postgres-region-a-reseed' -User 'railway_control' -Database 'railway_control' -SQL "SELECT string_agg(payment_intent_id::text,',' ORDER BY payment_intent_id) FROM public.payment_intents WHERE reservation_id IN ('$($m7Customer.Reservations[3])'::uuid,'$($m7Customer.Reservations[4])'::uuid)"
        $refundIDs = Get-M7Scalar -Service 'control-postgres-region-a-reseed' -User 'railway_control' -Database 'railway_control' -SQL "SELECT string_agg(refund_request_id::text,',' ORDER BY refund_request_id) FROM public.ticket_refund_requests WHERE ticket_order_id IN ('$($m7Orders[0].OrderID)'::uuid,'$($m7Orders[1].OrderID)'::uuid,'$($m7Orders[2].OrderID)'::uuid)"
        $ticketIDs = Get-M7Scalar -Service 'booking-shard-0-postgres-region-a-reseed' -User 'railway_booking' -Database 'railway_booking' -SQL "SELECT string_agg(id::text,',' ORDER BY id) FROM public.ticket_orders WHERE reservation_id IN ('$($m7Customer.Reservations[0])'::uuid,'$($m7Customer.Reservations[1])'::uuid,'$($m7Customer.Reservations[2])'::uuid,'$($m7Customer.Reservations[3])'::uuid,'$($m7Customer.Reservations[4])'::uuid)"
        foreach ($identitySet in @($paymentIDs,$refundIDs,$ticketIDs)) {
            if ([string]::IsNullOrWhiteSpace($identitySet) -or $identitySet.Length -gt 4096) { throw 'financial identity evidence was empty or unbounded' }
        }
        $webhookOutageMS = [Math]::Max(1,[int64]$webhookOutageEndedAt.Subtract($webhookOutageStartedAt).TotalMilliseconds)

        $endingSourceState = Get-M7SourceState
        if ($endingSourceState.SHA256 -ne $sourceState.SHA256 -or $endingSourceState.FileCount -ne $sourceState.FileCount) {
            throw 'source state changed during the evidence run'
        }
        $summary = [ordered]@{
            status='passed'; environment='disposable-same-host-docker'; replication_mode='asynchronous-streaming';
            source_commit=$sourceCommit; source_dirty=$sourceDirtyAtStart; source_state_exclusions=[string[]]$sourceState.Excluded;
            database_count=3; backups=$backupEvidence; restores=$restoreEvidence; settlement=$settlementEvidence;
            replication_observations=$replicationEvidence;
            durable_metrics=$metricsEvidence;
            software=[ordered]@{ stripe_adapter='internal/payment/provider/stripe'; stripe_api_version='2026-07-29.dahlia'; postgres_image='postgres:16.14-alpine'; pgbackrest=$pgbackrestVersion; databases=$postgresVersions };
            recovery_journals=@(
                [ordered]@{ kind='failover'; operation_id_sha256=(Get-M7SHA256 -Text $failoverOperationID); terminal_stage='source_retained_fenced'; checkpoint_version=21 },
                [ordered]@{ kind='failback'; operation_id_sha256=(Get-M7SHA256 -Text $failbackOperationID); terminal_stage='source_retained_fenced'; checkpoint_version=21 }
            );
            journal_process_crashes=$journalCrashEvidence;
            application_transaction_crashes=$applicationCrashEvidence;
            interrupted_payment_recovery=$interruptedPaymentEvidence;
            interrupted_full_refund_recovery=$interruptedFullRefundEvidence;
            interrupted_refund_recovery=$interruptedRefundEvidence;
            refunds=$refundEvidence; concurrent_refund_retries=100; conflicting_refund_selection_rejected=$true;
            physical_migration_interaction=$physicalMigrationInteractionEvidence;
            financial_continuity=$finalFinancial; settlement_reconciliation=$settlementReconciliationEvidence;
            typed_reconciliation=[ordered]@{ failover=$reconciliationObservation; failback=$failbackReconciliation };
            financial_identity_evidence=[ordered]@{
                payment_count=($paymentIDs -split ',').Count; payment_ids_sha256=(Get-M7SHA256 -Text $paymentIDs);
                refund_count=($refundIDs -split ',').Count; refund_ids_sha256=(Get-M7SHA256 -Text $refundIDs);
                ticket_order_count=($ticketIDs -split ',').Count; ticket_order_ids_sha256=(Get-M7SHA256 -Text $ticketIDs);
                truncated=$false
            };
            webhook_durability=[ordered]@{
                overlap_events=2; post_rotation_current_events=1; retired_key_events=0; failover_retry_events=1;
                ack_before_commit_rejected=$true; passive_generation_verified=$true; previous_key_retired=$true;
                outage_started_at=$webhookOutageStartedAt.ToString('o'); outage_ended_at=$webhookOutageEndedAt.ToString('o'); outage_ms=$webhookOutageMS;
                retried_event_id_sha256=(Get-M7SHA256 -Text "evt_m7_ack_$suffix");
                stripe_rotation=$stripeRotationEvidence
            };
            rpo_acceptance_bounds=[ordered]@{ maximum_missing_markers_per_database=1; maximum_missing_wal_bytes_per_database=536870912 };
            observed_rpo=[ordered]@{ missing_markers=$missingMarkers; databases=$rpoEvidence; claim_scope='this bounded run only' };
            failback_rpo=[ordered]@{ missing_markers=[int](($failbackRPOEvidence | Measure-Object -Property missing_records -Sum).Sum); databases=$failbackRPOEvidence; claim_scope='this bounded run only' };
            observed_rto_seconds=[Math]::Round(([double]$failoverRTOEvidence.duration_ms / 1000),3);
            failback_seconds=[Math]::Round(([double]$failbackRTOEvidence.duration_ms / 1000),3);
            rto_conditions=[ordered]@{ failover=$failoverRTOEvidence; failback=$failbackRTOEvidence };
            redis_loss_boundary=$redisRecoveryEvidence;
            promoted_single_shard_outage=$singleShardOutageEvidence;
            exact_k6_modules=10; final_reconciliation=[ordered]@{ passed=$true; truncated=$false; authoritative=$true };
            final_active_region='region-a'; final_epoch=3; final_passive_region='region-b'; phases=$phaseEvidence;
            limitations=@('same-host Docker networks are not independent regions','external fencing is project-scoped container stoppage','observed RPO and RTO are not production guarantees')
            source_state_sha256=$sourceState.SHA256; source_file_count=$sourceState.FileCount; source_state_excluded_count=$sourceState.Excluded.Count;
            rendered_compose_config_sha256=$renderedDigest
        }
        Write-M7JSON -Name 'milestone-7-dr-evidence-summary.json' -Value $summary
    } catch {
        $runError = $_
    } finally {
        $teardownStatus = 'passed'
        if ($started) {
            $down = Invoke-M7Compose -AllowFailure -Arguments @('--profile','dr-failback','--profile','dr-restore','--profile','dr-tools','--profile','dr-app','--profile','dr-tests','down','-v','--remove-orphans','--timeout','20')
            if ($down.ExitCode -ne 0) { $teardownStatus = 'failed' }
        }
        if ($teardownStatus -ne 'passed' -and $null -eq $runError) {
            $runError = [System.Exception]::new('project-scoped teardown failed')
        }
        $completed = [DateTimeOffset]::UtcNow
        $manifestStatus = 'passed'
        $errorCategory = $null
        if ($null -ne $runError -or $teardownStatus -ne 'passed') { $manifestStatus='failed'; $errorCategory='dr_evidence_failed' }
        Write-M7JSON -Name 'run-manifest.json' -Value ([ordered]@{
            status=$manifestStatus; started_at=$start.ToString('o'); completed_at=$completed.ToString('o');
            duration_seconds=[Math]::Round($completed.Subtract($start).TotalSeconds,3);
            project_name=$ProjectName; source_state_sha256=$sourceState.SHA256; source_file_count=$sourceState.FileCount;
            source_commit=$sourceCommit; source_dirty=$sourceDirtyAtStart; source_state_exclusions=[string[]]$sourceState.Excluded;
            rendered_compose_config_sha256=$renderedDigest; source_state_verified=($null -eq $runError);
            evidence_secret_scan='pending'; teardown=$teardownStatus; error_category=$errorCategory
        })
        Assert-M7EvidenceSecretSafe
        $manifestPath = Join-Path $EvidenceDirectory 'run-manifest.json'
        $manifest = Get-Content -Raw -LiteralPath $manifestPath | ConvertFrom-Json
        $manifest.evidence_secret_scan = 'passed'
        Write-M7JSON -Name 'run-manifest.json' -Value $manifest
        Write-M7EvidenceIndex
    }
    if ($null -ne $runError) { throw $runError }
} finally {
    foreach ($entry in $originalEnvironment.GetEnumerator()) {
        if ($null -eq $entry.Value) { Remove-Item "Env:$($entry.Key)" -ErrorAction SilentlyContinue }
        else { Set-Item "Env:$($entry.Key)" -Value $entry.Value }
    }
    $controlReplicationSecret = $null
    $shard0ReplicationSecret = $null
    $shard1ReplicationSecret = $null
    $controlCipherSecret = $null
    $shard0CipherSecret = $null
    $shard1CipherSecret = $null
    if ([System.IO.Directory]::Exists($secretDirectory)) { [System.IO.Directory]::Delete($secretDirectory, $true) }
}
