$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$root = Split-Path -Parent $PSScriptRoot
$primaryBootstrap = Get-Content -Raw (Join-Path $root 'deploy/postgres/dr/10-replication.sh')
$standbyBootstrap = Get-Content -Raw (Join-Path $root 'deploy/postgres/dr/start-standby.sh')
$primaryStartup = Get-Content -Raw (Join-Path $root 'deploy/postgres/dr/start-primary.sh')
$primaryHealth = Get-Content -Raw (Join-Path $root 'deploy/postgres/dr/primary-health.sh')
$standbyHealth = Get-Content -Raw (Join-Path $root 'deploy/postgres/dr/standby-health.sh')
$compose = Get-Content -Raw (Join-Path $root 'docker-compose.dr.yml')
$evidenceRunner = Get-Content -Raw (Join-Path $root 'scripts/run-milestone-7-dr-evidence.ps1')

$failures = [System.Collections.Generic.List[string]]::new()

function Require-Token {
    param(
        [string]$Content,
        [string]$Token,
        [string]$Message
    )
    if (-not $Content.Contains($Token)) {
        $failures.Add($Message)
    }
}

function Reject-Pattern {
    param(
        [string]$Content,
        [string]$Pattern,
        [string]$Message
    )
    if ($Content -match $Pattern) {
        $failures.Add($Message)
    }
}

Require-Token $primaryBootstrap 'DR_REPLICATION_SOURCE_CIDR' 'primary bootstrap must require an explicit source CIDR'
Require-Token $primaryBootstrap 'hostssl replication' 'physical replication HBA must require TLS'
Require-Token $primaryBootstrap 'ALTER ROLE' 're-running the primary bootstrap must rotate the replication credential'
Require-Token $primaryBootstrap 'pg_hba.replication.conf' 'replication HBA must use a replaceable managed include'
Require-Token $primaryBootstrap 'include_if_exists "pg_hba.replication.conf"' 'the managed HBA include filename must use PostgreSQL HBA double-quote syntax'
Reject-Pattern $primaryBootstrap "include_if_exists\s+'pg_hba\.replication\.conf'" 'single quotes become part of an HBA include filename and must not be used'
Require-Token $primaryBootstrap 'chown postgres:postgres "$hba_include"' 'a live root bootstrap must leave the restricted HBA include readable by PostgreSQL'
Require-Token $primaryBootstrap 'SHOW data_checksums' 'primary bootstrap must fail closed when initdb data checksums are disabled'
Reject-Pattern $primaryBootstrap '(?m)^host\s+replication\s+\S+\s+all\s+' 'replication HBA must not admit every source address'

Require-Token $standbyBootstrap 'sslmode=verify-full' 'standby connections must authenticate the primary certificate and hostname'
Require-Token $standbyBootstrap 'DR_REPLICATION_TLS_ROOT_CERT_FILE' 'standby bootstrap must require a trusted replication CA'
Require-Token $standbyBootstrap 'archive-get %f %p' 'standby recovery must fall back to the WAL archive'
Require-Token $standbyBootstrap 'pg_replication_slots' 'standby bootstrap must inspect an existing physical slot before reuse'
Require-Token $standbyBootstrap 'wal_status' 'standby bootstrap must reject or rebuild a lost physical slot'
Require-Token $standbyBootstrap 'find "$PGDATA" -mindepth 1' 'base-backup cleanup must remove dotfiles as well as ordinary files'
Require-Token $standbyBootstrap 'DR_FORCE_RESEED' 'an initialized non-standby data directory must require explicit destructive reseed authority'
Require-Token $standbyBootstrap 'Data page checksum version' 'standby startup must reject clusters without data checksums'
Require-Token $standbyBootstrap 'basebackup-in-progress' 'standby bootstrap must record an interrupted base-backup marker'
Reject-Pattern $standbyBootstrap 'rm\s+-rf\s+"\$\{PGDATA:\?\}"/\*' 'base-backup cleanup must not leave hidden partial files behind'
Reject-Pattern $standbyBootstrap 'sslmode=prefer' 'replication must not silently downgrade TLS authentication'

Require-Token $primaryStartup '-c ssl=on' 'primary startup must enable PostgreSQL server TLS'
Require-Token $primaryStartup 'replication-server.key' 'primary startup must install a restricted external TLS key'
Require-Token $primaryHealth "current_setting('data_checksums')" 'primary health must assert data checksums'
Require-Token $primaryHealth 'pg_stat_archiver' 'primary health must assert current WAL archive health'
Require-Token $standbyHealth 'pg_stat_wal_receiver' 'standby health must assert a streaming WAL receiver'
Require-Token $standbyHealth 'pg_last_xact_replay_timestamp' 'standby health must reject stale replay'
Require-Token $standbyHealth 'timeline_id' 'standby health must observe a live timeline'

foreach ($token in @(
    'railway_control_replicator', 'railway_shard_0_replicator', 'railway_shard_1_replicator',
    'dr_control_replication_password', 'dr_shard_0_replication_password', 'dr_shard_1_replication_password',
    'POSTGRES_INITDB_ARGS: --data-checksums', 'DR_REPLICATION_TLS_CERT_FILE', 'DR_REPLICATION_TLS_KEY_FILE'
)) {
    Require-Token $compose $token "compose replication contract is missing $token"
}
foreach ($replicationDNS in @(
    'control-postgres-replication', 'control-postgres-region-b-replication', 'control-postgres-region-a-reseed-replication',
    'booking-shard-0-postgres-replication', 'booking-shard-0-postgres-region-b-replication', 'booking-shard-0-postgres-region-a-reseed-replication',
    'booking-shard-1-postgres-replication', 'booking-shard-1-postgres-region-b-replication', 'booking-shard-1-postgres-region-a-reseed-replication'
)) {
    Require-Token $compose $replicationDNS "compose must give each PostgreSQL endpoint an unambiguous replication-network DNS alias: $replicationDNS"
    Require-Token $evidenceRunner "ReplicationDNS='$replicationDNS'" "replication TLS certificate must cover the isolated DNS alias: $replicationDNS"
}
Require-Token $evidenceRunner 'subjectAltName=DNS:$($endpoint.DNS),DNS:$($endpoint.ReplicationDNS)' 'replication certificate SANs must authenticate both service and isolated replication DNS names'
foreach ($token in @(
    'pgbackrest-control-repository', 'pgbackrest-shard-0-repository', 'pgbackrest-shard-1-repository',
    'pgbackrest_control_cipher_pass', 'pgbackrest_shard_0_cipher_pass', 'pgbackrest_shard_1_cipher_pass',
    'dr_control_region_a_tls_key', 'dr_control_region_b_tls_key', 'dr_control_region_a_reseed_tls_key',
    ('dr_shard_0_region_a_tls_' + 'key'), ('dr_shard_0_region_b_tls_' + 'key'), ('dr_shard_0_region_a_reseed_tls_' + 'key'),
    ('dr_shard_1_region_a_tls_' + 'key'), ('dr_shard_1_region_b_tls_' + 'key'), ('dr_shard_1_region_a_reseed_tls_' + 'key')
)) {
    Require-Token $compose $token "compose isolation contract is missing $token"
}
Reject-Pattern $compose '(?m)^\s{2}pgbackrest-repository:\s*$' 'compose must not expose one repository volume to all databases'
Reject-Pattern $compose '(?m)^\s{2}dr_replication_tls_key:\s*$' 'compose must not provision one TLS private key to all endpoints'
Reject-Pattern $compose '(?m)^\s{2}pgbackrest_cipher_pass:\s*$' 'compose must not provision one backup cipher to all databases'
Require-Token $compose '/etc/railway/configure-replication.sh' 'live primaries must expose the bounded pair-specific credential rotation helper'
Reject-Pattern $compose '(?m)^\s+DR_REPLICATION_USER:\s+railway_replicator\s*$' 'compose must not share one replication identity across database pairs'
Require-Token $evidenceRunner "window_definition='acknowledged source marker observation to promoted target replay observation'" 'RPO evidence must state its marker-derived replication observation window'
Require-Token $evidenceRunner 'source_observed_at=' 'RPO evidence must record source observation time'
Require-Token $evidenceRunner 'replay_observed_at=' 'RPO evidence must record target replay observation time'
Require-Token $evidenceRunner 'target_marker_count=$targetMarkerCount' 'RPO evidence must derive missing records from the promoted marker observation'
Reject-Pattern $evidenceRunner 'missing_records=0' 'RPO evidence must not force an observed zero-record loss'
Reject-Pattern $evidenceRunner '(?m)RPOWindowMS\s*=.*Subtract\(\$fail(?:over|back)Start\)' 'RPO must not reuse end-to-end failover or failback elapsed time'
Require-Token $evidenceRunner 'pg_stat_ssl' 'evidence must prove the replication connection negotiated TLS'
Require-Token $evidenceRunner 'safe_wal_size' 'evidence must observe bounded slot WAL headroom'
Require-Token $evidenceRunner 'pg_stat_archiver' 'evidence must observe WAL archive success and failure state'

if ($failures.Count -gt 0) {
    foreach ($failure in $failures) {
        Write-Error $failure
    }
    exit 1
}

Write-Host 'PostgreSQL DR replication contract checks passed.'
