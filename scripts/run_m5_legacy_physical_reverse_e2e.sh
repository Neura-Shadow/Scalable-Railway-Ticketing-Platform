#!/usr/bin/env bash
set -euo pipefail

: "${CONTROL_DATABASE_URL:?CONTROL_DATABASE_URL is required}"
: "${BOOKING_SHARD_0_DATABASE_URL:?BOOKING_SHARD_0_DATABASE_URL is required}"
: "${BOOKING_SHARD_1_DATABASE_URL:?BOOKING_SHARD_1_DATABASE_URL is required}"

# Milestone 7 write guards require every application-shaped write to identify
# the durable active authority. This acceptance fixture operates against the
# migration's initial Region A, epoch 1 authority.
export PGOPTIONS="${PGOPTIONS:-} -c railway.deployment_region=region-a -c railway.deployment_role=active -c railway.region_epoch=1 -c railway.regional_writes_enabled=true"
export DEPLOYMENT_REGION=region-a
export DEPLOYMENT_ROLE=active
export REGION_EPOCH=1
export REGIONAL_WRITES_ENABLED=true

run_id=86000000-0000-4000-8000-000000000001
forward_id=86000000-0000-4000-8000-000000000002
reverse_id=86000000-0000-4000-8000-000000000003
command_id=86000000-0000-4000-8000-000000000004
outbox_id=86000000-0000-4000-8000-000000000005
seat_id=86000000-0000-4000-8000-000000000006
coach_id=86000000-0000-4000-8000-000000000007
train_id=86000000-0000-4000-8000-000000000008
route_id=86000000-0000-4000-8000-000000000009
station_a=86000000-0000-4000-8000-00000000000a
station_b=86000000-0000-4000-8000-00000000000b

admin_bin="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/physical-shard-admin-m5-e2e"
go build -o "$admin_bin" ./cmd/physical-shard-admin
trap 'rm -f "$admin_bin"' EXIT

admin() { "$admin_bin" "$@"; }

psql "$CONTROL_DATABASE_URL" --set=ON_ERROR_STOP=1 <<SQL
BEGIN;
INSERT INTO public.stations(id,code,name,timezone) VALUES
 ('$station_a','M5A','M5 acceptance A','UTC'),
 ('$station_b','M5B','M5 acceptance B','UTC');
INSERT INTO public.routes(id,code,name,operating_timezone)
 VALUES('$route_id','M5-REVERSE','M5 reverse acceptance','UTC');
INSERT INTO public.route_stops(route_id,station_id,stop_index,arrival_offset_minutes,departure_offset_minutes) VALUES
 ('$route_id','$station_a',0,0,0),('$route_id','$station_b',1,60,60);
INSERT INTO public.trains(id,code,name) VALUES('$train_id','M5TRAIN','M5 train');
INSERT INTO public.coaches(id,train_id,coach_number,seat_class)
 VALUES('$coach_id','$train_id','1','standard');
INSERT INTO public.seats(id,coach_id,seat_number,seat_type)
 VALUES('$seat_id','$coach_id','1A','window');
INSERT INTO public.train_runs(
 id,train_id,route_id,service_date,scheduled_departure_at,segment_count
) VALUES('$run_id','$train_id','$route_id',date '2036-01-01',timestamptz '2036-01-01 00:00:00Z',1);
DO \$\$
BEGIN
 IF NOT EXISTS (
   SELECT 1 FROM public.train_run_shard_assignments AS assignment
   JOIN public.train_run_write_fences AS fence USING (train_run_id)
   WHERE assignment.train_run_id='$run_id'
     AND assignment.shard_id='legacy'
     AND assignment.assignment_generation=1
     AND assignment.assignment_state='stable'
     AND assignment.availability_generation=1
     AND fence.assignment_generation=1 AND fence.write_enabled
 ) THEN
   RAISE EXCEPTION 'train-run bootstrap did not create the legacy generation-1 writer';
 END IF;
END
\$\$;
INSERT INTO public.seat_inventory(
 train_run_id,segment_count,seat_id,seat_class,occupied_segments,version
) VALUES('$run_id',1,'$seat_id','standard',B'0',0);
UPDATE public.booking_shards
SET enabled=true,write_enabled=true,state='active',health_state='healthy',
    last_health_checked_at=clock_timestamp(),write_disabled_reason=NULL
WHERE shard_id IN ('physical-shard-0','physical-shard-1');
COMMIT;
SQL

admin plan-migration --migration-id "$forward_id" --train-run-id "$run_id" \
  --target-shard physical-shard-0 --confirm
admin enable-capture --migration-id "$forward_id" --confirm
admin enable-capture --migration-id "$forward_id" --confirm
admin start-base-copy --migration-id "$forward_id" --confirm
psql "$CONTROL_DATABASE_URL" --set=ON_ERROR_STOP=1 -c \
  "UPDATE public.seat_inventory SET version=version+1 WHERE train_run_id='$run_id'"
for _ in 1 2 3 4; do admin resume-base-copy --migration-id "$forward_id" --confirm; done
admin replay-journal --migration-id "$forward_id" --confirm
admin validate-online --migration-id "$forward_id" --confirm
admin begin-quiesce --migration-id "$forward_id" --confirm
admin final-catchup --migration-id "$forward_id" --confirm
for _ in 1 2 3 4; do admin cutover --migration-id "$forward_id" --confirm; done

psql "$BOOKING_SHARD_0_DATABASE_URL" --set=ON_ERROR_STOP=1 <<SQL
BEGIN;
INSERT INTO public.booking_command_receipts(
 command_id,train_run_id,assignment_generation,command_type,request_fingerprint
) VALUES('$command_id','$run_id',2,'seat.disable',decode(repeat('22',32),'hex'));
UPDATE public.seat_inventory SET version=version+1
 WHERE train_run_id='$run_id' AND assignment_generation=2;
INSERT INTO public.outbox_events(
 id,train_run_id,assignment_generation,aggregate_type,aggregate_id,event_type,payload
) VALUES('$outbox_id','$run_id',2,'booking_command','$command_id','booking_command.finalized','{}');
UPDATE public.train_run_target_write_evidence
SET successful_write_count=successful_write_count+1,
    first_successful_write_at=COALESCE(first_successful_write_at,clock_timestamp()),
    last_successful_write_at=clock_timestamp(),last_command_id='$command_id'
WHERE train_run_id='$run_id' AND assignment_generation=2;
UPDATE public.booking_command_receipts
SET status='succeeded',result_type='seat',result_id='$seat_id',result_source_version=41,
    completed_at=clock_timestamp()
WHERE command_id='$command_id';
COMMIT;
SQL

psql "$BOOKING_SHARD_0_DATABASE_URL" --set=ON_ERROR_STOP=1 <<SQL
DO \$\$
BEGIN
 IF NOT EXISTS (
   SELECT 1 FROM public.booking_command_receipts
   WHERE command_id='$command_id' AND train_run_id='$run_id'
     AND assignment_generation=2 AND result_source_version=41
 ) THEN
   RAISE EXCEPTION 'operator receipt source version was not persisted';
 END IF;
END
\$\$;
SQL

admin plan-reverse-migration --migration-id "$forward_id" \
  --reverse-migration-id "$reverse_id" --generation 3 --confirm

# A normal or cross-transaction legacy writer remains fenced while physical is
# authoritative. Test it before target preparation clears the retained rows so
# success cannot be hidden by a zero-row update. A residual authorization row
# is inert outside its own txid.
psql "$CONTROL_DATABASE_URL" --set=ON_ERROR_STOP=1 -c \
  "INSERT INTO public.physical_control_target_apply_authorizations(migration_id,train_run_id,target_shard_id,target_generation,transaction_id) VALUES('$reverse_id','$run_id','legacy',3,txid_current())"
if psql "$CONTROL_DATABASE_URL" --set=ON_ERROR_STOP=1 -c \
  "UPDATE public.seat_inventory SET version=version+1 WHERE train_run_id='$run_id'"; then
  echo 'cross-transaction legacy write unexpectedly succeeded' >&2
  exit 1
fi
psql "$CONTROL_DATABASE_URL" --set=ON_ERROR_STOP=1 -c \
  "DELETE FROM public.physical_control_target_apply_authorizations WHERE migration_id='$reverse_id'"

admin enable-capture --migration-id "$reverse_id" --confirm
admin enable-capture --migration-id "$reverse_id" --confirm
admin start-base-copy --migration-id "$reverse_id" --confirm
psql "$BOOKING_SHARD_0_DATABASE_URL" --set=ON_ERROR_STOP=1 -c \
  "UPDATE public.seat_inventory SET version=version+1 WHERE train_run_id='$run_id' AND assignment_generation=2"
for _ in 1 2 3 4 5 6; do admin resume-base-copy --migration-id "$reverse_id" --confirm; done
admin replay-journal --migration-id "$reverse_id" --confirm
admin validate-online --migration-id "$reverse_id" --confirm
admin begin-quiesce --migration-id "$reverse_id" --confirm
admin final-catchup --migration-id "$reverse_id" --confirm
for _ in 1 2 3 4; do admin cutover --migration-id "$reverse_id" --confirm; done

# Reverse targets the bounded v8 control layout, which has no shard receipt
# relation. The retained physical source remains the durable historical copy.
psql "$BOOKING_SHARD_0_DATABASE_URL" --set=ON_ERROR_STOP=1 <<SQL
DO \$\$
BEGIN
 IF NOT EXISTS (
   SELECT 1 FROM public.booking_command_receipts
   WHERE command_id='$command_id' AND train_run_id='$run_id'
     AND assignment_generation=2 AND result_source_version=41
 ) THEN
   RAISE EXCEPTION 'reverse removed or changed retained operator receipt';
 END IF;
END
\$\$;
SQL

psql "$CONTROL_DATABASE_URL" --set=ON_ERROR_STOP=1 <<SQL
DO \$\$
DECLARE assignment_ok boolean; residual integer;
BEGIN
 SELECT assignment.shard_id='legacy'
    AND assignment.assignment_generation=3
    AND assignment.assignment_state='rollback_window'
    AND fence.assignment_generation=3 AND fence.write_enabled
 INTO assignment_ok
 FROM public.train_run_shard_assignments AS assignment
 JOIN public.train_run_write_fences AS fence USING(train_run_id)
 WHERE assignment.train_run_id='$run_id';
 SELECT count(*) INTO residual FROM public.physical_control_target_apply_authorizations;
 IF NOT coalesce(assignment_ok,false) OR residual<>0 THEN
   RAISE EXCEPTION 'reverse acceptance invariant failed';
 END IF;
END
\$\$;
UPDATE public.seat_inventory SET version=version+1 WHERE train_run_id='$run_id';
SQL

echo 'legacy -> physical target write -> legacy reverse acceptance passed'
