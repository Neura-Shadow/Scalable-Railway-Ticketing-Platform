package readmodel_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/clock"
	readmodel "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/readmodel"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestRebuildTrainRunCreatesEveryPricedForwardJourney(t *testing.T) {
	conn := openMigrationSevenDatabase(t)
	trainRunID := seedProjectionSource(t, conn)
	rebuiltAt := time.Date(2026, time.July, 22, 8, 0, 0, 0, time.UTC)
	store, err := readmodel.NewStore(conn, clock.NewDeterministic(rebuiltAt))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	result, err := store.RebuildTrainRun(context.Background(), trainRunID.String())
	if err != nil {
		t.Fatalf("RebuildTrainRun() error = %v", err)
	}
	if result.TrainRunID != trainRunID.String() || result.RowsWritten != 6 || result.Deleted {
		t.Fatalf("RebuildTrainRun() = %+v, want six current rows", result)
	}

	rows, err := conn.Query(context.Background(), `
		SELECT
			from_station_code,
			to_station_code,
			from_stop_index,
			to_stop_index,
			seat_class,
			fare_amount_minor,
			departure_at,
			arrival_at,
			rebuilt_at
		FROM train_run_journey_read_model
		WHERE train_run_id = $1
		ORDER BY from_stop_index, to_stop_index, seat_class
	`, trainRunID)
	if err != nil {
		t.Fatalf("read rebuilt journeys: %v", err)
	}
	journeys, err := pgx.CollectRows(rows, pgx.RowToStructByPos[projectionJourney])
	if err != nil {
		t.Fatalf("collect rebuilt journeys: %v", err)
	}
	if len(journeys) != 6 {
		t.Fatalf("rebuilt journeys = %d, want 6", len(journeys))
	}
	for _, journey := range journeys {
		if journey.FromStopIndex >= journey.ToStopIndex {
			t.Fatalf("rebuilt reverse journey = %+v", journey)
		}
		if !journey.RebuiltAt.Equal(rebuiltAt) {
			t.Fatalf("rebuilt_at = %s, want %s", journey.RebuiltAt, rebuiltAt)
		}
	}

	assertProjectedJourney(t, journeys, "TPE", "KHH", "standard", 1200,
		time.Date(2026, time.July, 19, 16, 5, 0, 0, time.UTC),
		time.Date(2026, time.July, 20, 17, 0, 0, 0, time.UTC),
	)
	assertProjectedJourney(t, journeys, "TXG", "KHH", "business", 1800,
		time.Date(2026, time.July, 19, 17, 5, 0, 0, time.UTC),
		time.Date(2026, time.July, 20, 17, 0, 0, 0, time.UTC),
	)
}

func TestMigrationSevenRejectsUnknownProjectionSeatClass(t *testing.T) {
	conn := openMigrationSevenDatabase(t)
	trainRunID := seedProjectionSource(t, conn)
	store, err := readmodel.NewStore(conn, clock.NewDeterministic(time.Now().UTC()))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	if _, err := store.RebuildTrainRun(context.Background(), trainRunID.String()); err != nil {
		t.Fatalf("RebuildTrainRun() error = %v", err)
	}

	_, err = conn.Exec(context.Background(), `
		UPDATE train_run_journey_read_model
		SET seat_class = 'platinum'
		WHERE train_run_id = $1
		  AND seat_class = 'standard'
	`, trainRunID)
	var pgError *pgconn.PgError
	if !errors.As(err, &pgError) || pgError.Code != "23514" {
		t.Fatalf("unknown projection seat class error = %v, want check violation", err)
	}
}

func TestDeleteTrainRunProjectionLeavesAuthoritativeSourceUntouched(t *testing.T) {
	conn := openMigrationSevenDatabase(t)
	trainRunID := seedProjectionSource(t, conn)
	store, err := readmodel.NewStore(conn, clock.NewDeterministic(time.Now().UTC()))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	if _, err := store.RebuildTrainRun(context.Background(), trainRunID.String()); err != nil {
		t.Fatalf("RebuildTrainRun() error = %v", err)
	}

	deleted, err := store.DeleteTrainRunProjection(context.Background(), trainRunID.String())
	if err != nil {
		t.Fatalf("DeleteTrainRunProjection() error = %v", err)
	}
	if deleted != 6 {
		t.Fatalf("DeleteTrainRunProjection() = %d, want 6", deleted)
	}
	var sourceRows, projectionRows int
	if err := conn.QueryRow(context.Background(), `
		SELECT
			(SELECT count(*) FROM train_runs WHERE id = $1),
			(SELECT count(*) FROM train_run_journey_read_model WHERE train_run_id = $1)
	`, trainRunID).Scan(&sourceRows, &projectionRows); err != nil {
		t.Fatalf("inspect source after projection delete: %v", err)
	}
	if sourceRows != 1 || projectionRows != 0 {
		t.Fatalf("rows after projection delete = source %d projection %d, want 1 and 0", sourceRows, projectionRows)
	}
}

func TestProcessTrainRunEventCommitsOneReceiptAndSkipsDuplicateRebuild(t *testing.T) {
	conn := openMigrationSevenDatabase(t)
	trainRunID := seedProjectionSource(t, conn)
	processedAt := time.Date(2026, time.July, 22, 9, 0, 0, 0, time.UTC)
	store, err := readmodel.NewStore(conn, clock.NewDeterministic(processedAt))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	event := readmodel.ProjectionEvent{
		ConsumerName:  "railway-read-model",
		EventID:       uuid.NewString(),
		EventType:     "trainrun.updated",
		AggregateType: "train_run",
		AggregateID:   trainRunID.String(),
	}

	first, err := store.ProcessTrainRunEvent(context.Background(), event)
	if err != nil {
		t.Fatalf("ProcessTrainRunEvent(first) error = %v", err)
	}
	if first.Duplicate || first.RowsWritten != 6 {
		t.Fatalf("ProcessTrainRunEvent(first) = %+v, want six-row rebuild", first)
	}
	if _, err := conn.Exec(context.Background(), `
		UPDATE train_run_journey_read_model
		SET fare_amount_minor = 9999
		WHERE train_run_id = $1
		  AND from_stop_index = 0
		  AND to_stop_index = 1
		  AND seat_class = 'standard'
	`, trainRunID); err != nil {
		t.Fatalf("inject post-rebuild projection marker: %v", err)
	}

	duplicate, err := store.ProcessTrainRunEvent(context.Background(), event)
	if err != nil {
		t.Fatalf("ProcessTrainRunEvent(duplicate) error = %v", err)
	}
	if !duplicate.Duplicate || duplicate.RowsWritten != 0 {
		t.Fatalf("ProcessTrainRunEvent(duplicate) = %+v, want duplicate no-op", duplicate)
	}
	var receiptRows int
	var markedFare int64
	if err := conn.QueryRow(context.Background(), `
		SELECT
			(SELECT count(*) FROM read_model_event_receipts WHERE consumer_name = $1 AND event_id = $2),
			(SELECT fare_amount_minor FROM train_run_journey_read_model
			 WHERE train_run_id = $3 AND from_stop_index = 0 AND to_stop_index = 1 AND seat_class = 'standard')
	`, event.ConsumerName, event.EventID, trainRunID).Scan(&receiptRows, &markedFare); err != nil {
		t.Fatalf("inspect duplicate event result: %v", err)
	}
	if receiptRows != 1 || markedFare != 9999 {
		t.Fatalf("duplicate event state = receipts %d fare %d, want 1 and 9999", receiptRows, markedFare)
	}
}

func TestProcessTrainRunEventReloadsCurrentStateForOutOfOrderEvent(t *testing.T) {
	conn := openMigrationSevenDatabase(t)
	trainRunID := seedProjectionSource(t, conn)
	store, err := readmodel.NewStore(conn, clock.NewDeterministic(time.Now().UTC()))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	if _, err := store.ProcessTrainRunEvent(context.Background(), projectionTrainRunEvent(trainRunID)); err != nil {
		t.Fatalf("ProcessTrainRunEvent(initial) error = %v", err)
	}
	if _, err := conn.Exec(context.Background(), `
		UPDATE fares
		SET amount_minor = 1350,
			updated_at = updated_at + interval '1 minute'
		WHERE train_run_id = $1
		  AND from_stop_index = 0
		  AND to_stop_index = 2
		  AND seat_class = 'standard'
	`, trainRunID); err != nil {
		t.Fatalf("update authoritative current fare: %v", err)
	}

	olderTransportEvent := projectionTrainRunEvent(trainRunID)
	olderTransportEvent.EventType = "trainrun.created"
	if _, err := store.ProcessTrainRunEvent(context.Background(), olderTransportEvent); err != nil {
		t.Fatalf("ProcessTrainRunEvent(out-of-order) error = %v", err)
	}
	var projectedFare int64
	if err := conn.QueryRow(context.Background(), `
		SELECT fare_amount_minor
		FROM train_run_journey_read_model
		WHERE train_run_id = $1
		  AND from_stop_index = 0
		  AND to_stop_index = 2
		  AND seat_class = 'standard'
	`, trainRunID).Scan(&projectedFare); err != nil {
		t.Fatalf("read current-state projection: %v", err)
	}
	if projectedFare != 1350 {
		t.Fatalf("out-of-order event projected fare = %d, want current source fare 1350", projectedFare)
	}
}

func TestProcessTrainRunEventRollsBackReceiptAndCompleteProjectionOnWriteFailure(t *testing.T) {
	conn := openMigrationSevenDatabase(t)
	trainRunID := seedProjectionSource(t, conn)
	store, err := readmodel.NewStore(conn, clock.NewDeterministic(time.Now().UTC()))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	if _, err := store.ProcessTrainRunEvent(context.Background(), projectionTrainRunEvent(trainRunID)); err != nil {
		t.Fatalf("ProcessTrainRunEvent(initial) error = %v", err)
	}
	if _, err := conn.Exec(context.Background(), `
		UPDATE fares
		SET amount_minor = 1350,
			updated_at = updated_at + interval '1 minute'
		WHERE train_run_id = $1
		  AND from_stop_index = 0
		  AND to_stop_index = 2
		  AND seat_class = 'standard'
	`, trainRunID); err != nil {
		t.Fatalf("update source for projection failure injection: %v", err)
	}
	if _, err := conn.Exec(context.Background(), `
		CREATE FUNCTION reject_projection_test_write()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $trigger$
		BEGIN
			IF NEW.fare_amount_minor = 1350 THEN
				RAISE EXCEPTION 'injected projection failure';
			END IF;
			RETURN NEW;
		END
		$trigger$;
	`); err != nil {
		t.Fatalf("create projection failure function: %v", err)
	}
	if _, err := conn.Exec(context.Background(), `
		CREATE TRIGGER reject_projection_test_write
		BEFORE INSERT ON train_run_journey_read_model
		FOR EACH ROW EXECUTE FUNCTION reject_projection_test_write()
	`); err != nil {
		t.Fatalf("install projection failure injection: %v", err)
	}
	event := projectionTrainRunEvent(trainRunID)
	if _, err := store.ProcessTrainRunEvent(context.Background(), event); !errors.Is(err, readmodel.ErrPersistence) {
		t.Fatalf("ProcessTrainRunEvent(failure) error = %v, want ErrPersistence", err)
	}

	var receiptRows, projectionRows int
	var projectedFare int64
	if err := conn.QueryRow(context.Background(), `
		SELECT
			(SELECT count(*) FROM read_model_event_receipts WHERE consumer_name = $1 AND event_id = $2),
			(SELECT count(*) FROM train_run_journey_read_model WHERE train_run_id = $3),
			(SELECT fare_amount_minor FROM train_run_journey_read_model
			 WHERE train_run_id = $3 AND from_stop_index = 0 AND to_stop_index = 2 AND seat_class = 'standard')
	`, event.ConsumerName, event.EventID, trainRunID).Scan(&receiptRows, &projectionRows, &projectedFare); err != nil {
		t.Fatalf("inspect failed event transaction: %v", err)
	}
	if receiptRows != 0 || projectionRows != 6 || projectedFare != 1200 {
		t.Fatalf(
			"failed event state = receipts %d rows %d fare %d, want 0, 6, 1200",
			receiptRows,
			projectionRows,
			projectedFare,
		)
	}
}

func TestRebuildAllIsBoundedAndResumesAfterOpaqueCursor(t *testing.T) {
	conn := openMigrationSevenDatabase(t)
	firstTrainRunID := seedProjectionSource(t, conn)
	secondTrainRunID := cloneProjectionTrainRun(t, conn, firstTrainRunID)
	store, err := readmodel.NewStore(conn, clock.NewDeterministic(time.Now().UTC()))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	first, err := store.RebuildAll(context.Background(), readmodel.RebuildAllOptions{Limit: 1})
	if err != nil {
		t.Fatalf("RebuildAll(first) error = %v", err)
	}
	if first.TrainRunsRebuilt != 1 || first.RowsWritten != 6 || !first.HasMore || first.NextCursor == "" {
		t.Fatalf("RebuildAll(first) = %+v, want bounded first page", first)
	}
	second, err := store.RebuildAll(context.Background(), readmodel.RebuildAllOptions{
		After: first.NextCursor,
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("RebuildAll(second) error = %v", err)
	}
	if second.TrainRunsRebuilt != 1 || second.RowsWritten != 6 || second.HasMore || second.NextCursor == first.NextCursor {
		t.Fatalf("RebuildAll(second) = %+v, want final distinct page", second)
	}
	var firstRows, secondRows int
	if err := conn.QueryRow(context.Background(), `
		SELECT
			(SELECT count(*) FROM train_run_journey_read_model WHERE train_run_id = $1),
			(SELECT count(*) FROM train_run_journey_read_model WHERE train_run_id = $2)
	`, firstTrainRunID, secondTrainRunID).Scan(&firstRows, &secondRows); err != nil {
		t.Fatalf("inspect resumable rebuild: %v", err)
	}
	if firstRows != 6 || secondRows != 6 {
		t.Fatalf("resumable rebuild rows = %d/%d, want 6/6", firstRows, secondRows)
	}
}

func TestReconcileTrainRunDetectsMissingAndMismatchedRowsWithoutRepair(t *testing.T) {
	conn := openMigrationSevenDatabase(t)
	trainRunID := seedProjectionSource(t, conn)
	store, err := readmodel.NewStore(conn, clock.NewDeterministic(time.Now().UTC()))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	if _, err := store.RebuildTrainRun(context.Background(), trainRunID.String()); err != nil {
		t.Fatalf("RebuildTrainRun() error = %v", err)
	}
	consistent, err := store.ReconcileTrainRun(context.Background(), trainRunID.String())
	if err != nil {
		t.Fatalf("ReconcileTrainRun(consistent) error = %v", err)
	}
	if !consistent.Consistent || consistent.ExpectedRows != 6 || consistent.ActualRows != 6 {
		t.Fatalf("ReconcileTrainRun(consistent) = %+v", consistent)
	}
	if _, err := conn.Exec(context.Background(), `
		DELETE FROM train_run_journey_read_model
		WHERE train_run_id = $1
		  AND from_stop_index = 0
		  AND to_stop_index = 1
		  AND seat_class = 'standard'
	`, trainRunID); err != nil {
		t.Fatalf("inject missing projection row: %v", err)
	}
	if _, err := conn.Exec(context.Background(), `
		UPDATE train_run_journey_read_model
		SET fare_amount_minor = 9999
		WHERE train_run_id = $1
		  AND from_stop_index = 1
		  AND to_stop_index = 2
		  AND seat_class = 'business'
	`, trainRunID); err != nil {
		t.Fatalf("inject mismatched projection row: %v", err)
	}

	mismatch, err := store.ReconcileTrainRun(context.Background(), trainRunID.String())
	if err != nil {
		t.Fatalf("ReconcileTrainRun(mismatch) error = %v", err)
	}
	if mismatch.Consistent || mismatch.MissingRows != 1 || mismatch.MismatchedRows != 1 || mismatch.ActualRows != 5 {
		t.Fatalf("ReconcileTrainRun(mismatch) = %+v, want one missing and one mismatched", mismatch)
	}
	var rowsAfter int
	if err := conn.QueryRow(context.Background(), `
		SELECT count(*) FROM train_run_journey_read_model WHERE train_run_id = $1
	`, trainRunID).Scan(&rowsAfter); err != nil {
		t.Fatalf("inspect detect-only reconciliation: %v", err)
	}
	if rowsAfter != 5 {
		t.Fatalf("detect-only reconciliation repaired rows = %d, want unchanged 5", rowsAfter)
	}
}

func TestRepeatedRebuildIsDeterministicAndRetainsCancelledStatus(t *testing.T) {
	conn := openMigrationSevenDatabase(t)
	trainRunID := seedProjectionSource(t, conn)
	if _, err := conn.Exec(context.Background(), `
		UPDATE train_runs SET status = 'cancelled' WHERE id = $1
	`, trainRunID); err != nil {
		t.Fatalf("cancel projection source train run: %v", err)
	}
	store, err := readmodel.NewStore(conn, clock.NewDeterministic(
		time.Date(2026, time.July, 22, 10, 0, 0, 0, time.UTC),
	))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	if _, err := store.RebuildTrainRun(context.Background(), trainRunID.String()); err != nil {
		t.Fatalf("RebuildTrainRun(first) error = %v", err)
	}
	firstChecksum := projectionChecksum(t, conn, trainRunID)
	if _, err := store.RebuildTrainRun(context.Background(), trainRunID.String()); err != nil {
		t.Fatalf("RebuildTrainRun(second) error = %v", err)
	}
	secondChecksum := projectionChecksum(t, conn, trainRunID)
	if firstChecksum != secondChecksum {
		t.Fatalf("repeated rebuild checksum = %q then %q", firstChecksum, secondChecksum)
	}
	var cancelledRows int
	if err := conn.QueryRow(context.Background(), `
		SELECT count(*)
		FROM train_run_journey_read_model
		WHERE train_run_id = $1 AND train_run_status = 'cancelled'
	`, trainRunID).Scan(&cancelledRows); err != nil {
		t.Fatalf("inspect cancelled projection: %v", err)
	}
	if cancelledRows != 6 {
		t.Fatalf("cancelled projection rows = %d, want 6", cancelledRows)
	}
}

func TestRebuildMissingTrainRunIsIdempotentDelete(t *testing.T) {
	conn := openMigrationSevenDatabase(t)
	store, err := readmodel.NewStore(conn, clock.NewDeterministic(time.Now().UTC()))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	missingID := uuid.New()
	for attempt := 0; attempt < 2; attempt++ {
		result, err := store.RebuildTrainRun(context.Background(), missingID.String())
		if err != nil {
			t.Fatalf("RebuildTrainRun(missing attempt %d) error = %v", attempt+1, err)
		}
		if !result.Deleted || result.RowsWritten != 0 || result.TrainRunID != missingID.String() {
			t.Fatalf("RebuildTrainRun(missing attempt %d) = %+v", attempt+1, result)
		}
	}
}

func projectionTrainRunEvent(trainRunID uuid.UUID) readmodel.ProjectionEvent {
	return readmodel.ProjectionEvent{
		ConsumerName:  "railway-read-model",
		EventID:       uuid.NewString(),
		EventType:     "trainrun.updated",
		AggregateType: "train_run",
		AggregateID:   trainRunID.String(),
	}
}

func cloneProjectionTrainRun(t *testing.T, conn *pgx.Conn, sourceTrainRunID uuid.UUID) uuid.UUID {
	t.Helper()
	secondTrainRunID := uuid.New()
	if _, err := conn.Exec(context.Background(), `
		INSERT INTO train_runs (
			id,
			train_id,
			route_id,
			service_date,
			scheduled_departure_at,
			status,
			segment_count
		)
		SELECT
			$1,
			train_id,
			route_id,
			service_date + 1,
			scheduled_departure_at + interval '1 day',
			status,
			segment_count
		FROM train_runs
		WHERE id = $2
	`, secondTrainRunID, sourceTrainRunID); err != nil {
		t.Fatalf("clone projection train run: %v", err)
	}
	return secondTrainRunID
}

func projectionChecksum(t *testing.T, conn *pgx.Conn, trainRunID uuid.UUID) string {
	t.Helper()
	var checksum string
	if err := conn.QueryRow(context.Background(), `
		SELECT md5(string_agg(
			concat_ws('|',
				train_run_id,
				route_id,
				train_id,
				train_code,
				service_date,
				train_run_status,
				from_station_id,
				from_station_code,
				from_station_name,
				from_stop_index,
				to_station_id,
				to_station_code,
				to_station_name,
				to_stop_index,
				departure_at,
				arrival_at,
				seat_class,
				fare_amount_minor,
				currency,
				source_updated_at
			),
			E'\n' ORDER BY from_stop_index, to_stop_index, seat_class
		))
		FROM train_run_journey_read_model
		WHERE train_run_id = $1
	`, trainRunID).Scan(&checksum); err != nil {
		t.Fatalf("checksum projection: %v", err)
	}
	return checksum
}

type projectionJourney struct {
	FromStationCode string
	ToStationCode   string
	FromStopIndex   int
	ToStopIndex     int
	SeatClass       string
	FareAmountMinor int64
	DepartureAt     time.Time
	ArrivalAt       time.Time
	RebuiltAt       time.Time
}

func assertProjectedJourney(
	t *testing.T,
	journeys []projectionJourney,
	origin string,
	destination string,
	seatClass string,
	fare int64,
	departure time.Time,
	arrival time.Time,
) {
	t.Helper()
	for _, journey := range journeys {
		if journey.FromStationCode == origin && journey.ToStationCode == destination && journey.SeatClass == seatClass {
			if journey.FareAmountMinor != fare || !journey.DepartureAt.Equal(departure) || !journey.ArrivalAt.Equal(arrival) {
				t.Fatalf("projected journey = %+v, want fare %d and times %s/%s", journey, fare, departure, arrival)
			}
			return
		}
	}
	t.Fatalf("projected journey %s-%s %s not found", origin, destination, seatClass)
}

func seedProjectionSource(t *testing.T, conn *pgx.Conn) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin projection source seed: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	stationIDs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	if _, err := tx.Exec(ctx, `
		INSERT INTO stations (id, code, name, timezone)
		VALUES
			($1, 'TPE', 'Taipei', 'Asia/Taipei'),
			($2, 'TXG', 'Taichung', 'Asia/Taipei'),
			($3, 'KHH', 'Kaohsiung', 'Asia/Taipei')
	`, stationIDs[0], stationIDs[1], stationIDs[2]); err != nil {
		t.Fatalf("seed projection stations: %v", err)
	}
	routeID := uuid.New()
	if _, err := tx.Exec(ctx, `
		INSERT INTO routes (id, code, name, operating_timezone)
		VALUES ($1, 'WEST', 'Western Line', 'Asia/Taipei')
	`, routeID); err != nil {
		t.Fatalf("seed projection route: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO route_stops (
			route_id,
			station_id,
			stop_index,
			arrival_offset_minutes,
			departure_offset_minutes
		) VALUES
			($1, $2, 0, 0, 5),
			($1, $3, 1, 60, 65),
			($1, $4, 2, 1500, 1505)
	`, routeID, stationIDs[0], stationIDs[1], stationIDs[2]); err != nil {
		t.Fatalf("seed projection route stops: %v", err)
	}
	trainID := uuid.New()
	if _, err := tx.Exec(ctx, `
		INSERT INTO trains (id, code, name)
		VALUES ($1, 'TR200', 'Projection Express')
	`, trainID); err != nil {
		t.Fatalf("seed projection train: %v", err)
	}
	trainRunID := uuid.New()
	if _, err := tx.Exec(ctx, `
		INSERT INTO train_runs (
			id,
			train_id,
			route_id,
			service_date,
			scheduled_departure_at,
			status,
			segment_count
		) VALUES ($1, $2, $3, DATE '2026-07-20', TIMESTAMPTZ '2026-07-19 16:05:00+00', 'scheduled', 2)
	`, trainRunID, trainID, routeID); err != nil {
		t.Fatalf("seed projection train run: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fares (
			route_id,
			from_stop_index,
			to_stop_index,
			seat_class,
			amount_minor,
			currency
		) VALUES
			($1, 0, 1, 'standard', 500, 'TWD'),
			($1, 0, 2, 'standard', 1000, 'TWD'),
			($1, 1, 2, 'standard', 700, 'TWD'),
			($1, 0, 1, 'business', 900, 'TWD'),
			($1, 0, 2, 'business', 1600, 'TWD'),
			($1, 1, 2, 'business', 1800, 'TWD')
	`, routeID); err != nil {
		t.Fatalf("seed projection route fares: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fares (
			train_run_id,
			from_stop_index,
			to_stop_index,
			seat_class,
			amount_minor,
			currency
		) VALUES ($1, 0, 2, 'standard', 1200, 'TWD')
	`, trainRunID); err != nil {
		t.Fatalf("seed projection run fare: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit projection source seed: %v", err)
	}
	return trainRunID
}
