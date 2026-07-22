package readmodel

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type ReconcileResult struct {
	TrainRunID     string
	ExpectedRows   int
	ActualRows     int
	MissingRows    int
	ExtraRows      int
	DuplicateRows  int
	StaleRows      int
	MismatchedRows int
	InvalidRows    int
	Consistent     bool
}

type projectionSnapshot struct {
	trainRunID      uuid.UUID
	routeID         uuid.UUID
	trainID         uuid.UUID
	trainCode       string
	serviceDate     time.Time
	status          string
	fromStationID   uuid.UUID
	fromStationCode string
	fromStationName string
	fromStopIndex   int
	toStationID     uuid.UUID
	toStationCode   string
	toStationName   string
	toStopIndex     int
	departureAt     time.Time
	arrivalAt       time.Time
	seatClass       string
	fareAmountMinor int64
	currency        string
	sourceUpdatedAt time.Time
}

func (s *Store) ReconcileTrainRun(ctx context.Context, rawTrainRunID string) (ReconcileResult, error) {
	trainRunID, err := uuid.Parse(rawTrainRunID)
	if err != nil || trainRunID == uuid.Nil {
		return ReconcileResult{}, ErrInvalidTrainRunID
	}
	observedAt := s.clock.Now().UTC()
	if observedAt.IsZero() {
		return ReconcileResult{}, ErrInvalidStore
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return ReconcileResult{}, fmt.Errorf("%w: begin projection reconciliation", ErrPersistence)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	expectedValues, err := loadProjectionRows(ctx, tx, trainRunID, observedAt)
	if err != nil {
		return ReconcileResult{}, err
	}
	expected := make([]projectionSnapshot, 0, len(expectedValues))
	for _, values := range expectedValues {
		snapshot, err := snapshotFromProjectionValues(values)
		if err != nil {
			return ReconcileResult{}, err
		}
		expected = append(expected, snapshot)
	}
	actual, err := loadActualProjection(ctx, tx, trainRunID)
	if err != nil {
		return ReconcileResult{}, err
	}
	result := compareProjectionSnapshots(trainRunID, expected, actual)
	if err := tx.Commit(ctx); err != nil {
		return ReconcileResult{}, fmt.Errorf("%w: commit projection reconciliation", ErrPersistence)
	}
	return result, nil
}

func loadActualProjection(ctx context.Context, tx pgx.Tx, trainRunID uuid.UUID) ([]projectionSnapshot, error) {
	rows, err := tx.Query(ctx, `
		SELECT
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
		FROM train_run_journey_read_model
		WHERE train_run_id = $1
		ORDER BY from_stop_index, to_stop_index, seat_class
		LIMIT $2
	`, trainRunID, MaxProjectionRowsPerTrainRun+1)
	if err != nil {
		return nil, fmt.Errorf("%w: query current projection", ErrPersistence)
	}
	defer rows.Close()
	actual := make([]projectionSnapshot, 0)
	for rows.Next() {
		var snapshot projectionSnapshot
		if err := rows.Scan(
			&snapshot.trainRunID,
			&snapshot.routeID,
			&snapshot.trainID,
			&snapshot.trainCode,
			&snapshot.serviceDate,
			&snapshot.status,
			&snapshot.fromStationID,
			&snapshot.fromStationCode,
			&snapshot.fromStationName,
			&snapshot.fromStopIndex,
			&snapshot.toStationID,
			&snapshot.toStationCode,
			&snapshot.toStationName,
			&snapshot.toStopIndex,
			&snapshot.departureAt,
			&snapshot.arrivalAt,
			&snapshot.seatClass,
			&snapshot.fareAmountMinor,
			&snapshot.currency,
			&snapshot.sourceUpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("%w: scan current projection", ErrPersistence)
		}
		actual = append(actual, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate current projection", ErrPersistence)
	}
	if len(actual) > MaxProjectionRowsPerTrainRun {
		return nil, ErrProjectionLimit
	}
	return actual, nil
}

func snapshotFromProjectionValues(values []any) (projectionSnapshot, error) {
	if len(values) != 21 {
		return projectionSnapshot{}, ErrProjectionSource
	}
	snapshot, ok := projectionSnapshotFromKnownValues(values)
	if !ok {
		return projectionSnapshot{}, ErrProjectionSource
	}
	return snapshot, nil
}

func projectionSnapshotFromKnownValues(values []any) (projectionSnapshot, bool) {
	trainRunID, ok0 := values[0].(uuid.UUID)
	routeID, ok1 := values[1].(uuid.UUID)
	trainID, ok2 := values[2].(uuid.UUID)
	trainCode, ok3 := values[3].(string)
	serviceDate, ok4 := values[4].(time.Time)
	status, ok5 := values[5].(string)
	fromStationID, ok6 := values[6].(uuid.UUID)
	fromStationCode, ok7 := values[7].(string)
	fromStationName, ok8 := values[8].(string)
	fromStopIndex, ok9 := values[9].(int)
	toStationID, ok10 := values[10].(uuid.UUID)
	toStationCode, ok11 := values[11].(string)
	toStationName, ok12 := values[12].(string)
	toStopIndex, ok13 := values[13].(int)
	departureAt, ok14 := values[14].(time.Time)
	arrivalAt, ok15 := values[15].(time.Time)
	seatClass, ok16 := values[16].(string)
	fareAmountMinor, ok17 := values[17].(int64)
	currency, ok18 := values[18].(string)
	sourceUpdatedAt, ok19 := values[19].(time.Time)
	if !(ok0 && ok1 && ok2 && ok3 && ok4 && ok5 && ok6 && ok7 && ok8 && ok9 && ok10 && ok11 && ok12 && ok13 && ok14 && ok15 && ok16 && ok17 && ok18 && ok19) {
		return projectionSnapshot{}, false
	}
	return projectionSnapshot{
		trainRunID: trainRunID, routeID: routeID, trainID: trainID, trainCode: trainCode,
		serviceDate: serviceDate, status: status, fromStationID: fromStationID,
		fromStationCode: fromStationCode, fromStationName: fromStationName,
		fromStopIndex: fromStopIndex, toStationID: toStationID, toStationCode: toStationCode,
		toStationName: toStationName, toStopIndex: toStopIndex, departureAt: departureAt,
		arrivalAt: arrivalAt, seatClass: seatClass, fareAmountMinor: fareAmountMinor,
		currency: currency, sourceUpdatedAt: sourceUpdatedAt,
	}, true
}

func compareProjectionSnapshots(
	trainRunID uuid.UUID,
	expected []projectionSnapshot,
	actual []projectionSnapshot,
) ReconcileResult {
	result := ReconcileResult{
		TrainRunID: trainRunID.String(), ExpectedRows: len(expected), ActualRows: len(actual),
	}
	expectedByKey := make(map[string]projectionSnapshot, len(expected))
	for _, snapshot := range expected {
		expectedByKey[snapshot.key()] = snapshot
	}
	actualByKey := make(map[string]projectionSnapshot, len(actual))
	for _, snapshot := range actual {
		key := snapshot.key()
		if _, exists := actualByKey[key]; exists {
			result.DuplicateRows++
		}
		actualByKey[key] = snapshot
		if !snapshot.valid() {
			result.InvalidRows++
		}
	}
	for key, expectedSnapshot := range expectedByKey {
		actualSnapshot, exists := actualByKey[key]
		if !exists {
			result.MissingRows++
			continue
		}
		if actualSnapshot.fingerprint() != expectedSnapshot.fingerprint() {
			result.MismatchedRows++
		}
		if actualSnapshot.sourceUpdatedAt.Before(expectedSnapshot.sourceUpdatedAt) {
			result.StaleRows++
		}
	}
	for key := range actualByKey {
		if _, exists := expectedByKey[key]; !exists {
			result.ExtraRows++
		}
	}
	result.Consistent = result.MissingRows == 0 && result.ExtraRows == 0 &&
		result.DuplicateRows == 0 && result.StaleRows == 0 &&
		result.MismatchedRows == 0 && result.InvalidRows == 0
	return result
}

func (snapshot projectionSnapshot) key() string {
	return snapshot.trainRunID.String() + "|" + strconv.Itoa(snapshot.fromStopIndex) + "|" +
		strconv.Itoa(snapshot.toStopIndex) + "|" + snapshot.seatClass
}

func (snapshot projectionSnapshot) fingerprint() string {
	fields := []string{
		snapshot.routeID.String(), snapshot.trainID.String(), snapshot.trainCode,
		snapshot.serviceDate.UTC().Format(time.RFC3339Nano), snapshot.status,
		snapshot.fromStationID.String(), snapshot.fromStationCode, snapshot.fromStationName,
		strconv.Itoa(snapshot.fromStopIndex), snapshot.toStationID.String(), snapshot.toStationCode,
		snapshot.toStationName, strconv.Itoa(snapshot.toStopIndex),
		snapshot.departureAt.UTC().Format(time.RFC3339Nano), snapshot.arrivalAt.UTC().Format(time.RFC3339Nano),
		snapshot.seatClass, strconv.FormatInt(snapshot.fareAmountMinor, 10), snapshot.currency,
	}
	return strings.Join(fields, "\x00")
}

func (snapshot projectionSnapshot) valid() bool {
	return snapshot.trainRunID != uuid.Nil && snapshot.routeID != uuid.Nil && snapshot.trainID != uuid.Nil &&
		snapshot.fromStationID != uuid.Nil && snapshot.toStationID != uuid.Nil &&
		snapshot.fromStopIndex >= 0 && snapshot.toStopIndex > snapshot.fromStopIndex &&
		snapshot.arrivalAt.After(snapshot.departureAt) && snapshot.fareAmountMinor >= 0 &&
		(snapshot.seatClass == "standard" || snapshot.seatClass == "business") &&
		(snapshot.status == "scheduled" || snapshot.status == "cancelled" || snapshot.status == "departed" || snapshot.status == "completed") &&
		len(snapshot.currency) == 3
}
