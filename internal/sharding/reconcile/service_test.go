package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/control"
	"github.com/google/uuid"
)

type fakeSource struct {
	catalog           []catalogObservation
	catalogError      error
	assignments       []assignmentObservation
	assignmentError   error
	fences            map[fixedStorage][]fenceObservation
	fenceErrors       map[fixedStorage]error
	locators          map[locatorKind][]locatorObservation
	resources         map[fixedStorage]map[locatorKind][]resourceObservation
	resourceErrors    map[fixedStorage]error
	coverage          map[fixedStorage]locatorCoverage
	coverageErrors    map[fixedStorage]error
	migration         migrationObservation
	migrationFound    bool
	migrationError    error
	snapshots         map[fixedStorage]storageSnapshot
	snapshotErrors    map[fixedStorage]error
	central           centralMigrationSnapshot
	centralError      error
	fenceCallOrder    []fixedStorage
	resourceCallOrder []fixedStorage
}

func (fake *fakeSource) Catalog(context.Context) ([]catalogObservation, error) {
	return append([]catalogObservation(nil), fake.catalog...), fake.catalogError
}

func (fake *fakeSource) AssignmentPage(
	_ context.Context,
	after uuid.UUID,
	limit int,
) ([]assignmentObservation, bool, error) {
	if fake.assignmentError != nil {
		return nil, false, fake.assignmentError
	}
	rows := make([]assignmentObservation, 0, limit)
	for _, row := range fake.assignments {
		if row.TrainRunID.String() <= after.String() {
			continue
		}
		rows = append(rows, row)
		if len(rows) == limit+1 {
			break
		}
	}
	more := len(rows) > limit
	if more {
		rows = rows[:limit]
	}
	return rows, more, nil
}

func (fake *fakeSource) Fences(
	_ context.Context,
	storage fixedStorage,
	ids []uuid.UUID,
) ([]fenceObservation, error) {
	fake.fenceCallOrder = append(fake.fenceCallOrder, storage)
	if err := fake.fenceErrors[storage]; err != nil {
		return nil, err
	}
	allowed := make(map[uuid.UUID]struct{}, len(ids))
	for _, id := range ids {
		allowed[id] = struct{}{}
	}
	var result []fenceObservation
	for _, row := range fake.fences[storage] {
		if _, exists := allowed[row.TrainRunID]; exists {
			result = append(result, row)
		}
	}
	return result, nil
}

func (fake *fakeSource) LocatorPage(
	_ context.Context,
	kind locatorKind,
	after uuid.UUID,
	_ LocatorFilter,
	limit int,
) ([]locatorObservation, bool, error) {
	var result []locatorObservation
	for _, row := range fake.locators[kind] {
		if row.ID.String() <= after.String() {
			continue
		}
		result = append(result, row)
		if len(result) == limit+1 {
			break
		}
	}
	more := len(result) > limit
	if more {
		result = result[:limit]
	}
	return result, more, nil
}

func (fake *fakeSource) Resources(
	_ context.Context,
	storage fixedStorage,
	kind locatorKind,
	ids []uuid.UUID,
) ([]resourceObservation, error) {
	fake.resourceCallOrder = append(fake.resourceCallOrder, storage)
	if err := fake.resourceErrors[storage]; err != nil {
		return nil, err
	}
	allowed := make(map[uuid.UUID]struct{}, len(ids))
	for _, id := range ids {
		allowed[id] = struct{}{}
	}
	var result []resourceObservation
	for _, row := range fake.resources[storage][kind] {
		if _, exists := allowed[row.ID]; exists {
			result = append(result, row)
		}
	}
	return result, nil
}

func (fake *fakeSource) LocatorCoverage(
	_ context.Context,
	storage fixedStorage,
	_ LocatorFilter,
	_ int64,
) (locatorCoverage, error) {
	if err := fake.coverageErrors[storage]; err != nil {
		return locatorCoverage{}, err
	}
	return fake.coverage[storage], nil
}

func (fake *fakeSource) Migration(
	context.Context,
	uuid.UUID,
) (migrationObservation, bool, error) {
	return fake.migration, fake.migrationFound, fake.migrationError
}

func (fake *fakeSource) StorageSnapshot(
	_ context.Context,
	storage fixedStorage,
	_ uuid.UUID,
	_ int64,
) (storageSnapshot, error) {
	if err := fake.snapshotErrors[storage]; err != nil {
		return storageSnapshot{}, err
	}
	return fake.snapshots[storage], nil
}

func (fake *fakeSource) CentralMigrationSnapshot(
	context.Context,
	migrationObservation,
	int64,
) (centralMigrationSnapshot, error) {
	return fake.central, fake.centralError
}

func TestAssignmentsHealthyUsesDeterministicFixedFanout(t *testing.T) {
	runID := uuid.MustParse("10000000-0000-0000-0000-000000000001")
	fake := &fakeSource{
		catalog: healthyCatalog(),
		assignments: []assignmentObservation{{
			TrainRunID: runID, AssignmentPresent: true, ShardID: "legacy", Generation: 1,
			State: "stable", CatalogPresent: true, CatalogEnabled: true, CatalogWriteEnabled: true,
		}},
		fences: map[fixedStorage][]fenceObservation{
			storageLegacy: {{TrainRunID: runID, Generation: 1, Enabled: true}},
		},
		fenceErrors: make(map[fixedStorage]error),
	}
	service := newService(fake)
	service.now = fixedNow
	report, err := service.Assignments(context.Background(), Limits{PageSize: 10, MaxPages: 20, MaxRows: 100})
	if err != nil {
		t.Fatalf("Assignments() error = %v", err)
	}
	if report.Completeness != CompletenessComplete || report.Violations != 0 || !report.ReadOnly {
		t.Fatalf("unexpected report: %+v", report)
	}
	wantOrder := []fixedStorage{storageLegacy, storageZero, storageOne}
	if len(fake.fenceCallOrder) != len(wantOrder) {
		t.Fatalf("fence calls = %v", fake.fenceCallOrder)
	}
	for index := range wantOrder {
		if fake.fenceCallOrder[index] != wantOrder[index] || report.Shards[index].ShardID != wantOrder[index].shardID() {
			t.Fatalf("fanout order = %v shards = %+v", fake.fenceCallOrder, report.Shards)
		}
	}
}

func TestAssignmentsReturnsPartialWithoutHidingHealthyShards(t *testing.T) {
	runID := uuid.MustParse("10000000-0000-0000-0000-000000000002")
	fake := &fakeSource{
		catalog: healthyCatalog(),
		assignments: []assignmentObservation{{
			TrainRunID: runID, AssignmentPresent: true, ShardID: "legacy", Generation: 1,
			State: "stable", CatalogPresent: true, CatalogEnabled: true, CatalogWriteEnabled: true,
		}},
		fences: map[fixedStorage][]fenceObservation{
			storageLegacy: {{TrainRunID: runID, Generation: 1, Enabled: true}},
		},
		fenceErrors: map[fixedStorage]error{storageZero: errors.New("permission denied")},
	}
	report, err := newService(fake).Assignments(
		context.Background(), Limits{PageSize: 10, MaxPages: 20, MaxRows: 100},
	)
	if !errors.Is(err, ErrPartial) || report.Completeness != CompletenessPartial {
		t.Fatalf("report=%+v error=%v, want partial", report, err)
	}
	if report.Shards[0].Status != "healthy" || report.Shards[1].Status != "unavailable" ||
		report.Shards[2].Status != "healthy" {
		t.Fatalf("shard reports hide healthy targets: %+v", report.Shards)
	}
	if report.Shards[1].Failure != "query_failed" {
		t.Fatalf("failure leaked backend detail: %+v", report.Shards[1])
	}
}

func TestAssignmentsReturnsPartialWhenCentralPagingFailsAfterCatalog(t *testing.T) {
	fake := &fakeSource{
		catalog:         healthyCatalog(),
		assignmentError: errors.New("central page unavailable"),
		fences:          make(map[fixedStorage][]fenceObservation),
		fenceErrors:     make(map[fixedStorage]error),
	}
	report, err := newService(fake).Assignments(
		context.Background(), Limits{PageSize: 10, MaxPages: 20, MaxRows: 100},
	)
	if !errors.Is(err, ErrPartial) || errors.Is(err, ErrUnavailable) ||
		report.Completeness != CompletenessPartial {
		t.Fatalf("report=%+v error=%v, want partial central result", report, err)
	}
	if len(report.Failures) != 1 || report.Failures[0] != "query_failed" {
		t.Fatalf("failures = %v", report.Failures)
	}
}

func TestAssignmentsReturnsUnavailableWhenCatalogCannotBeRead(t *testing.T) {
	fake := &fakeSource{catalogError: errors.New("catalog unavailable")}
	report, err := newService(fake).Assignments(
		context.Background(), Limits{PageSize: 10, MaxPages: 20, MaxRows: 100},
	)
	if !errors.Is(err, ErrUnavailable) || report.Completeness != CompletenessUnavailable {
		t.Fatalf("report=%+v error=%v, want unavailable", report, err)
	}
}

func TestLocatorsChecksRouteMetadataAndObservesRetainedDuplicate(t *testing.T) {
	runID := uuid.MustParse("20000000-0000-0000-0000-000000000001")
	reservationID := uuid.MustParse("21000000-0000-0000-0000-000000000001")
	ownerID := uuid.MustParse("22000000-0000-0000-0000-000000000001")
	locator := locatorObservation{
		ID: reservationID, TrainRunID: runID, ShardID: "legacy", Generation: 1,
		OwnerID: ownerID, AssignmentPresent: true, AssignmentShardID: "legacy", AssignmentGeneration: 1,
	}
	resource := resourceObservation{ID: reservationID, TrainRunID: runID, OwnerID: ownerID}
	fake := &fakeSource{
		locators: map[locatorKind][]locatorObservation{locatorReservation: {locator}},
		resources: map[fixedStorage]map[locatorKind][]resourceObservation{
			storageLegacy: {locatorReservation: {resource}},
			storageZero:   {locatorReservation: {resource}},
			storageOne:    {},
		},
		resourceErrors: make(map[fixedStorage]error),
		coverage: map[fixedStorage]locatorCoverage{
			storageLegacy: {Counts: DatasetCounts{Reservations: 1}},
			storageZero:   {Counts: DatasetCounts{Reservations: 1}},
			storageOne:    {},
		},
		coverageErrors: make(map[fixedStorage]error),
	}
	report, err := newService(fake).Locators(
		context.Background(), LocatorFilter{}, Limits{PageSize: 10, MaxPages: 30, MaxRows: 100},
	)
	if err != nil || report.Violations != 0 || report.Completeness != CompletenessComplete {
		t.Fatalf("Locators() report=%+v error=%v", report, err)
	}
	duplicate := findCheck(t, report.Checks, "reservation_physical_duplicates")
	if duplicate.Observed != 1 || duplicate.Violations != 0 {
		t.Fatalf("duplicate check = %+v", duplicate)
	}
	if len(report.Deferred) != 1 || report.Deferred[0] != "retained_source_duplicate_cleanup_classification" {
		t.Fatalf("deferred checks = %v", report.Deferred)
	}
}

func TestLocatorsTreatsRouteMismatchAsViolation(t *testing.T) {
	runID := uuid.MustParse("20000000-0000-0000-0000-000000000002")
	reservationID := uuid.MustParse("21000000-0000-0000-0000-000000000002")
	ownerID := uuid.MustParse("22000000-0000-0000-0000-000000000002")
	fake := &fakeSource{
		locators: map[locatorKind][]locatorObservation{locatorReservation: {{
			ID: reservationID, TrainRunID: runID, ShardID: "legacy", Generation: 1,
			OwnerID: ownerID, AssignmentPresent: true, AssignmentShardID: "shard-0", AssignmentGeneration: 2,
		}}},
		resources: map[fixedStorage]map[locatorKind][]resourceObservation{
			storageLegacy: {locatorReservation: {{ID: reservationID, TrainRunID: runID, OwnerID: ownerID}}},
			storageZero:   {}, storageOne: {},
		},
		resourceErrors: make(map[fixedStorage]error),
		coverage: map[fixedStorage]locatorCoverage{
			storageLegacy: {}, storageZero: {}, storageOne: {},
		},
		coverageErrors: make(map[fixedStorage]error),
	}
	report, err := newService(fake).Locators(
		context.Background(), LocatorFilter{}, Limits{PageSize: 10, MaxPages: 30, MaxRows: 100},
	)
	if !errors.Is(err, ErrViolations) || report.Violations != 1 {
		t.Fatalf("report=%+v error=%v, want route violation", report, err)
	}
}

func TestMigrationChecksCountsFencesAndCentralProvenance(t *testing.T) {
	migrationID := uuid.MustParse("30000000-0000-0000-0000-000000000001")
	runID := uuid.MustParse("31000000-0000-0000-0000-000000000001")
	record := healthyMigration(t, migrationID, runID)
	counts := DatasetCounts{
		Inventory: 3, Reservations: 2, ReservationSeats: 2,
		TicketOrders: 1, Tickets: 2, IdempotencyRecords: 2,
	}
	fake := &fakeSource{
		migration: record, migrationFound: true,
		fences: map[fixedStorage][]fenceObservation{
			storageLegacy: {{TrainRunID: runID, Generation: 1, Enabled: false}},
			storageZero:   {{TrainRunID: runID, Generation: 2, Enabled: false}},
		},
		fenceErrors: make(map[fixedStorage]error),
		snapshots: map[fixedStorage]storageSnapshot{
			storageLegacy: {Counts: counts}, storageZero: {Counts: counts},
		},
		snapshotErrors: make(map[fixedStorage]error),
		central: centralMigrationSnapshot{
			QuotaClaims: 2, IdempotencyClaims: 2, ReservationLocators: 2,
			TicketOrderLocators: 1, TicketLocators: 2, OutboxEvents: 4,
			MigrationsForTrainRun: 1,
		},
	}
	report, err := newService(fake).Migration(
		context.Background(), migrationID, Limits{PageSize: 10, MaxPages: 30, MaxRows: 1_000},
	)
	if err != nil || report.Violations != 0 || report.Migration == nil {
		t.Fatalf("Migration() report=%+v error=%v", report, err)
	}
	if report.Migration.SourceCounts != counts || report.Migration.TargetCounts != counts ||
		report.Migration.QuotaClaims != 2 || report.Migration.OutboxEvents != 4 {
		t.Fatalf("migration counts = %+v", report.Migration)
	}
}

func TestMigrationDetectsDualWriterAndStaleSourceFence(t *testing.T) {
	migrationID := uuid.MustParse("30000000-0000-0000-0000-000000000002")
	runID := uuid.MustParse("31000000-0000-0000-0000-000000000002")
	record := healthyMigration(t, migrationID, runID)
	record.State = "rollback_window"
	record.AssignmentShardID = "shard-0"
	record.AssignmentGeneration = 2
	record.AssignmentState = "rollback_window"
	fake := &fakeSource{
		migration: record, migrationFound: true,
		fences: map[fixedStorage][]fenceObservation{
			storageLegacy: {{TrainRunID: runID, Generation: 1, Enabled: true}},
			storageZero:   {{TrainRunID: runID, Generation: 2, Enabled: true}},
		},
		fenceErrors: make(map[fixedStorage]error),
		snapshots: map[fixedStorage]storageSnapshot{
			storageLegacy: {}, storageZero: {},
		},
		snapshotErrors: make(map[fixedStorage]error),
		central:        centralMigrationSnapshot{MigrationsForTrainRun: 1, ActiveMigrations: 1},
	}
	report, err := newService(fake).Migration(
		context.Background(), migrationID, Limits{PageSize: 10, MaxPages: 30, MaxRows: 1_000},
	)
	if !errors.Is(err, ErrViolations) {
		t.Fatalf("Migration() error = %v, report=%+v", err, report)
	}
	if findCheck(t, report.Checks, "source_target_not_both_writable").Violations != 1 ||
		findCheck(t, report.Checks, "stale_source_fence").Violations != 1 {
		t.Fatalf("missing fence violations: %+v", report.Checks)
	}
}

func TestMigrationReportsCentralFailureAsPartialWithoutInventingLimit(t *testing.T) {
	migrationID := uuid.MustParse("30000000-0000-0000-0000-000000000003")
	runID := uuid.MustParse("31000000-0000-0000-0000-000000000003")
	record := healthyMigration(t, migrationID, runID)
	counts := DatasetCounts{
		Inventory: 3, Reservations: 2, ReservationSeats: 2,
		TicketOrders: 1, Tickets: 2, IdempotencyRecords: 2,
	}
	fake := &fakeSource{
		migration: record, migrationFound: true,
		fences: map[fixedStorage][]fenceObservation{
			storageLegacy: {{TrainRunID: runID, Generation: 1}},
			storageZero:   {{TrainRunID: runID, Generation: 2}},
		},
		fenceErrors: make(map[fixedStorage]error),
		snapshots: map[fixedStorage]storageSnapshot{
			storageLegacy: {Counts: counts}, storageZero: {Counts: counts},
		},
		snapshotErrors: make(map[fixedStorage]error),
		centralError:   errors.New("central unavailable"),
	}
	report, err := newService(fake).Migration(
		context.Background(), migrationID, Limits{PageSize: 10, MaxPages: 30, MaxRows: 1_000},
	)
	if !errors.Is(err, ErrPartial) || errors.Is(err, ErrLimitReached) ||
		report.Completeness != CompletenessPartial {
		t.Fatalf("report=%+v error=%v, want non-limit partial", report, err)
	}
	if len(report.Failures) != 1 || report.Failures[0] != "central_snapshot_query_failed" {
		t.Fatalf("sanitized central failures = %v", report.Failures)
	}
}

func TestAssignmentsFailsClosedAtPageLimit(t *testing.T) {
	runID := uuid.MustParse("40000000-0000-0000-0000-000000000001")
	fake := &fakeSource{
		catalog: healthyCatalog(),
		assignments: []assignmentObservation{{
			TrainRunID: runID, AssignmentPresent: true, ShardID: "legacy", Generation: 1,
			State: "stable", CatalogPresent: true, CatalogEnabled: true, CatalogWriteEnabled: true,
		}},
		fences: make(map[fixedStorage][]fenceObservation), fenceErrors: make(map[fixedStorage]error),
	}
	report, err := newService(fake).Assignments(
		context.Background(), Limits{PageSize: 1, MaxPages: 1, MaxRows: 10},
	)
	if !errors.Is(err, ErrPartial) || !errors.Is(err, ErrLimitReached) ||
		!report.Truncated || report.Completeness != CompletenessPartial {
		t.Fatalf("report=%+v error=%v, want bounded partial", report, err)
	}
}

func TestAssignmentsFailsClosedAtRowLimit(t *testing.T) {
	fake := &fakeSource{
		catalog: healthyCatalog(), fences: make(map[fixedStorage][]fenceObservation),
		fenceErrors: make(map[fixedStorage]error),
	}
	report, err := newService(fake).Assignments(
		context.Background(), Limits{PageSize: 1, MaxPages: 10, MaxRows: 3},
	)
	if !errors.Is(err, ErrPartial) || !errors.Is(err, ErrLimitReached) || !report.Truncated {
		t.Fatalf("report=%+v error=%v, want row-limit partial", report, err)
	}
}

func TestFixedStorageAllowlistRejectsUnknownIdentifier(t *testing.T) {
	if _, valid := storagePrefix(fixedStorage(99)); valid {
		t.Fatal("unknown storage reached SQL identifier mapping")
	}
	if _, valid := resourceQuery(fixedStorage(99), locatorReservation); valid {
		t.Fatal("unknown storage produced resource SQL")
	}
	if _, valid := fenceQuery(fixedStorage(99)); valid {
		t.Fatal("unknown storage produced fence SQL")
	}
}

func healthyCatalog() []catalogObservation {
	return []catalogObservation{
		{ShardID: "legacy", StorageKind: "legacy", Enabled: true, WriteEnabled: true, State: "active"},
		{ShardID: "shard-0", StorageKind: "schema", Enabled: true, WriteEnabled: true, State: "active"},
		{ShardID: "shard-1", StorageKind: "schema", Enabled: true, WriteEnabled: true, State: "active"},
	}
}

func healthyMigration(t *testing.T, migrationID, runID uuid.UUID) migrationObservation {
	t.Helper()
	outcome := control.ValidationOutcome{
		Snapshot: control.ValidationSnapshot{RowsExamined: 12}, Passed: true, CheckedAt: fixedNow(),
	}
	encoded, err := json.Marshal(outcome)
	if err != nil {
		t.Fatal(err)
	}
	return migrationObservation{
		ID: migrationID, TrainRunID: runID, SourceShardID: "legacy", TargetShardID: "shard-0",
		SourceGeneration: 1, TargetGeneration: 2, State: "validating", CopyPhase: "complete",
		CopyComplete: true, CopiedRows: 12, InventoryRowsCopied: 3, ReservationRowsCopied: 2,
		ReservationSeatRowsCopied: 2, TicketOrderRowsCopied: 1, TicketRowsCopied: 2,
		IdempotencyRowsCopied: 2, ValidationStatus: "passed", LastValidation: encoded,
		AssignmentPresent: true, AssignmentShardID: "legacy", AssignmentGeneration: 1,
		AssignmentState: "migrating", ActiveMigrationID: &migrationID,
		SourceCatalogEnabled: true, TargetCatalogEnabled: true,
	}
}

func findCheck(t *testing.T, checks []Check, name string) Check {
	t.Helper()
	for _, check := range checks {
		if check.Name == name {
			return check
		}
	}
	t.Fatalf("check %q not found in %+v", name, checks)
	return Check{}
}

func fixedNow() time.Time {
	return time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
}
