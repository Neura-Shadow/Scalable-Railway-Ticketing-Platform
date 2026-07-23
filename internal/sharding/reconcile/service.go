package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/control"
	"github.com/google/uuid"
)

type fixedStorage uint8

const (
	storageLegacy fixedStorage = iota
	storageZero
	storageOne
)

var fixedStorages = [...]fixedStorage{storageLegacy, storageZero, storageOne}

func (storage fixedStorage) shardID() string {
	switch storage {
	case storageLegacy:
		return "legacy"
	case storageZero:
		return "shard-0"
	case storageOne:
		return "shard-1"
	default:
		return ""
	}
}

func parseStorage(value string) (fixedStorage, bool) {
	switch value {
	case "legacy":
		return storageLegacy, true
	case "shard-0":
		return storageZero, true
	case "shard-1":
		return storageOne, true
	default:
		return 0, false
	}
}

type catalogObservation struct {
	ShardID      string
	StorageKind  string
	Enabled      bool
	WriteEnabled bool
	State        string
}

type assignmentObservation struct {
	TrainRunID          uuid.UUID
	AssignmentPresent   bool
	ShardID             string
	Generation          int64
	State               string
	ActiveMigrationID   *uuid.UUID
	CatalogPresent      bool
	CatalogEnabled      bool
	CatalogWriteEnabled bool
}

type fenceObservation struct {
	TrainRunID uuid.UUID
	Generation int64
	Enabled    bool
}

type locatorKind uint8

const (
	locatorReservation locatorKind = iota
	locatorTicketOrder
	locatorTicket
)

var locatorKinds = [...]locatorKind{locatorReservation, locatorTicketOrder, locatorTicket}

func (kind locatorKind) name() string {
	switch kind {
	case locatorReservation:
		return "reservation"
	case locatorTicketOrder:
		return "ticket_order"
	case locatorTicket:
		return "ticket"
	default:
		return "unknown"
	}
}

type locatorObservation struct {
	ID                   uuid.UUID
	TrainRunID           uuid.UUID
	ShardID              string
	Generation           int64
	OwnerID              uuid.UUID
	ReservationID        uuid.UUID
	TicketOrderID        uuid.UUID
	Status               string
	AmountMinor          int64
	Currency             string
	CreatedAt            time.Time
	AssignmentPresent    bool
	AssignmentShardID    string
	AssignmentGeneration int64
}

type resourceObservation struct {
	ID            uuid.UUID
	TrainRunID    uuid.UUID
	OwnerID       uuid.UUID
	ReservationID uuid.UUID
	TicketOrderID uuid.UUID
	Status        string
	AmountMinor   int64
	Currency      string
	CreatedAt     time.Time
}

type locatorCoverage struct {
	Counts                     DatasetCounts
	MissingReservationLocators int64
	InvalidReservationLocators int64
	MissingTicketOrderLocators int64
	InvalidTicketOrderLocators int64
	MissingTicketLocators      int64
	InvalidTicketLocators      int64
	Truncated                  bool
}

type migrationObservation struct {
	ID                        uuid.UUID
	TrainRunID                uuid.UUID
	SourceShardID             string
	TargetShardID             string
	SourceGeneration          int64
	TargetGeneration          int64
	State                     string
	CopyPhase                 string
	CopyComplete              bool
	CopiedRows                int64
	InventoryRowsCopied       int64
	ReservationRowsCopied     int64
	ReservationSeatRowsCopied int64
	TicketOrderRowsCopied     int64
	TicketRowsCopied          int64
	IdempotencyRowsCopied     int64
	ValidationStatus          string
	LastValidation            []byte
	AssignmentPresent         bool
	AssignmentShardID         string
	AssignmentGeneration      int64
	AssignmentState           string
	ActiveMigrationID         *uuid.UUID
	SourceCatalogEnabled      bool
	TargetCatalogEnabled      bool
}

func (observation migrationObservation) auditedRows() int64 {
	return observation.InventoryRowsCopied + observation.ReservationRowsCopied +
		observation.ReservationSeatRowsCopied + observation.TicketOrderRowsCopied +
		observation.TicketRowsCopied + observation.IdempotencyRowsCopied
}

type storageSnapshot struct {
	Counts                DatasetCounts
	SeatMaskViolations    int64
	OrphanActiveSeats     int64
	QuotaViolations       int64
	TicketViolations      int64
	IdempotencyViolations int64
	Truncated             bool
}

type centralMigrationSnapshot struct {
	QuotaClaims                int64
	IdempotencyClaims          int64
	ReservationLocators        int64
	TicketOrderLocators        int64
	TicketLocators             int64
	OutboxEvents               int64
	MigrationsForTrainRun      int64
	ActiveMigrations           int64
	GenerationWriteRows        int64
	LocatorRouteViolations     int64
	IdempotencyRouteViolations int64
	OutboxProvenanceViolations int64
	GenerationWriteViolations  int64
	Truncated                  bool
}

type dataSource interface {
	Catalog(context.Context) ([]catalogObservation, error)
	AssignmentPage(context.Context, uuid.UUID, int) ([]assignmentObservation, bool, error)
	Fences(context.Context, fixedStorage, []uuid.UUID) ([]fenceObservation, error)
	LocatorPage(context.Context, locatorKind, uuid.UUID, LocatorFilter, int) ([]locatorObservation, bool, error)
	Resources(context.Context, fixedStorage, locatorKind, []uuid.UUID) ([]resourceObservation, error)
	LocatorCoverage(context.Context, fixedStorage, LocatorFilter, int64) (locatorCoverage, error)
	Migration(context.Context, uuid.UUID) (migrationObservation, bool, error)
	StorageSnapshot(context.Context, fixedStorage, uuid.UUID, int64) (storageSnapshot, error)
	CentralMigrationSnapshot(context.Context, migrationObservation, int64) (centralMigrationSnapshot, error)
}

type service struct {
	source dataSource
	now    func() time.Time
}

func newService(source dataSource) *service {
	return &service{source: source, now: func() time.Time { return time.Now().UTC() }}
}

type budget struct {
	limits    Limits
	pages     int
	rows      int64
	truncated bool
}

func (budget *budget) canQuery() bool {
	if budget.pages >= budget.limits.MaxPages || budget.rows >= budget.limits.MaxRows {
		budget.truncated = true
		return false
	}
	budget.pages++
	return true
}

func (budget *budget) pageSize() int {
	remaining := budget.limits.MaxRows - budget.rows
	if remaining <= 0 {
		budget.truncated = true
		return 0
	}
	if remaining < int64(budget.limits.PageSize) {
		return int(remaining)
	}
	return budget.limits.PageSize
}

func (budget *budget) addRows(rows int64) bool {
	if rows < 0 || rows > budget.limits.MaxRows-budget.rows {
		budget.truncated = true
		return false
	}
	budget.rows += rows
	return true
}

func baseReport(scope string, now time.Time) Report {
	report := Report{
		Scope: scope, ReadOnly: true, Completeness: CompletenessComplete, CheckedAt: now,
	}
	for _, storage := range fixedStorages {
		report.Shards = append(report.Shards, ShardReport{ShardID: storage.shardID(), Status: "healthy"})
	}
	return report
}

func (service *service) Assignments(ctx context.Context, limits Limits) (Report, error) {
	if service == nil || service.source == nil || ctx == nil || !limits.Valid() {
		return Report{}, ErrInvalidInput
	}
	report := baseReport(ScopeAssignments, service.now())
	work := budget{limits: limits}
	if !work.canQuery() {
		return finishReport(report, work, true, false)
	}
	catalog, err := service.source.Catalog(ctx)
	if err != nil {
		return finishUnavailable(report, work, err)
	}
	if !work.addRows(int64(len(catalog))) {
		return finishReport(report, work, true, false)
	}
	catalogByID := make(map[string]catalogObservation, len(catalog))
	var catalogViolations int64
	for _, row := range catalog {
		storage, valid := parseStorage(row.ShardID)
		expectedKind := "schema"
		if storage == storageLegacy {
			expectedKind = "legacy"
		}
		if !valid || row.StorageKind != expectedKind {
			catalogViolations++
			continue
		}
		catalogByID[row.ShardID] = row
	}
	for _, storage := range fixedStorages {
		if _, exists := catalogByID[storage.shardID()]; !exists {
			catalogViolations++
		}
	}
	report.addCheck("fixed_catalog", int64(len(fixedStorages)), 0, catalogViolations)

	failedStorage := make(map[fixedStorage]bool)
	after := uuid.Nil
	for {
		pageSize := work.pageSize()
		if pageSize == 0 || !work.canQuery() {
			break
		}
		assignments, more, pageErr := service.source.AssignmentPage(ctx, after, pageSize)
		if pageErr != nil {
			return finishUnavailable(report, work, pageErr)
		}
		if !work.addRows(int64(len(assignments))) {
			break
		}
		if len(assignments) == 0 {
			break
		}
		ids := make([]uuid.UUID, 0, len(assignments))
		for _, assignment := range assignments {
			ids = append(ids, assignment.TrainRunID)
		}
		fences := make(map[fixedStorage]map[uuid.UUID]fenceObservation, len(fixedStorages))
		completeFencePage := true
		for index, storage := range fixedStorages {
			if failedStorage[storage] {
				completeFencePage = false
				continue
			}
			if !work.canQuery() {
				markShardFailure(&report.Shards[index], "limit_reached")
				failedStorage[storage] = true
				completeFencePage = false
				continue
			}
			rows, fenceErr := service.source.Fences(ctx, storage, ids)
			report.Shards[index].Pages++
			if fenceErr != nil {
				markShardFailure(&report.Shards[index], failureCategory(fenceErr))
				failedStorage[storage] = true
				completeFencePage = false
				continue
			}
			if !work.addRows(int64(len(rows))) {
				markShardFailure(&report.Shards[index], "row_limit")
				failedStorage[storage] = true
				completeFencePage = false
				continue
			}
			report.Shards[index].Rows += int64(len(rows))
			byRun := make(map[uuid.UUID]fenceObservation, len(rows))
			for _, fence := range rows {
				byRun[fence.TrainRunID] = fence
			}
			fences[storage] = byRun
		}
		for _, assignment := range assignments {
			service.checkAssignment(&report, assignment, catalogByID, fences, completeFencePage)
		}
		after = assignments[len(assignments)-1].TrainRunID
		if !more {
			break
		}
	}
	partial := work.truncated || len(failedStorage) != 0
	return finishReport(report, work, partial, false)
}

func (service *service) checkAssignment(
	report *Report,
	assignment assignmentObservation,
	catalog map[string]catalogObservation,
	fences map[fixedStorage]map[uuid.UUID]fenceObservation,
	completeFencePage bool,
) {
	missing := int64(0)
	if !assignment.AssignmentPresent {
		missing = 1
	}
	report.addCheck("assignment_exists", 1, 0, missing)
	if missing != 0 {
		return
	}
	_, validStorage := parseStorage(assignment.ShardID)
	catalogRow, catalogExists := catalog[assignment.ShardID]
	catalogViolation := int64(0)
	if !validStorage || !catalogExists || !assignment.CatalogPresent ||
		!assignment.CatalogEnabled || !catalogRow.Enabled {
		catalogViolation = 1
	}
	report.addCheck("assigned_catalog_enabled", 1, 0, catalogViolation)
	stateViolation := int64(0)
	switch assignment.State {
	case "stable":
		if assignment.ActiveMigrationID != nil {
			stateViolation = 1
		}
	case "draining", "migrating", "rollback_window":
		if assignment.ActiveMigrationID == nil {
			stateViolation = 1
		}
	default:
		stateViolation = 1
	}
	if assignment.Generation <= 0 {
		stateViolation = 1
	}
	report.addCheck("assignment_state", 1, 0, stateViolation)
	if !completeFencePage {
		return
	}

	writers := int64(0)
	activeFenceViolation := int64(0)
	for storage, byRun := range fences {
		fence, exists := byRun[assignment.TrainRunID]
		if exists && fence.Enabled {
			writers++
			if storage.shardID() != assignment.ShardID || fence.Generation != assignment.Generation {
				activeFenceViolation++
			}
		}
		if storage.shardID() == assignment.ShardID && (!exists || fence.Generation != assignment.Generation) {
			activeFenceViolation++
		}
	}
	expectedWriters := int64(1)
	if assignment.State == "migrating" {
		expectedWriters = 0
	}
	writerViolation := int64(0)
	if writers != expectedWriters || (writers == 1 && (!catalogRow.WriteEnabled || !assignment.CatalogWriteEnabled)) {
		writerViolation = 1
	}
	report.addCheck("active_fence_generation", 1, 0, activeFenceViolation)
	report.addCheck("exact_writer_count", 1, writers, writerViolation)
	bothWritable := int64(0)
	if writers > 1 {
		bothWritable = 1
	}
	report.addCheck("source_target_not_both_writable", 1, writers, bothWritable)
}

func (service *service) Locators(
	ctx context.Context,
	filter LocatorFilter,
	limits Limits,
) (Report, error) {
	if service == nil || service.source == nil || ctx == nil || !filter.valid() || !limits.Valid() {
		return Report{}, ErrInvalidInput
	}
	report := baseReport(ScopeLocators, service.now())
	report.Deferred = []string{"retained_source_duplicate_cleanup_classification"}
	work := budget{limits: limits}
	failedStorage := make(map[fixedStorage]bool)

	for _, kind := range locatorKinds {
		after := uuid.Nil
		for {
			pageSize := work.pageSize()
			if pageSize == 0 || !work.canQuery() {
				break
			}
			locators, more, err := service.source.LocatorPage(ctx, kind, after, filter, pageSize)
			if err != nil {
				return finishUnavailable(report, work, err)
			}
			if !work.addRows(int64(len(locators))) {
				break
			}
			if len(locators) == 0 {
				break
			}
			ids := make([]uuid.UUID, 0, len(locators))
			for _, locator := range locators {
				ids = append(ids, locator.ID)
			}
			resources := make(map[fixedStorage]map[uuid.UUID]resourceObservation, len(fixedStorages))
			for index, storage := range fixedStorages {
				if failedStorage[storage] {
					continue
				}
				if !work.canQuery() {
					markShardFailure(&report.Shards[index], "limit_reached")
					failedStorage[storage] = true
					continue
				}
				rows, resourceErr := service.source.Resources(ctx, storage, kind, ids)
				report.Shards[index].Pages++
				if resourceErr != nil {
					markShardFailure(&report.Shards[index], failureCategory(resourceErr))
					failedStorage[storage] = true
					continue
				}
				if !work.addRows(int64(len(rows))) {
					markShardFailure(&report.Shards[index], "row_limit")
					failedStorage[storage] = true
					continue
				}
				report.Shards[index].Rows += int64(len(rows))
				byID := make(map[uuid.UUID]resourceObservation, len(rows))
				for _, row := range rows {
					byID[row.ID] = row
				}
				resources[storage] = byID
			}
			for _, locator := range locators {
				service.checkLocator(&report, kind, locator, resources, failedStorage)
			}
			after = locators[len(locators)-1].ID
			if !more {
				break
			}
		}
	}

	for index, storage := range fixedStorages {
		if failedStorage[storage] {
			continue
		}
		remaining := work.limits.MaxRows - work.rows
		if remaining < 3 || !work.canQuery() {
			markShardFailure(&report.Shards[index], "limit_reached")
			failedStorage[storage] = true
			continue
		}
		coverage, err := service.source.LocatorCoverage(ctx, storage, filter, remaining/3)
		report.Shards[index].Pages++
		if err != nil {
			markShardFailure(&report.Shards[index], failureCategory(err))
			failedStorage[storage] = true
			continue
		}
		rows := coverage.Counts.Reservations + coverage.Counts.TicketOrders + coverage.Counts.Tickets
		if !work.addRows(rows) {
			markShardFailure(&report.Shards[index], "row_limit")
			failedStorage[storage] = true
			continue
		}
		report.Shards[index].Rows += rows
		if coverage.Truncated {
			markShardFailure(&report.Shards[index], "row_limit")
			failedStorage[storage] = true
			work.truncated = true
		}
		addShardCheck(&report.Shards[index], "reservation_locator_coverage", coverage.Counts.Reservations,
			coverage.MissingReservationLocators+coverage.InvalidReservationLocators)
		addShardCheck(&report.Shards[index], "ticket_order_locator_coverage", coverage.Counts.TicketOrders,
			coverage.MissingTicketOrderLocators+coverage.InvalidTicketOrderLocators)
		addShardCheck(&report.Shards[index], "ticket_locator_coverage", coverage.Counts.Tickets,
			coverage.MissingTicketLocators+coverage.InvalidTicketLocators)
		report.addCheck("local_resource_locator_coverage", rows, 0,
			coverage.MissingReservationLocators+coverage.InvalidReservationLocators+
				coverage.MissingTicketOrderLocators+coverage.InvalidTicketOrderLocators+
				coverage.MissingTicketLocators+coverage.InvalidTicketLocators)
	}
	partial := work.truncated || len(failedStorage) != 0
	return finishReport(report, work, partial, false)
}

func (service *service) checkLocator(
	report *Report,
	kind locatorKind,
	locator locatorObservation,
	resources map[fixedStorage]map[uuid.UUID]resourceObservation,
	failed map[fixedStorage]bool,
) {
	routeViolation := int64(0)
	storage, validStorage := parseStorage(locator.ShardID)
	if !validStorage || !locator.AssignmentPresent || locator.ShardID != locator.AssignmentShardID ||
		locator.Generation != locator.AssignmentGeneration || locator.Generation <= 0 {
		routeViolation = 1
	}
	report.addCheck(kind.name()+"_locator_route", 1, 0, routeViolation)

	physicalMatches := int64(0)
	for storageID, rows := range resources {
		if _, exists := rows[locator.ID]; exists {
			physicalMatches++
		}
		_ = storageID
	}
	report.addCheck(kind.name()+"_physical_duplicates", 1, max64(physicalMatches-1, 0), 0)
	if !validStorage || failed[storage] {
		return
	}
	resource, exists := resources[storage][locator.ID]
	missing := int64(0)
	if !exists {
		missing = 1
	}
	report.addCheck(kind.name()+"_resource_exists", 1, 0, missing)
	if !exists {
		return
	}
	mismatch := int64(0)
	if resource.TrainRunID != locator.TrainRunID || resource.OwnerID != locator.OwnerID {
		mismatch = 1
	}
	switch kind {
	case locatorTicketOrder:
		if resource.ReservationID != locator.ReservationID || resource.Status != locator.Status ||
			resource.AmountMinor != locator.AmountMinor || resource.Currency != locator.Currency ||
			!resource.CreatedAt.Equal(locator.CreatedAt) {
			mismatch = 1
		}
	case locatorTicket:
		if resource.ReservationID != locator.ReservationID ||
			resource.TicketOrderID != locator.TicketOrderID {
			mismatch = 1
		}
	}
	report.addCheck(kind.name()+"_locator_metadata", 1, 0, mismatch)
}

func (service *service) Migration(
	ctx context.Context,
	migrationID uuid.UUID,
	limits Limits,
) (Report, error) {
	if service == nil || service.source == nil || ctx == nil || migrationID == uuid.Nil || !limits.Valid() {
		return Report{}, ErrInvalidInput
	}
	report := baseReport(ScopeMigration, service.now())
	report.Deferred = []string{
		"source_row_cleanup_state_without_cleanup_ledger",
		"outbox_transition_completeness_without_event_ledger",
		"unresolved_legacy_idempotency_train_run_attribution",
	}
	work := budget{limits: limits}
	if !work.canQuery() {
		return finishReport(report, work, true, false)
	}
	record, found, err := service.source.Migration(ctx, migrationID)
	if err != nil {
		return finishUnavailable(report, work, err)
	}
	work.addRows(1)
	report.Migration = &MigrationSummary{Found: found}
	if !found {
		report.addCheck("migration_exists", 1, 0, 1)
		return finishReport(report, work, false, false)
	}
	sourceStorage, sourceValid := parseStorage(record.SourceShardID)
	targetStorage, targetValid := parseStorage(record.TargetShardID)
	summary := &MigrationSummary{
		Found: true, State: record.State, SourceShardID: record.SourceShardID,
		TargetShardID: record.TargetShardID, SourceGeneration: record.SourceGeneration,
		TargetGeneration: record.TargetGeneration, CopyComplete: record.CopyComplete,
		CopiedRows: record.CopiedRows, AuditedCopiedRows: record.auditedRows(),
		ValidationStatus: record.ValidationStatus,
	}
	report.Migration = summary
	report.addCheck("migration_exists", 1, 0, 0)
	recordViolation := int64(0)
	if !sourceValid || !targetValid || sourceStorage == targetStorage ||
		record.SourceGeneration <= 0 || record.TargetGeneration <= record.SourceGeneration ||
		!record.SourceCatalogEnabled || !record.TargetCatalogEnabled || !knownMigrationState(record.State) {
		recordViolation = 1
	}
	report.addCheck("migration_record", 1, 0, recordViolation)
	counterViolation := int64(0)
	if record.CopiedRows != record.auditedRows() || record.CopiedRows < 0 ||
		record.InventoryRowsCopied < 0 || record.ReservationRowsCopied < 0 ||
		record.ReservationSeatRowsCopied < 0 || record.TicketOrderRowsCopied < 0 ||
		record.TicketRowsCopied < 0 || record.IdempotencyRowsCopied < 0 ||
		record.CopyComplete != (record.CopyPhase == "complete") {
		counterViolation = 1
	}
	report.addCheck("migration_copy_counts", 1, 0, counterViolation)
	assignmentViolation := validateMigrationAssignment(record)
	report.addCheck("migration_assignment", 1, 0, assignmentViolation)
	validationViolation := validateLastValidation(record)
	report.addCheck("migration_validation", 1, 0, validationViolation)

	failedStorage := make(map[fixedStorage]bool)
	fences := make(map[fixedStorage]fenceObservation)
	for index, storage := range fixedStorages {
		if !work.canQuery() {
			markShardFailure(&report.Shards[index], "limit_reached")
			failedStorage[storage] = true
			continue
		}
		rows, fenceErr := service.source.Fences(ctx, storage, []uuid.UUID{record.TrainRunID})
		report.Shards[index].Pages++
		if fenceErr != nil {
			markShardFailure(&report.Shards[index], failureCategory(fenceErr))
			failedStorage[storage] = true
			continue
		}
		if !work.addRows(int64(len(rows))) {
			markShardFailure(&report.Shards[index], "row_limit")
			failedStorage[storage] = true
			continue
		}
		report.Shards[index].Rows += int64(len(rows))
		if len(rows) == 1 {
			fences[storage] = rows[0]
		}
	}
	if len(failedStorage) == 0 {
		service.checkMigrationFences(&report, record, sourceStorage, targetStorage, fences)
	}

	if sourceValid {
		service.inspectMigrationStorage(ctx, &report, &work, failedStorage, sourceStorage, record.TrainRunID, true)
	}
	if targetValid {
		service.inspectMigrationStorage(ctx, &report, &work, failedStorage, targetStorage, record.TrainRunID, false)
	}
	if sourceValid && targetValid && sourceStorage != targetStorage &&
		!failedStorage[sourceStorage] && !failedStorage[targetStorage] {
		countsViolation := int64(0)
		if requiresEqualCopy(record.State, record.CopyComplete) && summary.SourceCounts != summary.TargetCounts {
			countsViolation = 1
		}
		report.addCheck("source_target_dataset_counts", 6, 0, countsViolation)
		auditViolation := int64(0)
		if record.CopyComplete && record.auditedRows() != summary.TargetCounts.total() {
			auditViolation = 1
		}
		report.addCheck("migration_audit_vs_target_counts", 6, 0, auditViolation)
	}

	remaining := work.limits.MaxRows - work.rows
	centralFailed := false
	if remaining < 8 {
		work.truncated = true
	} else if work.canQuery() {
		central, centralErr := service.source.CentralMigrationSnapshot(ctx, record, remaining/8)
		if centralErr != nil {
			centralFailed = true
			report.Failures = append(report.Failures, "central_snapshot_"+failureCategory(centralErr))
		} else {
			centralRows := central.QuotaClaims + central.IdempotencyClaims + central.ReservationLocators +
				central.TicketOrderLocators + central.TicketLocators + central.OutboxEvents +
				central.MigrationsForTrainRun + central.GenerationWriteRows
			if !work.addRows(centralRows) || central.Truncated {
				work.truncated = true
			} else {
				summary.QuotaClaims = central.QuotaClaims
				summary.IdempotencyClaims = central.IdempotencyClaims
				summary.ReservationLocators = central.ReservationLocators
				summary.TicketOrderLocators = central.TicketOrderLocators
				summary.TicketLocators = central.TicketLocators
				summary.OutboxEvents = central.OutboxEvents
				summary.MigrationsForTrainRun = central.MigrationsForTrainRun
				summary.GenerationWriteRows = central.GenerationWriteRows
				report.addCheck("locator_route_generation", central.ReservationLocators+central.TicketOrderLocators+central.TicketLocators,
					0, central.LocatorRouteViolations)
				report.addCheck("idempotency_claim_route", central.IdempotencyClaims, 0, central.IdempotencyRouteViolations)
				report.addCheck("outbox_provenance", central.OutboxEvents, 0, central.OutboxProvenanceViolations)
				report.addCheck("migration_cardinality", central.MigrationsForTrainRun, central.ActiveMigrations,
					boolViolation(central.ActiveMigrations > 1))
				report.addCheck("generation_write_evidence", central.GenerationWriteRows, 0, central.GenerationWriteViolations)
			}
		}
	}
	partial := work.truncated || centralFailed || len(failedStorage) != 0
	return finishReport(report, work, partial, false)
}

func (service *service) inspectMigrationStorage(
	ctx context.Context,
	report *Report,
	work *budget,
	failed map[fixedStorage]bool,
	storage fixedStorage,
	trainRunID uuid.UUID,
	source bool,
) {
	index := slices.Index(fixedStorages[:], storage)
	if index < 0 || failed[storage] {
		return
	}
	remaining := work.limits.MaxRows - work.rows
	if remaining < 6 || !work.canQuery() {
		markShardFailure(&report.Shards[index], "limit_reached")
		failed[storage] = true
		return
	}
	snapshot, err := service.source.StorageSnapshot(ctx, storage, trainRunID, remaining/6)
	report.Shards[index].Pages++
	if err != nil {
		markShardFailure(&report.Shards[index], failureCategory(err))
		failed[storage] = true
		return
	}
	if !work.addRows(snapshot.Counts.total()) || snapshot.Truncated {
		markShardFailure(&report.Shards[index], "row_limit")
		failed[storage] = true
		work.truncated = true
		return
	}
	report.Shards[index].Rows += snapshot.Counts.total()
	if source {
		report.Migration.SourceCounts = snapshot.Counts
	} else {
		report.Migration.TargetCounts = snapshot.Counts
	}
	addShardCheck(&report.Shards[index], "seat_masks", snapshot.Counts.Inventory,
		snapshot.SeatMaskViolations+snapshot.OrphanActiveSeats)
	addShardCheck(&report.Shards[index], "quota_claims", snapshot.Counts.Reservations, snapshot.QuotaViolations)
	addShardCheck(&report.Shards[index], "ticket_integrity", snapshot.Counts.Tickets, snapshot.TicketViolations)
	addShardCheck(&report.Shards[index], "idempotency_claims", snapshot.Counts.IdempotencyRecords,
		snapshot.IdempotencyViolations)
	report.addCheck("seat_mask_integrity", snapshot.Counts.Inventory, 0,
		snapshot.SeatMaskViolations+snapshot.OrphanActiveSeats)
	report.addCheck("quota_claim_integrity", snapshot.Counts.Reservations, 0, snapshot.QuotaViolations)
	report.addCheck("ticket_integrity", snapshot.Counts.Tickets, 0, snapshot.TicketViolations)
	report.addCheck("idempotency_integrity", snapshot.Counts.IdempotencyRecords, 0,
		snapshot.IdempotencyViolations)
}

func (service *service) checkMigrationFences(
	report *Report,
	record migrationObservation,
	source fixedStorage,
	target fixedStorage,
	fences map[fixedStorage]fenceObservation,
) {
	sourceFence, sourceExists := fences[source]
	targetFence, targetExists := fences[target]
	presenceViolation := !sourceExists
	if record.State != "planned" && record.State != "draining" && record.State != "failed" && !targetExists {
		presenceViolation = true
	}
	report.addCheck("migration_fence_presence", 2, boolCount(sourceExists)+boolCount(targetExists),
		boolViolation(presenceViolation))
	bothWritable := sourceExists && sourceFence.Enabled && targetExists && targetFence.Enabled
	report.addCheck("source_target_not_both_writable", 1, boolCount(bothWritable), boolViolation(bothWritable))
	stateViolation := false
	switch record.State {
	case "planned", "draining":
		stateViolation = !sourceExists || !sourceFence.Enabled || (targetExists && targetFence.Enabled)
	case "copying", "validating", "cutover_ready", "cutting_over":
		stateViolation = !sourceExists || sourceFence.Enabled || !targetExists || targetFence.Enabled
	case "rollback_window", "completed":
		stateViolation = !sourceExists || sourceFence.Enabled || !targetExists || !targetFence.Enabled
	case "rolled_back":
		stateViolation = !sourceExists || !sourceFence.Enabled || (targetExists && targetFence.Enabled)
	}
	report.addCheck("migration_fence_state", 1, 0, boolViolation(stateViolation))
	staleSource := false
	if stateAtOrAfterCutover(record.State) {
		staleSource = !sourceExists || sourceFence.Enabled
	}
	report.addCheck("stale_source_fence", 1, boolCount(sourceExists && sourceFence.Enabled), boolViolation(staleSource))
	fenceGenerationViolation := false
	if sourceExists && sourceFence.Generation != record.SourceGeneration &&
		!(record.State == "rolled_back" && sourceFence.Generation > record.TargetGeneration) {
		fenceGenerationViolation = true
	}
	if targetExists && targetFence.Generation != record.TargetGeneration {
		fenceGenerationViolation = true
	}
	report.addCheck("migration_fence_generation", 2, 0, boolViolation(fenceGenerationViolation))
}

func validateMigrationAssignment(record migrationObservation) int64 {
	if !record.AssignmentPresent || record.AssignmentGeneration <= 0 {
		return 1
	}
	terminal := record.State == "completed" || record.State == "failed" || record.State == "rolled_back"
	if terminal {
		if record.ActiveMigrationID != nil || record.AssignmentState != "stable" {
			return 1
		}
		return 0
	}
	if record.ActiveMigrationID == nil || *record.ActiveMigrationID != record.ID {
		return 1
	}
	switch record.State {
	case "planned", "draining":
		if record.AssignmentShardID != record.SourceShardID ||
			record.AssignmentGeneration != record.SourceGeneration || record.AssignmentState != "draining" {
			return 1
		}
	case "copying", "validating", "cutover_ready", "cutting_over":
		if record.AssignmentShardID != record.SourceShardID ||
			record.AssignmentGeneration != record.SourceGeneration || record.AssignmentState != "migrating" {
			return 1
		}
	case "rollback_window":
		if record.AssignmentShardID != record.TargetShardID ||
			record.AssignmentGeneration != record.TargetGeneration || record.AssignmentState != "rollback_window" {
			return 1
		}
	default:
		return 1
	}
	return 0
}

func validateLastValidation(record migrationObservation) int64 {
	if record.ValidationStatus != "passed" {
		if record.State == "cutover_ready" || record.State == "cutting_over" ||
			record.State == "rollback_window" || record.State == "completed" {
			return 1
		}
		return 0
	}
	var outcome control.ValidationOutcome
	if len(record.LastValidation) == 0 || json.Unmarshal(record.LastValidation, &outcome) != nil ||
		!outcome.Passed || outcome.Snapshot.Truncated {
		return 1
	}
	return 0
}

func knownMigrationState(state string) bool {
	switch state {
	case "planned", "draining", "copying", "validating", "cutover_ready", "cutting_over",
		"rollback_window", "completed", "failed", "rolled_back":
		return true
	default:
		return false
	}
}

func requiresEqualCopy(state string, copyComplete bool) bool {
	if copyComplete {
		return true
	}
	switch state {
	case "validating", "cutover_ready", "cutting_over", "rollback_window", "completed":
		return true
	default:
		return false
	}
}

func stateAtOrAfterCutover(state string) bool {
	switch state {
	case "cutting_over", "rollback_window", "completed":
		return true
	default:
		return false
	}
}

func finishUnavailable(report Report, work budget, cause error) (Report, error) {
	report.Pages = work.pages
	report.Rows = work.rows
	if len(report.Failures) == 0 {
		report.Failures = append(report.Failures, failureCategory(cause))
	}
	if work.rows > 0 || len(report.Checks) > 0 {
		report.Completeness = CompletenessPartial
		if errors.Is(cause, context.DeadlineExceeded) || errors.Is(cause, context.Canceled) {
			return report, errors.Join(ErrPartial, cause)
		}
		return report, ErrPartial
	}
	report.Completeness = CompletenessUnavailable
	for index := range report.Shards {
		markShardFailure(&report.Shards[index], failureCategory(cause))
	}
	if errors.Is(cause, context.DeadlineExceeded) || errors.Is(cause, context.Canceled) {
		return report, errors.Join(ErrUnavailable, cause)
	}
	return report, ErrUnavailable
}

func finishReport(report Report, work budget, partial, unavailable bool) (Report, error) {
	report.Pages = work.pages
	report.Rows = work.rows
	report.Truncated = work.truncated
	if unavailable {
		report.Completeness = CompletenessUnavailable
		return report, ErrUnavailable
	}
	var resultErr error
	if partial {
		report.Completeness = CompletenessPartial
		resultErr = ErrPartial
		if work.truncated {
			resultErr = errors.Join(resultErr, ErrLimitReached)
		}
	}
	if report.Violations > 0 {
		resultErr = errors.Join(resultErr, fmt.Errorf("%w: %d", ErrViolations, report.Violations))
	}
	return report, resultErr
}

func markShardFailure(report *ShardReport, category string) {
	if report.Status == "healthy" {
		report.Status = "unavailable"
		report.Failure = category
	}
}

func addShardCheck(report *ShardReport, name string, checked, violations int64) {
	report.Checks = append(report.Checks, Check{Name: name, Checked: checked, Violations: violations})
}

func failureCategory(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "query_failed"
	}
}

func boolViolation(value bool) int64 {
	if value {
		return 1
	}
	return 0
}

func boolCount(value bool) int64 {
	return boolViolation(value)
}

func max64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}
