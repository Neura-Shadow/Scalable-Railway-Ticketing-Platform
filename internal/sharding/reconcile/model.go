// Package reconcile performs bounded, detect-only checks over the fixed
// Milestone 4 booking topology.
package reconcile

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

const (
	ScopeAssignments = "shard-assignments"
	ScopeLocators    = "shard-locators"
	ScopeMigration   = "shard-migration"

	DefaultPageSize = 100
	DefaultMaxPages = 10_000
	DefaultMaxRows  = int64(100_000)

	MaxPageSize = 1_000
	MaxPages    = 10_000
	MaxRows     = int64(1_000_000)
)

var (
	ErrInvalidInput = errors.New("invalid shard reconciliation input")
	ErrViolations   = errors.New("shard reconciliation violations detected")
	ErrPartial      = errors.New("shard reconciliation result is partial")
	ErrUnavailable  = errors.New("shard reconciliation unavailable")
	ErrLimitReached = errors.New("shard reconciliation limit reached")
)

type Completeness string

const (
	CompletenessComplete    Completeness = "complete"
	CompletenessPartial     Completeness = "partial"
	CompletenessUnavailable Completeness = "unavailable"
)

type Limits struct {
	PageSize int
	MaxPages int
	MaxRows  int64
}

func DefaultLimits() Limits {
	return Limits{PageSize: DefaultPageSize, MaxPages: DefaultMaxPages, MaxRows: DefaultMaxRows}
}

func (limits Limits) Valid() bool {
	return limits.PageSize >= 1 && limits.PageSize <= MaxPageSize &&
		limits.MaxPages >= 1 && limits.MaxPages <= MaxPages &&
		limits.MaxRows >= 1 && limits.MaxRows <= MaxRows
}

type LocatorFilter struct {
	TrainRunID *uuid.UUID
}

func (filter LocatorFilter) valid() bool {
	return filter.TrainRunID == nil || *filter.TrainRunID != uuid.Nil
}

type Check struct {
	Name       string `json:"name"`
	Checked    int64  `json:"checked"`
	Observed   int64  `json:"observed,omitempty"`
	Violations int64  `json:"violations"`
}

type ShardReport struct {
	ShardID string  `json:"shard_id"`
	Status  string  `json:"status"`
	Pages   int     `json:"pages"`
	Rows    int64   `json:"rows_examined"`
	Failure string  `json:"failure,omitempty"`
	Checks  []Check `json:"checks,omitempty"`
}

type DatasetCounts struct {
	Inventory          int64 `json:"inventory"`
	Reservations       int64 `json:"reservations"`
	ReservationSeats   int64 `json:"reservation_seats"`
	TicketOrders       int64 `json:"ticket_orders"`
	Tickets            int64 `json:"tickets"`
	IdempotencyRecords int64 `json:"idempotency_records"`
}

func (counts DatasetCounts) total() int64 {
	return counts.Inventory + counts.Reservations + counts.ReservationSeats +
		counts.TicketOrders + counts.Tickets + counts.IdempotencyRecords
}

type MigrationSummary struct {
	Found                 bool          `json:"found"`
	State                 string        `json:"state,omitempty"`
	SourceShardID         string        `json:"source_shard_id,omitempty"`
	TargetShardID         string        `json:"target_shard_id,omitempty"`
	SourceGeneration      int64         `json:"source_generation,omitempty"`
	TargetGeneration      int64         `json:"target_generation,omitempty"`
	CopyComplete          bool          `json:"copy_complete"`
	CopiedRows            int64         `json:"copied_rows"`
	AuditedCopiedRows     int64         `json:"audited_copied_rows"`
	ValidationStatus      string        `json:"validation_status,omitempty"`
	SourceCounts          DatasetCounts `json:"source_counts"`
	TargetCounts          DatasetCounts `json:"target_counts"`
	QuotaClaims           int64         `json:"quota_claims"`
	IdempotencyClaims     int64         `json:"idempotency_claims"`
	ReservationLocators   int64         `json:"reservation_locators"`
	TicketOrderLocators   int64         `json:"ticket_order_locators"`
	TicketLocators        int64         `json:"ticket_locators"`
	OutboxEvents          int64         `json:"outbox_events"`
	MigrationsForTrainRun int64         `json:"migrations_for_train_run"`
	GenerationWriteRows   int64         `json:"generation_write_rows"`
}

type Report struct {
	Scope        string            `json:"scope"`
	ReadOnly     bool              `json:"read_only"`
	Completeness Completeness      `json:"completeness"`
	Pages        int               `json:"pages"`
	Rows         int64             `json:"rows_examined"`
	Violations   int64             `json:"violations"`
	Truncated    bool              `json:"truncated"`
	Checks       []Check           `json:"checks,omitempty"`
	Shards       []ShardReport     `json:"shards,omitempty"`
	Migration    *MigrationSummary `json:"migration,omitempty"`
	Failures     []string          `json:"failures,omitempty"`
	Deferred     []string          `json:"deferred_checks,omitempty"`
	CheckedAt    time.Time         `json:"checked_at"`
}

func (report *Report) addCheck(name string, checked, observed, violations int64) {
	for index := range report.Checks {
		if report.Checks[index].Name == name {
			report.Checks[index].Checked += checked
			report.Checks[index].Observed += observed
			report.Checks[index].Violations += violations
			report.Violations += violations
			return
		}
	}
	report.Checks = append(report.Checks, Check{
		Name: name, Checked: checked, Observed: observed, Violations: violations,
	})
	report.Violations += violations
}
