package postgres

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
	"github.com/jackc/pgx/v5"
)

var ErrInvalidObservation = errors.New("regional database observation invalid")

// ObservationDB is the smallest read-only database seam required to inspect
// one fixed regional PostgreSQL member.
type ObservationDB interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// DatabaseObservation is a bounded, non-secret role and recovery observation.
// It deliberately contains no host, DSN, slot, credential, or database path.
type DatabaseObservation struct {
	Database      recovery.Database
	Role          recovery.DatabaseRole
	Position      recovery.ReplicationPosition
	Authority     authority.Snapshot
	SchemaVersion int
	SchemaDirty   bool
}

type TopologyObserver struct {
	databases recovery.DatabaseSet[ObservationDB]
}

func NewTopologyObserver(databases recovery.DatabaseSet[ObservationDB]) (*TopologyObserver, error) {
	if err := databases.Visit(func(_ recovery.Database, database ObservationDB) error {
		if database == nil {
			return ErrInvalidObservation
		}
		return nil
	}); err != nil {
		return nil, ErrInvalidObservation
	}
	return &TopologyObserver{databases: databases}, nil
}

func (observer *TopologyObserver) Observe(ctx context.Context) (recovery.DatabaseSet[DatabaseObservation], error) {
	if observer == nil || ctx == nil {
		return recovery.DatabaseSet[DatabaseObservation]{}, ErrInvalidObservation
	}
	observations := make(map[recovery.Database]DatabaseObservation, 3)
	err := observer.databases.Visit(func(identity recovery.Database, database ObservationDB) error {
		observation, err := observeDatabase(ctx, database, identity)
		if err != nil {
			return err
		}
		observations[identity] = observation
		return nil
	})
	if err != nil {
		return recovery.DatabaseSet[DatabaseObservation]{}, ErrInvalidObservation
	}
	return recovery.NewDatabaseSet(
		observations[recovery.DatabaseControl],
		observations[recovery.DatabaseShard0],
		observations[recovery.DatabaseShard1],
	), nil
}

func observeDatabase(ctx context.Context, database ObservationDB, identity recovery.Database) (DatabaseObservation, error) {
	var (
		inRecovery           bool
		lsn                  string
		timeline, rawEpoch   int64
		rawRegion, rawState  string
		writesEnabled, dirty bool
		version              int
	)
	if err := database.QueryRow(ctx, databaseObservationSQL).Scan(
		&inRecovery, &lsn, &timeline,
		&rawRegion, &rawEpoch, &rawState, &writesEnabled,
		&version, &dirty,
	); err != nil || timeline <= 0 || timeline > int64(^uint32(0)) || rawEpoch <= 0 || version <= 0 {
		return DatabaseObservation{}, ErrInvalidObservation
	}
	wal, err := parseLSN(lsn)
	if err != nil {
		return DatabaseObservation{}, err
	}
	position, err := recovery.NewReplicationPosition(uint32(timeline), wal)
	if err != nil {
		return DatabaseObservation{}, ErrInvalidObservation
	}
	region, err := authority.ParseRegion(rawRegion)
	if err != nil {
		return DatabaseObservation{}, ErrInvalidObservation
	}
	epoch, err := authority.NewEpoch(uint64(rawEpoch))
	if err != nil {
		return DatabaseObservation{}, ErrInvalidObservation
	}
	snapshot, err := authority.NewSnapshot(region, epoch, authority.State(rawState), writesEnabled)
	if err != nil {
		return DatabaseObservation{}, ErrInvalidObservation
	}
	role := recovery.DatabaseRolePrimary
	if inRecovery {
		role = recovery.DatabaseRoleStandby
	}
	return DatabaseObservation{
		Database: identity, Role: role, Position: position, Authority: snapshot,
		SchemaVersion: version, SchemaDirty: dirty,
	}, nil
}

func parseLSN(raw string) (uint64, error) {
	parts := strings.Split(raw, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return 0, ErrInvalidObservation
	}
	high, highErr := strconv.ParseUint(parts[0], 16, 32)
	low, lowErr := strconv.ParseUint(parts[1], 16, 32)
	value := high<<32 | low
	if highErr != nil || lowErr != nil || value == 0 {
		return 0, ErrInvalidObservation
	}
	return value, nil
}

const databaseObservationSQL = `
SELECT pg_is_in_recovery(),
       CASE WHEN pg_is_in_recovery()
            THEN pg_last_wal_replay_lsn()
            ELSE pg_current_wal_lsn()
       END::text,
       ((pg_control_checkpoint()).timeline_id)::bigint,
       authority.region,authority.epoch,authority.state,authority.writes_enabled,
       migrations.version,migrations.dirty
FROM public.regional_write_authority AS authority
CROSS JOIN public.schema_migrations AS migrations
WHERE authority.singleton
LIMIT 1`
