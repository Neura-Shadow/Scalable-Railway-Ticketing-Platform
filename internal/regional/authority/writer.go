package authority

import (
	"context"
	"errors"
)

var (
	ErrInvalidWriter      = errors.New("invalid regional writer")
	ErrRoleNotActive      = errors.New("deployment role is not active")
	ErrWritesDisabled     = errors.New("regional writes are disabled")
	ErrRegionMismatch     = errors.New("regional authority region mismatch")
	ErrEpochMismatch      = errors.New("regional authority epoch mismatch")
	ErrAuthorityNotActive = errors.New("regional authority is not active")
)

// ControlTransaction is the minimum database-local interface required to
// authorize a control mutation. Implementations must lock the returned row for
// the lifetime of the enclosing transaction.
type ControlTransaction interface {
	RegionalAuthority(context.Context) (Snapshot, error)
}

// TransactionRunner is a local database abstraction seam. The callback is a
// transaction program: external network, filesystem, provider, or cross-
// database I/O is forbidden by this contract.
type TransactionRunner[T any] interface {
	WithinTransaction(context.Context, func(T) error) error
}

// ControlWriter keeps regional validation and a control mutation inside one
// local transaction while preserving the caller's richer transaction type.
type ControlWriter[T ControlTransaction] struct {
	deployment Deployment
	db         TransactionRunner[T]
}

func NewControlWriter[T ControlTransaction](
	deployment Deployment,
	db TransactionRunner[T],
) (ControlWriter[T], error) {
	if db == nil {
		return ControlWriter[T]{}, ErrInvalidWriter
	}
	return ControlWriter[T]{deployment: deployment, db: db}, nil
}

// Write authorizes and runs one database-local mutation. Callers must not make
// external I/O from mutation; uncertain external effects belong outside the
// transaction and must use durable operation state.
func (writer ControlWriter[T]) Write(ctx context.Context, mutation func(T) error) error {
	if ctx == nil || mutation == nil {
		return ErrInvalidWriter
	}
	if err := writer.deployment.allowsNormalWrite(); err != nil {
		return err
	}
	return writer.db.WithinTransaction(ctx, func(tx T) error {
		snapshot, err := tx.RegionalAuthority(ctx)
		if err != nil {
			return err
		}
		if err := writer.deployment.matches(snapshot); err != nil {
			return err
		}
		return mutation(tx)
	})
}

func (deployment Deployment) allowsNormalWrite() error {
	if deployment.role != RoleActive {
		return ErrRoleNotActive
	}
	if !deployment.writesEnabled {
		return ErrWritesDisabled
	}
	return nil
}

func (deployment Deployment) matches(snapshot Snapshot) error {
	if snapshot.region != deployment.region {
		return ErrRegionMismatch
	}
	if snapshot.epoch != deployment.epoch {
		return ErrEpochMismatch
	}
	if snapshot.state != StateActive {
		return ErrAuthorityNotActive
	}
	if !snapshot.writesEnabled {
		return ErrWritesDisabled
	}
	return nil
}

// Authorize verifies that this process may mutate the database represented by
// a snapshot already locked by the caller's local transaction. It is the
// narrow integration seam for stores that must retain ownership of their
// existing transaction lifecycle.
func (deployment Deployment) Authorize(snapshot Snapshot) error {
	if err := deployment.allowsNormalWrite(); err != nil {
		return err
	}
	return deployment.matches(snapshot)
}
