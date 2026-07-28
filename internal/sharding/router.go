package sharding

// RouteErrorCode is a bounded internal routing failure category. It contains
// no shard, schema, generation, database, or customer identifier.
type RouteErrorCode string

const (
	RouteErrorAssignmentStale   RouteErrorCode = "shard_assignment_stale"
	RouteErrorWriteFenced       RouteErrorCode = "shard_write_fenced"
	RouteErrorShardUnavailable  RouteErrorCode = "shard_unavailable"
	RouteErrorTrainRunMigrating RouteErrorCode = "train_run_migrating"
	RouteErrorLocatorNotFound   RouteErrorCode = "shard_locator_not_found"
)

// RouteError is deliberately topology-neutral so it can safely cross the
// storage/application seam. HTTP handlers should collapse retryable routing
// categories into their existing public unavailable response.
type RouteError struct {
	code RouteErrorCode
}

func (err *RouteError) Error() string { return string(err.code) }

func (err *RouteError) Code() RouteErrorCode { return err.code }

var (
	ErrAssignmentStale   = &RouteError{code: RouteErrorAssignmentStale}
	ErrWriteFenced       = &RouteError{code: RouteErrorWriteFenced}
	ErrShardUnavailable  = &RouteError{code: RouteErrorShardUnavailable}
	ErrTrainRunMigrating = &RouteError{code: RouteErrorTrainRunMigrating}
	ErrLocatorNotFound   = &RouteError{code: RouteErrorLocatorNotFound}
)
