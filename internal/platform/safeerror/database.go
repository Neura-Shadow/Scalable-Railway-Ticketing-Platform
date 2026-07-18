// Package safeerror converts dependency failures into bounded public and log
// categories without retaining credential-bearing causes.
package safeerror

import "errors"

// DatabaseCategory identifies the only database failure details that may cross
// an operational logging or public-error boundary.
type DatabaseCategory uint8

const (
	DatabaseConfigurationInvalid DatabaseCategory = iota + 1
	DatabaseConnectionFailed
	MigrationConnectionFailed
	MigrationOperationFailed
	MigrationCloseFailed
)

var (
	ErrDatabaseConfigurationInvalid = errors.New("database connection configuration is invalid")
	ErrDatabaseConnectionFailed     = errors.New("database connection failed")
	ErrMigrationConnectionFailed    = errors.New("migration database connection failed")
	ErrMigrationOperationFailed     = errors.New("migration database operation failed")
	ErrMigrationCloseFailed         = errors.New("migration database close failed")
	ErrDatabaseOperationFailed      = errors.New("database operation failed")
)

// Database intentionally discards cause instead of wrapping it. Database
// drivers and URL parsers may echo complete connection strings, query secrets,
// or client-key paths in their errors, including for malformed input.
func Database(category DatabaseCategory, _ error) error {
	switch category {
	case DatabaseConfigurationInvalid:
		return ErrDatabaseConfigurationInvalid
	case DatabaseConnectionFailed:
		return ErrDatabaseConnectionFailed
	case MigrationConnectionFailed:
		return ErrMigrationConnectionFailed
	case MigrationOperationFailed:
		return ErrMigrationOperationFailed
	case MigrationCloseFailed:
		return ErrMigrationCloseFailed
	default:
		return ErrDatabaseOperationFailed
	}
}
