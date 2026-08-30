// Package recovery owns the fixed, operator-controlled active-passive database
// recovery state machine. It never accepts arbitrary hosts or commands.
package recovery

import "errors"

var ErrInvalidDatabase = errors.New("invalid regional database")

// Database is one of the three required PostgreSQL authorities. It is a logical
// identity, never a host, DSN, path, or shell argument.
type Database string

const (
	DatabaseControl Database = "control"
	DatabaseShard0  Database = "shard-0"
	DatabaseShard1  Database = "shard-1"
)

func ParseDatabase(raw string) (Database, error) {
	database := Database(raw)
	switch database {
	case DatabaseControl, DatabaseShard0, DatabaseShard1:
		return database, nil
	default:
		return "", ErrInvalidDatabase
	}
}

func (database Database) String() string { return string(database) }

// DatabaseSet is a total, bounded value for control, shard 0, and shard 1. It
// deliberately cannot represent an arbitrary or incomplete database fanout.
type DatabaseSet[T any] struct {
	control T
	shard0  T
	shard1  T
}

func NewDatabaseSet[T any](control, shard0, shard1 T) DatabaseSet[T] {
	return DatabaseSet[T]{control: control, shard0: shard0, shard1: shard1}
}

func (set DatabaseSet[T]) Control() T { return set.control }
func (set DatabaseSet[T]) Shard0() T  { return set.shard0 }
func (set DatabaseSet[T]) Shard1() T  { return set.shard1 }

// Value returns one member without exposing storage discovery.
func (set DatabaseSet[T]) Value(database Database) (T, error) {
	switch database {
	case DatabaseControl:
		return set.control, nil
	case DatabaseShard0:
		return set.shard0, nil
	case DatabaseShard1:
		return set.shard1, nil
	default:
		var zero T
		return zero, ErrInvalidDatabase
	}
}

// Visit always traverses the complete topology in promotion order.
func (set DatabaseSet[T]) Visit(visitor func(Database, T) error) error {
	if visitor == nil {
		return ErrInvalidDatabase
	}
	for _, member := range []struct {
		database Database
		value    T
	}{
		{database: DatabaseControl, value: set.control},
		{database: DatabaseShard0, value: set.shard0},
		{database: DatabaseShard1, value: set.shard1},
	} {
		if err := visitor(member.database, member.value); err != nil {
			return err
		}
	}
	return nil
}
