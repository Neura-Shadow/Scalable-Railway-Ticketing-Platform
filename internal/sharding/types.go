// Package sharding defines the fixed logical booking topology and the durable
// routing values shared by application and PostgreSQL adapters.
package sharding

import (
	"errors"

	"github.com/google/uuid"
)

var (
	ErrInvalidShardID              = errors.New("invalid booking shard ID")
	ErrInvalidAssignmentGeneration = errors.New("invalid assignment generation")
	ErrInvalidShardRoute           = errors.New("invalid shard route")
)

// ShardID is an opaque logical storage identity. It is deliberately not a SQL
// identifier; only the PostgreSQL routed-transaction adapter maps it to one of
// the fixed schema names.
type ShardID string

const (
	// SupportedFencingProtocolVersion is the highest train-run fencing
	// protocol this binary can enforce. Catalog rows requiring a newer
	// protocol must fail closed before a route can serve traffic.
	SupportedFencingProtocolVersion int32 = 1

	ShardLegacy ShardID = "legacy"
	ShardZero   ShardID = "shard-0"
	ShardOne    ShardID = "shard-1"
)

func ParseShardID(raw string) (ShardID, error) {
	id := ShardID(raw)
	switch id {
	case ShardLegacy, ShardZero, ShardOne:
		return id, nil
	default:
		return "", ErrInvalidShardID
	}
}

func (id ShardID) String() string { return string(id) }

// AssignmentGeneration is a positive, monotonically increasing fencing value.
// The catalog adapter owns monotonic updates; this value object prevents zero
// or negative generations from crossing the routing seam.
type AssignmentGeneration int64

func NewAssignmentGeneration(value int64) (AssignmentGeneration, error) {
	if value <= 0 {
		return 0, ErrInvalidAssignmentGeneration
	}
	return AssignmentGeneration(value), nil
}

func (generation AssignmentGeneration) Int64() int64 { return int64(generation) }

// ShardRoute is the complete authority observation a caller may cache. Schema
// identifiers deliberately do not cross this interface.
type ShardRoute struct {
	trainRunID uuid.UUID
	shardID    ShardID
	generation AssignmentGeneration
}

func NewShardRoute(trainRunID uuid.UUID, shardID ShardID, generation AssignmentGeneration) (ShardRoute, error) {
	if trainRunID == uuid.Nil || generation <= 0 {
		return ShardRoute{}, ErrInvalidShardRoute
	}
	if _, err := ParseShardID(shardID.String()); err != nil {
		return ShardRoute{}, ErrInvalidShardRoute
	}
	return ShardRoute{trainRunID: trainRunID, shardID: shardID, generation: generation}, nil
}

func (route ShardRoute) TrainRunID() uuid.UUID            { return route.trainRunID }
func (route ShardRoute) ShardID() ShardID                 { return route.shardID }
func (route ShardRoute) Generation() AssignmentGeneration { return route.generation }
