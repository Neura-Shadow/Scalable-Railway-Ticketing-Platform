// Package postgres implements the payment shard boundary without allowing a
// payment intent to bind permanently to one physical database.
package postgres

import (
	"context"
	"errors"

	paymentapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/application"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type ControlReader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

type Directory struct{ control ControlReader }

func NewDirectory(control ControlReader) (*Directory, error) {
	if control == nil {
		return nil, paymentapp.ErrPaymentUnavailable
	}
	return &Directory{control: control}, nil
}

func (directory *Directory) ResolveReservation(ctx context.Context, reservationID uuid.UUID, _ bool) (sharding.ShardRoute, error) {
	if directory == nil || directory.control == nil || ctx == nil || reservationID == uuid.Nil {
		return sharding.ShardRoute{}, paymentapp.ErrPaymentNotFound
	}
	var (
		trainRunID uuid.UUID
		rawShardID string
		generation int64
		state      string
		kind       string
	)
	err := directory.control.QueryRow(ctx, `
SELECT directory.train_run_id,directory.last_known_shard_id,
       directory.last_known_generation,directory.state,shard.storage_kind
FROM public.reservation_directory AS directory
JOIN public.booking_shards AS shard
  ON shard.shard_id=directory.last_known_shard_id
WHERE directory.reservation_id=$1`, reservationID).Scan(
		&trainRunID, &rawShardID, &generation, &state, &kind,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return sharding.ShardRoute{}, paymentapp.ErrPaymentNotFound
	}
	if err != nil || state != "active" || kind != "postgres" {
		return sharding.ShardRoute{}, sharding.ErrShardUnavailable
	}
	shardID, err := sharding.ParseShardID(rawShardID)
	if err != nil || (shardID != sharding.ShardPhysicalZero && shardID != sharding.ShardPhysicalOne) {
		return sharding.ShardRoute{}, sharding.ErrShardUnavailable
	}
	assignmentGeneration, err := sharding.NewAssignmentGeneration(generation)
	if err != nil {
		return sharding.ShardRoute{}, sharding.ErrShardUnavailable
	}
	route, err := sharding.NewShardRoute(trainRunID, shardID, assignmentGeneration)
	if err != nil {
		return sharding.ShardRoute{}, sharding.ErrShardUnavailable
	}
	return route, nil
}
