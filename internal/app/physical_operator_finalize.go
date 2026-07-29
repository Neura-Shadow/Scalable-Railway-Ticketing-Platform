package app

import (
	"context"
	"encoding/json"
	"errors"

	commandphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type operatorControlDB interface {
	Begin(context.Context) (pgx.Tx, error)
}

func appendControlProjectionEvent(ctx context.Context, tx pgx.Tx, trainRunID uuid.UUID, generation int64, aggregateType string, aggregateID uuid.UUID, eventType string, sourceVersion int64) error {
	payload, err := json.Marshal(map[string]any{
		"train_run_id": trainRunID, "assignment_generation": generation,
		"resource_id": aggregateID, "source_version": sourceVersion,
	})
	if err != nil {
		return err
	}
	eventID := commandphysical.OperatorSnapshotEventID(trainRunID, generation, aggregateID, eventType, sourceVersion)
	tag, err := tx.Exec(ctx, `INSERT INTO public.outbox_events(
 id,aggregate_type,aggregate_id,event_type,event_version,payload
) VALUES($1,$2,$3,$4,1,$5::jsonb) ON CONFLICT(id) DO NOTHING`,
		eventID, aggregateType, aggregateID, eventType, string(payload))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var semanticMatch bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(
 SELECT 1 FROM public.outbox_events
 WHERE id=$1 AND aggregate_type=$2 AND aggregate_id=$3
   AND event_type=$4 AND event_version=1 AND payload=$5::jsonb
)`, eventID, aggregateType, aggregateID, eventType, string(payload)).Scan(&semanticMatch)
	if err != nil || !semanticMatch {
		return errors.New("control projection event conflict")
	}
	return nil
}
