package app

import (
	"context"
	"errors"
	"time"

	bookingapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/application"
	bookingpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/postgres"
	querypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
)

const idempotencyRecordTTL = 24 * time.Hour

type reservationCommands interface {
	CreateHold(context.Context, bookingpostgres.CreateHoldParams) (bookingpostgres.CreateHoldResult, error)
	ConfirmReservation(context.Context, bookingpostgres.ReservationCommandParams) (bookingpostgres.ConfirmReservationResult, error)
	CancelReservation(context.Context, bookingpostgres.ReservationCommandParams) (bookingpostgres.CancelReservationResult, error)
}
type journeyResolver interface {
	ResolveJourney(context.Context, string, string, string) (querypostgres.Journey, error)
}

// ReservationDetail is the complete owner-scoped read model used after command
// execution. A PostgreSQL implementation is provided by PostgresReads.
type ReservationDetail struct {
	ID, Status, TrainRunID, OriginStationCode, DestinationStationCode, SeatClass string
	PassengerIDs                                                                 []string
	ExpiresAt                                                                    *time.Time
}
type reservationReader interface {
	GetReservationDetail(context.Context, uuid.UUID, uuid.UUID) (ReservationDetail, error)
}

type ReservationService struct {
	commands      reservationCommands
	journeys      journeyResolver
	reader        reservationReader
	clock         appClock
	holdTTL       time.Duration
	maxPassengers int
}

func NewReservationService(commands reservationCommands, journeys journeyResolver, reader reservationReader, clock appClock, holdTTL time.Duration, maxPassengers int) *ReservationService {
	return &ReservationService{commands: commands, journeys: journeys, reader: reader, clock: clock, holdTTL: holdTTL, maxPassengers: maxPassengers}
}

func (s *ReservationService) CreateHold(ctx context.Context, c httpapi.CreateReservationCommand) (httpapi.ReservationView, error) {
	if s == nil || s.commands == nil || s.journeys == nil || s.holdTTL <= 0 || s.maxPassengers <= 0 || len(c.PassengerIDs) < 1 || len(c.PassengerIDs) > s.maxPassengers {
		return httpapi.ReservationView{}, httpapi.ErrInvalidInput
	}
	owner, err := uuid.Parse(c.OwnerID)
	if err != nil {
		return httpapi.ReservationView{}, httpapi.ErrInvalidInput
	}
	run, err := uuid.Parse(c.TrainRunID)
	if err != nil {
		return httpapi.ReservationView{}, httpapi.ErrInvalidInput
	}
	passengers := make([]uuid.UUID, len(c.PassengerIDs))
	for i, id := range c.PassengerIDs {
		passengers[i], err = uuid.Parse(id)
		if err != nil {
			return httpapi.ReservationView{}, httpapi.ErrInvalidInput
		}
	}
	key, err := bookingapp.HashIdempotencyKey(c.IdempotencyKey)
	if err != nil {
		return httpapi.ReservationView{}, httpapi.ErrInvalidInput
	}
	fingerprint, err := bookingapp.FingerprintHoldRequest(bookingapp.HoldFingerprintInput{TrainRunID: c.TrainRunID, OriginCode: c.OriginStationCode, DestinationCode: c.DestinationStationCode, SeatClass: c.SeatClass, PassengerIDs: c.PassengerIDs})
	if err != nil {
		return httpapi.ReservationView{}, httpapi.ErrInvalidInput
	}
	journey, err := s.journeys.ResolveJourney(ctx, c.TrainRunID, c.OriginStationCode, c.DestinationStationCode)
	if err != nil {
		return httpapi.ReservationView{}, mapQueryError(err)
	}
	now := s.now()
	if now.IsZero() {
		return httpapi.ReservationView{}, httpapi.ErrUnavailable
	}
	expires := now.Add(s.holdTTL)
	result, err := s.commands.CreateHold(ctx, bookingpostgres.CreateHoldParams{UserID: owner, TrainRunID: run, FromStopIndex: journey.FromStopIndex, ToStopIndex: journey.ToStopIndex, SeatClass: c.SeatClass, PassengerIDs: passengers, HoldExpiresAt: expires, IdempotencyKeyHash: append([]byte(nil), key[:]...), RequestFingerprint: append([]byte(nil), fingerprint[:]...), IdempotencyExpiresAt: now.Add(idempotencyRecordTTL)})
	if err != nil {
		return httpapi.ReservationView{}, mapBookingError(err)
	}
	return s.readUUID(ctx, owner, result.ReservationID)
}

func (s *ReservationService) GetReservation(ctx context.Context, ownerID, reservationID string) (httpapi.ReservationView, error) {
	return s.read(ctx, ownerID, reservationID)
}
func (s *ReservationService) ConfirmReservation(ctx context.Context, c httpapi.ReservationMutationCommand) (httpapi.ReservationView, error) {
	owner, reservation, input, err := s.mutation(c, bookingapp.OperationReservationConfirm)
	if err != nil {
		return httpapi.ReservationView{}, err
	}
	if _, err = s.commands.ConfirmReservation(ctx, input); err != nil {
		return httpapi.ReservationView{}, mapBookingError(err)
	}
	return s.readUUID(ctx, owner, reservation)
}
func (s *ReservationService) CancelReservation(ctx context.Context, c httpapi.ReservationMutationCommand) (httpapi.ReservationView, error) {
	owner, reservation, input, err := s.mutation(c, bookingapp.OperationReservationCancel)
	if err != nil {
		return httpapi.ReservationView{}, err
	}
	if _, err = s.commands.CancelReservation(ctx, input); err != nil {
		return httpapi.ReservationView{}, mapBookingError(err)
	}
	return s.readUUID(ctx, owner, reservation)
}
func (s *ReservationService) mutation(c httpapi.ReservationMutationCommand, operation bookingapp.IdempotentOperation) (uuid.UUID, uuid.UUID, bookingpostgres.ReservationCommandParams, error) {
	if s == nil || s.commands == nil {
		return uuid.Nil, uuid.Nil, bookingpostgres.ReservationCommandParams{}, httpapi.ErrUnavailable
	}
	owner, err := uuid.Parse(c.OwnerID)
	if err != nil {
		return uuid.Nil, uuid.Nil, bookingpostgres.ReservationCommandParams{}, httpapi.ErrInvalidInput
	}
	reservation, err := uuid.Parse(c.ReservationID)
	if err != nil {
		return uuid.Nil, uuid.Nil, bookingpostgres.ReservationCommandParams{}, httpapi.ErrInvalidInput
	}
	key, err := bookingapp.HashIdempotencyKey(c.IdempotencyKey)
	if err != nil {
		return uuid.Nil, uuid.Nil, bookingpostgres.ReservationCommandParams{}, httpapi.ErrInvalidInput
	}
	fingerprint, err := bookingapp.FingerprintReservationCommand(operation, c.ReservationID)
	if err != nil {
		return uuid.Nil, uuid.Nil, bookingpostgres.ReservationCommandParams{}, httpapi.ErrInvalidInput
	}
	now := s.now()
	if now.IsZero() {
		return uuid.Nil, uuid.Nil, bookingpostgres.ReservationCommandParams{}, httpapi.ErrUnavailable
	}
	return owner, reservation, bookingpostgres.ReservationCommandParams{UserID: owner, ReservationID: reservation, Now: now, IdempotencyKeyHash: append([]byte(nil), key[:]...), RequestFingerprint: append([]byte(nil), fingerprint[:]...), IdempotencyExpiresAt: now.Add(idempotencyRecordTTL)}, nil
}
func (s *ReservationService) read(ctx context.Context, ownerID, reservationID string) (httpapi.ReservationView, error) {
	owner, err := uuid.Parse(ownerID)
	if err != nil {
		return httpapi.ReservationView{}, httpapi.ErrInvalidInput
	}
	reservation, err := uuid.Parse(reservationID)
	if err != nil {
		return httpapi.ReservationView{}, httpapi.ErrInvalidInput
	}
	return s.readUUID(ctx, owner, reservation)
}
func (s *ReservationService) readUUID(ctx context.Context, owner, reservation uuid.UUID) (httpapi.ReservationView, error) {
	if s == nil || s.reader == nil {
		return httpapi.ReservationView{}, httpapi.ErrUnavailable
	}
	d, err := s.reader.GetReservationDetail(ctx, owner, reservation)
	if err != nil {
		return httpapi.ReservationView{}, mapReadError(err)
	}
	return httpapi.ReservationView{ID: d.ID, Status: d.Status, TrainRunID: d.TrainRunID, OriginStationCode: d.OriginStationCode, DestinationStationCode: d.DestinationStationCode, SeatClass: d.SeatClass, PassengerIDs: append([]string(nil), d.PassengerIDs...), ExpiresAt: d.ExpiresAt}, nil
}
func (s *ReservationService) now() time.Time {
	if s.clock == nil {
		return time.Time{}
	}
	return s.clock.Now().UTC()
}

func mapBookingError(err error) error {
	switch {
	case errors.Is(err, bookingpostgres.ErrInvalidArgument):
		return httpapi.ErrInvalidInput
	case errors.Is(err, bookingpostgres.ErrNotFound):
		return httpapi.ErrNotFound
	case errors.Is(err, bookingpostgres.ErrInsufficientInventory), errors.Is(err, bookingpostgres.ErrNotBookable), errors.Is(err, bookingpostgres.ErrReservationExpired), errors.Is(err, bookingpostgres.ErrInvalidState), errors.Is(err, bookingpostgres.ErrIdempotencyConflict), errors.Is(err, bookingpostgres.ErrIdempotencyInProgress):
		return httpapi.ErrConflict
	default:
		return httpapi.ErrUnavailable
	}
}

var _ httpapi.ReservationService = (*ReservationService)(nil)
