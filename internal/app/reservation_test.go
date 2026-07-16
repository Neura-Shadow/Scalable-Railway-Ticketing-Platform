package app

import (
	"context"
	"errors"
	"testing"
	"time"

	bookingapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/application"
	bookingpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/postgres"
	querypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
)

type reservationCommandsFake struct {
	createInput  bookingpostgres.CreateHoldParams
	createResult bookingpostgres.CreateHoldResult
	confirmInput bookingpostgres.ReservationCommandParams
	cancelInput  bookingpostgres.ReservationCommandParams
	err          error
}

func (f *reservationCommandsFake) CreateHold(_ context.Context, input bookingpostgres.CreateHoldParams) (bookingpostgres.CreateHoldResult, error) {
	f.createInput = input
	return f.createResult, f.err
}
func (f *reservationCommandsFake) ConfirmReservation(_ context.Context, input bookingpostgres.ReservationCommandParams) (bookingpostgres.ConfirmReservationResult, error) {
	f.confirmInput = input
	return bookingpostgres.ConfirmReservationResult{ReservationID: input.ReservationID}, f.err
}
func (f *reservationCommandsFake) CancelReservation(_ context.Context, input bookingpostgres.ReservationCommandParams) (bookingpostgres.CancelReservationResult, error) {
	f.cancelInput = input
	return bookingpostgres.CancelReservationResult{ReservationID: input.ReservationID}, f.err
}

type journeyResolverFake struct {
	input   [3]string
	journey querypostgres.Journey
	err     error
}

func (f *journeyResolverFake) ResolveJourney(_ context.Context, run, origin, destination string) (querypostgres.Journey, error) {
	f.input = [3]string{run, origin, destination}
	return f.journey, f.err
}

type reservationReaderFake struct {
	detail ReservationDetail
	err    error
}

type reservationMetricsSpy struct {
	reservation int
	outbox      map[string]int
}

func (s *reservationMetricsSpy) RecordReservation(_, result, _ string) {
	if result == "success" {
		s.reservation++
	}
}
func (s *reservationMetricsSpy) RecordOutbox(operation, eventType, result, _ string) {
	if operation == "create" && result == "success" {
		if s.outbox == nil {
			s.outbox = map[string]int{}
		}
		s.outbox[eventType]++
	}
}

func (f *reservationReaderFake) GetReservationDetail(context.Context, uuid.UUID, uuid.UUID) (ReservationDetail, error) {
	return f.detail, f.err
}

func TestReservationCreateResolvesJourneyAndHashesIdempotencyMaterial(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	owner, run, p1, p2, reservationID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	commands := &reservationCommandsFake{createResult: bookingpostgres.CreateHoldResult{ReservationID: reservationID}}
	journeys := &journeyResolverFake{journey: querypostgres.Journey{TrainRunID: run.String(), FromStopIndex: 2, ToStopIndex: 5}}
	reader := &reservationReaderFake{detail: ReservationDetail{ID: reservationID.String(), Status: "held", TrainRunID: run.String(), OriginStationCode: "TPE", DestinationStationCode: "KHH", SeatClass: "standard", PassengerIDs: []string{p1.String(), p2.String()}, ExpiresAt: timePointer(now.Add(10 * time.Minute))}}
	service := NewReservationService(commands, journeys, reader, fixedClock{now}, 10*time.Minute, 6)
	command := httpapi.CreateReservationCommand{OwnerID: owner.String(), IdempotencyKey: "raw-key", TrainRunID: run.String(), OriginStationCode: "TPE", DestinationStationCode: "KHH", SeatClass: "standard", PassengerIDs: []string{p1.String(), p2.String()}}

	view, err := service.CreateHold(context.Background(), command)
	if err != nil {
		t.Fatalf("CreateHold() error = %v", err)
	}
	if journeys.input != [3]string{run.String(), "TPE", "KHH"} {
		t.Fatalf("journey input = %#v", journeys.input)
	}
	input := commands.createInput
	if input.FromStopIndex != 2 || input.ToStopIndex != 5 || input.HoldExpiresAt != now.Add(10*time.Minute) || view.ID != reservationID.String() || view.Status != "held" {
		t.Fatalf("input=%#v view=%#v", input, view)
	}
	wantKey, _ := bookingapp.HashIdempotencyKey("raw-key")
	wantFingerprint, _ := bookingapp.FingerprintHoldRequest(bookingapp.HoldFingerprintInput{TrainRunID: run.String(), OriginCode: "TPE", DestinationCode: "KHH", SeatClass: "standard", PassengerIDs: command.PassengerIDs})
	if string(input.IdempotencyKeyHash) != string(wantKey[:]) || string(input.RequestFingerprint) != string(wantFingerprint[:]) {
		t.Fatal("idempotency key or request fingerprint was not hashed authoritatively")
	}
	if string(input.IdempotencyKeyHash) == "raw-key" {
		t.Fatal("raw idempotency key crossed persistence seam")
	}
}

func TestReservationCreateReplayReturnsCurrentAuthoritativeState(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	owner, run, passenger, reservationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	commands := &reservationCommandsFake{createResult: bookingpostgres.CreateHoldResult{ReservationID: reservationID, Replayed: true}}
	reader := &reservationReaderFake{detail: ReservationDetail{ID: reservationID.String(), Status: "confirmed", TrainRunID: run.String()}}
	view, err := NewReservationService(commands, &journeyResolverFake{journey: querypostgres.Journey{FromStopIndex: 0, ToStopIndex: 1}}, reader, fixedClock{now}, 10*time.Minute, 6).CreateHold(context.Background(), httpapi.CreateReservationCommand{OwnerID: owner.String(), IdempotencyKey: "same-key", TrainRunID: run.String(), OriginStationCode: "TPE", DestinationStationCode: "KHH", SeatClass: "standard", PassengerIDs: []string{passenger.String()}})
	if err != nil || view.Status != "confirmed" || view.ExpiresAt != nil {
		t.Fatalf("replay view = %#v, %v", view, err)
	}
}

func TestReservationCreateMetricsCountCommittedCreationButNotReplay(t *testing.T) {
	now := time.Now().UTC()
	owner, run, passenger, reservationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	commands := &reservationCommandsFake{createResult: bookingpostgres.CreateHoldResult{ReservationID: reservationID}}
	reader := &reservationReaderFake{detail: ReservationDetail{ID: reservationID.String(), Status: "held"}}
	metrics := &reservationMetricsSpy{}
	service := NewReservationService(commands, &journeyResolverFake{journey: querypostgres.Journey{FromStopIndex: 0, ToStopIndex: 1}}, reader, fixedClock{now}, time.Minute, 6, metrics)
	command := httpapi.CreateReservationCommand{OwnerID: owner.String(), IdempotencyKey: "metric-key", TrainRunID: run.String(), OriginStationCode: "TPE", DestinationStationCode: "KHH", SeatClass: "standard", PassengerIDs: []string{passenger.String()}}
	if _, err := service.CreateHold(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	commands.createResult.Replayed = true
	if _, err := service.CreateHold(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	if metrics.reservation != 1 || metrics.outbox["reservation.held"] != 1 {
		t.Fatalf("metrics = reservations:%d outbox:%v", metrics.reservation, metrics.outbox)
	}
}

func timePointer(value time.Time) *time.Time { return &value }

func TestReservationCreateEnforcesConfiguredPassengerMaximumBeforeQueries(t *testing.T) {
	journeys := &journeyResolverFake{}
	_, err := NewReservationService(&reservationCommandsFake{}, journeys, &reservationReaderFake{}, fixedClock{time.Now()}, time.Minute, 1).CreateHold(context.Background(), httpapi.CreateReservationCommand{PassengerIDs: []string{"one", "two"}})
	if err != httpapi.ErrInvalidInput {
		t.Fatalf("error = %v", err)
	}
	if journeys.input != [3]string{} {
		t.Fatal("journey lookup occurred for rejected command")
	}
}

func TestReservationMutationsHashOperationSpecificFingerprintsAndReturnAuthoritativeView(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	owner, reservationID := uuid.New(), uuid.New()
	commands := &reservationCommandsFake{}
	reader := &reservationReaderFake{detail: ReservationDetail{ID: reservationID.String(), Status: "confirmed", TrainRunID: "run"}}
	service := NewReservationService(commands, &journeyResolverFake{}, reader, fixedClock{now}, time.Minute, 6)
	view, err := service.ConfirmReservation(context.Background(), httpapi.ReservationMutationCommand{OwnerID: owner.String(), ReservationID: reservationID.String(), IdempotencyKey: "confirm-key"})
	if err != nil || view.Status != "confirmed" {
		t.Fatalf("ConfirmReservation() = %#v, %v", view, err)
	}
	wantKey, _ := bookingapp.HashIdempotencyKey("confirm-key")
	wantFingerprint, _ := bookingapp.FingerprintReservationCommand(bookingapp.OperationReservationConfirm, reservationID.String())
	if string(commands.confirmInput.IdempotencyKeyHash) != string(wantKey[:]) || string(commands.confirmInput.RequestFingerprint) != string(wantFingerprint[:]) {
		t.Fatal("confirm idempotency material mismatch")
	}
}

func TestReservationMapsTypedStoreFailuresToSafeSentinels(t *testing.T) {
	tests := []struct{ err, want error }{{bookingpostgres.ErrInsufficientInventory, httpapi.ErrConflict}, {bookingpostgres.ErrIdempotencyConflict, httpapi.ErrConflict}, {bookingpostgres.ErrPassengerConflict, httpapi.ErrConflict}, {bookingpostgres.ErrInvalidState, httpapi.ErrConflict}, {bookingpostgres.ErrNotFound, httpapi.ErrNotFound}, {bookingpostgres.ErrInvalidArgument, httpapi.ErrInvalidInput}, {errors.New("database secret"), httpapi.ErrUnavailable}}
	for _, test := range tests {
		service := NewReservationService(&reservationCommandsFake{err: test.err}, &journeyResolverFake{journey: querypostgres.Journey{FromStopIndex: 0, ToStopIndex: 1}}, &reservationReaderFake{}, fixedClock{time.Now()}, time.Minute, 6)
		_, err := service.CreateHold(context.Background(), httpapi.CreateReservationCommand{OwnerID: uuid.NewString(), IdempotencyKey: "key", TrainRunID: uuid.NewString(), OriginStationCode: "TPE", DestinationStationCode: "KHH", SeatClass: "standard", PassengerIDs: []string{uuid.NewString()}})
		if err != test.want {
			t.Fatalf("error %v mapped to %v, want %v", test.err, err, test.want)
		}
	}
}
