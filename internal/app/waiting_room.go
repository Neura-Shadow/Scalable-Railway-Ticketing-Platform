package app

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"time"

	admissiondomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	admissionpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/postgres"
	admissionredis "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/redis"
	offeringdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	querypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
)

var admissionOwnerHashDomain = []byte("railway-admission-owner/v1\x00")

const waitingRoomLocatorMargin = 5 * time.Minute

type waitingRoomPolicyReader interface {
	GetPolicy(context.Context, uuid.UUID, offeringdomain.SeatClass) (admissiondomain.HotTrainPolicy, error)
}

type waitingRoomControl interface {
	Join(context.Context, admissionredis.JoinRequest) (admissiondomain.WaitingRoomEntry, bool, error)
	PutEntryLocator(context.Context, string, admissionredis.PolicyScope, time.Duration) error
	ResolveEntryLocator(context.Context, string) (admissionredis.PolicyScope, error)
	Get(context.Context, admissionredis.PolicyScope, string, [sha256.Size]byte) (admissiondomain.WaitingRoomEntry, error)
	InspectDelivery(context.Context, admissionredis.PolicyScope, string, [sha256.Size]byte) (admissiondomain.TokenDeliveryFields, error)
	ClaimDelivery(context.Context, admissionredis.PolicyScope, string, [sha256.Size]byte) (admissiondomain.TokenDeliveryFields, error)
	Cancel(context.Context, admissionredis.PolicyScope, string, [sha256.Size]byte) (admissionredis.CancelResult, error)
}

type tokenReconstructor interface {
	Reconstruct(admissiondomain.TokenDeliveryFields) (string, error)
}

type waitingRoomMetrics interface {
	RecordWaitingRoomJoin(result, reason, seatClass string)
	RecordWaitingRoomCancel(result, reason, seatClass string)
}

type WaitingRoomService struct {
	policies waitingRoomPolicyReader
	journeys journeyResolver
	control  waitingRoomControl
	tokens   tokenReconstructor
	metrics  waitingRoomMetrics
}

func NewWaitingRoomService(
	policies waitingRoomPolicyReader,
	journeys journeyResolver,
	control waitingRoomControl,
	tokens tokenReconstructor,
	metrics ...waitingRoomMetrics,
) *WaitingRoomService {
	service := &WaitingRoomService{policies: policies, journeys: journeys, control: control, tokens: tokens}
	if len(metrics) > 0 {
		service.metrics = metrics[0]
	}
	return service
}

func (s *WaitingRoomService) JoinWaitingRoom(ctx context.Context, command httpapi.JoinWaitingRoomCommand) (httpapi.WaitingRoomEntryView, error) {
	if s == nil || s.policies == nil || s.journeys == nil || s.control == nil || s.tokens == nil ||
		command.PassengerCount < 1 || command.PassengerCount > admissiondomain.MaxAdmissionPassengers {
		return httpapi.WaitingRoomEntryView{}, httpapi.ErrInvalidInput
	}
	ownerID, err := uuid.Parse(command.OwnerID)
	if err != nil {
		return httpapi.WaitingRoomEntryView{}, httpapi.ErrInvalidInput
	}
	trainRunID, err := uuid.Parse(command.TrainRunID)
	if err != nil {
		return httpapi.WaitingRoomEntryView{}, httpapi.ErrInvalidInput
	}
	seatClass, err := offeringdomain.ParseSeatClass(command.SeatClass)
	if err != nil {
		return httpapi.WaitingRoomEntryView{}, httpapi.ErrInvalidInput
	}
	policy, err := s.policies.GetPolicy(ctx, trainRunID, seatClass)
	if err != nil {
		return httpapi.WaitingRoomEntryView{}, mapWaitingRoomPolicyError(err)
	}
	if !policy.Enabled {
		return httpapi.WaitingRoomEntryView{}, httpapi.ErrNotFound
	}
	journey, err := s.journeys.ResolveJourney(
		ctx, trainRunID.String(), command.OriginStationCode, command.DestinationStationCode,
	)
	if err != nil {
		return httpapi.WaitingRoomEntryView{}, mapQueryError(err)
	}
	fingerprint, err := admissiondomain.FingerprintAdmissionRequest(admissiondomain.AdmissionFingerprintInput{
		TrainRunID: trainRunID.String(), FromStopIndex: journey.FromStopIndex, ToStopIndex: journey.ToStopIndex,
		SeatClass: seatClass.String(), PassengerCount: command.PassengerCount,
	})
	if err != nil {
		return httpapi.WaitingRoomEntryView{}, httpapi.ErrInvalidInput
	}
	ownerHash := hashAdmissionOwner(ownerID)
	scope := scopeForWaitingRoomPolicy(policy)
	entry, duplicate, err := s.control.Join(ctx, admissionredis.JoinRequest{
		Scope: scope, EntryID: uuid.NewString(), OwnerHash: ownerHash, AdmissionFingerprint: fingerprint,
		FromStopIndex: journey.FromStopIndex, ToStopIndex: journey.ToStopIndex,
		PassengerCount: command.PassengerCount, MaxQueueSize: policy.Limits.MaxQueueSize,
		EntryTTL: policy.Limits.QueueEntryTTL,
	})
	if err != nil {
		s.recordJoinFailure(err, seatClass.String())
		return httpapi.WaitingRoomEntryView{}, mapWaitingRoomError(err)
	}
	if err := s.control.PutEntryLocator(ctx, entry.ID, scope, policy.Limits.QueueEntryTTL+waitingRoomLocatorMargin); err != nil {
		s.recordJoinFailure(err, seatClass.String())
		return httpapi.WaitingRoomEntryView{}, mapWaitingRoomError(err)
	}
	if s.metrics != nil {
		result := "success"
		if duplicate {
			result = "duplicate"
		}
		s.metrics.RecordWaitingRoomJoin(result, "none", seatClass.String())
	}
	return s.entryViewWithDelivery(ctx, scope, ownerHash, entry, policy.Limits.AdmissionRatePerSecond)
}

func (s *WaitingRoomService) GetWaitingRoomEntry(ctx context.Context, ownerID, entryID string) (httpapi.WaitingRoomEntryView, error) {
	ownerHash, scope, entry, err := s.locatedOwnedEntry(ctx, ownerID, entryID)
	if err != nil {
		return httpapi.WaitingRoomEntryView{}, err
	}
	return s.entryViewWithDelivery(ctx, scope, ownerHash, entry, 1)
}

func (s *WaitingRoomService) CancelWaitingRoomEntry(ctx context.Context, ownerID, entryID string) (httpapi.WaitingRoomEntryView, error) {
	ownerHash, scope, entry, err := s.locatedOwnedEntry(ctx, ownerID, entryID)
	if err != nil {
		return httpapi.WaitingRoomEntryView{}, err
	}
	cancelled, err := s.control.Cancel(ctx, scope, entry.ID, ownerHash)
	if err != nil {
		if s.metrics != nil {
			s.metrics.RecordWaitingRoomCancel("failure", waitingRoomReason(err), scope.SeatClass)
		}
		return httpapi.WaitingRoomEntryView{}, mapWaitingRoomError(err)
	}
	// Queued cancellations physically remove policy state and their exact
	// locator. Issued cancellations retain a bounded token tombstone. In both
	// cases the pre-cancel read is the authoritative response payload.
	entry.Status = admissiondomain.EntryCancelled
	entry.Position.Approximate = 0
	if s.metrics != nil {
		reason := "none"
		if cancelled.LocatorCleanupPending {
			reason = "locator_cleanup_pending"
		}
		s.metrics.RecordWaitingRoomCancel("success", reason, scope.SeatClass)
	}
	return waitingRoomEntryView(entry, 0), nil
}

func (s *WaitingRoomService) locatedOwnedEntry(ctx context.Context, rawOwnerID, entryID string) ([sha256.Size]byte, admissionredis.PolicyScope, admissiondomain.WaitingRoomEntry, error) {
	var emptyHash [sha256.Size]byte
	if s == nil || s.control == nil || s.tokens == nil {
		return emptyHash, admissionredis.PolicyScope{}, admissiondomain.WaitingRoomEntry{}, httpapi.ErrUnavailable
	}
	ownerID, err := uuid.Parse(rawOwnerID)
	if err != nil {
		return emptyHash, admissionredis.PolicyScope{}, admissiondomain.WaitingRoomEntry{}, httpapi.ErrInvalidInput
	}
	if _, err := uuid.Parse(entryID); err != nil {
		return emptyHash, admissionredis.PolicyScope{}, admissiondomain.WaitingRoomEntry{}, httpapi.ErrInvalidInput
	}
	scope, err := s.control.ResolveEntryLocator(ctx, entryID)
	if err != nil {
		return emptyHash, admissionredis.PolicyScope{}, admissiondomain.WaitingRoomEntry{}, mapWaitingRoomError(err)
	}
	ownerHash := hashAdmissionOwner(ownerID)
	entry, err := s.control.Get(ctx, scope, entryID, ownerHash)
	if err != nil {
		return emptyHash, admissionredis.PolicyScope{}, admissiondomain.WaitingRoomEntry{}, mapWaitingRoomError(err)
	}
	return ownerHash, scope, entry, nil
}

func (s *WaitingRoomService) entryViewWithDelivery(
	ctx context.Context,
	scope admissionredis.PolicyScope,
	ownerHash [sha256.Size]byte,
	entry admissiondomain.WaitingRoomEntry,
	rate int,
) (httpapi.WaitingRoomEntryView, error) {
	view := waitingRoomEntryView(entry, retryAfterFor(entry, rate))
	if entry.Status != admissiondomain.EntryAdmitted {
		return view, nil
	}
	preflightFields, err := s.control.InspectDelivery(ctx, scope, entry.ID, ownerHash)
	if errors.Is(err, admissionredis.ErrNotFound) {
		// The credential was already delivered. Status remains observable, but
		// at-most-once delivery never reconstructs it again.
		return view, nil
	}
	if errors.Is(err, admissionredis.ErrTerminal) {
		return httpapi.WaitingRoomEntryView{}, httpapi.ErrAdmissionExpired
	}
	if err != nil {
		return httpapi.WaitingRoomEntryView{}, mapWaitingRoomError(err)
	}
	rawToken, err := s.tokens.Reconstruct(preflightFields)
	if err != nil || rawToken == "" {
		return httpapi.WaitingRoomEntryView{}, httpapi.ErrUnavailable
	}
	claimedFields, err := s.control.ClaimDelivery(ctx, scope, entry.ID, ownerHash)
	if errors.Is(err, admissionredis.ErrNotFound) {
		// A concurrent response claimed delivery after our read-only preflight.
		return view, nil
	}
	if errors.Is(err, admissionredis.ErrTerminal) {
		return httpapi.WaitingRoomEntryView{}, httpapi.ErrAdmissionExpired
	}
	if err != nil {
		return httpapi.WaitingRoomEntryView{}, mapWaitingRoomError(err)
	}
	claimedRaw, err := s.tokens.Reconstruct(claimedFields)
	if err != nil || claimedRaw == "" ||
		subtle.ConstantTimeCompare([]byte(rawToken), []byte(claimedRaw)) != 1 {
		return httpapi.WaitingRoomEntryView{}, httpapi.ErrUnavailable
	}
	view.AdmissionToken = claimedRaw
	return view, nil
}

func waitingRoomEntryView(entry admissiondomain.WaitingRoomEntry, retryAfter int) httpapi.WaitingRoomEntryView {
	return httpapi.WaitingRoomEntryView{
		EntryID: entry.ID, Status: string(entry.Status), JoinedAt: entry.JoinedAt, ExpiresAt: entry.ExpiresAt,
		AdmittedAt: entry.AdmittedAt, ApproximatePosition: entry.Position.Approximate,
		RetryAfterSeconds: retryAfter,
	}
}

func retryAfterFor(entry admissiondomain.WaitingRoomEntry, rate int) int {
	if entry.Status != admissiondomain.EntryQueued {
		return 0
	}
	if rate < 1 {
		rate = 1
	}
	position := entry.Position.Approximate
	if position < 1 {
		position = 1
	}
	seconds := int((position + int64(rate) - 1) / int64(rate))
	if seconds < 1 {
		seconds = 1
	}
	if seconds > 60 {
		seconds = 60
	}
	return seconds
}

func hashAdmissionOwner(ownerID uuid.UUID) [sha256.Size]byte {
	input := make([]byte, 0, len(admissionOwnerHashDomain)+36)
	input = append(input, admissionOwnerHashDomain...)
	input = append(input, ownerID.String()...)
	return sha256.Sum256(input)
}

func scopeForWaitingRoomPolicy(policy admissiondomain.HotTrainPolicy) admissionredis.PolicyScope {
	return admissionredis.PolicyScope{
		PolicyID: policy.ID.String(), TrainRunID: policy.TrainRunID.String(),
		SeatClass: policy.SeatClass.String(), Version: policy.Version,
	}
}

func mapWaitingRoomPolicyError(err error) error {
	if errors.Is(err, admissionpostgres.ErrNotFound) {
		return httpapi.ErrNotFound
	}
	if errors.Is(err, admissionpostgres.ErrInvalidInput) {
		return httpapi.ErrInvalidInput
	}
	return httpapi.ErrUnavailable
}

func mapWaitingRoomError(err error) error {
	switch {
	case errors.Is(err, admissionredis.ErrInvalidInput):
		return httpapi.ErrInvalidInput
	case errors.Is(err, admissionredis.ErrQueueFull):
		return httpapi.WithRetryAfter(httpapi.ErrQueueFull, 5)
	case errors.Is(err, admissionredis.ErrJoinConflict), errors.Is(err, admissionredis.ErrTokenMismatch):
		return httpapi.ErrConflict
	case errors.Is(err, admissionredis.ErrInProgress):
		return httpapi.ErrAdmissionInProgress
	case errors.Is(err, admissionredis.ErrNotFound), errors.Is(err, admissionredis.ErrOwnerMismatch):
		return httpapi.ErrNotFound
	case errors.Is(err, admissionredis.ErrTerminal):
		return httpapi.ErrConflict
	case errors.Is(err, admissionredis.ErrBackendUnavailable),
		errors.Is(err, admissionredis.ErrPolicyMismatch),
		errors.Is(err, admissionredis.ErrContinuityLost):
		return httpapi.WithRetryAfter(httpapi.ErrUnavailable, 1)
	default:
		return httpapi.ErrUnavailable
	}
}

func waitingRoomReason(err error) string {
	switch {
	case errors.Is(err, admissionredis.ErrQueueFull):
		return "queue_full"
	case errors.Is(err, admissionredis.ErrJoinConflict):
		return "conflict"
	case errors.Is(err, admissionredis.ErrContinuityLost):
		return "continuity"
	case errors.Is(err, admissionredis.ErrBackendUnavailable):
		return "backend"
	case errors.Is(err, admissionredis.ErrInProgress):
		return "processing"
	default:
		return "unknown"
	}
}

func (s *WaitingRoomService) recordJoinFailure(err error, seatClass string) {
	if s.metrics != nil {
		s.metrics.RecordWaitingRoomJoin("failure", waitingRoomReason(err), seatClass)
	}
}

var (
	_ httpapi.WaitingRoomService = (*WaitingRoomService)(nil)
	_ journeyResolver            = (*querypostgres.Store)(nil)
)
