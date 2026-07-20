package app

import (
	"context"
	"crypto/sha256"
	"errors"
	"time"

	admissiondomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	admissionpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/postgres"
	admissionredis "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/redis"
	bookingapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/application"
	bookingpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/postgres"
	offeringdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	querypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
)

const idempotencyRecordTTL = 24 * time.Hour

const admissionMutationTimeout = time.Second

type reservationCommands interface {
	CreateHold(context.Context, bookingpostgres.CreateHoldParams) (bookingpostgres.CreateHoldResult, error)
	ConfirmReservation(context.Context, bookingpostgres.ReservationCommandParams) (bookingpostgres.ConfirmReservationResult, error)
	CancelReservation(context.Context, bookingpostgres.ReservationCommandParams) (bookingpostgres.CancelReservationResult, error)
}
type journeyResolver interface {
	ResolveJourney(context.Context, string, string, string) (querypostgres.Journey, error)
}
type completedCreateHoldReader interface {
	LookupCompletedCreateHold(context.Context, bookingpostgres.CompletedCreateHoldLookupParams) (bookingpostgres.CreateHoldResult, bool, error)
}
type reservationPolicyReader interface {
	GetPolicy(context.Context, uuid.UUID, offeringdomain.SeatClass) (admissiondomain.HotTrainPolicy, error)
}
type reservationAdmissionControl interface {
	InspectToken(context.Context, admissionredis.PolicyScope, [sha256.Size]byte) (admissiondomain.TokenDeliveryFields, error)
	Acquire(context.Context, admissionredis.AcquireRequest) (admissionredis.AcquireResult, error)
	Release(context.Context, admissionredis.LeaseMutation, bool) error
	Finalize(context.Context, admissionredis.LeaseMutation) error
}
type committedAdmissionFinalizer interface {
	FinalizeCommitted(context.Context, admissionredis.CommittedMutation) error
}
type admissionTokenVerifier interface {
	Verify(string, admissiondomain.TokenDeliveryFields) error
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

type reservationMetrics interface {
	RecordReservation(operation, result, reason string)
	RecordOutbox(operation, eventType, result, reason string)
}
type reservationAdmissionMetrics interface {
	RecordAdmissionToken(operation, result, reason string)
	RecordReservationQuotaRejected(reason, seatClass string)
	RecordReservationBackpressureRejected(reason, seatClass string)
	RecordHotTrainReservation(result, reason, seatClass string, duration time.Duration)
}

type ReservationService struct {
	commands         reservationCommands
	replays          completedCreateHoldReader
	journeys         journeyResolver
	reader           reservationReader
	policies         reservationPolicyReader
	admission        reservationAdmissionControl
	tokenVerifier    admissionTokenVerifier
	executionSlots   *ExecutionSlots
	clock            appClock
	holdTTL          time.Duration
	databaseTimeout  time.Duration
	maxPassengers    int
	metrics          reservationMetrics
	admissionMetrics reservationAdmissionMetrics
}

func NewReservationService(commands reservationCommands, journeys journeyResolver, reader reservationReader, clock appClock, holdTTL time.Duration, maxPassengers int, metrics ...reservationMetrics) *ReservationService {
	service := &ReservationService{commands: commands, journeys: journeys, reader: reader, clock: clock, holdTTL: holdTTL, maxPassengers: maxPassengers}
	if len(metrics) > 0 {
		service.metrics = metrics[0]
		if admissionMetrics, ok := metrics[0].(reservationAdmissionMetrics); ok {
			service.admissionMetrics = admissionMetrics
		}
	}
	return service
}

func NewAdmissionProtectedReservationService(
	commands reservationCommands,
	replays completedCreateHoldReader,
	journeys journeyResolver,
	reader reservationReader,
	policies reservationPolicyReader,
	admission reservationAdmissionControl,
	tokenVerifier admissionTokenVerifier,
	executionSlots *ExecutionSlots,
	clock appClock,
	holdTTL time.Duration,
	maxPassengers int,
	metrics ...reservationMetrics,
) *ReservationService {
	service := NewReservationService(commands, journeys, reader, clock, holdTTL, maxPassengers, metrics...)
	service.replays = replays
	service.policies = policies
	service.admission = admission
	service.tokenVerifier = tokenVerifier
	service.executionSlots = executionSlots
	return service
}

// WithDatabaseCommandTimeout bounds every reservation transaction. Production
// validation requires this timeout to be strictly shorter than the minimum
// admission processing lease, so an expired lease can never overlap a live
// database transaction from the previous attempt.
func (s *ReservationService) WithDatabaseCommandTimeout(timeout time.Duration) *ReservationService {
	if s != nil && timeout > 0 {
		s.databaseTimeout = timeout
	}
	return s
}

type preparedCreateHold struct {
	owner              uuid.UUID
	run                uuid.UUID
	passengers         []uuid.UUID
	idempotencyKeyHash [sha256.Size]byte
	bookingFingerprint [sha256.Size]byte
}

func (s *ReservationService) CreateHold(ctx context.Context, c httpapi.CreateReservationCommand) (httpapi.ReservationView, error) {
	if s == nil || s.commands == nil || s.journeys == nil || s.holdTTL <= 0 || s.maxPassengers <= 0 || len(c.PassengerIDs) < 1 || len(c.PassengerIDs) > s.maxPassengers {
		return httpapi.ReservationView{}, httpapi.ErrInvalidInput
	}
	prepared, err := prepareCreateHold(c)
	if err != nil {
		return httpapi.ReservationView{}, err
	}
	if replay, found, err := s.lookupCompletedCreateHold(ctx, prepared); err != nil {
		return httpapi.ReservationView{}, err
	} else if found {
		s.repairCommittedAdmission(ctx, c.AdmissionToken, prepared)
		return s.readUUID(ctx, prepared.owner, replay.ReservationID)
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
	input := bookingpostgres.CreateHoldParams{
		UserID: prepared.owner, TrainRunID: prepared.run,
		FromStopIndex: journey.FromStopIndex, ToStopIndex: journey.ToStopIndex,
		SeatClass: c.SeatClass, PassengerIDs: prepared.passengers,
		HoldExpiresAt: expires, IdempotencyKeyHash: append([]byte(nil), prepared.idempotencyKeyHash[:]...),
		RequestFingerprint:   append([]byte(nil), prepared.bookingFingerprint[:]...),
		IdempotencyExpiresAt: now.Add(idempotencyRecordTTL),
	}
	policy, hot, err := s.resolveReservationPolicy(ctx, prepared.run, c.SeatClass)
	if err != nil {
		return httpapi.ReservationView{}, err
	}
	if !hot {
		result, err := s.executeCreateHold(ctx, input, c.SeatClass)
		if err != nil {
			return httpapi.ReservationView{}, err
		}
		return s.completeCreateHold(ctx, prepared.owner, result)
	}
	input.AdmissionPolicy = &bookingpostgres.AdmissionPolicyDecision{
		PolicyID: policy.ID,
		Version:  policy.Version,
	}
	startedAt := now
	result, err := s.createHotHold(ctx, c, prepared, journey, policy, input)
	if err != nil {
		s.recordHotReservation(err, c.SeatClass, startedAt)
		return httpapi.ReservationView{}, err
	}
	s.recordHotReservation(nil, c.SeatClass, startedAt)
	return s.completeCreateHold(ctx, prepared.owner, result)
}

func prepareCreateHold(c httpapi.CreateReservationCommand) (preparedCreateHold, error) {
	owner, err := uuid.Parse(c.OwnerID)
	if err != nil {
		return preparedCreateHold{}, httpapi.ErrInvalidInput
	}
	run, err := uuid.Parse(c.TrainRunID)
	if err != nil {
		return preparedCreateHold{}, httpapi.ErrInvalidInput
	}
	passengers := make([]uuid.UUID, len(c.PassengerIDs))
	for index, id := range c.PassengerIDs {
		passengers[index], err = uuid.Parse(id)
		if err != nil {
			return preparedCreateHold{}, httpapi.ErrInvalidInput
		}
	}
	key, err := bookingapp.HashIdempotencyKey(c.IdempotencyKey)
	if err != nil {
		return preparedCreateHold{}, httpapi.ErrInvalidInput
	}
	fingerprint, err := bookingapp.FingerprintHoldRequest(bookingapp.HoldFingerprintInput{
		TrainRunID: c.TrainRunID, OriginCode: c.OriginStationCode, DestinationCode: c.DestinationStationCode,
		SeatClass: c.SeatClass, PassengerIDs: c.PassengerIDs,
	})
	if err != nil {
		return preparedCreateHold{}, httpapi.ErrInvalidInput
	}
	return preparedCreateHold{
		owner: owner, run: run, passengers: passengers,
		idempotencyKeyHash: key, bookingFingerprint: fingerprint,
	}, nil
}

func (s *ReservationService) lookupCompletedCreateHold(
	ctx context.Context,
	prepared preparedCreateHold,
) (bookingpostgres.CreateHoldResult, bool, error) {
	if s.replays == nil {
		return bookingpostgres.CreateHoldResult{}, false, nil
	}
	result, found, err := s.replays.LookupCompletedCreateHold(ctx, bookingpostgres.CompletedCreateHoldLookupParams{
		UserID: prepared.owner, IdempotencyKeyHash: prepared.idempotencyKeyHash[:],
		RequestFingerprint: prepared.bookingFingerprint[:],
	})
	if err != nil {
		return bookingpostgres.CreateHoldResult{}, false, mapBookingError(err)
	}
	return result, found, nil
}

func (s *ReservationService) resolveReservationPolicy(
	ctx context.Context,
	runID uuid.UUID,
	rawSeatClass string,
) (admissiondomain.HotTrainPolicy, bool, error) {
	if s.policies == nil {
		return admissiondomain.HotTrainPolicy{}, false, nil
	}
	seatClass, err := offeringdomain.ParseSeatClass(rawSeatClass)
	if err != nil {
		return admissiondomain.HotTrainPolicy{}, false, httpapi.ErrInvalidInput
	}
	policy, err := s.policies.GetPolicy(ctx, runID, seatClass)
	if errors.Is(err, admissionpostgres.ErrNotFound) {
		return admissiondomain.HotTrainPolicy{}, false, nil
	}
	if err != nil {
		return admissiondomain.HotTrainPolicy{}, false, httpapi.ErrUnavailable
	}
	return policy, policy.Enabled, nil
}

func (s *ReservationService) executeCreateHold(
	ctx context.Context,
	input bookingpostgres.CreateHoldParams,
	seatClass string,
) (bookingpostgres.CreateHoldResult, error) {
	release, acquired := s.tryExecutionSlot()
	if !acquired {
		if s.admissionMetrics != nil {
			s.admissionMetrics.RecordReservationBackpressureRejected("capacity", seatClass)
		}
		return bookingpostgres.CreateHoldResult{}, httpapi.WithRetryAfter(httpapi.ErrReservationBackpressure, 1)
	}
	defer release()
	databaseCtx, cancel := s.databaseCommandContext(ctx)
	defer cancel()
	result, err := s.commands.CreateHold(databaseCtx, input)
	if err != nil {
		if errors.Is(err, bookingpostgres.ErrReservationQuotaExceeded) && s.admissionMetrics != nil {
			s.admissionMetrics.RecordReservationQuotaRejected("active_hold_limit", seatClass)
		}
		return bookingpostgres.CreateHoldResult{}, mapBookingError(err)
	}
	return result, nil
}

func (s *ReservationService) createHotHold(
	ctx context.Context,
	command httpapi.CreateReservationCommand,
	prepared preparedCreateHold,
	journey querypostgres.Journey,
	policy admissiondomain.HotTrainPolicy,
	input bookingpostgres.CreateHoldParams,
) (bookingpostgres.CreateHoldResult, error) {
	if s.admission == nil || s.tokenVerifier == nil || command.AdmissionToken == "" {
		return bookingpostgres.CreateHoldResult{}, httpapi.ErrAdmissionRequired
	}
	admissionFingerprint, err := admissiondomain.FingerprintAdmissionRequest(admissiondomain.AdmissionFingerprintInput{
		TrainRunID: prepared.run.String(), FromStopIndex: journey.FromStopIndex, ToStopIndex: journey.ToStopIndex,
		SeatClass: command.SeatClass, PassengerCount: len(prepared.passengers),
	})
	if err != nil {
		return bookingpostgres.CreateHoldResult{}, httpapi.ErrInvalidInput
	}
	scope := scopeForWaitingRoomPolicy(policy)
	tokenHash := sha256.Sum256([]byte(command.AdmissionToken))
	fields, err := s.admission.InspectToken(ctx, scope, tokenHash)
	if err != nil {
		s.recordAdmissionTokenError("acquire", err)
		return bookingpostgres.CreateHoldResult{}, mapAdmissionAcquireError(err)
	}
	if err := s.tokenVerifier.Verify(command.AdmissionToken, fields); err != nil {
		if s.admissionMetrics != nil {
			s.admissionMetrics.RecordAdmissionToken("acquire", "conflict", "mac_invalid")
		}
		return bookingpostgres.CreateHoldResult{}, httpapi.ErrAdmissionInvalid
	}
	leaseOwner := uuid.NewString()
	acquired, err := s.admission.Acquire(ctx, admissionredis.AcquireRequest{
		Scope: scope, TokenHash: tokenHash, OwnerHash: hashAdmissionOwner(prepared.owner),
		AdmissionFingerprint: admissionFingerprint, BookingFingerprint: prepared.bookingFingerprint,
		IdempotencyKeyHash: prepared.idempotencyKeyHash, FromStopIndex: journey.FromStopIndex,
		ToStopIndex: journey.ToStopIndex, PassengerCount: len(prepared.passengers),
		LeaseOwner: leaseOwner, ProcessingLease: policy.Limits.ProcessingLease,
	})
	if err != nil {
		s.recordAdmissionTokenError("acquire", err)
		return bookingpostgres.CreateHoldResult{}, mapAdmissionAcquireError(err)
	}
	if s.admissionMetrics != nil {
		s.admissionMetrics.RecordAdmissionToken("acquire", string(acquired.Decision), "none")
	}
	if acquired.Decision == admissiondomain.DecisionRetryAllowed ||
		acquired.Decision == admissiondomain.DecisionReplayAllowed {
		if replay, found, lookupErr := s.lookupCompletedCreateHold(ctx, prepared); lookupErr != nil {
			return bookingpostgres.CreateHoldResult{}, lookupErr
		} else if found {
			s.repairCommittedAdmission(ctx, command.AdmissionToken, prepared)
			return replay, nil
		}
		if acquired.Decision == admissiondomain.DecisionRetryAllowed {
			return bookingpostgres.CreateHoldResult{}, httpapi.WithRetryAfter(
				httpapi.ErrAdmissionInProgress, boundedRetrySeconds(acquired.RetryAfter),
			)
		}
		return bookingpostgres.CreateHoldResult{}, httpapi.ErrUnavailable
	}
	if acquired.Decision != admissiondomain.DecisionAcquired ||
		acquired.LeaseOwner == "" || acquired.LeaseGeneration < 1 {
		return bookingpostgres.CreateHoldResult{}, httpapi.ErrUnavailable
	}
	mutation := admissionredis.LeaseMutation{
		Scope: scope, TokenHash: tokenHash, OwnerHash: hashAdmissionOwner(prepared.owner),
		BookingFingerprint: prepared.bookingFingerprint, IdempotencyKeyHash: prepared.idempotencyKeyHash,
		LeaseOwner: acquired.LeaseOwner, LeaseGeneration: acquired.LeaseGeneration,
	}
	releaseSlot, slotAcquired := s.tryExecutionSlot()
	if !slotAcquired {
		cleanupCtx, cancel := admissionCleanupContext(ctx)
		releaseErr := s.admission.Release(cleanupCtx, mutation, false)
		cancel()
		if s.admissionMetrics != nil {
			s.admissionMetrics.RecordReservationBackpressureRejected("capacity", command.SeatClass)
			if releaseErr != nil {
				s.recordAdmissionTokenError("release", releaseErr)
			} else {
				s.admissionMetrics.RecordAdmissionToken("release", "released", "backpressure")
			}
		}
		return bookingpostgres.CreateHoldResult{}, httpapi.WithRetryAfter(httpapi.ErrReservationBackpressure, 1)
	}
	defer releaseSlot()
	databaseCtx, cancelDatabase := s.databaseCommandContext(ctx)
	result, bookingErr := s.commands.CreateHold(databaseCtx, input)
	cancelDatabase()
	if bookingErr != nil {
		permanent := permanentBookingFailure(bookingErr)
		cleanupCtx, cancel := admissionCleanupContext(ctx)
		releaseErr := s.admission.Release(cleanupCtx, mutation, permanent)
		cancel()
		if errors.Is(bookingErr, bookingpostgres.ErrReservationQuotaExceeded) && s.admissionMetrics != nil {
			s.admissionMetrics.RecordReservationQuotaRejected("active_hold_limit", command.SeatClass)
		}
		if s.admissionMetrics != nil {
			if releaseErr != nil {
				s.recordAdmissionTokenError("release", releaseErr)
			} else {
				s.admissionMetrics.RecordAdmissionToken("release", "released", bookingFailureReason(bookingErr))
			}
		}
		return bookingpostgres.CreateHoldResult{}, mapBookingError(bookingErr)
	}
	// PostgreSQL has committed. Redis finalization is best-effort here: the
	// replay-first durable lookup above prevents another create even if this
	// mutation is interrupted or Redis is unavailable.
	cleanupCtx, cancelFinalize := admissionCleanupContext(ctx)
	finalizeErr := s.admission.Finalize(cleanupCtx, mutation)
	cancelFinalize()
	if s.admissionMetrics != nil {
		if finalizeErr != nil {
			s.admissionMetrics.RecordAdmissionToken("consume", "failure", "backend")
		} else {
			s.admissionMetrics.RecordAdmissionToken("consume", "consumed", "none")
		}
	}
	return result, nil
}

func (s *ReservationService) databaseCommandContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if s == nil || s.databaseTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, s.databaseTimeout)
}

func admissionCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), admissionMutationTimeout)
}

func (s *ReservationService) repairCommittedAdmission(
	ctx context.Context,
	rawToken string,
	prepared preparedCreateHold,
) {
	finalizer, ok := s.admission.(committedAdmissionFinalizer)
	if !ok || rawToken == "" {
		return
	}
	cleanupCtx, cancel := admissionCleanupContext(ctx)
	err := finalizer.FinalizeCommitted(cleanupCtx, admissionredis.CommittedMutation{
		TokenHash:          sha256.Sum256([]byte(rawToken)),
		OwnerHash:          hashAdmissionOwner(prepared.owner),
		BookingFingerprint: prepared.bookingFingerprint,
		IdempotencyKeyHash: prepared.idempotencyKeyHash,
	})
	cancel()
	if err != nil {
		s.recordAdmissionTokenError("consume", err)
	}
}

func (s *ReservationService) completeCreateHold(
	ctx context.Context,
	owner uuid.UUID,
	result bookingpostgres.CreateHoldResult,
) (httpapi.ReservationView, error) {
	if s.metrics != nil && !result.Replayed {
		s.metrics.RecordReservation("hold", "success", "none")
		s.metrics.RecordOutbox("create", "reservation.held", "success", "none")
	}
	return s.readUUID(ctx, owner, result.ReservationID)
}

func (s *ReservationService) tryExecutionSlot() (func(), bool) {
	if s.executionSlots == nil {
		return func() {}, true
	}
	return s.executionSlots.TryAcquire()
}

func boundedRetrySeconds(duration time.Duration) int {
	seconds := int((duration + time.Second - 1) / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	if seconds > 60 {
		seconds = 60
	}
	return seconds
}

func permanentBookingFailure(err error) bool {
	return errors.Is(err, bookingpostgres.ErrInsufficientInventory) ||
		errors.Is(err, bookingpostgres.ErrReservationQuotaExceeded) ||
		errors.Is(err, bookingpostgres.ErrNotBookable) ||
		errors.Is(err, bookingpostgres.ErrPassengerConflict) ||
		errors.Is(err, bookingpostgres.ErrInvalidState) ||
		errors.Is(err, bookingpostgres.ErrIdempotencyConflict) ||
		errors.Is(err, bookingpostgres.ErrAdmissionPolicyChanged) ||
		errors.Is(err, bookingpostgres.ErrInvalidArgument) ||
		errors.Is(err, bookingpostgres.ErrNotFound)
}

func bookingFailureReason(err error) string {
	switch {
	case errors.Is(err, bookingpostgres.ErrInsufficientInventory):
		return "no_inventory"
	case errors.Is(err, bookingpostgres.ErrReservationQuotaExceeded):
		return "quota"
	case errors.Is(err, bookingpostgres.ErrPassengerConflict):
		return "conflict"
	case errors.Is(err, bookingpostgres.ErrIdempotencyConflict):
		return "conflict"
	case errors.Is(err, bookingpostgres.ErrAdmissionRequired),
		errors.Is(err, bookingpostgres.ErrAdmissionPolicyChanged):
		return "policy_version"
	default:
		return "database"
	}
}

func admissionTokenFailure(err error) (result, reason string) {
	switch {
	case errors.Is(err, admissionredis.ErrTerminal):
		return "expired", "token_expired"
	case errors.Is(err, admissionredis.ErrTokenMismatch),
		errors.Is(err, admissionredis.ErrOwnerMismatch),
		errors.Is(err, admissionredis.ErrNotFound):
		return "conflict", "binding_conflict"
	case errors.Is(err, admissionredis.ErrPolicyMismatch):
		return "failure", "policy_version"
	case errors.Is(err, admissionredis.ErrContinuityLost):
		return "failure", "continuity_lost"
	case errors.Is(err, admissionredis.ErrBackendUnavailable):
		return "failure", "redis"
	case errors.Is(err, admissionredis.ErrInvalidInput):
		return "failure", "invalid_request"
	default:
		return "failure", "unknown"
	}
}

func (s *ReservationService) recordAdmissionTokenError(operation string, err error) {
	if s.admissionMetrics == nil {
		return
	}
	result, reason := admissionTokenFailure(err)
	s.admissionMetrics.RecordAdmissionToken(operation, result, reason)
}

func mapAdmissionAcquireError(err error) error {
	switch {
	case errors.Is(err, admissionredis.ErrTerminal):
		return httpapi.ErrAdmissionExpired
	case errors.Is(err, admissionredis.ErrTokenMismatch),
		errors.Is(err, admissionredis.ErrOwnerMismatch),
		errors.Is(err, admissionredis.ErrNotFound):
		return httpapi.ErrAdmissionInvalid
	case errors.Is(err, admissionredis.ErrInvalidInput):
		return httpapi.ErrInvalidInput
	case errors.Is(err, admissionredis.ErrBackendUnavailable),
		errors.Is(err, admissionredis.ErrPolicyMismatch),
		errors.Is(err, admissionredis.ErrContinuityLost):
		return httpapi.WithRetryAfter(httpapi.ErrUnavailable, 1)
	default:
		return httpapi.ErrUnavailable
	}
}

func (s *ReservationService) recordHotReservation(err error, seatClass string, startedAt time.Time) {
	if s.admissionMetrics == nil {
		return
	}
	duration := time.Duration(0)
	if finishedAt := s.now(); !startedAt.IsZero() && !finishedAt.IsZero() {
		duration = finishedAt.Sub(startedAt)
	}
	result, reason := "success", "none"
	if err != nil {
		result, reason = "failure", hotReservationFailureReason(err)
		if errors.Is(err, httpapi.ErrConflict) {
			result = "conflict"
		}
	}
	s.admissionMetrics.RecordHotTrainReservation(result, reason, seatClass, duration)
}

func hotReservationFailureReason(err error) string {
	switch {
	case errors.Is(err, httpapi.ErrAdmissionRequired):
		return "admission_required"
	case errors.Is(err, httpapi.ErrAdmissionInvalid):
		return "token_invalid"
	case errors.Is(err, httpapi.ErrAdmissionExpired):
		return "token_expired"
	case errors.Is(err, httpapi.ErrAdmissionInProgress):
		return "processing"
	case errors.Is(err, httpapi.ErrReservationQuotaExceeded):
		return "quota"
	case errors.Is(err, httpapi.ErrReservationBackpressure):
		return "backpressure"
	case errors.Is(err, httpapi.ErrConflict):
		return "conflict"
	case errors.Is(err, httpapi.ErrInvalidInput):
		return "invalid_request"
	case errors.Is(err, httpapi.ErrUnavailable):
		return "unavailable"
	default:
		return "database"
	}
}

func (s *ReservationService) GetReservation(ctx context.Context, ownerID, reservationID string) (httpapi.ReservationView, error) {
	return s.read(ctx, ownerID, reservationID)
}
func (s *ReservationService) ConfirmReservation(ctx context.Context, c httpapi.ReservationMutationCommand) (httpapi.ReservationView, error) {
	owner, reservation, input, err := s.mutation(c, bookingapp.OperationReservationConfirm)
	if err != nil {
		return httpapi.ReservationView{}, err
	}
	result, err := s.commands.ConfirmReservation(ctx, input)
	if err != nil {
		return httpapi.ReservationView{}, mapBookingError(err)
	}
	if s.metrics != nil && !result.Replayed {
		s.metrics.RecordReservation("confirm", "success", "none")
		s.metrics.RecordOutbox("create", "reservation.confirmed", "success", "none")
		for range result.TicketCount {
			s.metrics.RecordOutbox("create", "ticket.created", "success", "none")
		}
	}
	return s.readUUID(ctx, owner, reservation)
}
func (s *ReservationService) CancelReservation(ctx context.Context, c httpapi.ReservationMutationCommand) (httpapi.ReservationView, error) {
	owner, reservation, input, err := s.mutation(c, bookingapp.OperationReservationCancel)
	if err != nil {
		return httpapi.ReservationView{}, err
	}
	result, err := s.commands.CancelReservation(ctx, input)
	if err != nil {
		return httpapi.ReservationView{}, mapBookingError(err)
	}
	if s.metrics != nil && !result.Replayed {
		s.metrics.RecordReservation("cancel", "success", "none")
		s.metrics.RecordOutbox("create", "reservation.cancelled", "success", "none")
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
	case errors.Is(err, bookingpostgres.ErrReservationQuotaExceeded):
		return httpapi.WithRetryAfter(httpapi.ErrReservationQuotaExceeded, 60)
	case errors.Is(err, bookingpostgres.ErrAdmissionRequired):
		return httpapi.ErrAdmissionRequired
	case errors.Is(err, bookingpostgres.ErrAdmissionPolicyChanged):
		return httpapi.ErrAdmissionExpired
	case errors.Is(err, bookingpostgres.ErrInsufficientInventory), errors.Is(err, bookingpostgres.ErrNotBookable), errors.Is(err, bookingpostgres.ErrReservationExpired), errors.Is(err, bookingpostgres.ErrPassengerConflict), errors.Is(err, bookingpostgres.ErrInvalidState), errors.Is(err, bookingpostgres.ErrIdempotencyConflict), errors.Is(err, bookingpostgres.ErrIdempotencyInProgress):
		return httpapi.ErrConflict
	default:
		return httpapi.ErrUnavailable
	}
}

var _ httpapi.ReservationService = (*ReservationService)(nil)
