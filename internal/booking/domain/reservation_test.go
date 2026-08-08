package domain_test

import (
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/domain"
)

func TestHeldReservationCanBeConfirmedBeforeExpiry(t *testing.T) {
	t.Parallel()

	expiresAt := time.Date(2026, 7, 15, 12, 10, 0, 0, time.UTC)
	reservation, err := domain.NewHeldReservation(expiresAt)
	if err != nil {
		t.Fatal(err)
	}

	changed, err := reservation.Confirm(expiresAt.Add(-time.Second))
	if err != nil {
		t.Fatalf("Confirm() error = %v", err)
	}
	if !changed {
		t.Fatal("Confirm() changed = false, want true")
	}
	if got, want := reservation.Status(), domain.ReservationStatusConfirmed; got != want {
		t.Fatalf("Status() = %q, want %q", got, want)
	}
}

func TestReservationRejectsInvalidTransitionsAndStabilizesRepeats(t *testing.T) {
	t.Parallel()

	expiresAt := time.Date(2026, 7, 15, 12, 10, 0, 0, time.UTC)

	confirmed, _ := domain.NewHeldReservation(expiresAt)
	if _, err := confirmed.Confirm(expiresAt.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if changed, err := confirmed.Confirm(expiresAt.Add(-time.Second)); err != nil || changed {
		t.Fatalf("repeated Confirm() changed=%t error=%v, want false/nil", changed, err)
	}
	if _, err := confirmed.Expire(expiresAt); !errors.Is(err, domain.ErrInvalidReservationTransition) {
		t.Fatalf("confirmed Expire() error=%v, want ErrInvalidReservationTransition", err)
	}

	expired, _ := domain.NewHeldReservation(expiresAt)
	if _, err := expired.Expire(expiresAt); err != nil {
		t.Fatal(err)
	}
	if _, err := expired.Confirm(expiresAt.Add(time.Second)); !errors.Is(err, domain.ErrInvalidReservationTransition) {
		t.Fatalf("expired Confirm() error=%v, want ErrInvalidReservationTransition", err)
	}
	if _, err := expired.Cancel(); !errors.Is(err, domain.ErrInvalidReservationTransition) {
		t.Fatalf("expired Cancel() error=%v, want ErrInvalidReservationTransition", err)
	}

	cancelled, _ := domain.NewHeldReservation(expiresAt)
	if _, err := cancelled.Cancel(); err != nil {
		t.Fatal(err)
	}
	if changed, err := cancelled.Cancel(); err != nil || changed {
		t.Fatalf("repeated Cancel() changed=%t error=%v, want false/nil", changed, err)
	}
	if _, err := cancelled.Confirm(expiresAt.Add(-time.Second)); !errors.Is(err, domain.ErrInvalidReservationTransition) {
		t.Fatalf("cancelled Confirm() error=%v, want ErrInvalidReservationTransition", err)
	}

	early, _ := domain.NewHeldReservation(expiresAt)
	if _, err := early.Expire(expiresAt.Add(-time.Second)); !errors.Is(err, domain.ErrReservationNotExpired) {
		t.Fatalf("early Expire() error=%v, want ErrReservationNotExpired", err)
	}
	atDeadline, _ := domain.NewHeldReservation(expiresAt)
	if _, err := atDeadline.Confirm(expiresAt); !errors.Is(err, domain.ErrReservationExpired) {
		t.Fatalf("deadline Confirm() error=%v, want ErrReservationExpired", err)
	}
}

func TestHeldReservationExpiresAtDeadline(t *testing.T) {
	t.Parallel()

	expiresAt := time.Date(2026, 7, 15, 12, 10, 0, 0, time.UTC)
	reservation, err := domain.NewHeldReservation(expiresAt)
	if err != nil {
		t.Fatal(err)
	}

	changed, err := reservation.Expire(expiresAt)
	if err != nil {
		t.Fatalf("Expire() error = %v", err)
	}
	if !changed || reservation.Status() != domain.ReservationStatusExpired {
		t.Fatalf("Expire() changed=%t status=%q, want true/%q", changed, reservation.Status(), domain.ReservationStatusExpired)
	}
}

func TestHeldAndConfirmedReservationsCanBeCancelled(t *testing.T) {
	t.Parallel()

	expiresAt := time.Date(2026, 7, 15, 12, 10, 0, 0, time.UTC)
	for _, confirmFirst := range []bool{false, true} {
		reservation, err := domain.NewHeldReservation(expiresAt)
		if err != nil {
			t.Fatal(err)
		}
		if confirmFirst {
			if _, err := reservation.Confirm(expiresAt.Add(-time.Second)); err != nil {
				t.Fatal(err)
			}
		}

		changed, err := reservation.Cancel()
		if err != nil {
			t.Fatalf("Cancel() error = %v", err)
		}
		if !changed || reservation.Status() != domain.ReservationStatusCancelled {
			t.Fatalf("Cancel() changed=%t status=%q, want true/%q", changed, reservation.Status(), domain.ReservationStatusCancelled)
		}
	}
}

func TestPaymentReservationStatesRequireExplicitReconciliationAndCompensation(t *testing.T) {
	t.Parallel()
	expiresAt := time.Date(2026, 8, 5, 12, 10, 0, 0, time.UTC)
	reservation, err := domain.NewHeldReservation(expiresAt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reservation.BeginPayment(expiresAt.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := reservation.Expire(expiresAt.Add(time.Hour)); !errors.Is(err, domain.ErrInvalidReservationTransition) {
		t.Fatalf("payment_pending Expire() error = %v", err)
	}
	if _, err := reservation.ReviewPayment(); err != nil {
		t.Fatal(err)
	}
	if _, err := reservation.Expire(expiresAt.Add(time.Hour)); !errors.Is(err, domain.ErrInvalidReservationTransition) {
		t.Fatalf("payment_review Expire() error = %v", err)
	}
	if _, err := reservation.BeginRefund(); err != nil {
		t.Fatal(err)
	}
	if _, err := reservation.CompleteCompensation(); err != nil {
		t.Fatal(err)
	}
	if reservation.Status() != domain.ReservationStatusCancelled {
		t.Fatalf("Status() = %q", reservation.Status())
	}
}
