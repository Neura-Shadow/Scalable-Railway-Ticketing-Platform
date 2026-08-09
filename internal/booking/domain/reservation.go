package domain

import (
	"errors"
	"time"
)

type ReservationStatus string

const (
	ReservationStatusHeld           ReservationStatus = "held"
	ReservationStatusConfirmed      ReservationStatus = "confirmed"
	ReservationStatusExpired        ReservationStatus = "expired"
	ReservationStatusCancelled      ReservationStatus = "cancelled"
	ReservationStatusPaymentPending ReservationStatus = "payment_pending"
	ReservationStatusPaymentReview  ReservationStatus = "payment_review"
	ReservationStatusRefundPending  ReservationStatus = "refund_pending"
)

var (
	ErrInvalidReservation           = errors.New("invalid reservation")
	ErrReservationExpired           = errors.New("reservation expired")
	ErrReservationNotExpired        = errors.New("reservation has not expired")
	ErrInvalidReservationTransition = errors.New("invalid reservation transition")
)

type Reservation struct {
	status    ReservationStatus
	expiresAt time.Time
}

func NewHeldReservation(expiresAt time.Time) (*Reservation, error) {
	if expiresAt.IsZero() {
		return nil, ErrInvalidReservation
	}
	return &Reservation{status: ReservationStatusHeld, expiresAt: expiresAt.UTC()}, nil
}

func (r *Reservation) Confirm(now time.Time) (bool, error) {
	if r.status == ReservationStatusConfirmed {
		return false, nil
	}
	if r.status != ReservationStatusHeld {
		return false, ErrInvalidReservationTransition
	}
	if !now.Before(r.expiresAt) {
		return false, ErrReservationExpired
	}
	r.status = ReservationStatusConfirmed
	return true, nil
}

func (r *Reservation) Expire(now time.Time) (bool, error) {
	if r.status == ReservationStatusExpired {
		return false, nil
	}
	if r.status != ReservationStatusHeld {
		return false, ErrInvalidReservationTransition
	}
	if now.Before(r.expiresAt) {
		return false, ErrReservationNotExpired
	}
	r.status = ReservationStatusExpired
	return true, nil
}

func (r *Reservation) Cancel() (bool, error) {
	if r.status == ReservationStatusCancelled {
		return false, nil
	}
	if r.status != ReservationStatusHeld && r.status != ReservationStatusConfirmed {
		return false, ErrInvalidReservationTransition
	}
	r.status = ReservationStatusCancelled
	return true, nil
}

func (r *Reservation) Status() ReservationStatus {
	return r.status
}

func (r *Reservation) BeginPayment(now time.Time) (bool, error) {
	if r.status == ReservationStatusPaymentPending {
		return false, nil
	}
	if r.status != ReservationStatusHeld {
		return false, ErrInvalidReservationTransition
	}
	if !now.Before(r.expiresAt) {
		return false, ErrReservationExpired
	}
	r.status = ReservationStatusPaymentPending
	return true, nil
}

func (r *Reservation) ReviewPayment() (bool, error) {
	if r.status == ReservationStatusPaymentReview {
		return false, nil
	}
	if r.status != ReservationStatusPaymentPending {
		return false, ErrInvalidReservationTransition
	}
	r.status = ReservationStatusPaymentReview
	return true, nil
}

func (r *Reservation) ResumePayment() (bool, error) {
	if r.status == ReservationStatusPaymentPending {
		return false, nil
	}
	if r.status != ReservationStatusPaymentReview {
		return false, ErrInvalidReservationTransition
	}
	r.status = ReservationStatusPaymentPending
	return true, nil
}

func (r *Reservation) ConfirmPayment() (bool, error) {
	if r.status == ReservationStatusConfirmed {
		return false, nil
	}
	if r.status != ReservationStatusPaymentPending && r.status != ReservationStatusPaymentReview {
		return false, ErrInvalidReservationTransition
	}
	r.status = ReservationStatusConfirmed
	return true, nil
}

func (r *Reservation) BeginRefund() (bool, error) {
	if r.status == ReservationStatusRefundPending {
		return false, nil
	}
	if r.status != ReservationStatusConfirmed && r.status != ReservationStatusPaymentReview {
		return false, ErrInvalidReservationTransition
	}
	r.status = ReservationStatusRefundPending
	return true, nil
}

func (r *Reservation) CompleteCompensation() (bool, error) {
	if r.status == ReservationStatusCancelled {
		return false, nil
	}
	if r.status != ReservationStatusRefundPending {
		return false, ErrInvalidReservationTransition
	}
	r.status = ReservationStatusCancelled
	return true, nil
}
