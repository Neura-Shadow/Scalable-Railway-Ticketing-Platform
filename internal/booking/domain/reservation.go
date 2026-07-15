package domain

import (
	"errors"
	"time"
)

type ReservationStatus string

const (
	ReservationStatusHeld      ReservationStatus = "held"
	ReservationStatusConfirmed ReservationStatus = "confirmed"
	ReservationStatusExpired   ReservationStatus = "expired"
	ReservationStatusCancelled ReservationStatus = "cancelled"
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
