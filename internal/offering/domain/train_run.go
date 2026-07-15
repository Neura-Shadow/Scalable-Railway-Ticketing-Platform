package domain

import (
	"errors"
	"strings"
)

var (
	ErrInvalidTrainRunStatus     = errors.New("invalid train run status")
	ErrInvalidSegmentCount       = errors.New("invalid train run segment count")
	ErrSegmentCountMismatch      = errors.New("train run segment count does not match route")
	ErrTrainRunNotBookable       = errors.New("train run is not bookable")
	ErrInvalidTrainRunTransition = errors.New("invalid train run status transition")
)

type TrainRunStatus string

const (
	TrainRunStatusScheduled TrainRunStatus = "scheduled"
	TrainRunStatusBoarding  TrainRunStatus = "boarding"
	TrainRunStatusDeparted  TrainRunStatus = "departed"
	TrainRunStatusCancelled TrainRunStatus = "cancelled"
	TrainRunStatusCompleted TrainRunStatus = "completed"
)

func ParseTrainRunStatus(input string) (TrainRunStatus, error) {
	status := TrainRunStatus(strings.ToLower(strings.TrimSpace(input)))
	if !status.IsValid() {
		return "", ErrInvalidTrainRunStatus
	}
	return status, nil
}

func (s TrainRunStatus) IsValid() bool {
	switch s {
	case TrainRunStatusScheduled,
		TrainRunStatusBoarding,
		TrainRunStatusDeparted,
		TrainRunStatusCancelled,
		TrainRunStatusCompleted:
		return true
	default:
		return false
	}
}

func (s TrainRunStatus) String() string {
	return string(s)
}

// ValidateTrainRunTransition keeps terminal runs terminal and makes repeated
// operator commands idempotent.
func ValidateTrainRunTransition(current, next TrainRunStatus) error {
	if !current.IsValid() || !next.IsValid() {
		return ErrInvalidTrainRunStatus
	}
	if current == next {
		return nil
	}
	switch current {
	case TrainRunStatusScheduled:
		if next == TrainRunStatusBoarding || next == TrainRunStatusCancelled {
			return nil
		}
	case TrainRunStatusBoarding:
		if next == TrainRunStatusDeparted || next == TrainRunStatusCancelled {
			return nil
		}
	case TrainRunStatusDeparted:
		if next == TrainRunStatusCompleted {
			return nil
		}
	}
	return ErrInvalidTrainRunTransition
}

type TrainRun struct {
	status       TrainRunStatus
	segmentCount int
}

func NewTrainRun(status TrainRunStatus, segmentCount int) (TrainRun, error) {
	if !status.IsValid() {
		return TrainRun{}, ErrInvalidTrainRunStatus
	}
	if segmentCount <= 0 {
		return TrainRun{}, ErrInvalidSegmentCount
	}
	return TrainRun{status: status, segmentCount: segmentCount}, nil
}

func (r TrainRun) Status() TrainRunStatus {
	return r.status
}

func (r TrainRun) SegmentCount() int {
	return r.segmentCount
}

func (r TrainRun) IsBookable() bool {
	return r.status == TrainRunStatusScheduled
}

func (r TrainRun) EnsureBookable() error {
	if !r.IsBookable() {
		return ErrTrainRunNotBookable
	}
	return nil
}

func (r TrainRun) ValidateRoute(route Route) error {
	if route.SegmentCount() <= 0 {
		return ErrInvalidRoute
	}
	if r.segmentCount != route.SegmentCount() {
		return ErrSegmentCountMismatch
	}
	return nil
}
