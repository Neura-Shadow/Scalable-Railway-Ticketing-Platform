package domain_test

import (
	"errors"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
)

func TestParseTrainRunStatusAcceptsEverySupportedStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  domain.TrainRunStatus
	}{
		{input: " scheduled ", want: domain.TrainRunStatusScheduled},
		{input: "BOARDING", want: domain.TrainRunStatusBoarding},
		{input: "departed", want: domain.TrainRunStatusDeparted},
		{input: "cancelled", want: domain.TrainRunStatusCancelled},
		{input: "completed", want: domain.TrainRunStatusCompleted},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			got, err := domain.ParseTrainRunStatus(tt.input)
			if err != nil {
				t.Fatalf("ParseTrainRunStatus() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("ParseTrainRunStatus() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseTrainRunStatusRejectsUnknownStatus(t *testing.T) {
	t.Parallel()

	if _, err := domain.ParseTrainRunStatus("delayed"); !errors.Is(err, domain.ErrInvalidTrainRunStatus) {
		t.Fatalf("ParseTrainRunStatus() error = %v, want ErrInvalidTrainRunStatus", err)
	}
}

func TestScheduledTrainRunIsBookable(t *testing.T) {
	t.Parallel()

	run, err := domain.NewTrainRun(domain.TrainRunStatusScheduled, 3)
	if err != nil {
		t.Fatalf("NewTrainRun() error = %v", err)
	}
	if !run.IsBookable() {
		t.Fatal("scheduled train run should be bookable")
	}
	if err := run.EnsureBookable(); err != nil {
		t.Fatalf("EnsureBookable() error = %v", err)
	}
	if got, want := run.SegmentCount(), 3; got != want {
		t.Fatalf("SegmentCount() = %d, want %d", got, want)
	}
}

func TestOnlyScheduledTrainRunsAreBookable(t *testing.T) {
	t.Parallel()

	statuses := []domain.TrainRunStatus{
		domain.TrainRunStatusBoarding,
		domain.TrainRunStatusDeparted,
		domain.TrainRunStatusCancelled,
		domain.TrainRunStatusCompleted,
	}
	for _, status := range statuses {
		status := status
		t.Run(status.String(), func(t *testing.T) {
			t.Parallel()

			run, err := domain.NewTrainRun(status, 3)
			if err != nil {
				t.Fatal(err)
			}
			if run.IsBookable() {
				t.Fatalf("%s train run must not be bookable", status)
			}
			if err := run.EnsureBookable(); !errors.Is(err, domain.ErrTrainRunNotBookable) {
				t.Fatalf("EnsureBookable() error = %v, want ErrTrainRunNotBookable", err)
			}
		})
	}
}

func TestNewTrainRunRejectsInvalidStatusAndSegmentCount(t *testing.T) {
	t.Parallel()

	if _, err := domain.NewTrainRun(domain.TrainRunStatus("delayed"), 3); !errors.Is(err, domain.ErrInvalidTrainRunStatus) {
		t.Fatalf("NewTrainRun(invalid status) error = %v, want ErrInvalidTrainRunStatus", err)
	}
	if _, err := domain.NewTrainRun(domain.TrainRunStatusScheduled, 0); !errors.Is(err, domain.ErrInvalidSegmentCount) {
		t.Fatalf("NewTrainRun(zero segments) error = %v, want ErrInvalidSegmentCount", err)
	}
}

func TestTrainRunSegmentCountMustMatchRoute(t *testing.T) {
	t.Parallel()

	route, err := domain.NewRoute("WEST", "Western", []domain.RouteStop{
		mustRouteStop(t, "TPE", 0, 0, 10),
		mustRouteStop(t, "TXG", 1, 80, 85),
		mustRouteStop(t, "KHH", 2, 150, 155),
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := domain.NewTrainRun(domain.TrainRunStatusScheduled, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := run.ValidateRoute(route); !errors.Is(err, domain.ErrSegmentCountMismatch) {
		t.Fatalf("ValidateRoute() error = %v, want ErrSegmentCountMismatch", err)
	}

	matchingRun, err := domain.NewTrainRun(domain.TrainRunStatusScheduled, route.SegmentCount())
	if err != nil {
		t.Fatal(err)
	}
	if err := matchingRun.ValidateRoute(route); err != nil {
		t.Fatalf("ValidateRoute() error = %v", err)
	}
}
