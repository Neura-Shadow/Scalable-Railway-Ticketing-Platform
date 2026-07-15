package application_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/eventrelay/application"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/eventrelay/domain"
	"github.com/google/uuid"
)

func TestWorkerPublishesOutsideClaimAndFinalizesEachEvent(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 15, 1, 2, 3, 0, time.UTC)
	store := &fakeStore{events: []domain.Event{{ID: uuid.New(), EventType: "reservation.held", Attempts: 1}}}
	publisher := &fakePublisher{store: store}
	worker := newWorker(t, store, publisher, fixedClock{now})

	result, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Claimed != 1 || result.Published != 1 || store.published != 1 {
		t.Fatalf("result = %+v, finalized = %d", result, store.published)
	}
	if publisher.publishedDuringClaim {
		t.Fatal("publisher ran while claim transaction was active")
	}
}

func TestWorkerRecordsSuccessfulClaimForEachClaimedEvent(t *testing.T) {
	now := time.Now().UTC()
	store := &fakeStore{events: []domain.Event{
		{ID: uuid.New(), EventType: "reservation.held", Attempts: 1},
		{ID: uuid.New(), EventType: "ticket.created", Attempts: 1},
	}}
	metrics := &metricsSpy{}
	worker, err := application.NewWorker(store, &fakePublisher{store: store}, fixedClock{now}, metrics, application.Config{
		WorkerID: "worker-1", BatchSize: 10, MaxAttempts: 5,
		ProcessingTimeout: time.Minute, RetryBase: time.Second, RetryMax: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if metrics.claimSuccess != 2 {
		t.Fatalf("successful claim metrics = %d, want 2", metrics.claimSuccess)
	}
}

func TestWorkerContinuesAfterPublishFailureAndSchedulesRetry(t *testing.T) {
	t.Parallel()

	first := uuid.New()
	second := uuid.New()
	store := &fakeStore{events: []domain.Event{{ID: first, EventType: "reservation.held", Attempts: 1}, {ID: second, EventType: "reservation.confirmed", Attempts: 1}}}
	publisher := &fakePublisher{store: store, fail: map[uuid.UUID]error{first: errors.New("offline")}}
	worker := newWorker(t, store, publisher, fixedClock{time.Now()})

	result, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Retried != 1 || result.Published != 1 || store.failed != 1 || store.published != 1 {
		t.Fatalf("result = %+v store=%+v", result, store)
	}
}

func TestWorkerDeadLettersAtBoundedAttemptCount(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	store := &fakeStore{events: []domain.Event{{ID: id, EventType: "reservation.held", Attempts: 5}}}
	publisher := &fakePublisher{store: store, fail: map[uuid.UUID]error{id: errors.New("offline")}}
	worker := newWorker(t, store, publisher, fixedClock{time.Now()})

	result, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.DeadLetter != 1 || !store.deadLetter {
		t.Fatalf("result = %+v deadLetter=%t", result, store.deadLetter)
	}
}

func newWorker(t *testing.T, store *fakeStore, publisher *fakePublisher, clock fixedClock) *application.Worker {
	t.Helper()
	worker, err := application.NewWorker(store, publisher, clock, nil, application.Config{
		WorkerID: "worker-1", BatchSize: 10, MaxAttempts: 5,
		ProcessingTimeout: time.Minute, RetryBase: time.Second, RetryMax: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type fakeStore struct {
	events     []domain.Event
	claiming   bool
	published  int
	failed     int
	deadLetter bool
}

func (s *fakeStore) Claim(_ context.Context, _ string, _ int, _, _ time.Time) ([]domain.Event, error) {
	s.claiming = true
	result := append([]domain.Event(nil), s.events...)
	s.claiming = false
	return result, nil
}
func (s *fakeStore) MarkPublished(_ context.Context, _ uuid.UUID, _ string, _ time.Time) error {
	s.published++
	return nil
}
func (s *fakeStore) MarkFailed(_ context.Context, _ uuid.UUID, _ string, _ time.Time, deadLetter bool) error {
	s.failed++
	s.deadLetter = deadLetter
	return nil
}

type fakePublisher struct {
	store                *fakeStore
	fail                 map[uuid.UUID]error
	publishedDuringClaim bool
}

type metricsSpy struct{ claimSuccess int }

func (s *metricsSpy) RecordOutbox(operation, _, result, _ string) {
	if operation == "claim" && result == "success" {
		s.claimSuccess++
	}
}

func (p *fakePublisher) Publish(_ context.Context, event domain.Event) error {
	p.publishedDuringClaim = p.publishedDuringClaim || p.store.claiming
	return p.fail[event.ID]
}
