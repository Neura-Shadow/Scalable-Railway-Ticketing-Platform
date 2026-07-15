package app

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
)

func TestReadinessReportsEveryBoundedComponentWithoutLeakingProbeErrors(t *testing.T) {
	checker := newReadinessChecker(
		func(context.Context) error { return errors.New("postgres://user:secret@host") },
		func(context.Context) error { return nil },
		func(context.Context) (int, bool, error) { return 3, false, nil },
		func() error { return errors.New("JWT_SECRET detail") },
	)
	checks, err := checker.CheckReadiness(context.Background())
	if err != nil {
		t.Fatalf("CheckReadiness() leaked error: %v", err)
	}
	want := []httpapi.ReadinessCheck{{Name: "postgres", Ready: false}, {Name: "redis", Ready: true}, {Name: "migrations", Ready: false}, {Name: "configuration", Ready: false}}
	if !reflect.DeepEqual(checks, want) {
		t.Fatalf("checks=%#v", checks)
	}
}

func TestReadinessRequiresCleanCurrentSchemaVersion(t *testing.T) {
	for _, test := range []struct {
		version      int
		dirty, ready bool
	}{{currentSchemaVersion, false, true}, {currentSchemaVersion - 1, false, false}, {currentSchemaVersion, true, false}} {
		checker := newReadinessChecker(func(context.Context) error { return nil }, func(context.Context) error { return nil }, func(context.Context) (int, bool, error) { return test.version, test.dirty, nil }, func() error { return nil })
		checks, _ := checker.CheckReadiness(context.Background())
		if checks[2].Ready != test.ready {
			t.Fatalf("version=%d dirty=%v ready=%v", test.version, test.dirty, checks[2].Ready)
		}
	}
}
