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
		func(context.Context) error { return errors.New("schema topology detail") },
		func() error { return errors.New("JWT_SECRET detail") },
	)
	checks, err := checker.CheckReadiness(context.Background())
	if err != nil {
		t.Fatalf("CheckReadiness() leaked error: %v", err)
	}
	want := []httpapi.ReadinessCheck{
		{Name: "postgres", Ready: false},
		{Name: "redis", Ready: true, Optional: true},
		{Name: "migrations", Ready: false},
		{Name: "shard_catalog", Ready: false},
		{Name: "configuration", Ready: false},
	}
	if !reflect.DeepEqual(checks, want) {
		t.Fatalf("checks=%#v", checks)
	}
}

func TestReadinessMarksRedisFailureAsOptionalDegradation(t *testing.T) {
	checker := newReadinessChecker(
		func(context.Context) error { return nil },
		func(context.Context) error { return errors.New("redis unavailable") },
		func(context.Context) (int, bool, error) { return currentSchemaVersion, false, nil },
		func(context.Context) error { return nil },
		func() error { return nil },
	)
	checks, err := checker.CheckReadiness(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if checks[1].Name != "redis" || checks[1].Ready || !checks[1].Optional {
		t.Fatalf("redis readiness = %+v, want optional degradation", checks[1])
	}
}

func TestReadinessRequiresCleanCurrentSchemaVersion(t *testing.T) {
	for _, test := range []struct {
		version      int
		dirty, ready bool
	}{{currentSchemaVersion, false, true}, {currentSchemaVersion - 1, false, false}, {currentSchemaVersion, true, false}} {
		checker := newReadinessChecker(func(context.Context) error { return nil }, func(context.Context) error { return nil }, func(context.Context) (int, bool, error) { return test.version, test.dirty, nil }, func(context.Context) error { return nil }, func() error { return nil })
		checks, _ := checker.CheckReadiness(context.Background())
		if checks[2].Ready != test.ready {
			t.Fatalf("version=%d dirty=%v ready=%v", test.version, test.dirty, checks[2].Ready)
		}
	}
}

func TestReadinessAllowsOneDegradedShardWhenAnotherServingShardExists(t *testing.T) {
	checker := newReadinessChecker(
		func(context.Context) error { return nil },
		func(context.Context) error { return nil },
		func(context.Context) (int, bool, error) { return currentSchemaVersion, false, nil },
		func(context.Context) error { return nil },
		func() error { return nil },
	)
	checks, _ := checker.CheckReadiness(context.Background())
	if !checks[3].Ready || checks[3].Optional {
		t.Fatalf("shard catalog readiness = %+v", checks[3])
	}
}
