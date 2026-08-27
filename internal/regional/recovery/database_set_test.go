package recovery_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
)

func TestDatabaseSetHasExactlyTheThreeRequiredDatabasesInStableOrder(t *testing.T) {
	t.Parallel()

	set := recovery.NewDatabaseSet("control-value", "zero-value", "one-value")
	var got []string
	err := set.Visit(func(database recovery.Database, value string) error {
		got = append(got, database.String()+"="+value)
		return nil
	})
	if err != nil {
		t.Fatalf("Visit() error = %v", err)
	}
	want := []string{
		"control=control-value",
		"shard-0=zero-value",
		"shard-1=one-value",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Visit() = %v, want %v", got, want)
	}
	if _, err := recovery.ParseDatabase("shard-2"); !errors.Is(err, recovery.ErrInvalidDatabase) {
		t.Fatalf("ParseDatabase(shard-2) error = %v, want ErrInvalidDatabase", err)
	}
}
