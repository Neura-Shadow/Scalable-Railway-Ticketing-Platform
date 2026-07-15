package domain_test

import (
	"errors"
	"math"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/domain"
)

func TestMoneyAddUsesMinorUnits(t *testing.T) {
	t.Parallel()

	left, err := domain.NewMoney(12_550, "TWD")
	if err != nil {
		t.Fatal(err)
	}
	right, err := domain.NewMoney(450, "TWD")
	if err != nil {
		t.Fatal(err)
	}

	total, err := left.Add(right)
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if got, want := total.AmountMinor(), int64(13_000); got != want {
		t.Fatalf("AmountMinor() = %d, want %d", got, want)
	}
}

func TestMoneyMultiplyRejectsOverflow(t *testing.T) {
	t.Parallel()

	money, err := domain.NewMoney(math.MaxInt64/2+1, "TWD")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := money.Multiply(2); !errors.Is(err, domain.ErrMoneyOverflow) {
		t.Fatalf("Multiply() error = %v, want ErrMoneyOverflow", err)
	}
}

func TestMoneyRejectsInvalidValuesAndCurrencyMismatch(t *testing.T) {
	t.Parallel()

	if _, err := domain.NewMoney(-1, "TWD"); !errors.Is(err, domain.ErrInvalidMoney) {
		t.Fatalf("negative NewMoney() error = %v, want ErrInvalidMoney", err)
	}
	if _, err := domain.NewMoney(1, "twd"); !errors.Is(err, domain.ErrInvalidMoney) {
		t.Fatalf("lowercase NewMoney() error = %v, want ErrInvalidMoney", err)
	}

	twd, _ := domain.NewMoney(1, "TWD")
	usd, _ := domain.NewMoney(1, "USD")
	if _, err := twd.Add(usd); !errors.Is(err, domain.ErrCurrencyMismatch) {
		t.Fatalf("Add() error = %v, want ErrCurrencyMismatch", err)
	}
	max, _ := domain.NewMoney(math.MaxInt64, "TWD")
	if _, err := max.Add(twd); !errors.Is(err, domain.ErrMoneyOverflow) {
		t.Fatalf("overflow Add() error = %v, want ErrMoneyOverflow", err)
	}
}
