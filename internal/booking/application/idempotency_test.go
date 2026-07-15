package application_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/application"
)

func TestHashIdempotencyKeyIsStableAndDoesNotExposeRawKey(t *testing.T) {
	t.Parallel()

	const raw = "client-key-with-sensitive-entropy"
	first, err := application.HashIdempotencyKey(raw)
	if err != nil {
		t.Fatal(err)
	}
	second, err := application.HashIdempotencyKey(raw)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("same raw key produced different hashes")
	}
	if strings.Contains(string(first[:]), raw) {
		t.Fatal("hash representation contains raw key")
	}
}

func TestHashIdempotencyKeyRejectsEmptyAndOversizedKeys(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{"", strings.Repeat("x", application.MaxIdempotencyKeyBytes+1)} {
		if _, err := application.HashIdempotencyKey(raw); !errors.Is(err, application.ErrInvalidIdempotencyKey) {
			t.Fatalf("HashIdempotencyKey(%d bytes) error = %v", len(raw), err)
		}
	}
}

func TestFingerprintHoldRequestCanonicalizesBoundedFields(t *testing.T) {
	t.Parallel()

	base := application.HoldFingerprintInput{
		TrainRunID:      " AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE ",
		OriginCode:      " tpe ",
		DestinationCode: "khh",
		SeatClass:       " BUSINESS ",
		PassengerIDs:    []string{" PASSENGER-1 ", "PASSENGER-2"},
	}
	canonical := application.HoldFingerprintInput{
		TrainRunID:      "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
		OriginCode:      "TPE",
		DestinationCode: "KHH",
		SeatClass:       "business",
		PassengerIDs:    []string{"passenger-1", "passenger-2"},
	}

	first, err := application.FingerprintHoldRequest(base)
	if err != nil {
		t.Fatal(err)
	}
	second, err := application.FingerprintHoldRequest(canonical)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("equivalent canonical requests produced different fingerprints")
	}
}

func TestFingerprintHoldRequestSortsPassengersAndIncludesEveryCommandField(t *testing.T) {
	t.Parallel()

	base := application.HoldFingerprintInput{
		TrainRunID:      "run-1",
		OriginCode:      "TPE",
		DestinationCode: "KHH",
		SeatClass:       "standard",
		PassengerIDs:    []string{"passenger-1", "passenger-2"},
	}
	want, err := application.FingerprintHoldRequest(base)
	if err != nil {
		t.Fatal(err)
	}
	reordered := base
	reordered.PassengerIDs = []string{"passenger-2", "passenger-1"}
	reorderedFingerprint, err := application.FingerprintHoldRequest(reordered)
	if err != nil {
		t.Fatal(err)
	}
	if reorderedFingerprint != want {
		t.Fatal("equivalent passenger set produced a different fingerprint")
	}

	variants := []application.HoldFingerprintInput{
		{TrainRunID: "run-2", OriginCode: "TPE", DestinationCode: "KHH", SeatClass: "standard", PassengerIDs: []string{"passenger-1", "passenger-2"}},
		{TrainRunID: "run-1", OriginCode: "TXG", DestinationCode: "KHH", SeatClass: "standard", PassengerIDs: []string{"passenger-1", "passenger-2"}},
		{TrainRunID: "run-1", OriginCode: "TPE", DestinationCode: "TXG", SeatClass: "standard", PassengerIDs: []string{"passenger-1", "passenger-2"}},
		{TrainRunID: "run-1", OriginCode: "TPE", DestinationCode: "KHH", SeatClass: "business", PassengerIDs: []string{"passenger-1", "passenger-2"}},
		{TrainRunID: "run-1", OriginCode: "TPE", DestinationCode: "KHH", SeatClass: "standard", PassengerIDs: []string{"passenger-1", "passenger-3"}},
	}
	for _, variant := range variants {
		got, err := application.FingerprintHoldRequest(variant)
		if err != nil {
			t.Fatal(err)
		}
		if got == want {
			t.Fatalf("changed request field did not change fingerprint: %+v", variant)
		}
	}
}

func TestFingerprintHoldRequestRejectsInvalidIdentity(t *testing.T) {
	t.Parallel()

	invalid := []application.HoldFingerprintInput{
		{},
		{TrainRunID: "run", OriginCode: "TPE", DestinationCode: "TPE", SeatClass: "standard", PassengerIDs: []string{"p1"}},
		{TrainRunID: "run", OriginCode: "TPE", DestinationCode: "KHH", SeatClass: "standard", PassengerIDs: []string{"p1", " P1 "}},
	}
	for _, input := range invalid {
		if _, err := application.FingerprintHoldRequest(input); !errors.Is(err, application.ErrInvalidHoldRequest) {
			t.Fatalf("FingerprintHoldRequest(%+v) error = %v", input, err)
		}
	}
}

func TestFingerprintReservationCommandSeparatesOperations(t *testing.T) {
	t.Parallel()

	confirm, err := application.FingerprintReservationCommand(application.OperationReservationConfirm, " RESERVATION-1 ")
	if err != nil {
		t.Fatal(err)
	}
	cancel, err := application.FingerprintReservationCommand(application.OperationReservationCancel, "reservation-1")
	if err != nil {
		t.Fatal(err)
	}
	if confirm == cancel {
		t.Fatal("confirm and cancel produced the same fingerprint")
	}
	canonical, err := application.FingerprintReservationCommand(application.OperationReservationConfirm, "reservation-1")
	if err != nil {
		t.Fatal(err)
	}
	if canonical != confirm {
		t.Fatal("equivalent reservation IDs produced different fingerprints")
	}
}
