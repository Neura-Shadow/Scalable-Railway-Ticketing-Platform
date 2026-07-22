package domain_test

import (
	"errors"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	"github.com/google/uuid"
)

func TestAdmissionFingerprintCanonicalizesAndBindsRequestShape(t *testing.T) {
	runID := uuid.NewString()
	base := domain.AdmissionFingerprintInput{
		TrainRunID:     " " + runID + " ",
		FromStopIndex:  1,
		ToStopIndex:    4,
		SeatClass:      " BUSINESS ",
		PassengerCount: 3,
	}
	got, err := domain.FingerprintAdmissionRequest(base)
	if err != nil {
		t.Fatal(err)
	}
	equivalent, err := domain.FingerprintAdmissionRequest(domain.AdmissionFingerprintInput{
		TrainRunID: runID, FromStopIndex: 1, ToStopIndex: 4, SeatClass: "business", PassengerCount: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != equivalent {
		t.Fatal("equivalent admission requests produced different fingerprints")
	}

	variants := []domain.AdmissionFingerprintInput{
		{TrainRunID: uuid.NewString(), FromStopIndex: 1, ToStopIndex: 4, SeatClass: "business", PassengerCount: 3},
		{TrainRunID: runID, FromStopIndex: 0, ToStopIndex: 4, SeatClass: "business", PassengerCount: 3},
		{TrainRunID: runID, FromStopIndex: 1, ToStopIndex: 5, SeatClass: "business", PassengerCount: 3},
		{TrainRunID: runID, FromStopIndex: 1, ToStopIndex: 4, SeatClass: "first", PassengerCount: 3},
		{TrainRunID: runID, FromStopIndex: 1, ToStopIndex: 4, SeatClass: "business", PassengerCount: 2},
	}
	for index, variant := range variants {
		fingerprint, err := domain.FingerprintAdmissionRequest(variant)
		if err != nil {
			t.Fatalf("variant %d: %v", index, err)
		}
		if fingerprint == got {
			t.Fatalf("variant %d did not change the fingerprint", index)
		}
	}
}

func TestAdmissionFingerprintRejectsInvalidRequestShape(t *testing.T) {
	valid := domain.AdmissionFingerprintInput{
		TrainRunID: uuid.NewString(), FromStopIndex: 1, ToStopIndex: 4, SeatClass: "standard", PassengerCount: 1,
	}
	cases := []domain.AdmissionFingerprintInput{
		{TrainRunID: "not-a-uuid", FromStopIndex: 1, ToStopIndex: 4, SeatClass: "standard", PassengerCount: 1},
		{TrainRunID: valid.TrainRunID, FromStopIndex: -1, ToStopIndex: 4, SeatClass: "standard", PassengerCount: 1},
		{TrainRunID: valid.TrainRunID, FromStopIndex: 4, ToStopIndex: 4, SeatClass: "standard", PassengerCount: 1},
		{TrainRunID: valid.TrainRunID, FromStopIndex: 1, ToStopIndex: 4, SeatClass: "premium", PassengerCount: 1},
		{TrainRunID: valid.TrainRunID, FromStopIndex: 1, ToStopIndex: 4, SeatClass: "standard", PassengerCount: 0},
		{TrainRunID: valid.TrainRunID, FromStopIndex: 1, ToStopIndex: 4, SeatClass: "standard", PassengerCount: 101},
	}
	for index, input := range cases {
		if _, err := domain.FingerprintAdmissionRequest(input); !errors.Is(err, domain.ErrInvalidAdmissionRequest) {
			t.Fatalf("case %d error = %v, want ErrInvalidAdmissionRequest", index, err)
		}
	}
}
