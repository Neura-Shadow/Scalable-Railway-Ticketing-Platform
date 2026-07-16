package domain_test

import (
	"errors"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
)

func TestParseSeatClassAcceptsEverySupportedClass(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  domain.SeatClass
	}{
		{input: " standard ", want: domain.SeatClassStandard},
		{input: "BUSINESS", want: domain.SeatClassBusiness},
		{input: "first", want: domain.SeatClassFirst},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			got, err := domain.ParseSeatClass(tt.input)
			if err != nil {
				t.Fatalf("ParseSeatClass() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("ParseSeatClass() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseSeatClassRejectsUnknownValues(t *testing.T) {
	t.Parallel()

	if _, err := domain.ParseSeatClass("premium"); !errors.Is(err, domain.ErrInvalidSeatClass) {
		t.Fatalf("ParseSeatClass() error = %v, want ErrInvalidSeatClass", err)
	}
}
