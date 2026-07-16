package domain

import (
	"errors"
	"math"
)

var (
	ErrInvalidMoney     = errors.New("invalid money")
	ErrCurrencyMismatch = errors.New("currency mismatch")
	ErrMoneyOverflow    = errors.New("money overflow")
)

type Money struct {
	amountMinor int64
	currency    string
}

func NewMoney(amountMinor int64, currency string) (Money, error) {
	if amountMinor < 0 || !validCurrency(currency) {
		return Money{}, ErrInvalidMoney
	}
	return Money{amountMinor: amountMinor, currency: currency}, nil
}

func (m Money) Add(other Money) (Money, error) {
	if m.currency != other.currency {
		return Money{}, ErrCurrencyMismatch
	}
	if other.amountMinor > 0 && m.amountMinor > math.MaxInt64-other.amountMinor {
		return Money{}, ErrMoneyOverflow
	}
	return Money{amountMinor: m.amountMinor + other.amountMinor, currency: m.currency}, nil
}

func (m Money) Multiply(multiplier int64) (Money, error) {
	if multiplier < 0 {
		return Money{}, ErrInvalidMoney
	}
	if multiplier != 0 && m.amountMinor > math.MaxInt64/multiplier {
		return Money{}, ErrMoneyOverflow
	}
	return Money{amountMinor: m.amountMinor * multiplier, currency: m.currency}, nil
}

func (m Money) AmountMinor() int64 {
	return m.amountMinor
}

func (m Money) Currency() string {
	return m.currency
}

func validCurrency(value string) bool {
	if len(value) != 3 {
		return false
	}
	for index := range value {
		if value[index] < 'A' || value[index] > 'Z' {
			return false
		}
	}
	return true
}
