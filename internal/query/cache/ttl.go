package cache

import (
	"crypto/rand"
	"errors"
	"io"
	"math/big"
	"time"
)

const MaxCacheTTL = 24 * time.Hour

var ErrInvalidTTLPolicy = errors.New("invalid cache TTL policy")

type TTLPolicy struct {
	base   time.Duration
	jitter time.Duration
	random io.Reader
}

func NewTTLPolicy(base, jitter time.Duration, random io.Reader) (*TTLPolicy, error) {
	if base <= 0 || base > MaxCacheTTL || jitter < 0 || jitter > base || random == nil {
		return nil, ErrInvalidTTLPolicy
	}
	return &TTLPolicy{base: base, jitter: jitter, random: random}, nil
}

func (policy *TTLPolicy) Next() (time.Duration, error) {
	if policy.jitter == 0 {
		return policy.base, nil
	}
	maximum := big.NewInt(policy.jitter.Nanoseconds() + 1)
	value, err := rand.Int(policy.random, maximum)
	if err != nil {
		return 0, err
	}
	return policy.base + time.Duration(value.Int64()), nil
}
