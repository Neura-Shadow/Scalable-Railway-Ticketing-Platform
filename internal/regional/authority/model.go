// Package authority protects every regional control and booking-shard write
// with authority loaded from the same local database transaction.
package authority

import (
	"errors"
	"regexp"
)

var (
	ErrInvalidRegion     = errors.New("invalid deployment region")
	ErrInvalidRole       = errors.New("invalid deployment role")
	ErrInvalidEpoch      = errors.New("invalid regional epoch")
	ErrEpochNotNewer     = errors.New("regional epoch must increase")
	ErrInvalidState      = errors.New("invalid regional authority state")
	ErrInvalidDeployment = errors.New("invalid regional deployment")
	ErrInvalidSnapshot   = errors.New("invalid regional authority snapshot")
)

var regionPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)

// Region is a bounded authority identity, never a host, endpoint, or network
// location.
type Region string

func ParseRegion(raw string) (Region, error) {
	if !regionPattern.MatchString(raw) {
		return "", ErrInvalidRegion
	}
	return Region(raw), nil
}

func (region Region) String() string { return string(region) }

// Role is the configured process role. Only active processes may request a
// normal control or booking mutation.
type Role string

const (
	RoleActive   Role = "active"
	RolePassive  Role = "passive"
	RoleRecovery Role = "recovery"
)

func (role Role) valid() bool {
	return role == RoleActive || role == RolePassive || role == RoleRecovery
}

// Epoch is a positive monotonic regional fencing value.
type Epoch uint64

func NewEpoch(value uint64) (Epoch, error) {
	if value == 0 {
		return 0, ErrInvalidEpoch
	}
	return Epoch(value), nil
}

func (epoch Epoch) Uint64() uint64 { return uint64(epoch) }

// RequireNewerEpoch enforces monotonic authority across failover and failback.
func RequireNewerEpoch(current, candidate Epoch) error {
	if current == 0 || candidate == 0 {
		return ErrInvalidEpoch
	}
	if candidate <= current {
		return ErrEpochNotNewer
	}
	return nil
}

// State is the durable regional authority state.
type State string

const (
	StateActive    State = "active"
	StateDraining  State = "draining"
	StateFenced    State = "fenced"
	StatePromoting State = "promoting"
	StateRecovery  State = "recovery"
	StateFailed    State = "failed"
)

func (state State) valid() bool {
	switch state {
	case StateActive, StateDraining, StateFenced, StatePromoting, StateRecovery, StateFailed:
		return true
	default:
		return false
	}
}

// Deployment is immutable process configuration evaluated before opening a
// write transaction.
type Deployment struct {
	region        Region
	role          Role
	epoch         Epoch
	writesEnabled bool
}

func NewDeployment(region Region, role Role, epoch Epoch, writesEnabled bool) (Deployment, error) {
	if _, err := ParseRegion(region.String()); err != nil || !role.valid() || epoch == 0 {
		return Deployment{}, ErrInvalidDeployment
	}
	return Deployment{region: region, role: role, epoch: epoch, writesEnabled: writesEnabled}, nil
}

func (deployment Deployment) Region() Region      { return deployment.region }
func (deployment Deployment) Role() Role          { return deployment.role }
func (deployment Deployment) Epoch() Epoch        { return deployment.epoch }
func (deployment Deployment) WritesEnabled() bool { return deployment.writesEnabled }

// Snapshot is the authority row locked by a database-local write transaction.
type Snapshot struct {
	region        Region
	epoch         Epoch
	state         State
	writesEnabled bool
}

func NewSnapshot(region Region, epoch Epoch, state State, writesEnabled bool) (Snapshot, error) {
	if _, err := ParseRegion(region.String()); err != nil || epoch == 0 || !state.valid() {
		return Snapshot{}, ErrInvalidSnapshot
	}
	return Snapshot{region: region, epoch: epoch, state: state, writesEnabled: writesEnabled}, nil
}

func (snapshot Snapshot) Region() Region      { return snapshot.region }
func (snapshot Snapshot) Epoch() Epoch        { return snapshot.epoch }
func (snapshot Snapshot) State() State        { return snapshot.state }
func (snapshot Snapshot) WritesEnabled() bool { return snapshot.writesEnabled }
