package domain

import (
	"errors"
	"fmt"
)

var ErrInvalidRole = errors.New("invalid role")

type Role string

const (
	RoleCustomer Role = "customer"
	RoleAdmin    Role = "admin"
	RoleOperator Role = "operator"
)

func ParseRole(value string) (Role, error) {
	role := Role(value)
	if err := role.Validate(); err != nil {
		return "", err
	}
	return role, nil
}

func (r Role) Validate() error {
	switch r {
	case RoleCustomer, RoleAdmin, RoleOperator:
		return nil
	default:
		return fmt.Errorf("%w: %q", ErrInvalidRole, r)
	}
}
