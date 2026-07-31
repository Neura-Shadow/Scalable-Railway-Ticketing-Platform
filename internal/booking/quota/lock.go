// Package quota owns cross-path booking quota coordination primitives.
package quota

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/google/uuid"
)

const reservationUserLockNamespace = "railway/booking/reservation-quota/user/v1\x00"

// UserAdvisoryLockKey returns the stable PostgreSQL transaction-lock key used
// by legacy, logical-shard, and physical-shard reservation paths.
func UserAdvisoryLockKey(userID uuid.UUID) int64 {
	digestInput := make([]byte, 0, len(reservationUserLockNamespace)+len(userID))
	digestInput = append(digestInput, reservationUserLockNamespace...)
	digestInput = append(digestInput, userID[:]...)
	digest := sha256.Sum256(digestInput)
	return int64(binary.BigEndian.Uint64(digest[:8]))
}
