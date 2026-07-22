package domain

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"regexp"
	"time"
)

var (
	ErrInvalidAdmissionToken        = errors.New("invalid admission token")
	ErrUnknownAdmissionTokenKey     = errors.New("unknown admission token key")
	ErrInvalidAdmissionTokenKeyring = errors.New("invalid admission token keyring")
)

const (
	admissionTokenVersion = byte(1)
	tokenNonceSize        = 32
	tokenMACSize          = sha256.Size
	maxTokenKeyIDLength   = 64
)

var (
	admissionTokenDomain = []byte("railway-admission-token/v1")
	tokenKeyIDPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
)

type TokenClaims struct {
	KeyID                string
	PolicyID             string
	PolicyVersion        int64
	EntryID              string
	OwnerHash            [sha256.Size]byte
	AdmissionFingerprint [sha256.Size]byte
	IssuedAt             time.Time
	ExpiresAt            time.Time
}

type TokenDeliveryFields struct {
	Claims    TokenClaims
	Nonce     [tokenNonceSize]byte
	TokenHash [sha256.Size]byte
}

type IssuedToken struct {
	Raw    string
	Hash   [sha256.Size]byte
	Fields TokenDeliveryFields
}

type TokenKeyring struct {
	issueKeyID string
	keys       map[string][sha256.Size]byte
	random     io.Reader
}

func NewTokenKeyring(issueKeyID string, configured map[string][]byte) (*TokenKeyring, error) {
	if len(configured) == 0 {
		return nil, ErrInvalidAdmissionTokenKeyring
	}
	keys := make(map[string][sha256.Size]byte, len(configured))
	for keyID, material := range configured {
		if !validTokenKeyID(keyID) || len(material) != sha256.Size {
			return nil, ErrInvalidAdmissionTokenKeyring
		}
		var key [sha256.Size]byte
		copy(key[:], material)
		keys[keyID] = key
	}
	if issueKeyID != "" {
		if !validTokenKeyID(issueKeyID) {
			return nil, ErrInvalidAdmissionTokenKeyring
		}
		if _, exists := keys[issueKeyID]; !exists {
			return nil, ErrInvalidAdmissionTokenKeyring
		}
	}
	return &TokenKeyring{issueKeyID: issueKeyID, keys: keys, random: rand.Reader}, nil
}

func (k *TokenKeyring) CanAccept(keyID string) bool {
	if k == nil {
		return false
	}
	_, exists := k.keys[keyID]
	return exists
}

func (k *TokenKeyring) Issue(claims TokenClaims) (IssuedToken, error) {
	if k == nil || k.issueKeyID == "" {
		return IssuedToken{}, ErrInvalidAdmissionToken
	}
	claims.KeyID = k.issueKeyID
	if !validTokenClaims(claims) {
		return IssuedToken{}, ErrInvalidAdmissionToken
	}
	var nonce [tokenNonceSize]byte
	if _, err := io.ReadFull(k.random, nonce[:]); err != nil {
		return IssuedToken{}, ErrInvalidAdmissionToken
	}
	mac := k.sign(claims, nonce)
	raw := encodeRawToken(claims.KeyID, mac)
	tokenHash := sha256.Sum256([]byte(raw))
	fields := TokenDeliveryFields{Claims: claims, Nonce: nonce, TokenHash: tokenHash}
	return IssuedToken{Raw: raw, Hash: tokenHash, Fields: fields}, nil
}

// Reconstruct derives the bearer from the secret key and immutable Redis
// metadata, then proves that it is the bearer committed by TokenHash. Redis
// intentionally stores neither this bearer nor its MAC.
func (k *TokenKeyring) Reconstruct(fields TokenDeliveryFields) (string, error) {
	if err := k.verifyFields(fields); err != nil {
		return "", err
	}
	mac := k.sign(fields.Claims, fields.Nonce)
	return encodeRawToken(fields.Claims.KeyID, mac), nil
}

// VerifyFields validates an immutable Redis issuance record by recomputing the
// bearer and comparing its SHA-256 digest with the stored commitment.
func (k *TokenKeyring) VerifyFields(fields TokenDeliveryFields) error {
	return k.verifyFields(fields)
}

func (k *TokenKeyring) Verify(raw string, fields TokenDeliveryFields) error {
	keyID, suppliedMAC, err := decodeRawToken(raw)
	if err != nil || keyID != fields.Claims.KeyID {
		return ErrInvalidAdmissionToken
	}
	if err := k.verifyFields(fields); err != nil {
		return err
	}
	expectedMAC := k.sign(fields.Claims, fields.Nonce)
	if !hmac.Equal(expectedMAC[:], suppliedMAC[:]) {
		return ErrInvalidAdmissionToken
	}
	suppliedHash := sha256.Sum256([]byte(raw))
	if !hmac.Equal(suppliedHash[:], fields.TokenHash[:]) {
		return ErrInvalidAdmissionToken
	}
	return nil
}

func (k *TokenKeyring) verifyFields(fields TokenDeliveryFields) error {
	if k == nil || !validTokenClaims(fields.Claims) {
		return ErrInvalidAdmissionToken
	}
	if _, exists := k.keys[fields.Claims.KeyID]; !exists {
		return ErrUnknownAdmissionTokenKey
	}
	expectedMAC := k.sign(fields.Claims, fields.Nonce)
	raw := encodeRawToken(fields.Claims.KeyID, expectedMAC)
	expectedHash := sha256.Sum256([]byte(raw))
	if !hmac.Equal(expectedHash[:], fields.TokenHash[:]) {
		return ErrInvalidAdmissionToken
	}
	return nil
}

func (k *TokenKeyring) sign(claims TokenClaims, nonce [tokenNonceSize]byte) [tokenMACSize]byte {
	key := k.keys[claims.KeyID]
	mac := hmac.New(sha256.New, key[:])
	var canonical bytes.Buffer
	canonical.WriteByte(admissionTokenVersion)
	writeLengthPrefixed(&canonical, admissionTokenDomain)
	writeLengthPrefixed(&canonical, []byte(claims.KeyID))
	writeLengthPrefixed(&canonical, []byte(claims.PolicyID))
	_ = binary.Write(&canonical, binary.BigEndian, claims.PolicyVersion)
	writeLengthPrefixed(&canonical, []byte(claims.EntryID))
	writeLengthPrefixed(&canonical, claims.OwnerHash[:])
	writeLengthPrefixed(&canonical, claims.AdmissionFingerprint[:])
	_ = binary.Write(&canonical, binary.BigEndian, claims.IssuedAt.UTC().UnixMilli())
	_ = binary.Write(&canonical, binary.BigEndian, claims.ExpiresAt.UTC().UnixMilli())
	writeLengthPrefixed(&canonical, nonce[:])
	_, _ = mac.Write(canonical.Bytes())
	var result [tokenMACSize]byte
	copy(result[:], mac.Sum(nil))
	return result
}

func validTokenClaims(claims TokenClaims) bool {
	return validTokenKeyID(claims.KeyID) && claims.PolicyID != "" && claims.PolicyVersion > 0 &&
		claims.EntryID != "" && !claims.IssuedAt.IsZero() && claims.ExpiresAt.After(claims.IssuedAt)
}

func validTokenKeyID(value string) bool {
	return len(value) <= maxTokenKeyIDLength && tokenKeyIDPattern.MatchString(value)
}

func encodeRawToken(keyID string, mac [tokenMACSize]byte) string {
	var envelope bytes.Buffer
	envelope.WriteByte(admissionTokenVersion)
	_ = binary.Write(&envelope, binary.BigEndian, uint16(len(keyID)))
	_, _ = envelope.WriteString(keyID)
	_, _ = envelope.Write(mac[:])
	return base64.RawURLEncoding.EncodeToString(envelope.Bytes())
}

func decodeRawToken(raw string) (string, [tokenMACSize]byte, error) {
	var mac [tokenMACSize]byte
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(raw)
	if err != nil || len(decoded) < 1+2+1+tokenMACSize || decoded[0] != admissionTokenVersion {
		return "", mac, ErrInvalidAdmissionToken
	}
	keyLength := int(binary.BigEndian.Uint16(decoded[1:3]))
	if keyLength < 1 || keyLength > maxTokenKeyIDLength || len(decoded) != 3+keyLength+tokenMACSize {
		return "", mac, ErrInvalidAdmissionToken
	}
	keyID := string(decoded[3 : 3+keyLength])
	if !validTokenKeyID(keyID) {
		return "", mac, ErrInvalidAdmissionToken
	}
	copy(mac[:], decoded[3+keyLength:])
	return keyID, mac, nil
}
