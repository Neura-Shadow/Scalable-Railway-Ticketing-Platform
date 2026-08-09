package sandbox

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
)

const (
	durableStateVersion = 1
	minStateBytes       = 4 << 10
	maxStateBytes       = 64 << 20
	maxStateRecords     = 100000
)

// StateStore persists the sandbox provider's opaque versioned state. Save must
// return an error only while the previously loaded snapshot remains
// authoritative; once a new snapshot is installed it must return nil. The
// sandbox owns serialization and validation; adapters only retain bounded
// bytes. Reusing one store across Service instances models a provider restart.
type StateStore interface {
	Load() ([]byte, error)
	Save([]byte) error
}

type memoryStateStore struct {
	mu    sync.Mutex
	state []byte
}

// NewMemoryStateStore returns an ephemeral adapter that may be reused across
// Service instances in tests without writing provider state to disk.
func NewMemoryStateStore() StateStore { return &memoryStateStore{} }

func (s *memoryStateStore) Load() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.state...), nil
}

func (s *memoryStateStore) Save(state []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = append(s.state[:0], state...)
	return nil
}

type fileStateStore struct {
	path     string
	maxBytes int64
	mu       sync.Mutex
}

type fileStateRecord struct {
	Version int             `json:"version"`
	State   json.RawMessage `json:"state"`
	SHA256  string          `json:"sha256"`
}

// NewFileStateStore returns a bounded atomic-snapshot adapter for disposable
// sandbox restarts. The path must be absolute and must not be a symlink.
func NewFileStateStore(path string, maxBytes int64) (StateStore, error) {
	path = filepath.Clean(strings.TrimSpace(path))
	if path == "." || !filepath.IsAbs(path) || maxBytes < minStateBytes || maxBytes > maxStateBytes {
		return nil, errors.New("payment sandbox state configuration is invalid")
	}
	return &fileStateStore{path: path, maxBytes: maxBytes}, nil
}

func (s *fileStateStore) Load() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	info, err := os.Lstat(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > s.maxBytes {
		return nil, errors.New("payment sandbox state is unavailable")
	}
	contents, err := os.ReadFile(s.path)
	if err != nil || int64(len(contents)) > s.maxBytes {
		return nil, errors.New("payment sandbox state is unavailable")
	}
	complete := contents
	if len(complete) != 0 && complete[len(complete)-1] != '\n' {
		lastNewline := bytes.LastIndexByte(complete, '\n')
		if lastNewline < 0 {
			return nil, errors.New("payment sandbox state is invalid")
		}
		complete = complete[:lastNewline+1]
	}
	lines := bytes.Split(complete, []byte{'\n'})
	for i := len(lines) - 1; i >= 0; i-- {
		line := bytes.TrimSpace(lines[i])
		if len(line) != 0 {
			decoder := json.NewDecoder(bytes.NewReader(line))
			decoder.DisallowUnknownFields()
			var record fileStateRecord
			if err := decoder.Decode(&record); err != nil || record.Version != durableStateVersion || len(record.State) == 0 {
				return nil, errors.New("payment sandbox state is invalid")
			}
			if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
				return nil, errors.New("payment sandbox state is invalid")
			}
			sum := sha256.Sum256(record.State)
			if !strings.EqualFold(record.SHA256, hex.EncodeToString(sum[:])) {
				return nil, errors.New("payment sandbox state is invalid")
			}
			return append([]byte(nil), record.State...), nil
		}
	}
	return nil, nil
}

func (s *fileStateStore) Save(state []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(state) == 0 {
		return errors.New("payment sandbox state is unavailable")
	}
	sum := sha256.Sum256(state)
	record, err := json.Marshal(fileStateRecord{Version: durableStateVersion, State: append([]byte(nil), state...), SHA256: hex.EncodeToString(sum[:])})
	if err != nil || int64(len(record)+1) > s.maxBytes {
		return errors.New("payment sandbox state is unavailable")
	}
	if info, err := os.Lstat(s.path); err == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("payment sandbox state is unavailable")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.New("payment sandbox state is unavailable")
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return errors.New("payment sandbox state is unavailable")
	}
	file, err := os.CreateTemp(filepath.Dir(s.path), ".provider-state-*.tmp")
	if err != nil {
		return errors.New("payment sandbox state is unavailable")
	}
	tempPath := file.Name()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tempPath)
		}
	}()
	if err = file.Chmod(0o600); err == nil {
		_, err = file.Write(append(record, '\n'))
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil || closeErr != nil || os.Rename(tempPath, s.path) != nil {
		return errors.New("payment sandbox state is unavailable")
	}
	committed = true
	// The file contents are synced before rename. Directory sync is best effort:
	// StateStore's contract cannot report an error after the new snapshot became
	// authoritative without making the caller misclassify the commit outcome.
	if directory, openErr := os.Open(filepath.Dir(s.path)); openErr == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	return nil
}

type durableState struct {
	Version       int                 `json:"version"`
	Payments      []durablePayment    `json:"payments"`
	Idempotency   []durableIdempotent `json:"idempotency"`
	Webhooks      []durableWebhook    `json:"webhooks"`
	NextPayment   uint64              `json:"next_payment"`
	NextOperation uint64              `json:"next_operation"`
	NextEvent     uint64              `json:"next_event"`
	Step          uint64              `json:"step"`
}

type durablePayment struct {
	ID        string          `json:"id"`
	IntentID  string          `json:"intent_id"`
	Token     string          `json:"synthetic_token"`
	Status    provider.Status `json:"status"`
	Amount    int64           `json:"amount_minor"`
	Currency  string          `json:"currency"`
	Captured  int64           `json:"captured_minor"`
	Refunded  int64           `json:"refunded_minor"`
	UpdatedAt time.Time       `json:"updated_at"`
}

type durableIdempotent struct {
	KeyHash     string                    `json:"key_hash"`
	Fingerprint string                    `json:"fingerprint"`
	Checkout    *provider.Checkout        `json:"checkout,omitempty"`
	Operation   *provider.OperationResult `json:"operation,omitempty"`
}

type durableWebhook struct {
	Event        provider.WebhookEvent `json:"event"`
	Sequence     uint64                `json:"sequence"`
	DeliverAfter uint64                `json:"deliver_after"`
}

type mutableState struct {
	payments      map[string]*paymentRecord
	idempotency   map[string]idempotentResult
	nextPayment   uint64
	nextOperation uint64
	nextEvent     uint64
	step          uint64
	webhooks      []QueuedWebhook
}

func idempotencyIdentity(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func (s *Service) mutableStateLocked() mutableState {
	state := mutableState{
		payments:    make(map[string]*paymentRecord, len(s.payments)),
		idempotency: make(map[string]idempotentResult, len(s.idempotency)),
		nextPayment: s.nextPayment, nextOperation: s.nextOperation,
		nextEvent: s.nextEvent, step: s.step,
		webhooks: make([]QueuedWebhook, len(s.webhooks)),
	}
	for id, record := range s.payments {
		clone := *record
		state.payments[id] = &clone
	}
	for key, result := range s.idempotency {
		state.idempotency[key] = result
	}
	for i, webhook := range s.webhooks {
		state.webhooks[i] = webhook
		state.webhooks[i].Body = append([]byte(nil), webhook.Body...)
	}
	return state
}

func (s *Service) restoreMutableStateLocked(state mutableState) {
	s.payments, s.idempotency = state.payments, state.idempotency
	s.nextPayment, s.nextOperation = state.nextPayment, state.nextOperation
	s.nextEvent, s.step, s.webhooks = state.nextEvent, state.step, state.webhooks
}

func (s *Service) durableStateLocked() durableState {
	state := durableState{
		Version: durableStateVersion, NextPayment: s.nextPayment,
		NextOperation: s.nextOperation, NextEvent: s.nextEvent, Step: s.step,
		Payments:    make([]durablePayment, 0, len(s.payments)),
		Idempotency: make([]durableIdempotent, 0, len(s.idempotency)),
		Webhooks:    make([]durableWebhook, 0, len(s.webhooks)),
	}
	for _, record := range s.payments {
		state.Payments = append(state.Payments, durablePayment{
			ID: record.id, IntentID: record.intentID, Token: record.token,
			Status: record.status, Amount: record.amount, Currency: record.currency,
			Captured: record.captured, Refunded: record.refunded, UpdatedAt: record.updatedAt,
		})
	}
	for keyHash, result := range s.idempotency {
		entry := durableIdempotent{KeyHash: keyHash, Fingerprint: result.fingerprint}
		if result.checkout.ProviderPaymentID != "" {
			checkout := result.checkout
			entry.Checkout = &checkout
		}
		if result.operation.ProviderOperationID != "" {
			operation := result.operation
			entry.Operation = &operation
		}
		state.Idempotency = append(state.Idempotency, entry)
	}
	for _, queued := range s.webhooks {
		state.Webhooks = append(state.Webhooks, durableWebhook{
			Event: queued.event, Sequence: queued.Sequence, DeliverAfter: queued.DeliverAfter,
		})
	}
	sort.Slice(state.Payments, func(i, j int) bool { return state.Payments[i].ID < state.Payments[j].ID })
	sort.Slice(state.Idempotency, func(i, j int) bool { return state.Idempotency[i].KeyHash < state.Idempotency[j].KeyHash })
	return state
}

func (s *Service) persistLocked() error {
	encoded, err := json.Marshal(s.durableStateLocked())
	if err != nil || s.stateStore.Save(encoded) != nil {
		return errors.New("payment sandbox state is unavailable")
	}
	return nil
}

func (s *Service) loadDurableState() error {
	encoded, err := s.stateStore.Load()
	if err != nil {
		return err
	}
	if len(encoded) == 0 {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var state durableState
	if err := decoder.Decode(&state); err != nil {
		return errors.New("payment sandbox state is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("payment sandbox state is invalid")
	}
	return s.applyDurableState(state)
}

func (s *Service) applyDurableState(state durableState) error {
	if state.Version != durableStateVersion || len(state.Payments) > maxStateRecords ||
		len(state.Idempotency) > maxStateRecords*2 || len(state.Webhooks) > s.maxWebhooks || len(state.Webhooks) > maxStateRecords {
		return errors.New("payment sandbox state is invalid")
	}
	payments := make(map[string]*paymentRecord, len(state.Payments))
	for _, item := range state.Payments {
		if !validIdentifier(item.ID) || !validIdentifier(item.IntentID) || !strings.HasPrefix(item.Token, "tok_sandbox_") ||
			!validStatus(item.Status) || item.Amount <= 0 || normalizeCurrency(item.Currency) != item.Currency || item.UpdatedAt.IsZero() ||
			item.Captured < 0 || item.Captured > item.Amount || item.Refunded < 0 || item.Refunded > item.Captured {
			return errors.New("payment sandbox state is invalid")
		}
		if _, exists := payments[item.ID]; exists {
			return errors.New("payment sandbox state is invalid")
		}
		payments[item.ID] = &paymentRecord{id: item.ID, intentID: item.IntentID, token: item.Token, status: item.Status, amount: item.Amount, currency: item.Currency, captured: item.Captured, refunded: item.Refunded, updatedAt: item.UpdatedAt}
	}
	idempotency := make(map[string]idempotentResult, len(state.Idempotency))
	for _, item := range state.Idempotency {
		if !validDigest(item.KeyHash) || !validDigest(item.Fingerprint) || (item.Checkout == nil) == (item.Operation == nil) {
			return errors.New("payment sandbox state is invalid")
		}
		if _, exists := idempotency[item.KeyHash]; exists {
			return errors.New("payment sandbox state is invalid")
		}
		result := idempotentResult{fingerprint: item.Fingerprint}
		if item.Checkout != nil {
			if _, exists := payments[item.Checkout.ProviderPaymentID]; !exists {
				return errors.New("payment sandbox state is invalid")
			}
			result.checkout = *item.Checkout
		} else {
			if _, exists := payments[item.Operation.ProviderPaymentID]; !exists {
				return errors.New("payment sandbox state is invalid")
			}
			result.operation = *item.Operation
		}
		idempotency[item.KeyHash] = result
	}
	webhooks := make([]QueuedWebhook, 0, len(state.Webhooks))
	for _, item := range state.Webhooks {
		event := item.Event
		record, exists := payments[event.ProviderPaymentID]
		if !exists || !validIdentifier(event.ProviderEventID) || !knownEventType(event.Type) ||
			!validStatus(event.Status) || event.AmountMinor != record.amount || event.Currency != record.currency ||
			event.OccurredAt.IsZero() || item.Sequence == 0 || item.Sequence > state.Step ||
			(item.DeliverAfter > state.Step && item.DeliverAfter-state.Step > 10000) {
			return errors.New("payment sandbox state is invalid")
		}
		body, err := json.Marshal(event)
		if err != nil || len(body) > s.maxWebhookBody {
			return errors.New("payment sandbox state is invalid")
		}
		queued, err := s.sign(body, s.now().UTC())
		if err != nil {
			return errors.New("payment sandbox state is invalid")
		}
		queued.event, queued.Sequence, queued.DeliverAfter = event, item.Sequence, item.DeliverAfter
		webhooks = append(webhooks, queued)
	}
	s.payments, s.idempotency = payments, idempotency
	s.nextPayment, s.nextOperation, s.nextEvent, s.step = state.NextPayment, state.NextOperation, state.NextEvent, state.Step
	s.webhooks = webhooks
	return nil
}

func validDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}
