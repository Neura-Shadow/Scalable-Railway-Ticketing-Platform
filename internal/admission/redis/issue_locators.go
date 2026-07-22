package admissionredis

import (
	"context"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	goredis "github.com/redis/go-redis/v9"
)

type issueLocatorKeys struct {
	entry string
	token string
}

// PutIssueLocators pipelines the write-ahead entry and token locators for one
// bounded issue batch. Each key still uses the conflict-safe locator script;
// the pipeline removes the per-candidate network round trips that could age an
// otherwise valid issue batch before the atomic admission script runs.
func (s *Store) PutIssueLocators(
	ctx context.Context,
	locators []IssueLocator,
	scope PolicyScope,
	entryTTL time.Duration,
	tokenTTL time.Duration,
) error {
	if s == nil || s.client == nil || len(locators) < 1 || len(locators) > MaxAdmissionBatch ||
		entryTTL < domain.MinQueueEntryTTL || entryTTL > domain.MaxQueueEntryTTL+cleanupMargin ||
		tokenTTL < domain.MinQueueEntryTTL || tokenTTL > domain.MaxQueueEntryTTL+cleanupMargin {
		return ErrInvalidInput
	}
	value, err := encodeLocatorScope(scope)
	if err != nil {
		return ErrInvalidInput
	}
	if _, err := s.policyKeys(scope); err != nil {
		return ErrInvalidInput
	}
	keys := make([]issueLocatorKeys, 0, len(locators))
	for _, locator := range locators {
		entryKey, entryErr := s.keys.EntryLocator(locator.EntryID)
		tokenKey, tokenErr := s.keys.TokenLocator(locator.TokenHash)
		if entryErr != nil || tokenErr != nil {
			return ErrInvalidInput
		}
		keys = append(keys, issueLocatorKeys{entry: entryKey, token: tokenKey})
	}

	commands := make([]*goredis.Cmd, 0, len(keys)*2)
	_, err = s.client.Pipelined(ctx, func(pipeline goredis.Pipeliner) error {
		for _, pair := range keys {
			commands = append(commands,
				pipeline.Eval(ctx, putLocatorScript, []string{pair.entry}, value, entryTTL.Milliseconds()),
				pipeline.Eval(ctx, putLocatorScript, []string{pair.token}, value, tokenTTL.Milliseconds()),
			)
		}
		return nil
	})
	if err != nil {
		return backendError(err)
	}
	for _, command := range commands {
		result, commandErr := command.Text()
		if commandErr != nil {
			return backendError(commandErr)
		}
		if err := resultError(result); err != nil {
			return err
		}
	}
	return nil
}

// DeleteTokenLocators removes only candidates that a successful Issue response
// definitively did not admit. Ambiguous Issue failures never call this method,
// preserving write-ahead repair data for any admission that may have executed.
func (s *Store) DeleteTokenLocators(ctx context.Context, tokenHashes [][32]byte) error {
	if s == nil || s.client == nil || len(tokenHashes) > MaxAdmissionBatch {
		return ErrInvalidInput
	}
	if len(tokenHashes) == 0 {
		return nil
	}
	keys := make([]string, 0, len(tokenHashes))
	for _, tokenHash := range tokenHashes {
		key, err := s.keys.TokenLocator(tokenHash)
		if err != nil {
			return ErrInvalidInput
		}
		keys = append(keys, key)
	}
	_, err := s.client.Pipelined(ctx, func(pipeline goredis.Pipeliner) error {
		for _, key := range keys {
			pipeline.Unlink(ctx, key)
		}
		return nil
	})
	if err != nil {
		return backendError(err)
	}
	return nil
}
