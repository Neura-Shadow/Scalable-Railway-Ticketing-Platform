// Package postgres provides a shell-free pgBackRest command adapter for the
// fixed regional PostgreSQL topology. The name reflects the protected store;
// it is not a generic backup backend.
package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/protection"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
)

var (
	ErrInvalidConfiguration = errors.New("pgBackRest command adapter configuration invalid")
	ErrInvocationRejected   = errors.New("pgBackRest invocation rejected")
	ErrCommandFailed        = errors.New("pgBackRest command failed")
	ErrOutputLimit          = errors.New("pgBackRest output exceeded bound")
	ErrInvalidOutput        = errors.New("pgBackRest normalized output invalid")
)

const maximumOutputBytes = 1 << 20

var (
	identityPattern  = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)
	backupSetPattern = regexp.MustCompile(`^[0-9]{8}-[0-9]{6}F(?:_[0-9]{8}-[0-9]{6}[DI])?$`)
)

type Config struct {
	Binary            string
	Stanzas           recovery.DatabaseSet[string]
	Repositories      map[string]int
	ValidationRoot    string
	ValidationTargets map[string]string
	MaxOutputBytes    int
	RestoreVerifier   RestoreVerifier
}

type RestoreObservationRequest struct {
	Target      string
	Database    recovery.Database
	DataPath    string
	PointInTime time.Time
}

type RestoreObservation struct {
	SchemaVersion int
	Timeline      uint32
	Facts         protection.RestoreFacts
}

// RestoreVerifier observes a booted isolated PostgreSQL target through a
// separate read-only channel. It must not derive facts from pgBackRest output.
type RestoreVerifier interface {
	Observe(context.Context, RestoreObservationRequest) (RestoreObservation, error)
}

type Executor interface {
	Execute(context.Context, string, []string, int) ([]byte, error)
}

type Runner struct {
	binary            string
	stanzas           recovery.DatabaseSet[string]
	repositories      map[string]int
	validationTargets map[string]string
	maxOutputBytes    int
	executor          Executor
	restoreVerifier   RestoreVerifier
}

func New(config Config, executor Executor) (*Runner, error) {
	if executor == nil || config.RestoreVerifier == nil || !validBinary(config.Binary) || config.MaxOutputBytes < 1024 ||
		config.MaxOutputBytes > maximumOutputBytes || len(config.Repositories) == 0 ||
		len(config.Repositories) > 4 || len(config.ValidationTargets) == 0 || len(config.ValidationTargets) > 6 ||
		strings.TrimSpace(config.ValidationRoot) == "" || !filepath.IsAbs(config.ValidationRoot) {
		return nil, ErrInvalidConfiguration
	}
	if err := config.Stanzas.Visit(func(_ recovery.Database, stanza string) error {
		if !identityPattern.MatchString(stanza) {
			return ErrInvalidConfiguration
		}
		return nil
	}); err != nil {
		return nil, ErrInvalidConfiguration
	}
	repositories := make(map[string]int, len(config.Repositories))
	for id, number := range config.Repositories {
		if !identityPattern.MatchString(id) || number < 1 || number > 4 {
			return nil, ErrInvalidConfiguration
		}
		repositories[id] = number
	}
	root, err := filepath.Abs(config.ValidationRoot)
	if err != nil || filepath.Clean(root) == filepath.VolumeName(root)+string(filepath.Separator) {
		return nil, ErrInvalidConfiguration
	}
	targets := make(map[string]string, len(config.ValidationTargets))
	for id, configuredPath := range config.ValidationTargets {
		if !identityPattern.MatchString(id) {
			return nil, ErrInvalidConfiguration
		}
		target, err := filepath.Abs(configuredPath)
		if err != nil {
			return nil, ErrInvalidConfiguration
		}
		relative, err := filepath.Rel(root, target)
		if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return nil, ErrInvalidConfiguration
		}
		targets[id] = filepath.Clean(target)
	}
	return &Runner{
		binary: filepath.Clean(config.Binary), stanzas: config.Stanzas,
		repositories: repositories, validationTargets: targets,
		maxOutputBytes: config.MaxOutputBytes, executor: executor, restoreVerifier: config.RestoreVerifier,
	}, nil
}

func (runner *Runner) Run(ctx context.Context, invocation protection.Invocation) (protection.Result, error) {
	if runner == nil || runner.executor == nil || ctx == nil {
		return protection.Result{}, ErrInvalidConfiguration
	}
	stanza, err := runner.stanzas.Value(invocation.Database())
	if err != nil {
		return protection.Result{}, ErrInvocationRejected
	}
	repository, ok := runner.repositories[invocation.Repository()]
	if !ok {
		return protection.Result{}, ErrInvocationRejected
	}
	base := []string{
		"--stanza=" + stanza,
		"--repo=" + strconv.Itoa(repository),
		"--log-level-console=off",
	}
	arguments := append([]string(nil), base...)
	needsInfo := true
	var restoreTarget string
	switch invocation.Operation() {
	case protection.OperationBackup:
		arguments = append(arguments, "backup", "--type=full")
	case protection.OperationVerify:
		if !backupSetPattern.MatchString(invocation.BackupSet()) {
			return protection.Result{}, ErrInvocationRejected
		}
		arguments = append(arguments, "verify", "--set="+invocation.BackupSet())
	case protection.OperationRestoreValidation:
		if !backupSetPattern.MatchString(invocation.BackupSet()) {
			return protection.Result{}, ErrInvocationRejected
		}
		target, ok := runner.validationTargets[invocation.Target()]
		if !ok {
			return protection.Result{}, ErrInvocationRejected
		}
		if invocation.PointInTime().IsZero() || invocation.PointInTime().Location() != time.UTC {
			return protection.Result{}, ErrInvocationRejected
		}
		arguments = append(arguments, "restore", "--set="+invocation.BackupSet(), "--pg1-path="+target,
			"--type=time", "--target="+invocation.PointInTime().Format(time.RFC3339Nano), "--target-action=promote")
		restoreTarget = target
	case protection.OperationExpireDryRun:
		if !backupSetPattern.MatchString(invocation.BackupSet()) {
			return protection.Result{}, ErrInvocationRejected
		}
		arguments = append(arguments, "expire", "--set="+invocation.BackupSet(), "--dry-run")
	case protection.OperationExpireConfirmed:
		if !validExpirationInvocation(invocation) {
			return protection.Result{}, ErrInvocationRejected
		}
		arguments = append(arguments, "expire", "--set="+invocation.BackupSet())
		needsInfo = false
	case protection.OperationExpireReconcile:
		if !validExpirationInvocation(invocation) {
			return protection.Result{}, ErrInvocationRejected
		}
		return runner.observeExpiration(ctx, base, invocation)
	default:
		return protection.Result{}, ErrInvocationRejected
	}
	_, err = runner.executor.Execute(ctx, runner.binary, arguments, runner.maxOutputBytes)
	if err != nil {
		return protection.Result{}, ErrCommandFailed
	}
	if !needsInfo {
		return runner.observeExpiration(ctx, base, invocation)
	}
	infoArguments := append(base, "info", "--output=json")
	if invocation.BackupSet() != "" {
		infoArguments = append(infoArguments, "--set="+invocation.BackupSet())
	}
	payload, err := runner.executor.Execute(ctx, runner.binary, infoArguments, runner.maxOutputBytes)
	if err != nil {
		return protection.Result{}, ErrCommandFailed
	}
	result, err := decodeInfoResult(payload, invocation.BackupSet())
	if err != nil {
		return protection.Result{}, err
	}
	switch invocation.Operation() {
	case protection.OperationVerify:
		result.RepositoryVerified = true
	case protection.OperationExpireDryRun:
		result.DryRun = true
	case protection.OperationRestoreValidation:
		if runner.restoreVerifier == nil {
			return protection.Result{}, ErrInvalidOutput
		}
		observation, observeErr := runner.restoreVerifier.Observe(ctx, RestoreObservationRequest{
			Target: invocation.Target(), Database: invocation.Database(), DataPath: restoreTarget,
			PointInTime: invocation.PointInTime(),
		})
		if observeErr != nil || observation.SchemaVersion <= 0 || observation.Timeline == 0 {
			return protection.Result{}, ErrInvalidOutput
		}
		result.SchemaVersion = observation.SchemaVersion
		result.Timeline = observation.Timeline
		result.RestoreFacts = observation.Facts
		result.Reconciled = true
		result.PointInTime = invocation.PointInTime()
	}
	return result, nil
}

func validExpirationInvocation(invocation protection.Invocation) bool {
	return backupSetPattern.MatchString(invocation.BackupSet()) &&
		invocation.Checksum() != (protection.Digest{}) && invocation.PlanDigest() != (protection.Digest{})
}

func (runner *Runner) observeExpiration(
	ctx context.Context,
	base []string,
	invocation protection.Invocation,
) (protection.Result, error) {
	payload, err := runner.executor.Execute(ctx, runner.binary, append(base, "info", "--output=json"), runner.maxOutputBytes)
	if err != nil {
		return protection.Result{}, ErrCommandFailed
	}
	present, observedChecksum, err := decodeRepositorySetObservation(payload, invocation.BackupSet())
	if err != nil || (present && observedChecksum != invocation.Checksum()) {
		return protection.Result{}, ErrInvalidOutput
	}
	return protection.Result{
		Success: true, BackupSet: invocation.BackupSet(), Checksum: invocation.Checksum(),
		PlanDigest: invocation.PlanDigest(), BackupPresent: present, Expired: !present,
		CompletedAt: time.Now().UTC(),
	}, nil
}

// Check runs pgBackRest's bounded repository/archive consistency check for one
// fixed database and allowlisted repository. It returns no raw command output.
func (runner *Runner) Check(ctx context.Context, database recovery.Database, repositoryID string) error {
	if runner == nil || runner.executor == nil || ctx == nil {
		return ErrInvalidConfiguration
	}
	stanza, err := runner.stanzas.Value(database)
	if err != nil {
		return ErrInvocationRejected
	}
	repository, ok := runner.repositories[repositoryID]
	if !ok {
		return ErrInvocationRejected
	}
	arguments := []string{
		"--stanza=" + stanza,
		"--repo=" + strconv.Itoa(repository),
		"--log-level-console=off",
		"check",
	}
	if _, err := runner.executor.Execute(ctx, runner.binary, arguments, runner.maxOutputBytes); err != nil {
		return ErrCommandFailed
	}
	return nil
}

type nativeInfoStanza struct {
	Backup []json.RawMessage `json:"backup"`
	Cipher string            `json:"cipher"`
	Status struct {
		Code int `json:"code"`
	} `json:"status"`
}

type nativeInfoBackup struct {
	Archive struct {
		Stop string `json:"stop"`
	} `json:"archive"`
	Error bool   `json:"error"`
	Label string `json:"label"`
	LSN   struct {
		Stop string `json:"stop"`
	} `json:"lsn"`
	Repo struct {
		Cipher string `json:"cipher"`
	} `json:"repo"`
	Timestamp struct {
		Stop int64 `json:"stop"`
	} `json:"timestamp"`
}

func decodeInfoResult(payload []byte, expectedSet string) (protection.Result, error) {
	if len(payload) == 0 || len(payload) > maximumOutputBytes {
		return protection.Result{}, ErrInvalidOutput
	}
	var documents []nativeInfoStanza
	if err := json.Unmarshal(payload, &documents); err != nil || len(documents) != 1 ||
		documents[0].Status.Code != 0 || documents[0].Cipher != "aes-256-cbc" || len(documents[0].Backup) == 0 {
		return protection.Result{}, ErrInvalidOutput
	}
	var selected json.RawMessage
	var backup nativeInfoBackup
	for _, candidate := range documents[0].Backup {
		var decoded nativeInfoBackup
		if json.Unmarshal(candidate, &decoded) != nil {
			return protection.Result{}, ErrInvalidOutput
		}
		if expectedSet == "" || decoded.Label == expectedSet {
			selected, backup = candidate, decoded
		}
	}
	if len(selected) == 0 || !backupSetPattern.MatchString(backup.Label) ||
		(expectedSet != "" && backup.Label != expectedSet) || backup.Error ||
		backup.Repo.Cipher != "aes-256-cbc" || backup.Timestamp.Stop <= 0 {
		return protection.Result{}, ErrInvalidOutput
	}
	timeline, err := archiveTimeline(backup.Archive.Stop)
	if err != nil {
		return protection.Result{}, ErrInvalidOutput
	}
	wal, err := parseLSN(backup.LSN.Stop)
	if err != nil {
		return protection.Result{}, ErrInvalidOutput
	}
	position, err := recovery.NewReplicationPosition(timeline, wal)
	if err != nil {
		return protection.Result{}, ErrInvalidOutput
	}
	var canonical any
	if json.Unmarshal(selected, &canonical) != nil {
		return protection.Result{}, ErrInvalidOutput
	}
	canonicalPayload, err := json.Marshal(canonical)
	if err != nil {
		return protection.Result{}, ErrInvalidOutput
	}
	checksum := protection.HashEvidence(canonicalPayload)
	return protection.Result{
		Success: true, BackupSet: backup.Label, Checksum: checksum,
		Encrypted: true, SourcePosition: position,
		CompletedAt: time.Unix(backup.Timestamp.Stop, 0).UTC(), Timeline: timeline,
	}, nil
}

func decodeRepositorySetObservation(payload []byte, expectedSet string) (bool, protection.Digest, error) {
	if len(payload) == 0 || len(payload) > maximumOutputBytes || !backupSetPattern.MatchString(expectedSet) {
		return false, protection.Digest{}, ErrInvalidOutput
	}
	var documents []nativeInfoStanza
	if err := json.Unmarshal(payload, &documents); err != nil || len(documents) != 1 ||
		documents[0].Status.Code != 0 || documents[0].Cipher != "aes-256-cbc" {
		return false, protection.Digest{}, ErrInvalidOutput
	}
	for _, candidate := range documents[0].Backup {
		var backup nativeInfoBackup
		if json.Unmarshal(candidate, &backup) != nil || !backupSetPattern.MatchString(backup.Label) {
			return false, protection.Digest{}, ErrInvalidOutput
		}
		if backup.Label != expectedSet {
			continue
		}
		if backup.Error || backup.Repo.Cipher != "aes-256-cbc" || backup.Timestamp.Stop <= 0 {
			return false, protection.Digest{}, ErrInvalidOutput
		}
		var canonical any
		if json.Unmarshal(candidate, &canonical) != nil {
			return false, protection.Digest{}, ErrInvalidOutput
		}
		canonicalPayload, err := json.Marshal(canonical)
		if err != nil {
			return false, protection.Digest{}, ErrInvalidOutput
		}
		return true, protection.HashEvidence(canonicalPayload), nil
	}
	return false, protection.Digest{}, nil
}

func archiveTimeline(segment string) (uint32, error) {
	if len(segment) != 24 {
		return 0, ErrInvalidOutput
	}
	value, err := strconv.ParseUint(segment[:8], 16, 32)
	if err != nil || value == 0 {
		return 0, ErrInvalidOutput
	}
	return uint32(value), nil
}

func parseLSN(value string) (uint64, error) {
	parts := strings.Split(value, "/")
	if len(parts) != 2 {
		return 0, ErrInvalidOutput
	}
	high, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return 0, ErrInvalidOutput
	}
	low, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return 0, ErrInvalidOutput
	}
	wal := high<<32 | low
	if wal == 0 {
		return 0, ErrInvalidOutput
	}
	return wal, nil
}

func validBinary(binary string) bool {
	if binary == "" || strings.ContainsAny(binary, "\r\n\x00") {
		return false
	}
	base := strings.ToLower(filepath.Base(binary))
	return base == "pgbackrest" || base == "pgbackrest.exe" || base == "pgbackrest-secret.sh"
}

// OSExecutor invokes pgBackRest directly without a shell and discards output
// beyond the configured bound before returning a sanitized error.
type OSExecutor struct{}

func (OSExecutor) Execute(ctx context.Context, binary string, arguments []string, limit int) ([]byte, error) {
	if ctx == nil || !validBinary(binary) || limit < 1024 || limit > maximumOutputBytes {
		return nil, ErrInvalidConfiguration
	}
	command := exec.CommandContext(ctx, binary, arguments...)
	stdout := newBoundedWriter(limit)
	stderr := newBoundedWriter(limit)
	command.Stdout = stdout
	command.Stderr = stderr
	if err := command.Run(); err != nil {
		return nil, ErrCommandFailed
	}
	if stdout.overflow || stderr.overflow {
		return nil, ErrOutputLimit
	}
	return append([]byte(nil), stdout.buffer.Bytes()...), nil
}

type boundedWriter struct {
	buffer   bytes.Buffer
	limit    int
	overflow bool
}

func newBoundedWriter(limit int) *boundedWriter { return &boundedWriter{limit: limit} }

func (writer *boundedWriter) Write(value []byte) (int, error) {
	remaining := writer.limit - writer.buffer.Len()
	if remaining > 0 {
		keep := len(value)
		if keep > remaining {
			keep = remaining
		}
		_, _ = writer.buffer.Write(value[:keep])
	}
	if len(value) > remaining {
		writer.overflow = true
	}
	return len(value), nil
}

var _ protection.Runner = (*Runner)(nil)
