package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/settlement"
	settlementpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/settlement/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/google/uuid"
)

const (
	defaultAdminPageSize = 100
	defaultAdminMaxPages = 10
	defaultAdminTimeout  = 2 * time.Minute
	maxAdminTimeout      = 10 * time.Minute
)

var (
	errAdminArguments = errors.New("invalid settlement admin arguments")
	errAdminWiring    = errors.New("settlement admin runtime wiring unavailable")
	adminIdentity     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$`)
	adminReviewer     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$`)
)

type commandConfig struct {
	Command      string
	Scope        settlement.DetectionScope
	PageSize     int
	MaxPages     int
	Timeout      time.Duration
	ReviewRunID  uuid.UUID
	ReviewerID   string
	Disposition  settlementpostgres.ReviewDisposition
	EvidenceHash [32]byte
	Authority    authority.Deployment
}

type adminBackend interface {
	RunOnce(context.Context, settlement.DetectionScope) (settlement.DetectionReport, error)
	AppendReview(context.Context, settlementpostgres.Review) error
}

type adminFactory func(context.Context, func(string) (string, bool), commandConfig) (adminBackend, func(), error)

type adminEnvelope struct {
	Status            string         `json:"status"`
	Command           string         `json:"command"`
	ReadOnly          bool           `json:"read_only"`
	AppendOnly        bool           `json:"append_only"`
	FinancialMutation bool           `json:"financial_mutation"`
	Pages             int            `json:"pages,omitempty"`
	Examined          int            `json:"examined,omitempty"`
	FindingCount      int            `json:"finding_count,omitempty"`
	Findings          map[string]int `json:"findings,omitempty"`
	Completed         bool           `json:"completed,omitempty"`
	Bounded           bool           `json:"bounded,omitempty"`
	Error             string         `json:"error,omitempty"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr, openAdminBackend))
}

func run(parent context.Context, args []string, lookup func(string) (string, bool), stdout, stderr io.Writer, factory adminFactory) int {
	if parent == nil || lookup == nil || stdout == nil || stderr == nil || factory == nil {
		return 2
	}
	cfg, err := parseCommand(args, lookup)
	if err != nil {
		_ = writeAdminEnvelope(stdout, adminEnvelope{
			Status: "rejected", Command: safeCommand(args), FinancialMutation: false, Error: "invalid_arguments",
		})
		fmt.Fprintln(stderr, "settlement-admin: arguments rejected")
		return 2
	}
	backend, closeBackend, err := factory(parent, lookup, cfg)
	if err != nil {
		_ = writeAdminEnvelope(stdout, adminEnvelope{
			Status: "failed", Command: cfg.Command, FinancialMutation: false, Error: "startup_failed",
		})
		fmt.Fprintln(stderr, "settlement-admin: startup failed")
		return 1
	}
	defer closeBackend()

	ctx, cancel := context.WithTimeout(parent, cfg.Timeout)
	defer cancel()
	if cfg.Command == "mark-reviewed" {
		review := settlementpostgres.Review{
			ID: uuid.New(), RunID: cfg.ReviewRunID, ReviewerID: cfg.ReviewerID,
			Disposition: cfg.Disposition, EvidenceHash: cfg.EvidenceHash, ReviewedAt: time.Now().UTC(),
		}
		err = backend.AppendReview(ctx, review)
		status := "completed"
		if err != nil {
			status = "failed"
		}
		_ = writeAdminEnvelope(stdout, adminEnvelope{
			Status: status, Command: cfg.Command, ReadOnly: false, AppendOnly: true,
			FinancialMutation: false, Error: boundedAdminError(err, true),
		})
		if err != nil {
			fmt.Fprintln(stderr, "settlement-admin: review append failed")
			return 1
		}
		return 0
	}

	report, err := backend.RunOnce(ctx, cfg.Scope)
	status := "completed"
	if err != nil {
		status = "failed"
	}
	findings := summarizeFindings(report.Findings)
	_ = writeAdminEnvelope(stdout, adminEnvelope{
		Status: status, Command: cfg.Command, ReadOnly: true, AppendOnly: true, FinancialMutation: false,
		Pages: report.Pages, Examined: report.Examined, FindingCount: len(report.Findings), Findings: findings,
		Completed: report.Completed, Bounded: report.Bounded, Error: boundedAdminError(err, false),
	})
	if err != nil {
		fmt.Fprintln(stderr, "settlement-admin: detection failed")
		return 1
	}
	return 0
}

func parseCommand(args []string, lookup func(string) (string, bool)) (commandConfig, error) {
	if len(args) == 0 {
		return commandConfig{}, errAdminArguments
	}
	command := args[0]
	cfg := commandConfig{
		Command:  command,
		PageSize: envInt(lookup, "SETTLEMENT_ADMIN_PAGE_SIZE", defaultAdminPageSize),
		MaxPages: envInt(lookup, "SETTLEMENT_ADMIN_MAX_PAGES", defaultAdminMaxPages),
		Timeout:  envDuration(lookup, "SETTLEMENT_ADMIN_TIMEOUT", defaultAdminTimeout),
	}
	deployment, err := adminDeployment(lookup)
	if err != nil {
		return commandConfig{}, errAdminArguments
	}
	cfg.Authority = deployment
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.IntVar(&cfg.PageSize, "page-size", cfg.PageSize, "bounded comparisons per page")
	flags.IntVar(&cfg.MaxPages, "max-pages", cfg.MaxPages, "bounded pages per run")
	flags.DurationVar(&cfg.Timeout, "timeout", cfg.Timeout, "command timeout")

	var value, from, to, scopeName, runID, reviewer, disposition, evidenceHash string
	switch command {
	case "inspect-batch":
		flags.StringVar(&value, "batch", "", "provider settlement batch identity")
		cfg.Scope.Kind = settlement.ScopeSettlement
	case "inspect-payout", "reconcile-payout":
		flags.StringVar(&value, "payout", "", "provider payout identity")
		cfg.Scope.Kind = settlement.ScopePayout
	case "inspect-transaction":
		flags.StringVar(&value, "transaction", "", "provider transaction correlation")
		cfg.Scope.Kind = settlement.ScopePayment
	case "reconcile-payment":
		flags.StringVar(&value, "payment", "", "payment correlation")
		cfg.Scope.Kind = settlement.ScopePayment
	case "reconcile-period":
		flags.StringVar(&from, "from", "", "inclusive UTC date")
		flags.StringVar(&to, "to", "", "exclusive UTC date")
		cfg.Scope.Kind = settlement.ScopePeriod
	case "export-sanitized-report":
		flags.StringVar(&scopeName, "scope", "", "payment, period, settlement, or payout")
		flags.StringVar(&value, "value", "", "bounded detection scope")
	case "mark-reviewed":
		flags.StringVar(&runID, "run", "", "reconciliation run UUID")
		flags.StringVar(&reviewer, "reviewer", "", "bounded operator identity")
		flags.StringVar(&disposition, "disposition", "", "review disposition")
		flags.StringVar(&evidenceHash, "evidence-hash", "", "SHA-256 evidence digest")
	default:
		return commandConfig{}, errAdminArguments
	}
	if flags.Parse(args[1:]) != nil || flags.NArg() != 0 || cfg.PageSize < 1 || cfg.PageSize > 1000 ||
		cfg.MaxPages < 1 || cfg.MaxPages > 100 || cfg.Timeout <= 0 || cfg.Timeout > maxAdminTimeout {
		return commandConfig{}, errAdminArguments
	}
	if command == "mark-reviewed" {
		return parseReviewConfig(cfg, runID, reviewer, disposition, evidenceHash)
	}
	if command == "reconcile-period" {
		value = strings.TrimSpace(from) + "/" + strings.TrimSpace(to)
	}
	if command == "export-sanitized-report" {
		cfg.Scope.Kind = settlement.DetectionScopeKind(strings.TrimSpace(scopeName))
	}
	value = strings.TrimSpace(value)
	if !validAdminScope(cfg.Scope.Kind, value) {
		return commandConfig{}, errAdminArguments
	}
	cfg.Scope.Value = value
	return cfg, nil
}

func parseReviewConfig(cfg commandConfig, runID, reviewer, disposition, encodedHash string) (commandConfig, error) {
	parsedRunID, err := uuid.Parse(strings.TrimSpace(runID))
	if err != nil || parsedRunID == uuid.Nil {
		return commandConfig{}, errAdminArguments
	}
	reviewer = strings.TrimSpace(reviewer)
	parsedDisposition := settlementpostgres.ReviewDisposition(strings.TrimSpace(disposition))
	decodedHash, err := hex.DecodeString(strings.TrimSpace(encodedHash))
	if err != nil || len(decodedHash) != 32 || !adminReviewer.MatchString(reviewer) ||
		!parsedDisposition.Valid() {
		return commandConfig{}, errAdminArguments
	}
	copy(cfg.EvidenceHash[:], decodedHash)
	var zero [32]byte
	if cfg.EvidenceHash == zero {
		return commandConfig{}, errAdminArguments
	}
	cfg.ReviewRunID = parsedRunID
	cfg.ReviewerID = reviewer
	cfg.Disposition = parsedDisposition
	return cfg, nil
}

func validAdminScope(kind settlement.DetectionScopeKind, value string) bool {
	if !kind.Valid() {
		return false
	}
	if kind != settlement.ScopePeriod {
		return adminIdentity.MatchString(value)
	}
	parts := strings.Split(value, "/")
	if len(parts) != 2 {
		return false
	}
	start, startErr := time.Parse("2006-01-02", parts[0])
	end, endErr := time.Parse("2006-01-02", parts[1])
	return startErr == nil && endErr == nil && end.After(start) && end.Sub(start) <= 366*24*time.Hour
}

type productionAdminBackend struct {
	detector *settlement.Detector
	store    *settlementpostgres.Store
}

func (backend *productionAdminBackend) RunOnce(ctx context.Context, scope settlement.DetectionScope) (settlement.DetectionReport, error) {
	return backend.detector.RunOnce(ctx, scope)
}

func (backend *productionAdminBackend) AppendReview(ctx context.Context, review settlementpostgres.Review) error {
	return backend.store.AppendReview(ctx, review)
}

func openAdminBackend(ctx context.Context, lookup func(string) (string, bool), cfg commandConfig) (adminBackend, func(), error) {
	databaseURL := envOr(lookup, "DATABASE_URL", "")
	if databaseURL == "" {
		return nil, func() {}, errAdminWiring
	}
	pool, err := postgresx.NewRegionalBoundedPool(ctx, databaseURL, 4, postgresx.RegionalSession{
		Region: cfg.Authority.Region().String(), Role: string(cfg.Authority.Role()),
		Epoch: int64(cfg.Authority.Epoch().Uint64()), WritesEnabled: cfg.Authority.WritesEnabled(),
	})
	if err != nil {
		return nil, func() {}, errAdminWiring
	}
	fail := func() (adminBackend, func(), error) {
		pool.Close()
		return nil, func() {}, errAdminWiring
	}
	store, err := settlementpostgres.New(pool, settlementpostgres.WithRegionalAuthority(cfg.Authority))
	if err != nil {
		return fail()
	}
	readyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := store.Ready(readyCtx); err != nil {
		return fail()
	}
	detector, err := settlement.NewDetector(store, settlement.DetectorConfig{PageSize: cfg.PageSize, MaxPages: cfg.MaxPages})
	if err != nil {
		return fail()
	}
	return &productionAdminBackend{detector: detector, store: store}, pool.Close, nil
}

func adminDeployment(lookup func(string) (string, bool)) (authority.Deployment, error) {
	region, err := authority.ParseRegion(envOr(lookup, "DEPLOYMENT_REGION", ""))
	if err != nil {
		return authority.Deployment{}, err
	}
	epochValue, err := strconv.ParseUint(envOr(lookup, "REGION_EPOCH", ""), 10, 64)
	if err != nil {
		return authority.Deployment{}, err
	}
	epoch, err := authority.NewEpoch(epochValue)
	if err != nil {
		return authority.Deployment{}, err
	}
	writes, err := strconv.ParseBool(envOr(lookup, "REGIONAL_WRITES_ENABLED", ""))
	if err != nil {
		return authority.Deployment{}, err
	}
	return authority.NewDeployment(region, authority.Role(envOr(lookup, "DEPLOYMENT_ROLE", "")), epoch, writes)
}

func summarizeFindings(findings []settlement.Finding) map[string]int {
	if len(findings) == 0 {
		return nil
	}
	result := make(map[string]int, len(findings))
	for _, finding := range findings {
		result[string(finding.Reason)]++
	}
	return result
}

func safeCommand(args []string) string {
	if len(args) == 0 {
		return "unknown"
	}
	switch args[0] {
	case "inspect-batch", "inspect-payout", "inspect-transaction", "reconcile-period",
		"reconcile-payment", "reconcile-payout", "export-sanitized-report", "mark-reviewed":
		return args[0]
	default:
		return "unknown"
	}
}

func boundedAdminError(err error, review bool) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, settlement.ErrInvalidDetectionScope):
		return "invalid_scope"
	case review && errors.Is(err, settlementpostgres.ErrInvalidReview):
		return "invalid_review"
	case review:
		return "review_append_failed"
	default:
		return "detection_failed"
	}
}

func writeAdminEnvelope(writer io.Writer, value adminEnvelope) error {
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(true)
	return encoder.Encode(value)
}

func envOr(lookup func(string) (string, bool), name, fallback string) string {
	if value, ok := lookup(name); ok && strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return fallback
}

func envInt(lookup func(string) (string, bool), name string, fallback int) int {
	value, err := strconv.Atoi(envOr(lookup, name, strconv.Itoa(fallback)))
	if err != nil {
		return 0
	}
	return value
}

func envDuration(lookup func(string) (string, bool), name string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(envOr(lookup, name, fallback.String()))
	if err != nil {
		return 0
	}
	return value
}
