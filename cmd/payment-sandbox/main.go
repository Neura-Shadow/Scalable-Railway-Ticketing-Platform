package main

import (
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/sandbox"
)

type commandConfig struct {
	address             string
	sandbox             sandbox.Config
	maxBodyBytes        int64
	faultControlEnabled bool
	faultControlToken   string
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger, os.Getenv); err != nil {
		logger.Error("payment sandbox stopped", "reason", publicReason(err))
		os.Exit(1)
	}
}

func run(logger *slog.Logger, getenv func(string) string) error {
	if logger == nil || getenv == nil {
		return errors.New("payment sandbox dependency unavailable")
	}
	config, err := loadConfig(getenv)
	if err != nil {
		return err
	}
	faults := sandbox.NewScript()
	config.sandbox.Faults = faults
	service, err := sandbox.New(config.sandbox)
	if err != nil {
		return err
	}
	handler, err := newHandler(service, handlerConfig{maxBodyBytes: config.maxBodyBytes, faultControlEnabled: config.faultControlEnabled, faultControlToken: config.faultControlToken, faults: faults})
	if err != nil {
		return err
	}
	server := &http.Server{
		Addr:              config.address,
		Handler:           handler,
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    32 << 10,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.ListenAndServe() }()
	select {
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdownContext)
	case err := <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return errors.New("payment sandbox HTTP server failed")
	}
}

func loadConfig(getenv func(string) string) (commandConfig, error) {
	environment := valueOr(getenv("PAYMENT_SANDBOX_ENVIRONMENT"), "development")
	address := valueOr(getenv("PAYMENT_SANDBOX_ADDRESS"), "127.0.0.1:8099")
	issueKeyID := strings.TrimSpace(getenv("PAYMENT_SANDBOX_WEBHOOK_ISSUE_KEY_ID"))
	keys, err := parseKeyring(getenv("PAYMENT_SANDBOX_WEBHOOK_KEYRING"))
	if err != nil || issueKeyID == "" {
		return commandConfig{}, errors.New("payment sandbox webhook keyring invalid")
	}
	maxBodyBytes, err := positiveInt64(valueOr(getenv("PAYMENT_SANDBOX_MAX_BODY_BYTES"), "65536"), 1<<20)
	if err != nil {
		return commandConfig{}, errors.New("payment sandbox body limit invalid")
	}
	replaySeconds, err := positiveInt64(valueOr(getenv("PAYMENT_SANDBOX_WEBHOOK_REPLAY_SECONDS"), "300"), 3600)
	if err != nil {
		return commandConfig{}, errors.New("payment sandbox replay tolerance invalid")
	}
	allowProduction, _ := strconv.ParseBool(getenv("PAYMENT_SANDBOX_ALLOW_PRODUCTION_DISPOSABLE_TEST_ONLY"))
	faultControlEnabled, _ := strconv.ParseBool(getenv("PAYMENT_SANDBOX_FAULT_CONTROL_ENABLED"))
	faultControlToken := getenv("PAYMENT_SANDBOX_FAULT_CONTROL_TOKEN")
	if faultControlEnabled && (strings.EqualFold(environment, "production") || len(faultControlToken) < 16) {
		return commandConfig{}, errors.New("payment sandbox fault control invalid")
	}
	stateMaxBytes, err := positiveInt64(valueOr(getenv("PAYMENT_SANDBOX_STATE_MAX_BYTES"), "16777216"), 64<<20)
	if err != nil {
		return commandConfig{}, errors.New("payment sandbox state configuration invalid")
	}
	stateStore, err := sandbox.NewFileStateStore(getenv("PAYMENT_SANDBOX_STATE_PATH"), stateMaxBytes)
	if err != nil {
		return commandConfig{}, errors.New("payment sandbox state configuration invalid")
	}
	return commandConfig{
		address:             address,
		maxBodyBytes:        maxBodyBytes,
		faultControlEnabled: faultControlEnabled,
		faultControlToken:   faultControlToken,
		sandbox: sandbox.Config{
			Environment: environment,
			AllowSandboxInProductionForDisposableTestOnly: allowProduction,
			WebhookKeys:            keys,
			IssueKeyID:             issueKeyID,
			WebhookMaxBodyBytes:    int(maxBodyBytes),
			WebhookReplayTolerance: time.Duration(replaySeconds) * time.Second,
			StateStore:             stateStore,
		},
	}, nil
}

func parseKeyring(encoded string) (map[string][]byte, error) {
	if len(encoded) == 0 || len(encoded) > 16<<10 {
		return nil, errors.New("invalid keyring")
	}
	keys := make(map[string][]byte)
	for _, entry := range strings.Split(encoded, ",") {
		parts := strings.SplitN(strings.TrimSpace(entry), "=", 2)
		if len(parts) != 2 || parts[0] == "" || len(parts[0]) > 64 || len(keys) >= 8 {
			return nil, errors.New("invalid keyring")
		}
		key, err := base64.StdEncoding.DecodeString(parts[1])
		if err != nil || len(key) < 16 || len(key) > 128 {
			return nil, errors.New("invalid keyring")
		}
		if _, exists := keys[parts[0]]; exists {
			return nil, errors.New("invalid keyring")
		}
		keys[parts[0]] = key
	}
	if len(keys) == 0 {
		return nil, errors.New("invalid keyring")
	}
	return keys, nil
}

func positiveInt64(value string, maximum int64) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 1 || parsed > maximum {
		return 0, errors.New("value out of range")
	}
	return parsed, nil
}

func valueOr(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

func publicReason(err error) string {
	if err == nil {
		return "none"
	}
	switch err.Error() {
	case "payment sandbox webhook keyring invalid", "payment sandbox body limit invalid", "payment sandbox replay tolerance invalid", "payment sandbox fault control invalid", "payment sandbox state configuration invalid", "payment sandbox state is invalid", "payment sandbox state is unavailable", "payment sandbox is disabled in production", "payment sandbox environment is invalid", "payment sandbox webhook body limit is invalid", "payment sandbox replay tolerance is invalid", "payment sandbox webhook queue limit is invalid", "payment sandbox webhook keyring is invalid", "payment sandbox issue key is invalid", "payment sandbox HTTP configuration invalid", "payment sandbox fault control configuration invalid", "payment sandbox HTTP server failed":
		return err.Error()
	default:
		return "payment sandbox failure"
	}
}
