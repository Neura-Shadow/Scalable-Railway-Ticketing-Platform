package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	admissionapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/application"
	admissiondomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	admissionpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/postgres"
	admissionredis "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/redis"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/clock"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
	platformmetrics "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/metrics"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/redisx"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/workerhttp"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

const admissionSchemaVersion = 9

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("admission worker stopped", "reason", publicWorkerReason(err))
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	if logger == nil {
		return errors.New("logger unavailable")
	}
	cfg, err := config.LoadFor(config.ProcessAdmissionWorker)
	if err != nil {
		return errors.New("configuration invalid")
	}
	keySelection, err := cfg.ParseAdmissionTokenKeys()
	if err != nil {
		return errors.New("token keyring invalid")
	}
	keyMaterial := make(map[string][]byte, len(keySelection.AcceptKeys))
	for keyID, key := range keySelection.AcceptKeys {
		keyMaterial[keyID] = append([]byte(nil), key[:]...)
	}
	keyring, err := admissiondomain.NewTokenKeyring(keySelection.IssueKeyID, keyMaterial)
	if err != nil {
		return errors.New("token keyring invalid")
	}

	rootContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := postgresx.NewBoundedPool(rootContext, cfg.DatabaseURL, cfg.ControlDatabaseMaxOpenConns)
	if err != nil {
		return errors.New("postgres configuration invalid")
	}
	defer pool.Close()
	redisClient := redis.NewClient(redisx.BoundedClientOptions(
		cfg.RedisAddress,
		cfg.RedisPassword,
		cfg.RedisTimeout,
	))
	defer func() { _ = redisClient.Close() }()

	policyStore, err := admissionpostgres.NewStore(pool)
	if err != nil {
		return errors.New("policy store initialization failed")
	}
	control, err := admissionredis.NewStore(redisClient, "railway-admission")
	if err != nil {
		return errors.New("admission control initialization failed")
	}
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.New(registry)
	if err != nil {
		return errors.New("metrics initialization failed")
	}
	worker, err := admissionapp.NewWorker(policyStore, control, keyring, cfg.AdmissionWorkerBatchSize, metrics)
	if err != nil {
		return errors.New("worker initialization failed")
	}
	readiness := admissionReadiness(pool, redisClient, cfg)
	healthServer, err := workerhttp.New(
		cfg.WorkerHTTPAddress, registry, readiness, admissionReadinessTimeout(cfg),
	)
	if err != nil {
		return errors.New("health server invalid")
	}
	listener, err := net.Listen("tcp", cfg.WorkerHTTPAddress)
	if err != nil {
		return errors.New("health listener unavailable")
	}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- healthServer.Serve(listener) }()
	defer shutdownAdmissionWorkerHTTP(healthServer, cfg.ShutdownTimeout)

	runPass := func() {
		passContext, cancel := context.WithTimeout(rootContext, cfg.WorkerPassTimeout)
		defer cancel()
		passStartedAt := time.Now().UTC()
		result, runErr := worker.RunOnce(passContext)
		passCompletedAt := time.Now().UTC()
		metrics.RecordAdmissionWorkerPass(runErr == nil, passCompletedAt.Sub(passStartedAt), passCompletedAt)
		metrics.RecordAdmissionWorkerState(result.QueueDepth, result.InflightAdmissions)
		if runErr != nil {
			logger.Error(
				"admission pass completed with isolated failures",
				"policies_seen", result.PoliciesSeen,
				"policies_processed", result.PoliciesProcessed,
				"issued", result.Issued,
				"queue_depth", result.QueueDepth,
				"inflight_admissions", result.InflightAdmissions,
				"failure_count", result.Failures,
			)
			return
		}
		logger.Info(
			"admission pass complete",
			"policies_seen", result.PoliciesSeen,
			"policies_processed", result.PoliciesProcessed,
			"issued", result.Issued,
			"recovered_leases", result.RecoveredLeases,
			"expired_tokens", result.ExpiredTokens,
			"expired_entries", result.ExpiredEntries,
			"queue_depth", result.QueueDepth,
			"inflight_admissions", result.InflightAdmissions,
		)
	}
	runInitialAdmissionPass(cfg.AdmissionWorkerEnabled, runPass)
	if !cfg.AdmissionWorkerEnabled {
		logger.Info("admission worker disabled", "category", "admission_worker_disabled")
		return waitForDisabledAdmissionWorker(rootContext, serverErrors)
	}

	ticker := clock.RealClock{}.NewTicker(cfg.AdmissionWorkerPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-rootContext.Done():
			return nil
		case serverErr := <-serverErrors:
			if serverErr != nil && !errors.Is(serverErr, http.ErrServerClosed) {
				return errors.New("health server failed")
			}
			return nil
		case <-ticker.C():
			runPass()
		}
	}
}

func admissionReadiness(pool *pgxpool.Pool, client redis.UniversalClient, cfg config.Config) workerhttp.ReadinessCheck {
	return func(ctx context.Context) error {
		if pool == nil || client == nil {
			return errors.New("worker dependency unavailable")
		}
		if err := pool.Ping(ctx); err != nil {
			return errors.New("worker dependency unavailable")
		}
		if err := client.Ping(ctx).Err(); err != nil {
			return errors.New("worker dependency unavailable")
		}
		var version int
		var dirty bool
		if err := pool.QueryRow(ctx, "SELECT version, dirty FROM schema_migrations LIMIT 1").Scan(&version, &dirty); err != nil ||
			version != admissionSchemaVersion || dirty {
			return errors.New("worker migration unavailable")
		}
		if err := cfg.ValidateFor(config.ProcessAdmissionWorker); err != nil {
			return errors.New("worker configuration invalid")
		}
		return nil
	}
}

func admissionReadinessTimeout(cfg config.Config) time.Duration {
	timeout := cfg.DatabaseTimeout
	if cfg.RedisTimeout > timeout {
		timeout = cfg.RedisTimeout
	}
	if timeout <= 0 || timeout > 10*time.Second {
		return 2 * time.Second
	}
	return timeout
}

func runInitialAdmissionPass(enabled bool, run func()) {
	if enabled && run != nil {
		run()
	}
}

func waitForDisabledAdmissionWorker(ctx context.Context, serverErrors <-chan error) error {
	select {
	case <-ctx.Done():
		return nil
	case serverErr := <-serverErrors:
		if serverErr != nil && !errors.Is(serverErr, http.ErrServerClosed) {
			return errors.New("health server failed")
		}
		return nil
	}
}

func shutdownAdmissionWorkerHTTP(server *http.Server, timeout time.Duration) {
	if server == nil || timeout <= 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		_ = server.Close()
	}
}

func publicWorkerReason(err error) string {
	if err == nil {
		return "none"
	}
	switch err.Error() {
	case "logger unavailable",
		"configuration invalid",
		"token keyring invalid",
		"postgres configuration invalid",
		"policy store initialization failed",
		"admission control initialization failed",
		"metrics initialization failed",
		"worker initialization failed",
		"health server invalid",
		"health listener unavailable",
		"health server failed":
		return err.Error()
	default:
		return "worker failure"
	}
}
