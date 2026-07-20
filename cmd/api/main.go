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

	admissiondomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	admissionpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/postgres"
	admissionredis "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/redis"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/app"
	bookingpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/postgres"
	offeringpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/clock"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
	platformmetrics "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/metrics"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/redisx"
	querypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

const (
	maxRequestBodyBytes = 1 << 20
	maxHeaderBytes      = 1 << 20
	defaultIdleTimeout  = 60 * time.Second
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("api stopped", "reason", publicReason(err))
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	if logger == nil {
		return errors.New("logger unavailable")
	}
	cfg, err := config.LoadFor(config.ProcessAPI)
	if err != nil {
		return errors.New("configuration invalid")
	}
	if cfg.Environment == config.EnvironmentProduction {
		gin.SetMode(gin.ReleaseMode)
	}

	signalContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	handlerContext, cancelHandlers := context.WithCancel(context.Background())
	defer cancelHandlers()

	pool, err := pgxpool.New(signalContext, cfg.DatabaseURL)
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

	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.New(registry)
	if err != nil {
		return errors.New("metrics initialization failed")
	}
	keySelection, err := cfg.ParseAdmissionTokenKeys()
	if err != nil {
		return errors.New("admission token keyring invalid")
	}
	admissionKeys := make(map[string][]byte, len(keySelection.AcceptKeys))
	for keyID, key := range keySelection.AcceptKeys {
		admissionKeys[keyID] = append([]byte(nil), key[:]...)
	}
	admissionKeyring, err := admissiondomain.NewTokenKeyring(keySelection.IssueKeyID, admissionKeys)
	if err != nil {
		return errors.New("admission token keyring invalid")
	}
	passwordClock := clock.RealClock{}
	auth, tokenParser, err := app.NewPostgresAuth(pool, cfg, passwordClock)
	if err != nil {
		return errors.New("authentication initialization failed")
	}
	passengers, err := app.NewPostgresPassengerService(pool, cfg.BcryptCost)
	if err != nil {
		return errors.New("passenger service initialization failed")
	}
	offeringStore, err := offeringpostgres.NewStore(pool)
	if err != nil {
		return errors.New("offering store initialization failed")
	}
	queryStore, err := querypostgres.NewStore(pool)
	if err != nil {
		return errors.New("query store initialization failed")
	}
	bookingStore := bookingpostgres.NewWithReservationQuotaLimits(pool, bookingpostgres.ReservationQuotaLimits{
		MaxActiveHoldsPerUser:            cfg.ReservationMaxActiveHoldsPerUser,
		MaxActiveHoldsPerUserPerTrainRun: cfg.ReservationMaxActiveHoldsPerUserPerTrainRun,
		MaxActivePassengersPerUser:       cfg.ReservationMaxActivePassengersPerUser,
	})
	policyStore, err := admissionpostgres.NewStore(pool)
	if err != nil {
		return errors.New("admission policy store initialization failed")
	}
	admissionControl, err := admissionredis.NewStore(redisClient, "railway-admission")
	if err != nil {
		return errors.New("admission control initialization failed")
	}
	executionSlots, err := app.NewExecutionSlots(cfg.ReservationMaxInflightPerInstance)
	if err != nil {
		return errors.New("reservation backpressure initialization failed")
	}
	reads := app.NewPostgresReads(pool)
	rateLimitBackend, err := redisx.NewRateLimiter(redisClient, "railway-api")
	if err != nil {
		return errors.New("rate limiter initialization failed")
	}

	router := httpapi.New(httpapi.Dependencies{
		Readiness:        app.NewReadinessChecker(pool, redisClient, cfg),
		ReadinessTimeout: readinessTimeout(cfg),
		TokenParser:      tokenParser,
		Reservations: app.NewAdmissionProtectedReservationService(
			bookingStore, bookingStore, queryStore, reads, policyStore, admissionControl,
			admissionKeyring, executionSlots, passwordClock, cfg.HoldTTL,
			cfg.MaxPassengersPerReservation, metrics,
		).WithDatabaseCommandTimeout(cfg.DatabaseTimeout),
		WaitingRoom:         app.NewWaitingRoomService(policyStore, queryStore, admissionControl, admissionKeyring, metrics),
		HotTrainPolicies:    app.NewHotTrainPolicyService(policyStore),
		MaxRequestBodyBytes: maxRequestBodyBytes,
		MaxPassengers:       cfg.MaxPassengersPerReservation,
		HTTPMetrics:         metrics,
		MetricsHandler:      promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
		Offering:            app.NewOfferingQueries(queryStore),
		Auth:                auth,
		RateLimiter:         app.NewRateLimiter(rateLimitBackend),
		Passengers:          passengers,
		Tickets:             app.NewTicketQueries(reads),
		Admin:               app.NewAdminCommands(offeringStore),
		Operator:            app.NewOperatorCommands(offeringStore, bookingStore, metrics),
	})
	if err := router.SetTrustedProxies(cfg.TrustedProxies); err != nil {
		return errors.New("trusted proxy configuration invalid")
	}

	server := &http.Server{
		Addr:              cfg.HTTPAddress,
		Handler:           router,
		ReadHeaderTimeout: cfg.HTTPReadTimeout,
		ReadTimeout:       cfg.HTTPReadTimeout,
		WriteTimeout:      cfg.HTTPWriteTimeout,
		IdleTimeout:       defaultIdleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
		BaseContext: func(net.Listener) context.Context {
			// The signal context must not be used here: Shutdown needs active
			// request contexts so in-flight database transactions can drain.
			return handlerContext
		},
	}
	listener, err := net.Listen("tcp", cfg.HTTPAddress)
	if err != nil {
		return errors.New("http listener failed")
	}
	logger.Info("api listening", "address", listener.Addr().String())
	return serveUntilShutdown(server, listener, signalContext.Done(), cfg.ShutdownTimeout)
}

func serveUntilShutdown(server *http.Server, listener net.Listener, shutdown <-chan struct{}, timeout time.Duration) error {
	if server == nil || listener == nil || shutdown == nil || timeout <= 0 {
		return errors.New("http lifecycle configuration invalid")
	}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.Serve(listener) }()

	select {
	case err := <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return errors.New("http listener failed")
	case <-shutdown:
	}

	shutdownContext, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := server.Shutdown(shutdownContext); err != nil {
		_ = server.Close()
		return errors.New("graceful shutdown failed")
	}
	if err := <-serverErrors; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return errors.New("http listener shutdown failed")
	}
	return nil
}

func readinessTimeout(cfg config.Config) time.Duration {
	timeout := cfg.DatabaseTimeout
	if cfg.RedisTimeout > timeout {
		timeout = cfg.RedisTimeout
	}
	if timeout <= 0 || timeout > 10*time.Second {
		return 2 * time.Second
	}
	return timeout
}

// publicReason deliberately bounds startup errors so connection strings and
// dependency error details never reach operational logs.
func publicReason(err error) string {
	if err == nil {
		return "none"
	}
	switch err.Error() {
	case "logger unavailable",
		"configuration invalid",
		"postgres configuration invalid",
		"metrics initialization failed",
		"admission token keyring invalid",
		"authentication initialization failed",
		"passenger service initialization failed",
		"offering store initialization failed",
		"query store initialization failed",
		"admission policy store initialization failed",
		"admission control initialization failed",
		"reservation backpressure initialization failed",
		"rate limiter initialization failed",
		"trusted proxy configuration invalid",
		"http listener failed",
		"http lifecycle configuration invalid",
		"graceful shutdown failed",
		"http listener shutdown failed":
		return err.Error()
	default:
		return "startup failure"
	}
}
