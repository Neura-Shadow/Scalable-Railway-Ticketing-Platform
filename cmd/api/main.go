package main

import (
	"context"
	"crypto/rand"
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
	bookingcommand "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	commandphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/physical"
	commandpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/operatorcommand"
	operatorpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/operatorcommand/postgres"
	bookingpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/postgres"
	offeringpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/postgres"
	paymentapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/application"
	paymentpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/postgres"
	paymentprovider "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	providerhttp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/httpclient"
	providerstripe "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/stripe"
	paymentrefund "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/refund"
	refundpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/refund/postgres"
	paymentshard "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	paymentshardpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard/postgres"
	paymentwebhook "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/webhook"
	webhookpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/webhook/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/clock"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
	platformmetrics "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/metrics"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/redisx"
	querycache "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/cache"
	querypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/postgres"
	queryreadmodel "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/readmodel"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	shardingpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/routecache"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

const (
	maxRequestBodyBytes               = 1 << 20
	maxHeaderBytes                    = 1 << 20
	defaultIdleTimeout                = 60 * time.Second
	projectionLagObservationInterval  = 5 * time.Second
	reconciliationObservationInterval = time.Minute
	databasePoolObservationInterval   = time.Second
)

type readModelOperationalStore interface {
	ProjectionLag(context.Context, string) (time.Duration, error)
	NextReconciliationTrainRun(context.Context, string) (string, bool, error)
	ReconcileTrainRun(context.Context, string) (queryreadmodel.ReconcileResult, error)
}

type readModelOperationalMetrics interface {
	SetProjectionLag(time.Duration)
	AddReconciliationMismatches(string, int)
}

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

	pool, err := postgresx.NewRegionalBoundedPool(signalContext, cfg.DatabaseURL, cfg.ControlDatabaseMaxOpenConns, regionalSession(cfg))
	if err != nil {
		return errors.New("postgres configuration invalid")
	}
	defer pool.Close()

	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.NewEventMetrics(registry)
	if err != nil {
		return errors.New("metrics initialization failed")
	}
	readMetrics, err := platformmetrics.NewReadModelMetrics(registry)
	if err != nil {
		return errors.New("read-model metrics initialization failed")
	}

	var physicalRegistry *shardphysical.Registry
	var physicalRouter *shardphysical.CatalogRouter
	var physicalRequestTimeout time.Duration
	if cfg.BookingShardMode == config.BookingShardModePhysical {
		physicalRequestTimeout = cfg.PhysicalShardQueryTimeout
		connections := make(map[string]shardphysical.ConnectionConfig, len(cfg.PhysicalShardConnections))
		for reference, dsn := range cfg.PhysicalShardConnections {
			shardID, parseErr := sharding.ParseShardID(reference)
			if parseErr != nil {
				return errors.New("physical shard registry initialization failed")
			}
			connections[reference] = shardphysical.ConnectionConfig{ShardID: shardID, DSN: dsn}
		}
		physicalRegistry, err = shardphysical.NewRegistry(signalContext, shardphysical.RegistryConfig{
			Connections: connections,
			MaxCount:    cfg.PhysicalShardMaxCount,
			Limits: shardphysical.PoolLimits{
				MaxOpenConns:     cfg.PhysicalShardMaxOpenConns,
				MaxIdleConns:     cfg.PhysicalShardMaxIdleConns,
				MaxLifetime:      cfg.PhysicalShardConnMaxLifetime,
				MaxIdleTime:      cfg.PhysicalShardConnMaxIdleTime,
				ConnectTimeout:   cfg.PhysicalShardConnectTimeout,
				StatementTimeout: cfg.PhysicalShardQueryTimeout,
				LockTimeout:      cfg.PhysicalShardQueryTimeout,
			},
		}, shardphysical.RegionalPGXPoolFactory(regionalSession(cfg)))
		if err != nil {
			return errors.New("physical shard registry initialization failed")
		}
		defer physicalRegistry.Close()
		physicalRouter, err = shardphysical.NewCatalogRouter(
			pool, physicalRegistry, cfg.BookingRouteCacheTTL, shardphysical.WithMetrics(metrics),
		)
		if err != nil {
			return errors.New("physical shard router initialization failed")
		}
	}
	stopDatabasePoolObserver := startDatabasePoolObserver(
		signalContext, metrics, pool, physicalRegistry, databasePoolObservationInterval,
	)
	defer stopDatabasePoolObserver()

	redisClient := redis.NewClient(redisx.BoundedClientOptions(
		cfg.RedisAddress,
		cfg.RedisPassword,
		cfg.RedisTimeout,
	))
	defer func() { _ = redisClient.Close() }()

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
	var shardRouter *shardingpostgres.Router
	if cfg.BookingShardMode == config.BookingShardModeSchemaPOC {
		allowedShards, parseErr := sharding.ParseShardIDs(cfg.BookingShardIDs)
		if parseErr != nil {
			return errors.New("booking shard allowlist initialization failed")
		}
		cache, cacheErr := routecache.New(routecache.Config{
			Enabled: cfg.BookingRouteCacheEnabled, TTL: cfg.BookingRouteCacheTTL,
			MaxEntries: cfg.BookingRouteCacheMaxEntries,
		})
		if cacheErr != nil {
			return errors.New("booking route cache initialization failed")
		}
		shardRouter, err = shardingpostgres.NewRouter(
			pool,
			cache,
			shardingpostgres.WithMetrics(metrics),
			shardingpostgres.WithQueryTimeout(cfg.BookingShardQueryTimeout),
			shardingpostgres.WithAllowedShards(allowedShards...),
		)
		if err != nil {
			return errors.New("booking shard router initialization failed")
		}
	}
	queryStore, err := querypostgres.NewStore(pool)
	if err != nil {
		return errors.New("query store initialization failed")
	}
	if shardRouter != nil {
		queryStore, err = querypostgres.NewShardedStore(pool, shardRouter)
		if err != nil {
			return errors.New("sharded query store initialization failed")
		}
	}
	var querySource querycache.SourceStore = queryStore
	if physicalRouter != nil {
		querySource, err = querypostgres.NewPhysicalStore(queryStore, physicalRouter)
		if err != nil {
			return errors.New("physical availability store initialization failed")
		}
	}
	projectionStore, err := queryreadmodel.NewStore(pool, clock.RealClock{})
	if err != nil {
		return errors.New("read-model store initialization failed")
	}
	stopReadModelObserver := startReadModelObserver(
		signalContext, projectionStore, readMetrics, cfg.DatabaseTimeout, logger,
	)
	defer stopReadModelObserver()
	versionManager, err := querycache.NewSecureVersionManager(redisClient)
	if err != nil {
		return errors.New("cache version manager initialization failed")
	}
	stationTTL, err := querycache.NewTTLPolicy(cfg.StationCacheTTL, cfg.StationCacheJitter, rand.Reader)
	if err != nil {
		return errors.New("station cache TTL configuration invalid")
	}
	searchTTL, err := querycache.NewTTLPolicy(cfg.SearchCacheTTL, cfg.SearchCacheJitter, rand.Reader)
	if err != nil {
		return errors.New("search cache TTL configuration invalid")
	}
	availabilityTTL, err := querycache.NewTTLPolicy(cfg.AvailabilityCacheTTL, cfg.AvailabilityCacheJitter, rand.Reader)
	if err != nil {
		return errors.New("availability cache TTL configuration invalid")
	}
	cachedQueryStore, err := querycache.NewStore(
		querySource,
		projectionStore,
		redisClient,
		versionManager,
		clock.RealClock{},
		stationTTL,
		searchTTL,
		availabilityTTL,
	)
	if err != nil {
		return errors.New("read cache store initialization failed")
	}
	cachedQueryStore, err = cachedQueryStore.WithPolicy(querycache.Policy{
		StationEnabled:        cfg.StationCacheEnabled,
		SearchEnabled:         cfg.TrainSearchCacheEnabled,
		SearchFallbackEnabled: cfg.TrainSearchFallbackEnabled,
		AvailabilityEnabled:   cfg.AvailabilityCacheEnabled,
		AvailabilityMaxStale:  cfg.AvailabilityCacheMaxStale,
	})
	if err != nil {
		return errors.New("read cache policy invalid")
	}
	cachedQueryStore.WithMetrics(readMetrics)
	bookingStore := bookingpostgres.NewWithReservationQuotaLimits(pool, bookingpostgres.ReservationQuotaLimits{
		MaxActiveHoldsPerUser:            cfg.ReservationMaxActiveHoldsPerUser,
		MaxActiveHoldsPerUserPerTrainRun: cfg.ReservationMaxActiveHoldsPerUserPerTrainRun,
		MaxActivePassengersPerUser:       cfg.ReservationMaxActivePassengersPerUser,
	})
	if shardRouter != nil {
		bookingStore, err = bookingpostgres.NewShardedWithReservationQuotaLimits(
			pool,
			shardRouter,
			bookingpostgres.ReservationQuotaLimits{
				MaxActiveHoldsPerUser:            cfg.ReservationMaxActiveHoldsPerUser,
				MaxActiveHoldsPerUserPerTrainRun: cfg.ReservationMaxActiveHoldsPerUserPerTrainRun,
				MaxActivePassengersPerUser:       cfg.ReservationMaxActivePassengersPerUser,
			},
		)
		if err != nil {
			return errors.New("sharded booking store initialization failed")
		}
	}
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
	if shardRouter != nil {
		reads, err = app.NewShardedPostgresReads(pool, shardRouter)
		if err != nil {
			return errors.New("sharded booking reads initialization failed")
		}
	}
	var physicalCommands *app.HybridReservationCommands
	var physicalReads *app.HybridReservationReader
	ticketQueries := app.NewTicketQueries(reads)
	if physicalRouter != nil {
		controlCommands, commandErr := commandpostgres.NewRepository(pool, commandpostgres.Options{
			LeaseTTL:                   cfg.HoldTTL,
			MaxActiveHoldsPerUser:      cfg.ReservationMaxActiveHoldsPerUser,
			MaxActiveHoldsPerTrainRun:  cfg.ReservationMaxActiveHoldsPerUserPerTrainRun,
			MaxActivePassengersPerUser: cfg.ReservationMaxActivePassengersPerUser,
			Metrics:                    metrics,
		})
		if commandErr != nil {
			return errors.New("physical booking control initialization failed")
		}
		shardExecutor, commandErr := commandphysical.NewExecutor(physicalRouter, commandphysical.Options{MaxHoldTTL: cfg.HoldTTL})
		if commandErr != nil {
			return errors.New("physical booking executor initialization failed")
		}
		observedExecutor := observedPhysicalExecutor{next: shardExecutor, metrics: metrics}
		coordinator, commandErr := bookingcommand.NewCoordinator(controlCommands, observedExecutor)
		if commandErr != nil {
			return errors.New("physical booking coordinator initialization failed")
		}
		observedCoordinator := observedPhysicalCoordinator{next: coordinator, metrics: metrics}
		physicalCommands, commandErr = app.NewHybridReservationCommands(
			pool, bookingStore, observedCoordinator, physicalRouter,
		)
		if commandErr != nil {
			return errors.New("hybrid booking command initialization failed")
		}
		physicalReads, commandErr = app.NewHybridReservationReader(pool, reads, physicalRouter)
		if commandErr != nil {
			return errors.New("physical booking read initialization failed")
		}
		physicalTickets, ticketErr := app.NewHybridTicketReader(pool, reads, physicalRouter)
		if ticketErr != nil {
			return errors.New("physical ticket read initialization failed")
		}
		ticketQueries = app.NewTicketQueries(physicalTickets)
	}
	rateLimitBackend, err := redisx.NewRateLimiter(redisClient, "railway-api")
	if err != nil {
		return errors.New("rate limiter initialization failed")
	}

	reservationService := app.NewAdmissionProtectedReservationService(
		bookingStore, bookingStore, queryStore, reads, policyStore, admissionControl,
		admissionKeyring, executionSlots, passwordClock, cfg.HoldTTL,
		cfg.MaxPassengersPerReservation, metrics,
	).WithDatabaseCommandTimeout(cfg.DatabaseTimeout)
	if physicalCommands != nil && physicalReads != nil {
		reservationService = app.NewAdmissionProtectedReservationService(
			physicalCommands, physicalCommands, queryStore, physicalReads, policyStore, admissionControl,
			admissionKeyring, executionSlots, passwordClock, cfg.HoldTTL,
			cfg.MaxPassengersPerReservation, metrics,
		).WithDatabaseCommandTimeout(cfg.DatabaseTimeout)
	}

	readiness := app.NewReadinessChecker(pool, redisClient, cfg)
	if physicalRegistry != nil {
		readiness = app.NewReadinessChecker(pool, redisClient, cfg, physicalRegistry)
	}

	operatorCommands := app.NewOperatorCommands(offeringStore, bookingStore, metrics)
	var operatorBookingState httpapi.OperatorBookingStateQueries
	if physicalRouter != nil {
		operatorExecutor, operatorErr := commandphysical.NewExecutor(physicalRouter, commandphysical.Options{MaxHoldTTL: cfg.HoldTTL})
		if operatorErr != nil {
			return errors.New("physical operator command initialization failed")
		}
		cancellation, cancellationErr := app.NewPhysicalTrainRunCancellation(pool, operatorExecutor)
		if cancellationErr != nil {
			return errors.New("physical operator routing initialization failed")
		}
		operatorStore, storeErr := operatorpostgres.NewStore(pool)
		if storeErr != nil {
			return errors.New("physical operator command store initialization failed")
		}
		fareResolver, fareErr := app.NewPostgresOperatorCommandFareResolver(pool)
		if fareErr != nil {
			return errors.New("physical operator fare resolver initialization failed")
		}
		operatorShard, shardErr := app.NewPhysicalOperatorCommandShardExecutor(physicalRouter, fareResolver, operatorExecutor)
		if shardErr != nil {
			return errors.New("physical operator shard adapter initialization failed")
		}
		durableFinalizer, finalizerErr := app.NewPostgresDurableOperatorCommandFinalizer(pool)
		if finalizerErr != nil {
			return errors.New("physical operator command finalizer initialization failed")
		}
		operatorCoordinator, coordinatorErr := operatorcommand.NewCoordinator(operatorStore, operatorShard, durableFinalizer)
		if coordinatorErr != nil {
			return errors.New("physical operator coordinator initialization failed")
		}
		snapshotMutations, mutationErr := app.NewDurablePhysicalOperatorSnapshotMutations(operatorCoordinator)
		if mutationErr != nil {
			return errors.New("physical operator snapshot routing initialization failed")
		}
		operatorCommands = app.NewPhysicalOperatorCommandsWithSnapshots(offeringStore, bookingStore, cancellation, snapshotMutations, metrics)
		operatorBookingState, mutationErr = app.NewPhysicalOperatorBookingStateReader(pool, physicalRouter)
		if mutationErr != nil {
			return errors.New("physical operator state reader initialization failed")
		}
	}
	var (
		paymentService  httpapi.PaymentService
		ticketRefunds   httpapi.TicketRefundService
		paymentWebhooks httpapi.PaymentWebhookService
	)
	if cfg.PaymentEnabled {
		if physicalRouter == nil {
			return errors.New("payment requires physical booking shards")
		}
		deployment, paymentErr := regionalDeployment(cfg)
		if paymentErr != nil {
			return errors.New("regional payment authority invalid")
		}
		controlStore, paymentErr := paymentpostgres.NewStore(pool, paymentpostgres.WithRegionalAuthority(deployment))
		if paymentErr != nil {
			return errors.New("payment control initialization failed")
		}
		directory, paymentErr := paymentshardpostgres.NewDirectory(pool)
		if paymentErr != nil {
			return errors.New("payment directory initialization failed")
		}
		shardStore, paymentErr := paymentshardpostgres.NewStore(physicalRouter, paymentshardpostgres.WithRegionalAuthority(deployment))
		if paymentErr != nil {
			return errors.New("payment shard store initialization failed")
		}
		shardGateway, paymentErr := paymentshard.NewGateway(directory, shardStore)
		if paymentErr != nil {
			return errors.New("payment shard gateway initialization failed")
		}
		useCases := paymentapp.NewService(controlStore, shardGateway, time.Now, uuid.New).
			WithProcessingGrace(cfg.PaymentProcessingGrace).
			WithProvider(string(cfg.PaymentProviderType))
		paymentService = app.NewPaymentService(useCases, metrics)
		var providerDescriptor paymentprovider.Descriptor
		switch cfg.PaymentProviderType {
		case config.PaymentProviderSandbox:
			providerDescriptor = providerhttp.Descriptor()
		case config.PaymentProviderStripe:
			providerDescriptor = providerstripe.Descriptor()
		default:
			return errors.New("payment provider initialization failed")
		}
		if err := providerDescriptor.Require(paymentprovider.SagaCapabilities()); err != nil {
			return errors.New("payment provider capability contract unsupported")
		}
		refundStore, paymentErr := refundpostgres.NewStore(pool, shardGateway, refundpostgres.Config{
			PartialRefundProviders: map[string]bool{providerDescriptor.Name: providerDescriptor.Capabilities.PartialRefund},
			RegionalAuthority:      &deployment,
		})
		if paymentErr != nil {
			return errors.New("ticket refund store initialization failed")
		}
		refundUseCases, paymentErr := paymentrefund.NewService(refundStore, paymentrefund.Policy{
			CutoffBeforeDeparture: cfg.TicketRefundCutoff, MaxTickets: cfg.MaxPassengersPerReservation,
		})
		if paymentErr != nil {
			return errors.New("ticket refund initialization failed")
		}
		ticketRefunds = app.NewTicketRefundService(refundUseCases, metrics)

		webhookRepository, paymentErr := webhookpostgres.NewRepository(pool, webhookpostgres.WithRegionalAuthority(deployment))
		if paymentErr != nil {
			return errors.New("payment webhook repository initialization failed")
		}
		var verifier paymentwebhook.Verifier
		switch cfg.PaymentProviderType {
		case config.PaymentProviderSandbox:
			webhookKeys, keyErr := cfg.ParsePaymentWebhookKeys()
			if keyErr != nil {
				return errors.New("payment webhook keyring invalid")
			}
			providerClient, providerErr := providerhttp.New(providerhttp.Config{
				BaseURL: cfg.PaymentProviderBaseURL, ConnectTimeout: cfg.PaymentProviderConnectTimeout,
				RequestTimeout:      cfg.PaymentProviderRequestTimeout,
				MaxResponseBytes:    int64(cfg.PaymentProviderMaxResponseBytes),
				MaxWebhookBodyBytes: int64(cfg.PaymentWebhookMaxBodyBytes), WebhookKeys: webhookKeys,
				WebhookClockSkew: cfg.PaymentWebhookClockSkew,
			})
			if providerErr != nil {
				return errors.New("payment provider initialization failed")
			}
			defer providerClient.CloseIdleConnections()
			verifier = providerClient
		case config.PaymentProviderStripe:
			keyIDs, secrets, keyErr := cfg.ParseStripeWebhookSecrets()
			if keyErr != nil {
				return errors.New("payment webhook keyring invalid")
			}
			rotationNow := time.Now().UTC()
			secretProofs := make(map[string][32]byte, len(keyIDs))
			for index, keyID := range keyIDs {
				secretProofs[keyID] = paymentwebhook.KeyProof(string(config.PaymentProviderStripe), cfg.PaymentProviderAccountID, keyID, secrets[index])
			}
			plan, keyErr := webhookRepository.SynchronizeKeyring(signalContext, string(config.PaymentProviderStripe), cfg.PaymentProviderAccountID, paymentwebhook.DesiredKeyring{
				PrimaryKeyID: cfg.PaymentWebhookPrimaryKeyID, AcceptedKeyIDs: keyIDs,
				SecretProofs: secretProofs, Grace: cfg.PaymentWebhookKeyRetirementGrace, Now: rotationNow,
			})
			if keyErr != nil {
				return errors.New("payment webhook keyring metadata synchronization failed")
			}
			secretNotAfter, keyErr := stripeWebhookSecretDeadlines(keyIDs, plan)
			if keyErr != nil {
				return errors.New("payment webhook keyring metadata invalid")
			}
			stripeVerifier, providerErr := providerstripe.NewWebhookVerifier(providerstripe.WebhookConfig{
				MaxBodyBytes: int64(cfg.PaymentWebhookMaxBodyBytes), Tolerance: cfg.PaymentWebhookClockSkew,
				Secrets: secrets, SecretIDs: keyIDs, SecretNotAfter: secretNotAfter, Now: time.Now,
				AccountID: cfg.PaymentProviderAccountID,
				LiveMode:  cfg.Environment == config.EnvironmentProduction,
			})
			if providerErr != nil {
				return errors.New("payment provider initialization failed")
			}
			verifier = stripeVerifier
			readiness.WithProbe("webhook_keyring", func(ctx context.Context) error {
				readinessDesired := paymentwebhook.DesiredKeyring{
					PrimaryKeyID: cfg.PaymentWebhookPrimaryKeyID, AcceptedKeyIDs: append([]string(nil), keyIDs...),
					SecretProofs: secretProofs, Grace: cfg.PaymentWebhookKeyRetirementGrace, Now: time.Now().UTC(),
				}
				return webhookRepository.ValidateKeyring(ctx, string(config.PaymentProviderStripe), cfg.PaymentProviderAccountID, readinessDesired)
			})
		default:
			return errors.New("payment provider initialization failed")
		}
		paymentWebhooks, paymentErr = paymentwebhook.NewService(paymentwebhook.Config{
			Providers:  map[string]paymentwebhook.Verifier{string(cfg.PaymentProviderType): verifier},
			Repository: webhookRepository, MaxBodyBytes: cfg.PaymentWebhookMaxBodyBytes,
			Now: time.Now, NewID: uuid.New, Metrics: metrics, Region: string(cfg.DeploymentRegion), Keyring: webhookRepository,
		})
		if paymentErr != nil {
			return errors.New("payment webhook initialization failed")
		}
	}
	router := httpapi.New(httpapi.Dependencies{
		Readiness:                  readiness,
		ReadinessTimeout:           readinessTimeout(cfg),
		PhysicalRequestTimeout:     physicalRequestTimeout,
		TokenParser:                tokenParser,
		Reservations:               reservationService,
		Payments:                   paymentService,
		TicketRefunds:              ticketRefunds,
		PaymentWebhooks:            paymentWebhooks,
		PaymentWebhookMaxBodyBytes: int64(cfg.PaymentWebhookMaxBodyBytes),
		PaymentWebhookTimeout:      cfg.DatabaseTimeout,
		PaymentRequiredForConfirm:  cfg.PaymentEnabled,
		WaitingRoom:                app.NewWaitingRoomService(policyStore, queryStore, admissionControl, admissionKeyring, metrics),
		HotTrainPolicies:           app.NewHotTrainPolicyService(policyStore),
		MaxRequestBodyBytes:        maxRequestBodyBytes,
		MaxPassengers:              cfg.MaxPassengersPerReservation,
		HTTPMetrics:                metrics,
		MetricsHandler:             promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
		Offering:                   app.NewOfferingQueries(cachedQueryStore),
		Auth:                       auth,
		RateLimiter:                app.NewRateLimiter(rateLimitBackend),
		Passengers:                 passengers,
		Tickets:                    ticketQueries,
		Admin:                      app.NewAdminCommands(offeringStore),
		Operator:                   operatorCommands,
		OperatorBookingState:       operatorBookingState,
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

func stripeWebhookSecretDeadlines(keyIDs []string, plan paymentwebhook.KeyringPlan) ([]time.Time, error) {
	deadlines := make([]time.Time, len(keyIDs))
	for index, keyID := range keyIDs {
		version, found := plan.ByID[keyID]
		if !found {
			return nil, paymentwebhook.ErrKeyringConflict
		}
		switch version.State {
		case paymentwebhook.KeyPrimary:
			if version.RetirementNotBefore != nil || version.RetiredAt != nil {
				return nil, paymentwebhook.ErrKeyringConflict
			}
		case paymentwebhook.KeyAccepted:
			if version.RetiredAt != nil {
				return nil, paymentwebhook.ErrKeyringConflict
			}
			if version.RetirementNotBefore != nil {
				deadlines[index] = version.RetirementNotBefore.UTC()
			}
		default:
			return nil, paymentwebhook.ErrKeyringConflict
		}
	}
	return deadlines, nil
}

func regionalSession(cfg config.Config) postgresx.RegionalSession {
	return postgresx.RegionalSession{
		Region: string(cfg.DeploymentRegion), Role: string(cfg.DeploymentRole),
		Epoch: cfg.RegionEpoch, WritesEnabled: cfg.RegionalWritesEnabled,
	}
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

func regionalDeployment(cfg config.Config) (authority.Deployment, error) {
	region, err := authority.ParseRegion(string(cfg.DeploymentRegion))
	if err != nil || cfg.RegionEpoch <= 0 {
		return authority.Deployment{}, authority.ErrInvalidDeployment
	}
	epoch, err := authority.NewEpoch(uint64(cfg.RegionEpoch))
	if err != nil {
		return authority.Deployment{}, err
	}
	role := authority.Role(cfg.DeploymentRole)
	return authority.NewDeployment(region, role, epoch, cfg.RegionalWritesEnabled)
}

func startReadModelObserver(
	parent context.Context,
	store readModelOperationalStore,
	metrics readModelOperationalMetrics,
	timeout time.Duration,
	logger *slog.Logger,
) func() {
	observerContext, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		lagTicker := time.NewTicker(projectionLagObservationInterval)
		defer lagTicker.Stop()
		reconciliationTicker := time.NewTicker(reconciliationObservationInterval)
		defer reconciliationTicker.Stop()
		cursor := ""
		observeLag := func() {
			ctx, stop := context.WithTimeout(observerContext, timeout)
			defer stop()
			lag, err := store.ProjectionLag(ctx, queryreadmodel.DurableConsumerName)
			if err != nil {
				logger.Warn("read-model observation failed", "operation", "projection_lag")
				return
			}
			metrics.SetProjectionLag(lag)
		}
		observeReconciliation := func() {
			ctx, stop := context.WithTimeout(observerContext, timeout)
			defer stop()
			candidate, found, err := store.NextReconciliationTrainRun(ctx, cursor)
			if err != nil {
				logger.Warn("read-model observation failed", "operation", "reconciliation_candidate")
				return
			}
			if !found {
				cursor = ""
				return
			}
			result, err := store.ReconcileTrainRun(ctx, candidate)
			if err != nil {
				logger.Warn("read-model observation failed", "operation", "reconciliation")
				return
			}
			recordReconciliationMismatches(metrics, result)
			cursor = candidate
		}
		observeLag()
		observeReconciliation()
		for {
			select {
			case <-observerContext.Done():
				return
			case <-lagTicker.C:
				observeLag()
			case <-reconciliationTicker.C:
				observeReconciliation()
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func recordReconciliationMismatches(metrics readModelOperationalMetrics, result queryreadmodel.ReconcileResult) {
	metrics.AddReconciliationMismatches("missing", result.MissingRows)
	metrics.AddReconciliationMismatches("extra", result.ExtraRows)
	metrics.AddReconciliationMismatches("duplicate", result.DuplicateRows)
	metrics.AddReconciliationMismatches("stale", result.StaleRows)
	metrics.AddReconciliationMismatches("mismatch", result.MismatchedRows)
	metrics.AddReconciliationMismatches("invalid", result.InvalidRows)
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
