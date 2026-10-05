// Package app wires the common runtime concerns of every Orbit service:
// configuration, logging, tracing, metrics, database, migrations, broker, HTTP
// health endpoints, and graceful shutdown.
//
// Service-specific business wiring lives in each cmd package; this package owns
// only the cross-cutting bootstrap so services stay small and consistent.
package app

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/sync/errgroup"

	"github.com/example/orbit/internal/platform/broker"
	"github.com/example/orbit/internal/platform/config"
	"github.com/example/orbit/internal/platform/httpx"
	"github.com/example/orbit/internal/platform/logger"
	"github.com/example/orbit/internal/platform/metrics"
	"github.com/example/orbit/internal/platform/migrate"
	"github.com/example/orbit/internal/platform/postgres"
	"github.com/example/orbit/internal/platform/telemetry"
)

// App is the runtime container for a single service.
type App struct {
	Cfg      config.Config
	Log      *slog.Logger
	Metrics  *metrics.Metrics
	Pool     *pgxpool.Pool
	Producer *broker.Producer
	Mux      *http.ServeMux

	health    *httpx.Health
	tasks     []namedTask
	shutdowns []func(context.Context) error
}

type namedTask struct {
	name string
	fn   func(context.Context) error
}

// Options controls bootstrap behaviour for a service.
type Options struct {
	// Migrations, when set, is embedded and applied at startup.
	Migrations     fs.FS
	MigrationsRoot string
	RunMigrations  bool
}

// New bootstraps a service. It is the only place that touches global process
// state (tracer provider, signal handling is deferred to Run).
func New(ctx context.Context, service string, opts Options) (*App, error) {
	cfg, err := config.Load(service)
	if err != nil {
		return nil, err
	}

	log := logger.New(cfg.LogLevel, cfg.LogFormat)
	log = log.With(slog.String("service", cfg.ServiceName), slog.String("env", cfg.Environment))

	shutdownTelemetry, err := telemetry.Setup(ctx, cfg.ServiceName, cfg.Environment, cfg.OTLPEndpoint)
	if err != nil {
		return nil, err
	}

	pool, err := postgres.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		_ = shutdownTelemetry(ctx)
		return nil, err
	}

	if opts.RunMigrations && opts.Migrations != nil {
		if err := migrate.Up(opts.Migrations, opts.MigrationsRoot, cfg.DatabaseURL); err != nil {
			pool.Close()
			_ = shutdownTelemetry(ctx)
			return nil, err
		}
		log.InfoContext(ctx, "migrations applied")
	}

	producer := broker.NewProducer(cfg.KafkaBrokers)
	m := metrics.New(cfg.ServiceName)

	a := &App{
		Cfg:      cfg,
		Log:      log,
		Metrics:  m,
		Pool:     pool,
		Producer: producer,
		Mux:      http.NewServeMux(),
		health:   httpx.NewHealth(),
	}

	a.health.Add("postgres", func(ctx context.Context) error { return pool.Ping(ctx) })
	a.health.Add("kafka", func(ctx context.Context) error { return broker.Ping(ctx, cfg.KafkaBrokers) })

	a.Mux.HandleFunc("GET /healthz", a.health.Live)
	a.Mux.HandleFunc("GET /readyz", a.health.Ready)
	a.Mux.Handle("GET /metrics", m.Handler())

	a.shutdowns = append(a.shutdowns, func(ctx context.Context) error {
		return shutdownTelemetry(ctx)
	})
	return a, nil
}

// Health exposes the readiness registry so services can add checks.
func (a *App) Health() *httpx.Health { return a.health }

// AddTask registers a long-running background job. Tasks must return when the
// context is cancelled.
func (a *App) AddTask(name string, fn func(context.Context) error) {
	a.tasks = append(a.tasks, namedTask{name: name, fn: fn})
}

// Run starts the HTTP server and all registered tasks, then blocks until the
// context is cancelled or a task fails. It performs an orderly shutdown.
func (a *App) Run(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Outermost first: correlation IDs and recovery wrap access logging and
	// metrics, which in turn wrap the instrumented mux.
	mux := httpx.Chain(
		otelhttp.NewHandler(a.Mux, a.Cfg.ServiceName),
		httpx.RequestID(),
		httpx.AccessLog(a.Log),
		accessMetrics(a.Metrics),
		httpx.Recovery(a.Log),
	)

	server := &http.Server{
		Addr:              a.Cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		a.Log.InfoContext(gctx, "http server listening", slog.String("addr", a.Cfg.HTTPAddr))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	})

	for _, task := range a.tasks {
		task := task
		g.Go(func() error {
			a.Log.InfoContext(gctx, "task started", slog.String("task", task.name))
			if err := task.fn(gctx); err != nil && gctx.Err() == nil {
				return fmt.Errorf("task %s: %w", task.name, err)
			}
			return nil
		})
	}

	g.Go(func() error {
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			a.Log.ErrorContext(ctx, "http shutdown error", slog.Any("error", err))
		}
		return nil
	})

	err := g.Wait()
	a.Close(context.Background())
	return err
}

// Close releases resources. It is safe to call multiple times.
func (a *App) Close(ctx context.Context) {
	if err := a.Producer.Close(); err != nil {
		a.Log.WarnContext(ctx, "producer close error", slog.Any("error", err))
	}
	for _, fn := range a.shutdowns {
		if err := fn(ctx); err != nil {
			a.Log.WarnContext(ctx, "shutdown hook error", slog.Any("error", err))
		}
	}
	if a.Pool != nil {
		a.Pool.Close()
	}
}

func accessMetrics(m *metrics.Metrics) httpx.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			next.ServeHTTP(w, r)
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			m.HTTPRequestMillis.WithLabelValues(r.Method, route, statusClass(w)).Observe(time.Since(start).Seconds())
		})
	}
}

func statusClass(w http.ResponseWriter) string {
	// AccessLog installs its own recorder; for metrics we only need the class.
	if sw, ok := w.(interface{ Status() int }); ok {
		return statusBucket(sw.Status())
	}
	return "2xx"
}

func statusBucket(status int) string {
	switch {
	case status >= 500:
		return "5xx"
	case status >= 400:
		return "4xx"
	case status >= 300:
		return "3xx"
	default:
		return "2xx"
	}
}
