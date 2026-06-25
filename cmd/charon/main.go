package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/elevran/charon/internal/api"
	charonpkg "github.com/elevran/charon/internal/charon"
	"github.com/elevran/charon/internal/config"
	"github.com/elevran/charon/internal/inference"
	"github.com/elevran/charon/internal/metrics"
	"github.com/elevran/charon/internal/proxy"
	"github.com/elevran/charon/internal/storage"
	"github.com/elevran/charon/internal/storage/filesystem"
	"github.com/elevran/charon/internal/storage/memory"
	pgstore "github.com/elevran/charon/internal/storage/postgres"
	s3store "github.com/elevran/charon/internal/storage/s3"
	sqlitestore "github.com/elevran/charon/internal/storage/sqlite"
	"github.com/elevran/charon/internal/store"
	"github.com/elevran/charon/internal/telemetry"
	"github.com/elevran/charon/internal/worker"
)

func main() {
	// Subcommand dispatch: "charon reconcile --config ..." runs a single
	// write-intent recovery sweep then exits. Any other invocation (or no
	// subcommand) starts the full server.
	subcmd := ""
	args := os.Args[1:]
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		subcmd = args[0]
		args = args[1:]
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if subcmd == "reconcile" {
		opts := config.NewReconcileOptions()
		fs := flag.NewFlagSet("charon reconcile", flag.ExitOnError)
		opts.AddFlags(fs)
		_ = fs.Parse(args)
		if err := opts.Complete(fs); err != nil {
			log.Error("complete reconcile config", "err", err)
			os.Exit(1)
		}
		if err := opts.Validate(); err != nil {
			log.Error("validate reconcile config", "err", err)
			os.Exit(1)
		}
		runReconcile(log, opts)
		return
	}

	opts := config.NewServerOptions()
	fs := flag.NewFlagSet("charon", flag.ExitOnError)
	opts.AddFlags(fs)
	_ = fs.Parse(args)
	if err := opts.Complete(fs); err != nil {
		log.Error("complete server config", "err", err)
		os.Exit(1)
	}
	if err := opts.Validate(); err != nil {
		log.Error("validate server config", "err", err)
		os.Exit(1)
	}

	idx, pay, cleanupFn := openStorage(opts.Storage.ToStorageConfig(), log)
	if cleanupFn != nil {
		defer func() { _ = cleanupFn() }()
	}

	// Initialise OTel tracing (no-op when ExporterURL is empty).
	charonTP, err := telemetry.Init(context.Background(), opts.Telemetry.CharonService, opts.Telemetry.ExporterURL)
	if err != nil {
		log.Error("init charon tracer", "err", err)
		os.Exit(1) //nolint:gocritic
	}
	if charonTP != nil {
		defer func() {
			shutdownCtx2, c2 := context.WithTimeout(context.Background(), 5*time.Second)
			defer c2()
			_ = charonTP.Shutdown(shutdownCtx2)
		}()
	}

	var proxyTP *sdktrace.TracerProvider
	if opts.ProxyEnabled {
		proxyTP, err = telemetry.Init(context.Background(), opts.Telemetry.ProxyService, opts.Telemetry.ExporterURL)
		if err != nil {
			log.Error("init proxy tracer", "err", err)
			os.Exit(1) //nolint:gocritic
		}
		if proxyTP != nil {
			defer func() {
				shutdownCtx3, c3 := context.WithTimeout(context.Background(), 5*time.Second)
				defer c3()
				_ = proxyTP.Shutdown(shutdownCtx3)
			}()
		}
	}

	svcCfg := store.Config{
		CheckpointInterval: opts.Storage.CheckpointInterval,
		TTLDays:            opts.Storage.TTLDays,
		MaxResponses:       opts.Storage.MaxResponses,
		MaxPayloadBytes:    int64(opts.Storage.MaxPayload),
		MaxChainDepth:      opts.Storage.MaxChainDepth,
		MaxContextBytes:    int64(opts.Storage.MaxContextBytes),
		TracerProvider:     charonTP,
	}
	svc := store.New(idx, pay, svcCfg, log)

	reg := prometheus.NewRegistry()
	if err := metrics.Register(reg, ""); err != nil {
		log.Error("register metrics", "err", err)
		os.Exit(1) //nolint:gocritic
	}

	// ── Charon internal API server (always starts) ─────────────────────────
	charonH := api.NewHandler(svc, log)
	charonSrv := api.NewServerWithRegistry(opts.CharonListen, charonH, log, reg, charonTP)

	// ── Proxy server (starts only when --proxy / proxy.enabled: true) ──────
	var proxySrv *api.Server
	if opts.ProxyEnabled {
		timeout := time.Duration(opts.InferenceTimeoutSeconds) * time.Second
		infClient := inference.New(opts.ProxyBackend, opts.ProxyAPIKey, timeout)
		charonClient := charonpkg.New(opts.ProxyCharonURL, timeout)
		proxyH := proxy.NewHandler(charonClient, infClient, log, opts.InferenceStoreBufferBytes)
		proxyMux := http.NewServeMux()
		proxy.RegisterHandlers(proxyMux, proxyH)
		proxySrv = api.NewServerFromMux(opts.ProxyListen, proxyMux, log, proxyTP)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var workerWG sync.WaitGroup
	workerWG.Add(2)
	go func() {
		defer workerWG.Done()
		worker.NewCleanerWithEviction(idx, pay, log, opts.WorkerTTLInterval,
			opts.Storage.MaxResponses, opts.Storage.EvictionHighWatermark).Run(ctx)
	}()
	go func() {
		defer workerWG.Done()
		worker.NewReconciler(idx, pay, log,
			opts.Storage.WriteIntentStaleThreshold,
			opts.WorkerRecoveryInterval).Run(ctx)
	}()

	go func() {
		log.Info("starting charon internal API", "addr", opts.CharonListen)
		if err := charonSrv.Start(); err != nil && err != http.ErrServerClosed {
			log.Error("charon server error", "err", err)
		}
	}()
	if opts.ProxyEnabled {
		go func() {
			log.Info("starting proxy server", "addr", opts.ProxyListen)
			if err := proxySrv.Start(); err != nil && err != http.ErrServerClosed {
				log.Error("proxy server error", "err", err)
			}
		}()
	} else {
		log.Info("proxy layer disabled — set proxy.enabled: true to enable")
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	log.Info("shutting down")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := charonSrv.Shutdown(shutdownCtx); err != nil {
		log.Error("charon shutdown error", "err", err)
	}
	if opts.ProxyEnabled {
		if err := proxySrv.Shutdown(shutdownCtx); err != nil {
			log.Error("proxy shutdown error", "err", err)
		}
	}

	workerDone := make(chan struct{})
	go func() { workerWG.Wait(); close(workerDone) }()
	select {
	case <-workerDone:
		log.Info("workers stopped cleanly")
	case <-shutdownCtx.Done():
		log.Warn("timed out waiting for workers to stop")
	}
}

// openStorage resolves the effective index and payload backends from cfg and
// returns an IndexStore, PayloadStore, and an optional cleanup function.
// Backend resolution order:
//  1. If index_backend / payload_backend are explicitly set, use them independently.
//  2. Otherwise fall back to the legacy backend field:
//     "sqlite"      → sqlite index + filesystem payloads
//     "postgres"    → postgres index + filesystem payloads
//     "postgres+s3" → postgres index + s3 payloads
//     "memory"      → in-memory index + in-memory payloads (default)
func openStorage(cfg config.StorageConfig, log *slog.Logger) (storage.IndexStore, storage.PayloadStore, func() error) {
	// Resolve effective backend names.
	indexBackend := cfg.IndexBackend
	payloadBackend := cfg.PayloadBackend

	if indexBackend == "" || payloadBackend == "" {
		// Derive from the legacy Backend field.
		switch cfg.Backend {
		case "sqlite":
			if indexBackend == "" {
				indexBackend = "sqlite"
			}
			if payloadBackend == "" {
				payloadBackend = "filesystem"
			}
		case "postgres":
			if indexBackend == "" {
				indexBackend = "postgres"
			}
			if payloadBackend == "" {
				payloadBackend = "filesystem"
			}
		case "postgres+s3":
			if indexBackend == "" {
				indexBackend = "postgres"
			}
			if payloadBackend == "" {
				payloadBackend = "s3"
			}
		default: // "memory"
			if indexBackend == "" {
				indexBackend = "memory"
			}
			if payloadBackend == "" {
				payloadBackend = "memory"
			}
		}
	}

	var (
		idx      storage.IndexStore
		pay      storage.PayloadStore
		cleanups []func() error
	)

	// Open index backend.
	switch indexBackend {
	case "sqlite":
		sqliteIdx, sqlitePay, cleanup, err := sqlitestore.Open(cfg, log)
		if err != nil {
			log.Error("open sqlite storage", "err", err)
			os.Exit(1)
		}
		cleanups = append(cleanups, cleanup)
		idx = sqliteIdx
		// If the payload backend is also filesystem (common case with sqlite),
		// reuse the sqlite-created filesystem store to avoid duplicating payDir logic.
		if payloadBackend == "filesystem" {
			pay = sqlitePay
		}
	case "postgres":
		pgIdx, cleanup, err := pgstore.OpenIndex(cfg, log)
		if err != nil {
			log.Error("open postgres index store", "err", err)
			os.Exit(1)
		}
		cleanups = append(cleanups, cleanup)
		idx = pgIdx
	default: // "memory"
		memIdx, memPay := memory.Open()
		idx = memIdx
		if payloadBackend == "memory" {
			pay = memPay
		}
	}

	// Open payload backend (only if not already set by the index case above).
	if pay == nil {
		switch payloadBackend {
		case "s3":
			s3Pay, err := s3store.Open(cfg)
			if err != nil {
				log.Error("open s3 payload store", "err", err)
				os.Exit(1)
			}
			pay = s3Pay
		case "filesystem":
			fsDir := cfg.DataDir + "/payloads"
			fsStore, err := filesystem.New(fsDir)
			if err != nil {
				log.Error("open filesystem payload store", "err", err, "dir", fsDir)
				os.Exit(1)
			}
			pay = fsStore
		default: // "memory"
			_, memPay := memory.Open()
			pay = memPay
		}
	}

	combined := func() error {
		var last error
		for _, fn := range cleanups {
			if err := fn(); err != nil {
				last = err
			}
		}
		return last
	}
	return idx, pay, combined
}

// runReconcile opens storage, runs a single write-intent recovery sweep, and exits.
func runReconcile(log *slog.Logger, opts *config.ReconcileOptions) {
	idx, pay, cleanup := openStorage(opts.Storage.ToStorageConfig(), log)
	if cleanup != nil {
		defer func() { _ = cleanup() }()
	}

	ctx := context.Background()
	r := worker.NewReconciler(idx, pay, log,
		opts.Storage.WriteIntentStaleThreshold,
		5*time.Minute) // RecoveryInterval not relevant for one-shot run
	r.RunOnce(ctx)
	log.Info("reconcile sweep complete")
}
