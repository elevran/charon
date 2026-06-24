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
	fs := flag.NewFlagSet("charon", flag.ExitOnError)
	configPath := fs.String("config", "", "path to config file")
	_ = fs.Parse(args)

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if subcmd == "reconcile" {
		runReconcile(log, *configPath)
		return
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
	}

	idx, pay, cleanupFn := openStorage(cfg.Charon.Storage, log)
	if cleanupFn != nil {
		defer func() { _ = cleanupFn() }()
	}

	svcCfg := store.Config{
		CheckpointInterval: cfg.Charon.Storage.CheckpointInterval,
		TTLDays:            cfg.Charon.Storage.TTLDays,
		MaxResponses:       cfg.Charon.Storage.MaxResponses,
		MaxPayloadBytes:    int64(cfg.Charon.Storage.MaxPayload),
	}
	svc := store.New(idx, pay, svcCfg, log)

	reg := prometheus.NewRegistry()
	if err := metrics.Register(reg, ""); err != nil {
		log.Error("register metrics", "err", err)
		os.Exit(1) //nolint:gocritic
	}

	// ── Charon internal API server (always starts) ─────────────────────────
	charonH := api.NewHandler(svc, log)
	charonSrv := api.NewServerWithRegistry(cfg.Charon.Listen, charonH, log, reg)

	// ── Proxy server (starts only when proxy.enabled: true) ────────────────
	var proxySrv *api.Server
	if cfg.Proxy.Enabled {
		timeout := time.Duration(cfg.Proxy.Inference.TimeoutSeconds) * time.Second
		infClient := inference.New(cfg.Proxy.Inference.BaseURL, cfg.Proxy.Inference.APIKey, timeout)
		charonClient := charonpkg.New(cfg.Proxy.CharonURL, timeout)
		proxyH := proxy.NewHandler(charonClient, infClient, log, cfg.Proxy.Inference.StoreBufferBytes)
		proxyMux := http.NewServeMux()
		proxy.RegisterHandlers(proxyMux, proxyH)
		proxySrv = api.NewServerFromMux(cfg.Proxy.Listen, proxyMux, log)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var workerWG sync.WaitGroup
	workerWG.Add(2)
	go func() {
		defer workerWG.Done()
		worker.NewCleaner(idx, pay, log, cfg.Charon.Workers.TTLInterval).Run(ctx)
	}()
	go func() {
		defer workerWG.Done()
		worker.NewReconciler(idx, pay, log,
			cfg.Charon.Storage.WriteIntentStaleThreshold,
			cfg.Charon.Workers.RecoveryInterval).Run(ctx)
	}()

	go func() {
		log.Info("starting charon internal API", "addr", cfg.Charon.Listen)
		if err := charonSrv.Start(); err != nil && err != http.ErrServerClosed {
			log.Error("charon server error", "err", err)
		}
	}()
	if cfg.Proxy.Enabled {
		go func() {
			log.Info("starting proxy server", "addr", cfg.Proxy.Listen)
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
	if cfg.Proxy.Enabled {
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
// Usage: charon reconcile --config config.yaml
func runReconcile(log *slog.Logger, configPath string) {
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
	}

	idx, pay, cleanup := openStorage(cfg.Charon.Storage, log)
	if cleanup != nil {
		defer func() { _ = cleanup() }()
	}

	ctx := context.Background()
	r := worker.NewReconciler(idx, pay, log,
		cfg.Charon.Storage.WriteIntentStaleThreshold,
		cfg.Charon.Workers.RecoveryInterval)
	r.RunOnce(ctx)
	log.Info("reconcile sweep complete")
}
