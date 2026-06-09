package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
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
	"github.com/elevran/charon/internal/storage/memory"
	sqlitestore "github.com/elevran/charon/internal/storage/sqlite"
	"github.com/elevran/charon/internal/store"
	"github.com/elevran/charon/internal/worker"
)

func main() {
	configPath := flag.String("config", "", "path to config file")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
	}

	var (
		idx     storage.IndexStore
		pay     storage.PayloadStore
		cleanup func() error
	)

	switch cfg.Storage.Backend {
	case "sqlite":
		var err error
		idx, pay, cleanup, err = sqlitestore.Open(cfg.Storage, log)
		if err != nil {
			log.Error("open sqlite storage", "err", err)
			os.Exit(1)
		}
		defer func() { _ = cleanup() }()
	default: // "memory"
		idx, pay = memory.Open()
	}

	svcCfg := store.Config{
		CheckpointInterval: cfg.Storage.CheckpointInterval,
		TTLDays:            cfg.Storage.TTLDays,
	}
	svc := store.New(idx, pay, svcCfg, log)

	reg := prometheus.NewRegistry()
	if err := metrics.Register(reg, ""); err != nil {
		log.Error("register metrics", "err", err)
		os.Exit(1) //nolint:gocritic // exitAfterDefer: SQLite close is intentionally skipped on startup failure
	}

	// ── Charon internal API server (port 8081 by default) ──────────────────
	charonH := api.NewHandler(svc, log)
	charonSrv := api.NewServerWithRegistry(cfg.Charon.Listen, charonH, log, reg)

	// ── Proxy server (port 8080 by default) ────────────────────────────────
	timeout := time.Duration(cfg.Inference.TimeoutSeconds) * time.Second
	infClient := inference.New(cfg.Inference.BaseURL, cfg.Inference.APIKey, timeout)
	charonClient := charonpkg.New(cfg.Charon.BaseURL, timeout)
	proxyH := proxy.NewHandler(charonClient, infClient, log, cfg.Inference.StoreBufferBytes)
	proxyMux := http.NewServeMux()
	proxy.RegisterHandlers(proxyMux, proxyH)
	proxySrv := api.NewServerFromMux(cfg.Server.Listen, proxyMux, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go worker.NewCleaner(idx, pay, log, cfg.Workers.TTLInterval).Run(ctx)
	go worker.NewReconciler(idx, pay, log, cfg.Storage.WriteIntentStaleThreshold, cfg.Workers.RecoveryInterval).Run(ctx)

	go func() {
		log.Info("starting charon internal API", "addr", cfg.Charon.Listen)
		if err := charonSrv.Start(); err != nil && err != http.ErrServerClosed {
			log.Error("charon server error", "err", err)
		}
	}()
	go func() {
		log.Info("starting proxy server", "addr", cfg.Server.Listen)
		if err := proxySrv.Start(); err != nil && err != http.ErrServerClosed {
			log.Error("proxy server error", "err", err)
		}
	}()

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
	if err := proxySrv.Shutdown(shutdownCtx); err != nil {
		log.Error("proxy shutdown error", "err", err)
	}
}
