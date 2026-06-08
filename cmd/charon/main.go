package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/elevran/charon/internal/api"
	"github.com/elevran/charon/internal/config"
	"github.com/elevran/charon/internal/metrics"
	"github.com/elevran/charon/internal/storage"
	"github.com/elevran/charon/internal/storage/filesystem"
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
		idx storage.IndexStore
		pay storage.PayloadStore
	)

	switch cfg.Storage.Backend {
	case "sqlite":
		if err := os.MkdirAll(cfg.Storage.DataDir, 0o755); err != nil {
			log.Error("create data dir", "err", err)
			os.Exit(1)
		}
		dbPath := filepath.Join(cfg.Storage.DataDir, "responses.db")
		db, err := sqlitestore.Open(dbPath, sqlitestore.Config{
			WALMode:       cfg.Storage.SQLite.WALMode,
			BusyTimeoutMs: cfg.Storage.SQLite.BusyTimeoutMs,
		})
		if err != nil {
			log.Error("open sqlite", "path", dbPath, "err", err)
			os.Exit(1)
		}
		defer sqlitestore.Close(db)

		payDir := filepath.Join(cfg.Storage.DataDir, "payloads")
		fsStore, err := filesystem.New(payDir)
		if err != nil {
			log.Error("open filesystem store", "dir", payDir, "err", err)
			os.Exit(1)
		}
		idx = sqlitestore.NewIndexStore(db)
		pay = fsStore
	default: // "memory"
		idx = memory.NewIndexStore()
		pay = memory.NewPayloadStore()
	}

	svcCfg := store.Config{
		CheckpointInterval: cfg.Storage.CheckpointInterval,
		TTLDays:            cfg.Storage.TTLDays,
	}
	svc := store.New(idx, pay, svcCfg, log)

	reg := prometheus.NewRegistry()
	if err := metrics.Register(reg, ""); err != nil {
		log.Error("register metrics", "err", err)
		os.Exit(1)
	}

	h := api.NewHandler(svc, log)
	srv := api.NewServerWithRegistry(cfg.Server.Listen, h, log, reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go worker.NewCleaner(idx, pay, log, cfg.Workers.TTLInterval).Run(ctx)
	go worker.NewReconciler(idx, pay, log, cfg.Storage.WriteIntentStaleThreshold, cfg.Workers.RecoveryInterval).Run(ctx)

	go func() {
		log.Info("starting server", "addr", cfg.Server.Listen)
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			log.Error("server error", "err", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	log.Info("shutting down")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("shutdown error", "err", err)
	}
}
