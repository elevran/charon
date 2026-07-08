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

	"github.com/elevran/charon/internal/chainstore"
	pebblebe "github.com/elevran/charon/internal/chainstore/pebble"
	"github.com/elevran/charon/internal/charonconfig"
	"github.com/elevran/charon/internal/server"
	"github.com/elevran/charon/internal/telemetry"
)

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	opts := charonconfig.NewCharonOptions()
	fs := flag.NewFlagSet("charon", flag.ExitOnError)
	opts.AddFlags(fs)
	_ = fs.Parse(os.Args[1:])
	if err := opts.Complete(fs); err != nil {
		log.Error("complete server config", "err", err)
		return err
	}
	if err := opts.Validate(); err != nil {
		log.Error("validate server config", "err", err)
		return err
	}

	reg := prometheus.NewRegistry()
	if err := server.RegisterMetrics(reg, ""); err != nil {
		log.Error("register metrics", "err", err)
		return err
	}

	cfg := chainstore.Config{
		MaxEntries:  opts.MaxResponses,
		MaxBytes:    int64(opts.MaxPayload),
		TTL:         time.Duration(opts.TTLDays) * 24 * time.Hour,
		TTLInterval: opts.WorkerTTLInterval,
		Registerer:  reg,
		Log:         log,
	}

	svc, err := pebblebe.Open(context.Background(), opts.DataDir, nil, cfg)
	if err != nil {
		log.Error("open chainstore", "err", err)
		return err
	}
	defer func() { _ = svc.Close() }()

	tp, err := telemetry.Init(context.Background(), opts.Telemetry.ServiceName, opts.Telemetry.ExporterURL)
	if err != nil {
		log.Error("init tracer", "err", err)
		return err
	}
	if tp != nil {
		defer func() {
			shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
			defer c()
			_ = tp.Shutdown(shutdownCtx)
		}()
	}

	h := server.NewHandler(svc, log).WithMaxBodyBytes(int64(opts.MaxPayload))
	srv := server.NewServerWithRegistry(opts.Listen, h, log, reg, tp)

	go func() {
		log.Info("starting charon", "addr", opts.Listen)
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			log.Error("charon server error", "err", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit
	log.Info("shutting down")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("charon shutdown error", "err", err)
	}
	return nil
}
