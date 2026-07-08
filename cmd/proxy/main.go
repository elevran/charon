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

	"github.com/elevran/charon/internal/config"
	"github.com/elevran/charon/internal/inference"
	"github.com/elevran/charon/internal/server"
	"github.com/elevran/charon/internal/telemetry"
	"github.com/elevran/charon/pkg/charon"
)

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	opts := config.NewServerOptions()
	fs := flag.NewFlagSet("proxy", flag.ExitOnError)
	// TODO: split config.ServerOptions into CharonOptions + ProxyOptions so this
	// binary doesn't silently accept (and ignore) Charon storage/TTL flags.
	opts.AddFlags(fs)
	_ = fs.Parse(os.Args[1:])
	if err := opts.Complete(fs); err != nil {
		log.Error("complete config", "err", err)
		return err
	}
	if err := opts.Validate(); err != nil {
		log.Error("validate config", "err", err)
		return err
	}

	tp, err := telemetry.Init(context.Background(), opts.Telemetry.ProxyService, opts.Telemetry.ExporterURL)
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

	timeout := time.Duration(opts.InferenceTimeoutSeconds) * time.Second
	infClient := inference.New(opts.ProxyBackend, opts.ProxyAPIKey, timeout)
	charonClient := charon.New(opts.ProxyCharonURL, timeout)

	h := NewHandler(charonClient, infClient, log)
	mux := http.NewServeMux()
	RegisterHandlers(mux, h)
	srv := server.NewServerFromMux(opts.ProxyListen, mux, log, tp)

	go func() {
		log.Info("starting proxy", "addr", opts.ProxyListen)
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			log.Error("proxy server error", "err", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit
	log.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("proxy shutdown error", "err", err)
	}
	return nil
}
