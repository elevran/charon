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
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/elevran/charon/internal/api"
	"github.com/elevran/charon/internal/chainstore"
	pebblebe "github.com/elevran/charon/internal/chainstore/pebble"
	charonpkg "github.com/elevran/charon/internal/charon"
	"github.com/elevran/charon/internal/config"
	"github.com/elevran/charon/internal/inference"
	"github.com/elevran/charon/internal/metrics"
	"github.com/elevran/charon/internal/proxy"
	"github.com/elevran/charon/internal/telemetry"
)

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	opts := config.NewServerOptions()
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

	cfg := chainstore.Config{
		MaxEntries:  opts.Storage.MaxResponses,
		MaxBytes:    int64(opts.Storage.MaxPayload),
		TTL:         time.Duration(opts.Storage.TTLDays) * 24 * time.Hour,
		TTLInterval: opts.WorkerTTLInterval,
		Log:         log,
	}

	svc, err := pebblebe.Open(context.Background(), opts.Storage.DataDir, nil, cfg)
	if err != nil {
		log.Error("open chainstore", "err", err)
		return err
	}
	defer func() { _ = svc.Close() }()

	// Initialise OTel tracing (no-op when ExporterURL is empty).
	charonTP, err := telemetry.Init(context.Background(), opts.Telemetry.CharonService, opts.Telemetry.ExporterURL)
	if err != nil {
		log.Error("init charon tracer", "err", err)
		return err
	}
	if charonTP != nil {
		defer func() {
			shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
			defer c()
			_ = charonTP.Shutdown(shutdownCtx)
		}()
	}

	var proxyTP *sdktrace.TracerProvider
	if opts.ProxyEnabled {
		proxyTP, err = telemetry.Init(context.Background(), opts.Telemetry.ProxyService, opts.Telemetry.ExporterURL)
		if err != nil {
			log.Error("init proxy tracer", "err", err)
			return err
		}
		if proxyTP != nil {
			defer func() {
				shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
				defer c()
				_ = proxyTP.Shutdown(shutdownCtx)
			}()
		}
	}

	reg := prometheus.NewRegistry()
	if err := metrics.Register(reg, ""); err != nil {
		log.Error("register metrics", "err", err)
		return err
	}

	// ── Charon internal API server (always starts) ─────────────────────────
	charonH := api.NewHandler(svc, log).WithMaxBodyBytes(int64(opts.Storage.MaxPayload))
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
	return nil
}
