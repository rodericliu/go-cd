package main

import (
	"cmp"
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"consumer-dispatcher/internal/admin"
	"consumer-dispatcher/internal/config"
	"consumer-dispatcher/internal/logging"
	"consumer-dispatcher/internal/manager"
)

// shutdownTimeout bounds the whole shutdown (HTTP drain + consumers).
const shutdownTimeout = 15 * time.Second

func main() {
	os.Exit(run())
}

func run() int {
	cfgPath := flag.String("config", envOr("CD_CONFIG", "configs/config.yaml"), "path to YAML config")
	flag.Parse()

	cfg, jobs, err := config.Load(*cfgPath)
	if err != nil {
		newLogger(slog.LevelInfo).Error("load config", "err", err)
		return 1
	}
	level := new(slog.LevelVar)
	level.Set(cfg.Log.SlogLevel())
	log, closeLog, err := logging.New(cfg.Log, level, os.Stdout)
	if err != nil {
		newLogger(slog.LevelInfo).Error("open log", "err", err)
		return 1
	}
	defer func() { _ = closeLog() }()
	slog.SetDefault(log)
	log.Info("config loaded", "jobs", len(jobs), "servers", len(cfg.Servers), "admin", cfg.Admin.Addr(),
		"log_level", cfg.Log.Level, "log_file", cfg.Log.File.Path, "log_failed_body", cfg.Log.FailedBody.Enabled)

	var tlsCfg *tls.Config
	if cfg.Admin.TLS.Enabled() {
		if tlsCfg, err = admin.TLSConfig(cfg.Admin.TLS); err != nil {
			log.Error("admin tls", "err", err)
			return 1
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mgr := manager.New(jobs, manager.Options{FailedBody: cfg.Log.FailedBody}, log)
	adminSrv := admin.New(mgr, admin.Options{
		Tokens:            cfg.Admin.Tokens,
		RequireClientCert: cfg.Admin.TLS.RequireClientCert(),
		UI:                cfg.Admin.UIEnabled(),
	}, log)
	if !adminSrv.AuthEnabled() {
		log.Warn("admin API has no authentication; set admin.tokens and/or admin.tls.client_ca_file")
	}
	rl := &reloader{path: *cfgPath, log: log, level: level, jobs: mgr, tokens: adminSrv.SetTokens, cur: cfg}
	adminSrv.SetReloadFunc(rl.reload)
	// Register before anything else can deliver SIGHUP, whose default
	// action would terminate the process.
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	mgr.StartAll()
	go reloadOnSignal(ctx, hup, rl, log)

	srv := &http.Server{
		Addr:              cfg.Admin.Addr(),
		Handler:           adminSrv.Handler(),
		TLSConfig:         tlsCfg,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		// /try waits for the consumer, so allow the longest job timeout.
		WriteTimeout: manager.MaxJobTimeout(jobs) + 10*time.Second,
		IdleTimeout:  60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("admin listening", "addr", cfg.Admin.Addr(), "tls", tlsCfg != nil,
			"token_auth", len(cfg.Admin.Tokens) > 0, "client_cert_auth", cfg.Admin.TLS.RequireClientCert(),
			"ui", cfg.Admin.UIEnabled())
		var err error
		if tlsCfg != nil {
			err = srv.ListenAndServeTLS("", "") // certificates come from TLSConfig
		} else {
			err = srv.ListenAndServe()
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	code := 0
	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case err := <-errCh:
		log.Error("admin server", "err", err)
		code = 1
	}

	// Reverse order: HTTP drain → consumers (finish in-flight, requeue on
	// timeout) → broker connections. One deadline covers all of it.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("admin shutdown", "err", err)
	}
	mgr.StopAll(shutdownCtx)
	log.Info("bye")
	return code
}

// reloadOnSignal reloads the config on each signal from hup until ctx ends.
func reloadOnSignal(ctx context.Context, hup <-chan os.Signal, rl *reloader, log *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-hup:
			log.Info("SIGHUP received, reloading config")
			// Errors are already logged by the reloader.
			_, _ = rl.reload(ctx, false)
		}
	}
}

func newLogger(level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}

func envOr(key, def string) string {
	return cmp.Or(os.Getenv(key), def)
}
