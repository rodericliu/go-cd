package main

import (
	"cmp"
	"context"
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
	"consumer-dispatcher/internal/manager"
)

func main() {
	cfgPath := flag.String("config", envOr("CD_CONFIG", "configs/config.yaml"), "path to YAML config")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	cfg, jobs, err := config.Load(*cfgPath)
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
	}
	log.Info("config loaded", "jobs", len(jobs), "servers", len(cfg.Servers), "admin", cfg.Admin.Addr())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mgr := manager.New(jobs, log)
	mgr.StartAll()

	srv := &http.Server{
		Addr:              cfg.Admin.Addr(),
		Handler:           admin.New(mgr, log).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("admin listening", "addr", cfg.Admin.Addr())
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case err := <-errCh:
		log.Error("admin server", "err", err)
	}

	// Reverse order: HTTP drain → consumers → broker connections.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("admin shutdown", "err", err)
	}
	mgr.StopAll()
	log.Info("bye")
}

func envOr(key, def string) string {
	return cmp.Or(os.Getenv(key), def)
}
