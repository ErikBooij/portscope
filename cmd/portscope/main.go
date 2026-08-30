package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
	"github.com/erikbooij/portscope/internal/proxy"
	"github.com/erikbooij/portscope/internal/proxy/httpadapter"
	"github.com/erikbooij/portscope/internal/proxy/mongoadapter"
	"github.com/erikbooij/portscope/internal/proxy/mysqladapter"
	"github.com/erikbooij/portscope/internal/proxy/postgresadapter"
	"github.com/erikbooij/portscope/internal/proxy/redisadapter"
	appserver "github.com/erikbooij/portscope/internal/server"
)

func main() {
	var address, dataDir string
	flag.StringVar(&address, "addr", "127.0.0.1:8090", "web interface address")
	flag.StringVar(&dataDir, "data", "data", "persistent data directory")
	flag.Parse()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	configuration, err := config.OpenStore(filepath.Join(dataDir, "upstreams.json"))
	if err != nil {
		logger.Error("open configuration", "error", err)
		os.Exit(1)
	}
	observations, err := observation.OpenStore(filepath.Join(dataDir, "interactions.jsonl"), 5000)
	if err != nil {
		logger.Error("open observations", "error", err)
		os.Exit(1)
	}
	manager := proxy.NewManager(observations, map[string]proxy.Factory{
		"http":          func() proxy.Adapter { return httpadapter.New() },
		"elasticsearch": func() proxy.Adapter { return httpadapter.New() },
		"redis":         func() proxy.Adapter { return redisadapter.New() },
		"mysql":         func() proxy.Adapter { return mysqladapter.New() },
		"postgres":      func() proxy.Adapter { return postgresadapter.New() },
		"mongodb":       func() proxy.Adapter { return mongoadapter.New() },
	})
	manager.Apply(ctx, configuration.List())
	defer manager.Close()
	server := &http.Server{Addr: address, Handler: appserver.New(ctx, configuration, observations, manager, logger).Handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 1 << 20}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	logger.Info("Portscope ready", "url", "http://"+address)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
