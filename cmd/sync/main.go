// Command sync runs the two-way GitHub <-> DevRev sync: webhook receivers, an
// event-log worker pool, and a periodic reconciler. Configuration is by
// environment variable; see .env.example.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ThatDeparted2061/devrev-github-sync/internal/devrev"
	"github.com/ThatDeparted2061/devrev-github-sync/internal/github"
	"github.com/ThatDeparted2061/devrev-github-sync/internal/metrics"
	"github.com/ThatDeparted2061/devrev-github-sync/internal/store"
	"github.com/ThatDeparted2061/devrev-github-sync/internal/syncer"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func must(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("missing required environment variable", "name", key)
		os.Exit(2)
	}
	return v
}

func main() {
	if os.Getenv("LOG_LEVEL") == "debug" {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, env("DATABASE_URL", "postgres://localhost:5432/devrev_sync?sslmode=disable"))
	if err != nil {
		slog.Error("postgres", "err", err)
		os.Exit(1)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		slog.Error("migrate", "err", err)
		os.Exit(1)
	}

	owner, repo, ok := strings.Cut(must("GITHUB_REPO"), "/")
	if !ok {
		slog.Error("GITHUB_REPO must be owner/repo")
		os.Exit(2)
	}
	s := &syncer.Syncer{
		Store: st,
		GH:    github.New(env("GITHUB_API", "https://api.github.com"), must("GITHUB_TOKEN"), owner, repo, metrics.APICall),
		DV:    devrev.New(env("DEVREV_API", "https://api.devrev.ai"), must("DEVREV_TOKEN"), must("DEVREV_PART_ID"), must("DEVREV_OWNER_ID"), metrics.APICall),
	}
	srv := &syncer.Server{Syncer: s, GitHubSecret: []byte(os.Getenv("GITHUB_WEBHOOK_SECRET")), DevRevSecret: []byte(os.Getenv("DEVREV_WEBHOOK_SECRET"))}
	if len(srv.GitHubSecret) == 0 || len(srv.DevRevSecret) == 0 {
		slog.Warn("webhook secret not set: signatures are NOT checked")
	}

	workers, _ := strconv.Atoi(env("WORKERS", "16"))
	maxAttempts, _ := strconv.Atoi(env("MAX_ATTEMPTS", "8"))
	every, err := time.ParseDuration(env("RECONCILE_EVERY", "5m"))
	if err != nil || every <= 0 {
		slog.Error("bad RECONCILE_EVERY", "err", err)
		os.Exit(2)
	}
	srv.Workers(ctx, workers, maxAttempts)
	go srv.ReconcileEvery(ctx, every)

	addr := ":" + env("PORT", "8090")
	hs := &http.Server{Addr: addr, Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := hs.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			slog.Error("listen", "err", err)
			os.Exit(1)
		}
	}()
	slog.Info("devrev-github-sync running", "addr", addr, "repo", owner+"/"+repo, "workers", workers, "reconcile_every", every)
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	hs.Shutdown(shutdown)
}
