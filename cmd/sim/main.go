// Command sim runs the simulated GitHub repository and DevRev org (see
// internal/sim) with chaos applied to every webhook it sends.
package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/ThatDeparted2061/devrev-github-sync/internal/sim"
)

func main() {
	var c sim.Config
	addr := flag.String("addr", ":9100", "listen address")
	flag.StringVar(&c.Repo, "repo", "acme/storefront", "simulated owner/repo")
	flag.StringVar(&c.SyncURL, "sync", "http://localhost:8090", "where to deliver webhooks")
	ghSecret := flag.String("github-secret", os.Getenv("GITHUB_WEBHOOK_SECRET"), "sign GitHub webhooks with this")
	dvSecret := flag.String("devrev-secret", os.Getenv("DEVREV_WEBHOOK_SECRET"), "sign DevRev webhooks with this")
	flag.Float64Var(&c.Duplicate, "dup", 0.20, "share of webhooks delivered twice")
	flag.Float64Var(&c.Drop, "drop", 0.05, "share of webhooks never delivered")
	flag.DurationVar(&c.MaxDelay, "max-delay", 2*time.Second, "random delivery delay (reorders deliveries)")
	flag.Float64Var(&c.API429, "api-429", 0.02, "share of API calls answered 429 with Retry-After")
	flag.Float64Var(&c.API5xx, "api-5xx", 0.01, "share of API calls answered 502")
	flag.DurationVar(&c.APILatency, "api-latency", 10*time.Millisecond, "random API latency, up to this")
	flag.BoolVar(&c.UniqueExternalRef, "unique-external-ref", false, "reject duplicate external_ref on works.create, as DevRev does")
	flag.Parse()
	c.GitHubSecret, c.DevRevSecret = []byte(*ghSecret), []byte(*dvSecret)

	slog.Info("sim listening", "addr", *addr, "repo", c.Repo, "dup", c.Duplicate, "drop", c.Drop, "max_delay", c.MaxDelay)
	if err := http.ListenAndServe(*addr, sim.New(c).Handler()); err != nil {
		slog.Error("listen", "err", err)
		os.Exit(1)
	}
}
