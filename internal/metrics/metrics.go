// Package metrics defines the sync's Prometheus metrics, served at /metrics.
package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	received = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sync_webhooks_total",
		Help: "Webhook deliveries by source and result: accepted, duplicate, ignored, bad_signature, bad_payload.",
	}, []string{"source", "result"})

	processed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sync_events_processed_total",
		Help: "Event-log outcomes: done, retry, dead (moved to the dead-letter queue), coalesced (covered by a later sync).",
	}, []string{"result"})

	converged = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sync_converge_total",
		Help: "What each object sync did: created, pushed_devrev, pushed_github, noop, gone.",
	}, []string{"action"})

	conflicts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sync_status_conflicts_total",
		Help: "Both sides changed status since the last sync, by which side won.",
	}, []string{"winner"})

	apiCalls = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sync_api_calls_total",
		Help: "Calls to GitHub and DevRev by HTTP status (0 = network error).",
	}, []string{"api", "status"})

	drift = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sync_drift_total",
		Help: "Reconciler findings: missing (no DevRev work), mismatch, duplicate_work, orphan_work.",
	}, []string{"kind"})

	reconcileSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "sync_reconcile_duration_seconds",
		Help:    "Time to diff both sides.",
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 12),
	})
)

func Webhook(source, result string) { received.WithLabelValues(source, result).Inc() }
func Processed(result string)       { processed.WithLabelValues(result).Inc() }
func Coalesced(n int)               { processed.WithLabelValues("coalesced").Add(float64(n)) }
func Converge(action string)        { converged.WithLabelValues(action).Inc() }
func Conflict(winner string)        { conflicts.WithLabelValues(winner).Inc() }
func Drift(kind string, n int)      { drift.WithLabelValues(kind).Add(float64(n)) }
func ReconcileTook(seconds float64) { reconcileSeconds.Observe(seconds) }

func APICall(api string, status int) { apiCalls.WithLabelValues(api, strconv.Itoa(status)).Inc() }
