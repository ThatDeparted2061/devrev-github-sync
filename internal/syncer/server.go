package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ThatDeparted2061/devrev-github-sync/internal/devrev"
	"github.com/ThatDeparted2061/devrev-github-sync/internal/github"
	"github.com/ThatDeparted2061/devrev-github-sync/internal/metrics"
	"github.com/ThatDeparted2061/devrev-github-sync/internal/retry"
	"github.com/ThatDeparted2061/devrev-github-sync/internal/store"
)

// Server receives webhooks and exposes the dead-letter queue.
type Server struct {
	Syncer        *Syncer
	GitHubSecret  []byte // empty skips signature checks (local testing only)
	DevRevSecret  []byte
	lastReconcile atomic.Pointer[Report]
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhooks/github", s.github)
	mux.HandleFunc("POST /webhooks/devrev", s.devrev)
	mux.HandleFunc("GET /dlq", s.dlq)
	mux.HandleFunc("POST /dlq/replay", s.replay)
	mux.HandleFunc("POST /reconcile", s.reconcileNow)
	mux.HandleFunc("GET /stats", s.stats)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.Handle("GET /metrics", promhttp.Handler())
	return mux
}

const maxPayload = 5 << 20

// github records an issues or pull_request delivery. The delivery ID is the
// idempotency key; everything else happens in the workers, so GitHub gets its
// 202 in a few milliseconds.
func (s *Server) github(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxPayload))
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	if len(s.GitHubSecret) > 0 && !github.VerifySignature(s.GitHubSecret, body, r.Header.Get("X-Hub-Signature-256")) {
		metrics.Webhook("github", "bad_signature")
		http.Error(w, "bad signature", http.StatusUnauthorized)
		return
	}
	event := r.Header.Get("X-GitHub-Event")
	if event == "ping" || (event != "issues" && event != "pull_request") {
		metrics.Webhook("github", "ignored")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	d, err := github.ParseDelivery(body)
	if err != nil {
		metrics.Webhook("github", "bad_payload")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if d.Repository.FullName != s.Syncer.GH.Repo() {
		metrics.Webhook("github", "ignored")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	s.record(w, r, store.Event{
		Source: "github", DeliveryID: r.Header.Get("X-GitHub-Delivery"), Type: event + "." + d.Action,
		ObjectKey: github.Key(d.Repository.FullName, d.Object().Number), Payload: body,
	})
}

func (s *Server) devrev(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxPayload))
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	e, err := devrev.ParseEvent(body)
	if err != nil {
		metrics.Webhook("devrev", "bad_payload")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if e.Type == "verify" && e.Verify != nil { // the one-time handshake when the webhook is created
		writeJSON(w, http.StatusOK, map[string]string{"challenge": e.Verify.Challenge})
		return
	}
	if len(s.DevRevSecret) > 0 && !devrev.VerifySignature(s.DevRevSecret, body, r.Header.Get("X-DevRev-Signature")) {
		metrics.Webhook("devrev", "bad_signature")
		http.Error(w, "bad signature", http.StatusUnauthorized)
		return
	}
	work := e.Work()
	key, ok := "", false
	if work != nil {
		key, ok = KeyFromRef(work.ExternalRef)
	}
	if !ok || e.ID == "" {
		metrics.Webhook("devrev", "ignored") // not one of ours
		w.WriteHeader(http.StatusNoContent)
		return
	}
	s.record(w, r, store.Event{Source: "devrev", DeliveryID: e.ID, Type: e.Type, ObjectKey: key, Payload: body})
}

func (s *Server) record(w http.ResponseWriter, r *http.Request, e store.Event) {
	if e.DeliveryID == "" {
		metrics.Webhook(e.Source, "bad_payload")
		http.Error(w, "missing delivery id", http.StatusBadRequest)
		return
	}
	inserted, err := s.Syncer.Store.Record(r.Context(), e)
	if err != nil {
		slog.Error("record event", "err", err)
		http.Error(w, "storage unavailable", http.StatusServiceUnavailable) // the sender will redeliver
		return
	}
	result := "accepted"
	if !inserted {
		result = "duplicate"
	}
	metrics.Webhook(e.Source, result)
	writeJSON(w, http.StatusAccepted, map[string]any{"result": result})
}

func (s *Server) dlq(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	evs, err := s.Syncer.Store.Dead(r.Context(), max(1, min(limit, 1000)))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	type row struct {
		ID         int64     `json:"id"`
		Source     string    `json:"source"`
		DeliveryID string    `json:"delivery_id"`
		Type       string    `json:"type"`
		Object     string    `json:"object"`
		Attempts   int       `json:"attempts"`
		LastError  string    `json:"last_error"`
		ReceivedAt time.Time `json:"received_at"`
	}
	out := []row{}
	for _, e := range evs {
		out = append(out, row{e.ID, e.Source, e.DeliveryID, e.Type, e.ObjectKey, e.Attempts, e.LastError, e.ReceivedAt})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) replay(w http.ResponseWriter, r *http.Request) {
	n, err := s.Syncer.Store.Replay(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"replayed": n})
}

func (s *Server) reconcileNow(w http.ResponseWriter, r *http.Request) {
	rep, err := s.Syncer.Reconcile(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	s.lastReconcile.Store(&rep)
	writeJSON(w, http.StatusOK, rep)
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	counts, err := s.Syncer.Store.Counts(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": counts, "last_reconcile": s.lastReconcile.Load()})
}

// Workers runs n event-processing loops until ctx is done.
func (s *Server) Workers(ctx context.Context, n, maxAttempts int) {
	for range n {
		go func() {
			for ctx.Err() == nil {
				evs, err := s.Syncer.Store.Claim(ctx, 4, 30*time.Second)
				if err != nil {
					if ctx.Err() == nil {
						slog.Error("claim", "err", err)
					}
					sleep(ctx, time.Second)
					continue
				}
				if len(evs) == 0 {
					sleep(ctx, 50*time.Millisecond) // ponytail: polling; LISTEN/NOTIFY if idle latency matters
					continue
				}
				for _, e := range evs {
					s.handle(ctx, e, maxAttempts)
				}
			}
		}()
	}
}

func (s *Server) handle(ctx context.Context, e store.Event, maxAttempts int) {
	defer func() {
		if p := recover(); p != nil { // one bad event must not take the worker down
			slog.Error("panic while syncing", "object", e.ObjectKey, "panic", p)
			s.Syncer.Store.Fail(ctx, e.ID, fmt.Errorf("panic: %v", p), time.Minute, e.Attempts >= maxAttempts)
		}
	}()
	var hint time.Time
	if e.Source == "github" {
		if d, err := github.ParseDelivery(e.Payload); err == nil {
			hint = d.Object().UpdatedAt
		}
	}
	err := s.Syncer.Process(ctx, e, hint)
	if err == nil {
		s.Syncer.Store.Done(ctx, e.ID)
		metrics.Processed("done")
		return
	}
	if ctx.Err() != nil {
		return // shutting down; the lease expires and another worker retries
	}
	dead := !Retryable(err) || e.Attempts >= maxAttempts
	backoff := retry.Backoff(e.Attempts-1, 500*time.Millisecond, 2*time.Minute)
	var se *retry.StatusError
	if errors.As(err, &se) && se.RetryAfter > backoff {
		backoff = se.RetryAfter
	}
	if dead {
		metrics.Processed("dead")
		slog.Warn("event moved to dead-letter queue", "id", e.ID, "object", e.ObjectKey, "attempts", e.Attempts, "err", err)
	} else {
		metrics.Processed("retry")
	}
	s.Syncer.Store.Fail(ctx, e.ID, err, backoff, dead)
}

// ReconcileEvery runs Reconcile on a timer until ctx is done.
func (s *Server) ReconcileEvery(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rep, err := s.Syncer.Reconcile(ctx)
			if err != nil {
				slog.Warn("reconcile failed", "err", err)
				continue
			}
			s.lastReconcile.Store(&rep)
			if rep.Queued > 0 {
				slog.Info("reconcile found drift", "missing", rep.Missing, "mismatched", rep.Mismatched, "queued", rep.Queued, "took", rep.Took)
			}
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
