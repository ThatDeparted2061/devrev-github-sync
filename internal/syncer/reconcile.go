package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ThatDeparted2061/devrev-github-sync/internal/devrev"
	"github.com/ThatDeparted2061/devrev-github-sync/internal/metrics"
	"github.com/ThatDeparted2061/devrev-github-sync/internal/store"
)

// Report is one reconciliation pass.
type Report struct {
	At         time.Time     `json:"at"`
	Took       time.Duration `json:"took_ns"`
	GitHub     int           `json:"github_objects"`
	Works      int           `json:"devrev_works"`
	Missing    int           `json:"missing"`        // GitHub object with no work item
	Mismatched int           `json:"mismatched"`     // title or status disagree
	Duplicates int           `json:"duplicate_work"` // extra work items for one GitHub object
	Orphans    int           `json:"orphan_work"`    // work item whose GitHub object is gone
	Queued     int           `json:"queued"`
}

// Reconcile diffs every GitHub issue/PR against every DevRev work item that
// points at this repository and queues a sync for each disagreement. The
// queued syncs go through the same event log, so they get retries and the
// dead-letter queue like any webhook.
// ponytail: full scan of both sides each pass, O(issues) API pages. Switch the
// GitHub side to ?since=<last pass> if repositories get large.
func (s *Syncer) Reconcile(ctx context.Context) (Report, error) {
	start := time.Now()
	r := Report{At: start}
	issues, err := s.GH.List(ctx, time.Time{})
	if err != nil {
		return r, fmt.Errorf("list github: %w", err)
	}
	works, err := s.DV.List(ctx)
	if err != nil {
		return r, fmt.Errorf("list devrev: %w", err)
	}
	prefix := ExternalRef(s.GH.Repo() + "#")
	byKey := map[string][]devrev.Work{}
	for _, w := range works {
		if key, ok := KeyFromRef(w.ExternalRef); ok && strings.HasPrefix(w.ExternalRef, prefix) {
			byKey[key] = append(byKey[key], w)
		}
	}
	r.GitHub, r.Works = len(issues), len(byKey)

	run := start.UTC().Format("20060102T150405.000")
	queue := func(key, why string) error {
		payload, _ := json.Marshal(map[string]string{"reason": why})
		ok, err := s.Store.Record(ctx, store.Event{Source: "reconcile", DeliveryID: run + "/" + key, Type: why, ObjectKey: key, Payload: payload})
		if ok {
			r.Queued++
		}
		return err
	}
	seen := map[string]bool{}
	for i := range issues {
		gh := &issues[i]
		key := s.GH.Key(gh.Number)
		seen[key] = true
		ws := byKey[key]
		switch {
		case len(ws) == 0:
			r.Missing++
			if err := queue(key, "missing"); err != nil {
				return r, err
			}
		case !InSync(gh, &ws[0]):
			r.Mismatched++
			if err := queue(key, "mismatch"); err != nil {
				return r, err
			}
		}
		if len(ws) > 1 {
			r.Duplicates += len(ws) - 1
		}
	}
	for key := range byKey {
		if !seen[key] {
			r.Orphans++
		}
	}
	r.Took = time.Since(start)
	metrics.Drift("missing", r.Missing)
	metrics.Drift("mismatch", r.Mismatched)
	metrics.Drift("duplicate_work", r.Duplicates)
	metrics.Drift("orphan_work", r.Orphans)
	metrics.ReconcileTook(r.Took.Seconds())
	return r, nil
}
