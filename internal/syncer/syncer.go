// Package syncer keeps a GitHub issue or pull request and its DevRev work item
// in agreement.
//
// It syncs state, not events. A webhook only says "object K changed". The
// worker then locks K, reads both sides as they are now, and converges them.
// That makes duplicates harmless (the second sync finds nothing to do) and
// ordering irrelevant (nobody applies a stale payload). Lost deliveries are
// caught by the reconciler, which compares both sides wholesale.
//
// Ownership: title and body flow GitHub -> DevRev. Status flows both ways, as a
// three-way merge against the status each side had at the last sync: if only
// one side changed, it wins. If both changed, the later change wins.
package syncer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ThatDeparted2061/devrev-github-sync/internal/devrev"
	"github.com/ThatDeparted2061/devrev-github-sync/internal/github"
	"github.com/ThatDeparted2061/devrev-github-sync/internal/metrics"
	"github.com/ThatDeparted2061/devrev-github-sync/internal/retry"
	"github.com/ThatDeparted2061/devrev-github-sync/internal/store"
)

// Status is a side-neutral status: "open" or "closed:<reason>".
type Status string

const (
	Open             Status = "open"
	ClosedCompleted  Status = "closed:completed"
	ClosedNotPlanned Status = "closed:not_planned"
	ClosedDuplicate  Status = "closed:duplicate"
)

// DevRev stage names (the default issue stage diagram).
const (
	StageTriage    = "triage"
	StageBacklog   = "backlog"
	StageInDev     = "in_development"
	StageCompleted = "completed"
	StageWontFix   = "wont_fix"
	StageDuplicate = "duplicate"
)

func GitHubStatus(i *github.Issue) Status {
	switch {
	case i.State == "open":
		return Open
	case i.IsPR() && i.Merged():
		return ClosedCompleted
	case i.IsPR():
		return ClosedNotPlanned
	case i.StateReason == "not_planned":
		return ClosedNotPlanned
	case i.StateReason == "duplicate":
		return ClosedDuplicate
	}
	return ClosedCompleted
}

func DevRevStatus(stage string) Status {
	switch stage {
	case StageCompleted:
		return ClosedCompleted
	case StageWontFix:
		return ClosedNotPlanned
	case StageDuplicate:
		return ClosedDuplicate
	}
	return Open
}

// stageFor picks the DevRev stage for a status. An open item keeps whatever
// open stage DevRev already has (triage, in_development, ...): GitHub's
// "open" carries less detail, so it should not overwrite it.
func stageFor(s Status, current string, isPR bool) string {
	switch s {
	case ClosedCompleted:
		return StageCompleted
	case ClosedNotPlanned:
		return StageWontFix
	case ClosedDuplicate:
		return StageDuplicate
	}
	switch {
	case current != "" && DevRevStatus(current) == Open:
		return current
	case isPR:
		return StageInDev
	case current == "":
		return StageTriage
	}
	return StageBacklog // reopened
}

func githubState(s Status) (state, reason string) {
	if s == Open {
		return "open", ""
	}
	return "closed", strings.TrimPrefix(string(s), "closed:")
}

// ExternalRef is the DevRev external_ref for a GitHub key.
func ExternalRef(key string) string { return "github:" + key }

// KeyFromRef reverses ExternalRef.
func KeyFromRef(ref string) (string, bool) { return strings.CutPrefix(ref, "github:") }

func bodyFor(i *github.Issue) string {
	return strings.TrimSpace(i.Body + "\n\nSynced from " + i.HTMLURL)
}

// InSync reports whether the two sides agree on everything the sync owns.
func InSync(i *github.Issue, w *devrev.Work) bool {
	return i.Title == w.Title && statusInSync(i, w)
}

func statusInSync(i *github.Issue, w *devrev.Work) bool {
	g, d := GitHubStatus(i), DevRevStatus(w.Stage.Name)
	// DevRev may call a PR done before it is merged; merging stays GitHub's call.
	return g == d || (i.IsPR() && g == Open && d == ClosedCompleted)
}

// ErrStale means GitHub returned an older version than the webhook described
// (replica lag); the event is retried.
var ErrStale = errors.New("github returned a stale read")

// Syncer converges objects.
type Syncer struct {
	Store *store.Store
	GH    *github.Client
	DV    *devrev.Client
}

// Converge brings key's two sides into agreement and returns what it did:
// created, pushed_devrev, pushed_github, noop or gone. hint, if set, is the
// updated_at the triggering webhook carried.
func (s *Syncer) Converge(ctx context.Context, key string, hint time.Time) (string, error) {
	_, number, err := github.ParseKey(key)
	if err != nil {
		return "", err
	}
	action := "noop"
	err = s.Store.Locked(ctx, key, func(tx pgx.Tx) error {
		gh, err := s.GH.Get(ctx, number)
		var se *retry.StatusError
		if errors.As(err, &se) && (se.Code == 404 || se.Code == 410) {
			action = "gone" // deleted or transferred: nothing to sync
			return nil
		}
		if err != nil {
			return err
		}
		if !hint.IsZero() && gh.UpdatedAt.Before(hint) {
			return ErrStale
		}
		link, err := store.GetLink(ctx, tx, key)
		if err != nil {
			return err
		}
		kind := map[bool]string{true: "pull", false: "issue"}[gh.IsPR()]

		var w *devrev.Work
		if link != nil {
			w, err = s.DV.Get(ctx, link.WorkID)
		} else {
			w, err = s.DV.FindByExternalRef(ctx, ExternalRef(key)) // survives a lost links table
		}
		if err != nil {
			return err
		}
		if w == nil {
			w, err = s.DV.Create(ctx, gh.Title, bodyFor(gh), stageFor(GitHubStatus(gh), "", gh.IsPR()), ExternalRef(key))
			if err != nil {
				return err
			}
			action = "created"
			return store.PutLink(ctx, tx, newLink(key, kind, gh, w, GitHubStatus(gh), DevRevStatus(w.Stage.Name)))
		}

		ghStatus, dvStatus := GitHubStatus(gh), DevRevStatus(w.Stage.Name)
		if link != nil {
			slog.Debug("converge", "key", key, "gh", ghStatus, "dv", dvStatus, "base_gh", link.GHStatus, "base_dv", link.DVStatus)
		}
		ghMoved := link == nil || Status(link.GHStatus) != ghStatus
		dvMoved := link != nil && Status(link.DVStatus) != dvStatus
		ghEdited := link == nil || gh.UpdatedAt.After(link.GHSeenAt)

		if InSync(gh, w) && !ghEdited {
			if link == nil || ghMoved || dvMoved {
				return store.PutLink(ctx, tx, newLink(key, kind, gh, w, ghStatus, dvStatus))
			}
			return nil
		}

		devrevWins := false
		if !statusInSync(gh, w) && link != nil && ghMoved != dvMoved {
			// One side looks unchanged, but it may have changed and changed back
			// since the last sync (A -> B -> A). Ask its history before letting
			// the other side overwrite it.
			if dvMoved {
				if ghAt, err := s.GH.StatusChangedAt(ctx, number); err != nil {
					return err
				} else if ghAt.After(link.GHSeenAt) {
					ghMoved = true
				}
			} else if changes, err := store.DevRevStageChanges(ctx, tx, key, link.DVSeenAt); err != nil {
				return err
			} else {
				for _, c := range changes {
					if DevRevStatus(c[0]) != DevRevStatus(c[1]) {
						dvMoved = true
					}
				}
			}
		}
		slog.Debug("decide", "key", key, "gh", ghStatus, "dv", dvStatus, "gh_moved", ghMoved, "dv_moved", dvMoved, "dv_modified", w.ModifiedDate)
		if !statusInSync(gh, w) && dvMoved {
			devrevWins = !ghMoved
			if ghMoved { // both sides changed status since the last sync: the later change wins
				ghAt, err := s.GH.StatusChangedAt(ctx, number)
				if err != nil {
					return err
				}
				devrevWins = w.ModifiedDate.After(ghAt)
				metrics.Conflict(map[bool]string{true: "devrev", false: "github"}[devrevWins])
			}
		}
		// The merge base becomes the status both sides were brought to, not
		// whatever the writes returned: if someone changed a side while this
		// sync ran, that side then differs from the base and wins next time.
		merged := ghStatus
		if devrevWins {
			merged = dvStatus
		}
		ghBase, dvBase := merged, merged
		if devrevWins {
			if gh.IsPR() && dvStatus == ClosedCompleted {
				ghBase = ghStatus // merging stays GitHub's call; the PR is left open
			} else {
				state, reason := githubState(dvStatus)
				if gh, err = s.GH.SetState(ctx, number, state, reason); err != nil {
					return err
				}
				action = "pushed_github"
			}
		}
		pushStage := !devrevWins && !statusInSync(gh, w)
		if gh.Title != w.Title || ghEdited || pushStage {
			stage := ""
			if pushStage {
				stage = stageFor(merged, w.Stage.Name, gh.IsPR())
			}
			if w, err = s.DV.Update(ctx, w.ID, gh.Title, bodyFor(gh), stage); err != nil {
				return err
			}
			if action == "noop" {
				action = "pushed_devrev"
			}
		}
		if !pushStage && !devrevWins {
			dvBase = dvStatus // status was already in sync (e.g. a PR DevRev calls done)
		}
		return store.PutLink(ctx, tx, newLink(key, kind, gh, w, ghBase, dvBase))
	})
	if err == nil {
		metrics.Converge(action)
	}
	return action, err
}

func newLink(key, kind string, gh *github.Issue, w *devrev.Work, ghBase, dvBase Status) store.Link {
	return store.Link{GitHubKey: key, Kind: kind, WorkID: w.ID, GHSeenAt: gh.UpdatedAt, DVSeenAt: w.ModifiedDate,
		GHStatus: string(ghBase), DVStatus: string(dvBase)}
}

// Retryable reports whether a failed sync should be retried.
func Retryable(err error) bool {
	var se *retry.StatusError
	if errors.As(err, &se) {
		return se.Retryable()
	}
	return true // network errors, stale reads, database hiccups
}

// Process handles one event from the log and, on success, closes out every
// other pending event for the same object that this sync already covered.
func (s *Syncer) Process(ctx context.Context, e store.Event, hint time.Time) error {
	start := time.Now()
	action, err := s.Converge(ctx, e.ObjectKey, hint)
	if err != nil {
		return fmt.Errorf("converge %s: %w", e.ObjectKey, err)
	}
	if n, err := s.Store.Coalesce(ctx, e.ObjectKey, start, e.ID); err == nil && n > 0 {
		metrics.Coalesced(int(n))
	}
	slog.Debug("synced", "key", e.ObjectKey, "source", e.Source, "action", action)
	return nil
}
