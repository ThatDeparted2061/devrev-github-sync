// Package store is the Postgres event log, idempotency table, dead-letter
// queue and GitHub-to-DevRev link table.
package store

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

type Store struct{ db *pgxpool.Pool }

func Open(ctx context.Context, url string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 64
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() { s.db.Close() }

// Migrate creates the tables. It is safe to run on every start.
func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.db.Exec(ctx, schema)
	return err
}

// Event is one webhook delivery (or reconciler finding) in the log.
type Event struct {
	ID         int64
	Source     string
	DeliveryID string
	Type       string
	ObjectKey  string
	Payload    json.RawMessage
	Attempts   int
	ReceivedAt time.Time
	LastError  string
}

// Record appends an event. It returns false, and stores nothing, if this
// delivery was already recorded: that is the whole idempotency check.
func (s *Store) Record(ctx context.Context, e Event) (bool, error) {
	tag, err := s.db.Exec(ctx, `
		INSERT INTO events (source, delivery_id, event_type, object_key, payload)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (source, delivery_id) DO NOTHING`,
		e.Source, e.DeliveryID, e.Type, e.ObjectKey, e.Payload)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// Claim leases up to n due events. A worker that dies loses its lease after
// lease, and the event becomes claimable again.
func (s *Store) Claim(ctx context.Context, n int, lease time.Duration) ([]Event, error) {
	rows, err := s.db.Query(ctx, `
		UPDATE events SET locked_until = now() + $2::interval, attempts = attempts + 1
		WHERE id IN (
			SELECT id FROM events
			WHERE status = 'pending' AND next_attempt_at <= now()
			  AND (locked_until IS NULL OR locked_until < now())
			ORDER BY id
			LIMIT $1
			FOR UPDATE SKIP LOCKED)
		RETURNING id, source, delivery_id, event_type, object_key, payload, attempts, received_at`,
		n, lease.String())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Event, error) {
		var e Event
		err := r.Scan(&e.ID, &e.Source, &e.DeliveryID, &e.Type, &e.ObjectKey, &e.Payload, &e.Attempts, &e.ReceivedAt)
		return e, err
	})
}

func (s *Store) Done(ctx context.Context, id int64) error {
	_, err := s.db.Exec(ctx, `UPDATE events SET status = 'done', done_at = now(), locked_until = NULL WHERE id = $1`, id)
	return err
}

// Fail schedules a retry at now+backoff, or moves the event to the
// dead-letter queue when dead is true.
func (s *Store) Fail(ctx context.Context, id int64, cause error, backoff time.Duration, dead bool) error {
	status := "pending"
	if dead {
		status = "dead"
	}
	_, err := s.db.Exec(ctx, `
		UPDATE events SET status = $2, last_error = $3, locked_until = NULL,
		       next_attempt_at = now() + $4::interval
		WHERE id = $1`, id, status, cause.Error(), backoff.String())
	return err
}

// Coalesce marks pending events for key received before cutoff as done: a
// sync that read both sides after cutoff already covered them.
func (s *Store) Coalesce(ctx context.Context, key string, cutoff time.Time, except int64) (int64, error) {
	tag, err := s.db.Exec(ctx, `
		UPDATE events SET status = 'done', coalesced = true, done_at = now(), locked_until = NULL
		WHERE object_key = $1 AND status = 'pending' AND received_at <= $2 AND id <> $3
		  AND (locked_until IS NULL OR locked_until < now())`, key, cutoff, except)
	return tag.RowsAffected(), err
}

// Link ties a GitHub issue or PR to its DevRev work item.
type Link struct {
	GitHubKey string
	Kind      string
	WorkID    string
	GHSeenAt  time.Time
	DVSeenAt  time.Time
	GHStatus  string // status on each side when last in sync: the base for a three-way merge
	DVStatus  string
}

// Locked runs fn in a transaction holding an advisory lock on key, so two
// workers never sync the same object at once. fn may call the GitHub and
// DevRev APIs; the lock is per object, so other objects are unaffected.
func (s *Store) Locked(ctx context.Context, key string, fn func(tx pgx.Tx) error) error {
	return pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, key); err != nil {
			return err
		}
		return fn(tx)
	})
}

func GetLink(ctx context.Context, tx pgx.Tx, key string) (*Link, error) {
	var l Link
	err := tx.QueryRow(ctx, `SELECT github_key, kind, work_id, gh_seen_at, dv_seen_at, gh_status, dv_status FROM links WHERE github_key = $1`, key).
		Scan(&l.GitHubKey, &l.Kind, &l.WorkID, &l.GHSeenAt, &l.DVSeenAt, &l.GHStatus, &l.DVStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return &l, err
}

func PutLink(ctx context.Context, tx pgx.Tx, l Link) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO links (github_key, kind, work_id, gh_seen_at, dv_seen_at, gh_status, dv_status) VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (github_key) DO UPDATE SET work_id = $3, gh_seen_at = $4, dv_seen_at = $5, gh_status = $6, dv_status = $7, synced_at = now()`,
		l.GitHubKey, l.Kind, l.WorkID, l.GHSeenAt, l.DVSeenAt, l.GHStatus, l.DVStatus)
	return err
}

// Links returns every link, for the reconciler.
func (s *Store) Links(ctx context.Context) (map[string]Link, error) {
	rows, err := s.db.Query(ctx, `SELECT github_key, kind, work_id, gh_seen_at, dv_seen_at, gh_status, dv_status FROM links`)
	if err != nil {
		return nil, err
	}
	out := map[string]Link{}
	_, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (struct{}, error) {
		var l Link
		err := r.Scan(&l.GitHubKey, &l.Kind, &l.WorkID, &l.GHSeenAt, &l.DVSeenAt, &l.GHStatus, &l.DVStatus)
		out[l.GitHubKey] = l
		return struct{}{}, err
	})
	return out, err
}

// Dead lists the dead-letter queue, newest first.
func (s *Store) Dead(ctx context.Context, limit int) ([]Event, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id, source, delivery_id, event_type, object_key, payload, attempts, received_at, coalesce(last_error, '')
		FROM events WHERE status = 'dead' ORDER BY id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Event, error) {
		var e Event
		err := r.Scan(&e.ID, &e.Source, &e.DeliveryID, &e.Type, &e.ObjectKey, &e.Payload, &e.Attempts, &e.ReceivedAt, &e.LastError)
		return e, err
	})
}

// Replay moves dead events back to pending with a fresh retry budget.
func (s *Store) Replay(ctx context.Context) (int64, error) {
	tag, err := s.db.Exec(ctx, `UPDATE events SET status = 'pending', attempts = 0, next_attempt_at = now(), locked_until = NULL WHERE status = 'dead'`)
	return tag.RowsAffected(), err
}

// Counts returns event counts by status, plus "coalesced".
func (s *Store) Counts(ctx context.Context) (map[string]int64, error) {
	rows, err := s.db.Query(ctx, `
		SELECT status, count(*) FROM events GROUP BY status
		UNION ALL SELECT 'coalesced', count(*) FROM events WHERE coalesced`)
	if err != nil {
		return nil, err
	}
	out := map[string]int64{}
	_, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (struct{}, error) {
		var k string
		var v int64
		err := r.Scan(&k, &v)
		out[k] = v
		return struct{}{}, err
	})
	return out, err
}

// Pending reports how many events still wait to be processed.
func (s *Store) Pending(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.QueryRow(ctx, `SELECT count(*) FROM events WHERE status = 'pending'`).Scan(&n)
	return n, err
}

// StageChange is one DevRev stage transition seen in a work_updated delivery.
type StageChange struct {
	From, To string
	At       time.Time // the work's modified_date after the change
}

// DevRevStageChanges returns the stage transitions in every DevRev
// work_updated delivery recorded for key whose work was modified after since.
//
// Times are kept at microseconds, truncated, which is how pgx stores a
// time.Time. The payload's nanosecond timestamp is parsed here rather than by
// Postgres, which would round it: a change at .087993731 would come back as
// .087994, a microsecond after the same change read from the API.
func DevRevStageChanges(ctx context.Context, tx pgx.Tx, key string, since time.Time) ([]StageChange, error) {
	since = since.Truncate(time.Microsecond)
	rows, err := tx.Query(ctx, `
		SELECT coalesce(payload #>> '{work_updated,old_work,stage,name}', ''), coalesce(payload #>> '{work_updated,work,stage,name}', ''),
		       payload #>> '{work_updated,work,modified_date}'
		FROM events
		WHERE object_key = $1 AND source = 'devrev' AND event_type = 'work_updated'
		  AND (payload #>> '{work_updated,work,modified_date}')::timestamptz >= $2`, key, since)
	if err != nil {
		return nil, err
	}
	all, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (StageChange, error) {
		var c StageChange
		var at string
		if err := r.Scan(&c.From, &c.To, &at); err != nil {
			return c, err
		}
		t, err := time.Parse(time.RFC3339Nano, at)
		c.At = t.Truncate(time.Microsecond)
		return c, err
	})
	if err != nil {
		return nil, err
	}
	out := all[:0]
	for _, c := range all {
		if c.At.After(since) {
			out = append(out, c)
		}
	}
	return out, nil
}

// DevRevDelivered reports whether a DevRev delivery describing the work as of
// modified (or later) has been recorded for key.
func DevRevDelivered(ctx context.Context, tx pgx.Tx, key string, modified time.Time) (bool, error) {
	var ok bool
	err := tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM events
		WHERE object_key = $1 AND source = 'devrev'
		  AND coalesce(payload #>> '{work_updated,work,modified_date}', payload #>> '{work_created,work,modified_date}')::timestamptz >= $2)`,
		key, modified.Truncate(time.Microsecond)).Scan(&ok)
	return ok, err
}
