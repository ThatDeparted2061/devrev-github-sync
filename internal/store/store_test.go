package store

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func testStore(t *testing.T) *Store {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://localhost:5432/postgres?sslmode=disable"
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Skip("no Postgres: " + err.Error())
	}
	db := "store_test_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+db); err != nil {
		t.Skip("cannot create test database: " + err.Error())
	}
	t.Cleanup(func() {
		admin.Exec(context.Background(), "DROP DATABASE "+db+" WITH (FORCE)")
		admin.Close(context.Background())
	})
	st, err := Open(ctx, strings.Replace(url, "/postgres?", "/"+db+"?", 1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return st
}

// A webhook's nanosecond modified_date must compare equal to the same change
// read from the API. Postgres rounds .087993731 up to .087994, which once made
// a change look newer than itself and stalled the sync on ErrStale for good.
func TestStageChangeTimesMatchTheAPI(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	api := time.Date(2026, 10, 14, 22, 36, 16, 87993731, time.UTC) // sub-microsecond part >= 500 ns
	payload := fmt.Sprintf(`{"id":"d1","type":"work_updated","work_updated":{"work":{"external_ref":"github:a/b#1","stage":{"name":"in_development"},"modified_date":%q},"old_work":{"stage":{"name":"wont_fix"}}}}`, api.Format(time.RFC3339Nano))
	if _, err := st.Record(ctx, Event{Source: "devrev", DeliveryID: "d1", Type: "work_updated", ObjectKey: "a/b#1", Payload: []byte(payload)}); err != nil {
		t.Fatal(err)
	}
	err := st.Locked(ctx, "a/b#1", func(tx pgx.Tx) error {
		cs, err := DevRevStageChanges(ctx, tx, "a/b#1", api.Add(-time.Millisecond))
		if err != nil {
			return err
		}
		if len(cs) != 1 || cs[0].At.After(api.Truncate(time.Microsecond)) || cs[0].To != "in_development" {
			t.Errorf("changes = %+v; want one at %v", cs, api.Truncate(time.Microsecond))
		}
		if cs, _ := DevRevStageChanges(ctx, tx, "a/b#1", api); len(cs) != 0 { // already seen: not "after since"
			t.Errorf("change at the seen time reported again: %+v", cs)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
