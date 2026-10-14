package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ThatDeparted2061/devrev-github-sync/internal/devrev"
	"github.com/ThatDeparted2061/devrev-github-sync/internal/github"
	"github.com/ThatDeparted2061/devrev-github-sync/internal/sim"
	"github.com/ThatDeparted2061/devrev-github-sync/internal/store"
)

func TestStatusMapping(t *testing.T) {
	merged := time.Now()
	pr := func(state string, m *time.Time) *github.Issue {
		i := &github.Issue{State: state}
		i.PullRequest = &struct {
			MergedAt *time.Time `json:"merged_at"`
		}{m}
		return i
	}
	for _, c := range []struct {
		issue *github.Issue
		want  Status
		stage string
	}{
		{&github.Issue{State: "open"}, Open, StageTriage},
		{&github.Issue{State: "closed", StateReason: "completed"}, ClosedCompleted, StageCompleted},
		{&github.Issue{State: "closed", StateReason: "not_planned"}, ClosedNotPlanned, StageWontFix},
		{&github.Issue{State: "closed", StateReason: "duplicate"}, ClosedDuplicate, StageDuplicate},
		{pr("open", nil), Open, StageInDev},
		{pr("closed", &merged), ClosedCompleted, StageCompleted},
		{pr("closed", nil), ClosedNotPlanned, StageWontFix},
	} {
		got := GitHubStatus(c.issue)
		if got != c.want || stageFor(got, "", c.issue.IsPR()) != c.stage || DevRevStatus(c.stage) != got {
			t.Errorf("%+v: status %s stage %s", c.issue, got, stageFor(got, "", c.issue.IsPR()))
		}
	}
	if stageFor(Open, StageInDev, false) != StageInDev {
		t.Error("an open GitHub issue must not reset DevRev's finer open stage")
	}
	if stageFor(Open, StageCompleted, false) != StageBacklog {
		t.Error("reopening should move a closed work item to backlog")
	}
}

// testEnv runs the simulator and the sync in-process against a real Postgres.
type testEnv struct {
	sim, sync *httptest.Server
	srv       *Server
	cancel    context.CancelFunc
}

func newEnv(t *testing.T, cfg sim.Config) *testEnv {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://localhost:5432/postgres?sslmode=disable"
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Skip("no Postgres: " + err.Error())
	}
	db := "sync_test_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+db); err != nil {
		t.Skip("cannot create test database: " + err.Error())
	}
	t.Cleanup(func() {
		admin.Exec(context.Background(), "DROP DATABASE "+db+" WITH (FORCE)")
		admin.Close(context.Background())
	})
	dbURL := strings.Replace(url, "/postgres?", "/"+db+"?", 1)
	st, err := store.Open(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	env := &testEnv{cancel: cancel}
	env.srv = &Server{GitHubSecret: []byte("gh-secret"), DevRevSecret: []byte("dv-secret")}
	env.sync = httptest.NewServer(env.srv.Handler())
	t.Cleanup(env.sync.Close)
	cfg.Repo, cfg.SyncURL = "acme/shop", env.sync.URL
	cfg.GitHubSecret, cfg.DevRevSecret = env.srv.GitHubSecret, env.srv.DevRevSecret
	env.sim = httptest.NewServer(sim.New(cfg).Handler())
	t.Cleanup(env.sim.Close)
	env.srv.Syncer = &Syncer{Store: st,
		GH: github.New(env.sim.URL, "t", "acme", "shop", nil),
		DV: devrev.New(env.sim.URL, "t", "PROD-1", "DEVU-1", nil)}
	env.srv.Workers(ctx, 8, 8)
	return env
}

func (e *testEnv) do(t *testing.T, path string, body any) {
	b, _ := json.Marshal(body)
	resp, err := http.Post(e.sim.URL+path, "application/json", strings.NewReader(string(b)))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
}

func (e *testEnv) truth(t *testing.T) sim.Truth {
	var tr sim.Truth
	resp, err := http.Get(e.sim.URL + "/admin/truth")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	json.NewDecoder(resp.Body).Decode(&tr)
	return tr
}

// waitFor polls until cond holds for the simulator's state.
func (e *testEnv) waitFor(t *testing.T, what string, cond func(sim.Truth) bool) {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond(e.truth(t)) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s: %+v", what, e.truth(t))
}

func TestTwoWaySyncWithDuplicates(t *testing.T) {
	env := newEnv(t, sim.Config{Duplicate: 1, MaxDelay: 20 * time.Millisecond}) // every webhook delivered twice
	env.do(t, "/admin/github/issues", map[string]any{"title": "Checkout fails"})
	env.waitFor(t, "work item created", func(tr sim.Truth) bool { return len(tr.Works) == 1 && tr.Works[0].Stage.Name == StageTriage })

	env.do(t, "/admin/github/issues/1/close", map[string]string{"reason": "not_planned"})
	env.waitFor(t, "stage follows GitHub close", func(tr sim.Truth) bool { return tr.Works[0].Stage.Name == StageWontFix })

	env.do(t, "/admin/devrev/stage", map[string]string{"key": "acme/shop#1", "stage": StageInDev})
	env.waitFor(t, "GitHub reopened from DevRev", func(tr sim.Truth) bool { return tr.Issues[0].Status == "open" })

	env.do(t, "/admin/github/issues/1/edit", map[string]string{"title": "Checkout fails on retry"})
	env.waitFor(t, "title synced", func(tr sim.Truth) bool { return tr.Works[0].Title == "Checkout fails on retry" })
	if tr := env.truth(t); len(tr.Works) != 1 || tr.Works[0].Stage.Name != StageInDev {
		t.Fatalf("want one work item still in_development, got %+v", tr.Works)
	}
}

func TestReconcilerHealsDroppedWebhooks(t *testing.T) {
	env := newEnv(t, sim.Config{Drop: 1}) // nothing is delivered
	for i := range 5 {
		env.do(t, "/admin/github/issues", map[string]any{"title": fmt.Sprintf("Issue %d", i)})
	}
	time.Sleep(100 * time.Millisecond)
	if n := len(env.truth(t).Works); n != 0 {
		t.Fatalf("no webhook arrived, so nothing should be synced yet: %d works", n)
	}
	rep, err := env.srv.Syncer.Reconcile(context.Background())
	if err != nil || rep.Missing != 5 || rep.Queued != 5 {
		t.Fatalf("reconcile: %+v %v", rep, err)
	}
	env.waitFor(t, "reconciler-queued syncs", func(tr sim.Truth) bool { return len(tr.Works) == 5 })
	if rep, _ := env.srv.Syncer.Reconcile(context.Background()); rep.Missing+rep.Mismatched != 0 {
		t.Fatalf("second pass should find no drift: %+v", rep)
	}
}

func TestBadSignatureRejected(t *testing.T) {
	env := newEnv(t, sim.Config{})
	req, _ := http.NewRequest(http.MethodPost, env.sync.URL+"/webhooks/github", strings.NewReader(`{"action":"opened"}`))
	req.Header.Set("X-GitHub-Event", "issues")
	req.Header.Set("X-GitHub-Delivery", "d1")
	req.Header.Set("X-Hub-Signature-256", "sha256=00")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", resp.StatusCode)
	}
}
