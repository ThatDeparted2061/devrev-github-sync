// Command chaos drives the benchmark. It plays the users of GitHub and DevRev
// against the simulator (which delivers the resulting webhooks with
// duplicates, drops and reordering), waits for the sync to settle, and checks
// the end state against the audit log of every user change.
//
//	chaos -events 10000 -objects 1000 -out bench/results/chaos.json
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ThatDeparted2061/devrev-github-sync/internal/sim"
	"github.com/ThatDeparted2061/devrev-github-sync/internal/syncer"
)

var client = &http.Client{Timeout: 30 * time.Second}

func post(url string, body any) (int, []byte) {
	b, _ := json.Marshal(body)
	resp, err := client.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return 0, nil
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}

func getJSON(url string, v any) error {
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(v)
}

// Result is the benchmark outcome.
type Result struct {
	UserEvents       int64            `json:"user_events"`         // changes made by simulated users
	PRs              int              `json:"pull_requests"`       //
	Issues           int              `json:"issues"`              //
	Webhooks         sim.Stats        `json:"webhooks"`            // everything the simulator sent, sync's own echoes included
	Sync             map[string]int64 `json:"sync_event_log"`      // event-log rows by status
	DuplicatesCaught int64            `json:"duplicate_deliveries_rejected"`
	Converge         map[string]int64 `json:"converge_actions"`
	DuplicateWork    int              `json:"duplicate_work_items"`
	MissingWork      int              `json:"missing_work_items"`
	LostUpdates      int              `json:"lost_updates"`
	Races            int              `json:"concurrent_edit_races"`
	LostKeys         []string         `json:"lost_keys,omitempty"`
	DriveSeconds     float64          `json:"drive_seconds"`
	HealSeconds      float64          `json:"drift_healed_seconds"` // last user change -> both sides agree everywhere
	Converged        bool             `json:"converged"`
	DropRate         float64          `json:"drop_rate"`
	DuplicateRate    float64          `json:"duplicate_rate"`
}

func main() {
	simURL := flag.String("sim", "http://localhost:9100", "simulator base URL")
	syncURL := flag.String("sync", "http://localhost:8090", "sync service base URL")
	events := flag.Int("events", 10000, "user changes to make in total (creates included)")
	objects := flag.Int("objects", 1000, "issues and PRs to create")
	prShare := flag.Float64("pr-share", 0.2, "share of objects that are pull requests")
	devrevShare := flag.Float64("devrev-share", 0.25, "share of later changes made in DevRev")
	rate := flag.Int("rate", 400, "user changes per second")
	settle := flag.Duration("settle", 3*time.Minute, "give up waiting for convergence after this long")
	out := flag.String("out", "", "write the JSON result here")
	md := flag.String("md", "", "write a markdown summary here")
	seed := flag.Uint64("seed", 7, "random seed")
	flag.Parse()

	rng := rand.New(rand.NewPCG(*seed, 1))
	var mu sync.Mutex
	type object struct {
		key     string
		number  int
		pr      bool
		busy    bool
		lastAt  time.Time
		version int
	}
	var objs []*object
	var made atomic.Int64
	start := time.Now()
	tick := time.NewTicker(time.Second / time.Duration(*rate))
	defer tick.Stop()

	// Phase 1: users open issues and pull requests.
	var wg sync.WaitGroup
	for i := range *objects {
		<-tick.C
		pr := rng.Float64() < *prShare
		wg.Go(func() {
			code, body := post(*simURL+"/admin/github/issues", map[string]any{"title": fmt.Sprintf("Bug %d: checkout fails on retry", i), "body": "Steps to reproduce...", "pr": pr})
			var r struct {
				Number int
				Key    string
			}
			if code != 201 || json.Unmarshal(body, &r) != nil {
				log.Printf("create failed: %d %s", code, body)
				return
			}
			made.Add(1)
			mu.Lock()
			objs = append(objs, &object{key: r.Key, number: r.Number, pr: pr, lastAt: time.Now()})
			mu.Unlock()
		})
	}
	wg.Wait()

	// Phase 2: users edit, close, reopen and merge on GitHub, and move work
	// items between stages in DevRev. One change per object at a time, at
	// least 100 ms apart, like people.
	stages := []string{syncer.StageTriage, syncer.StageBacklog, syncer.StageInDev, syncer.StageCompleted, syncer.StageWontFix, syncer.StageDuplicate}
	for made.Load() < int64(*events) {
		<-tick.C
		mu.Lock()
		o := objs[rng.IntN(len(objs))]
		if o.busy || time.Since(o.lastAt) < 100*time.Millisecond {
			mu.Unlock()
			continue
		}
		o.busy = true
		o.version++
		v := o.version
		x := rng.Float64()
		reason := []string{"completed", "not_planned", "duplicate"}[rng.IntN(3)]
		stage := stages[rng.IntN(len(stages))]
		mu.Unlock()
		wg.Go(func() {
			defer func() { mu.Lock(); o.busy, o.lastAt = false, time.Now(); mu.Unlock() }()
			base := fmt.Sprintf("%s/admin/github/issues/%d/", *simURL, o.number)
			var code int
			var body []byte
			switch {
			case x < *devrevShare && !o.pr:
				code, body = post(*simURL+"/admin/devrev/stage", map[string]string{"key": o.key, "stage": stage})
			case x < *devrevShare+0.35:
				code, body = post(base+"edit", map[string]string{"title": fmt.Sprintf("Bug %d (rev %d): checkout fails on retry", o.number, v), "body": "Updated repro"})
			case x < *devrevShare+0.55 && o.pr && rng.IntN(3) == 0:
				code, body = post(base+"merge", nil)
			case x < *devrevShare+0.55:
				code, body = post(base+"close", map[string]string{"reason": reason})
			default:
				code, body = post(base+"reopen", nil)
			}
			var r struct{ Changed bool }
			if code == 200 && json.Unmarshal(body, &r) == nil && r.Changed {
				made.Add(1)
			}
		})
	}
	wg.Wait()
	lastChange := time.Now()
	res := Result{UserEvents: made.Load(), DriveSeconds: lastChange.Sub(start).Seconds()}
	log.Printf("made %d user changes in %.1fs; waiting for the sync to settle", res.UserEvents, res.DriveSeconds)

	// Wait until no webhooks are in flight, the event log is drained, and
	// both systems agree on every object.
	deadline := lastChange.Add(*settle)
	var t sim.Truth
	for {
		var st struct {
			Events map[string]int64 `json:"events"`
		}
		getJSON(*syncURL+"/stats", &st)
		if err := getJSON(*simURL+"/admin/truth", &t); err == nil && t.Stats.InFlight == 0 && st.Events["pending"] == 0 {
			if c := check(t); c.DuplicateWork == 0 && c.MissingWork == 0 && c.LostUpdates == 0 && c.Races == 0 {
				res.Converged = true
				break
			}
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	res.HealSeconds = time.Since(lastChange).Seconds()
	c := check(t)
	res.DuplicateWork, res.MissingWork, res.LostUpdates, res.Races, res.LostKeys = c.DuplicateWork, c.MissingWork, c.LostUpdates, c.Races, c.LostKeys
	res.Webhooks = t.Stats
	for _, i := range t.Issues {
		if i.PR {
			res.PRs++
		} else {
			res.Issues++
		}
	}
	if res.Webhooks.Webhooks > 0 {
		res.DropRate = float64(res.Webhooks.Dropped) / float64(res.Webhooks.Webhooks)
		res.DuplicateRate = float64(res.Webhooks.Duplicated) / float64(res.Webhooks.Webhooks)
	}
	var st struct {
		Events map[string]int64 `json:"events"`
	}
	getJSON(*syncURL+"/stats", &st)
	res.Sync = st.Events
	res.DuplicatesCaught, res.Converge = scrape(*syncURL + "/metrics")

	b, _ := json.MarshalIndent(res, "", "  ")
	if *out != "" {
		os.MkdirAll(filepath.Dir(*out), 0o755)
		os.WriteFile(*out, b, 0o644)
	}
	fmt.Println(string(b))
	if *md != "" {
		os.WriteFile(*md, []byte(markdown(res)), 0o644)
	}
	if !res.Converged {
		os.Exit(1)
	}
}

type checked struct {
	DuplicateWork, MissingWork, LostUpdates, Races int
	LostKeys                                       []string
}

// raceWindow: a sync write this soon after a user's change to the same object
// may have been decided on a read taken just before that change. Neither
// GitHub's nor DevRev's update API takes a version precondition, so no sync
// can close that window; such cases are reported separately.
const raceWindow = 100 * time.Millisecond

// check compares the end state with what users last asked for. Status: the
// last user status change on either side wins. Title: the last GitHub title.
func check(t sim.Truth) checked {
	var c checked
	works := map[string][]int{}
	for i, w := range t.Works {
		k, _ := syncer.KeyFromRef(w.ExternalRef)
		works[k] = append(works[k], i)
	}
	type want struct {
		status, title string
		statusAt      time.Time
		lastUser      time.Time
	}
	wants := map[string]*want{}
	syncWrites := map[string][]time.Time{}
	for _, a := range t.Audit {
		w := wants[a.Key]
		if w == nil {
			w = &want{}
			wants[a.Key] = w
		}
		if a.Actor == "sync" {
			syncWrites[a.Key] = append(syncWrites[a.Key], a.At)
			continue
		}
		w.lastUser = a.At
		if a.Status != "" {
			w.status, w.statusAt = a.Status, a.At
		}
		if a.Title != "" {
			w.title = a.Title
		}
	}
	for _, i := range t.Issues {
		ws := works[i.Key]
		switch {
		case len(ws) == 0:
			c.MissingWork++
			continue
		case len(ws) > 1:
			c.DuplicateWork += len(ws) - 1
		}
		wk := t.Works[ws[0]]
		w := wants[i.Key]
		dv := string(syncer.DevRevStatus(wk.Stage.Name))
		ok := i.Title == w.title && wk.Title == w.title && i.Status == w.status &&
			(dv == w.status || (i.PR && w.status == "open" && dv == "closed:completed"))
		if ok {
			continue
		}
		race := false
		for _, at := range syncWrites[i.Key] {
			if d := at.Sub(w.lastUser); d >= 0 && d < raceWindow {
				race = true
			}
		}
		if race {
			c.Races++
		} else {
			c.LostUpdates++
			if len(c.LostKeys) < 20 {
				c.LostKeys = append(c.LostKeys, fmt.Sprintf("%s: want %q/%q, github %q/%q, devrev %q/%q", i.Key, w.status, w.title, i.Status, i.Title, dv, wk.Title))
			}
		}
	}
	return c
}

// scrape reads duplicate rejections and converge actions from /metrics.
func scrape(url string) (dups int64, actions map[string]int64) {
	actions = map[string]int64{}
	resp, err := client.Get(url)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		v, err := strconv.ParseFloat(line[strings.LastIndexByte(line, ' ')+1:], 64)
		if err != nil || strings.HasPrefix(line, "#") {
			continue
		}
		switch {
		case strings.HasPrefix(line, "sync_webhooks_total{") && strings.Contains(line, `result="duplicate"`):
			dups += int64(v)
		case strings.HasPrefix(line, "sync_converge_total{"):
			_, a, _ := strings.Cut(line, `action="`)
			a, _, _ = strings.Cut(a, `"`)
			actions[a] += int64(v)
		}
	}
	return
}

func markdown(r Result) string {
	var b strings.Builder
	yes := map[bool]string{true: "yes", false: "**no**"}
	fmt.Fprintf(&b, "| | |\n|---|---:|\n")
	row := func(k string, v any) { fmt.Fprintf(&b, "| %s | %v |\n", k, v) }
	row("User changes (GitHub + DevRev)", r.UserEvents)
	row("Objects (issues / pull requests)", fmt.Sprintf("%d / %d", r.Issues, r.PRs))
	row("Webhooks generated (incl. the sync's own echoes)", r.Webhooks.Webhooks)
	row("Delivered twice", fmt.Sprintf("%d (%.1f%%)", r.Webhooks.Duplicated, r.DuplicateRate*100))
	row("Never delivered", fmt.Sprintf("%d (%.1f%%)", r.Webhooks.Dropped, r.DropRate*100))
	row("Delivered out of order", "all (random 0-2 s delay each)")
	row("API calls answered 429 / 5xx", fmt.Sprintf("%d / %d", r.Webhooks.API429, r.Webhooks.API5xx))
	row("Duplicate deliveries rejected by delivery ID", r.DuplicatesCaught)
	row("Events coalesced into a later sync", r.Sync["coalesced"])
	row("Dead-lettered events", r.Sync["dead"])
	row("**Duplicate work items**", r.DuplicateWork)
	row("**Missing work items**", r.MissingWork)
	row("**Lost updates**", r.LostUpdates)
	row("Concurrent-edit races (see README)", r.Races)
	row("**Drift healed within** (last change -> both sides agree)", fmt.Sprintf("%.1f s", r.HealSeconds))
	row("Converged", yes[r.Converged])
	return b.String()
}
