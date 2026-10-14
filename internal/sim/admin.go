package sim

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ThatDeparted2061/devrev-github-sync/internal/devrev"
	"github.com/ThatDeparted2061/devrev-github-sync/internal/github"
)

// The /admin endpoints are the "users": people editing GitHub and DevRev.
// They never fail, and every change emits a webhook through the chaos layer.

func (s *Sim) adminCreate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Title, Body string
		PR          bool
	}
	json.NewDecoder(r.Body).Decode(&body)
	s.mu.Lock()
	n := s.next
	s.next++
	t := s.now()
	i := &ghIssue{Issue: github.Issue{Number: n, Title: body.Title, Body: body.Body, State: "open", UpdatedAt: t,
		HTMLURL: fmt.Sprintf("https://github.com/%s/issues/%d", s.cfg.Repo, n)}}
	if body.PR {
		i.PullRequest = &struct {
			MergedAt *time.Time `json:"merged_at"`
		}{}
		i.HTMLURL = fmt.Sprintf("https://github.com/%s/pull/%d", s.cfg.Repo, n)
	}
	s.issues[n] = i
	s.audit = append(s.audit, Write{At: t, Side: "github", Actor: "user", Key: s.key(n), Field: "create", Status: "open", Title: body.Title})
	s.mu.Unlock()
	s.githubWebhook("opened", i)
	writeJSON(w, 201, map[string]any{"number": n, "key": s.key(n)})
}

// adminGitHubOp: edit {title, body}, close {reason}, merge, reopen.
func (s *Sim) adminGitHubOp(w http.ResponseWriter, r *http.Request) {
	var body struct{ Title, Body, Reason string }
	json.NewDecoder(r.Body).Decode(&body)
	s.mu.Lock()
	i := s.issue(w, r)
	if i == nil {
		s.mu.Unlock()
		return
	}
	action := ""
	switch op := r.PathValue("op"); op {
	case "edit":
		if body.Title != "" && body.Title != i.Title {
			i.Title, i.Body, i.UpdatedAt = body.Title, body.Body, s.now()
			s.audit = append(s.audit, Write{At: i.UpdatedAt, Side: "github", Actor: "user", Key: s.key(i.Number), Field: "title", Title: i.Title})
			action = "edited"
		}
	case "close":
		action = s.setState(i, "closed", cmpOr(body.Reason, "completed"), false, "user")
	case "merge":
		if i.IsPR() {
			action = s.setState(i, "closed", "", true, "user")
		}
	case "reopen":
		action = s.setState(i, "open", "", false, "user")
	default:
		s.mu.Unlock()
		http.Error(w, "unknown op "+op, http.StatusBadRequest)
		return
	}
	s.mu.Unlock()
	if action != "" {
		s.githubWebhook(action, i)
	}
	writeJSON(w, 200, map[string]any{"changed": action != ""})
}

// adminStage is a DevRev user moving the work item for a GitHub key to a stage.
func (s *Sim) adminStage(w http.ResponseWriter, r *http.Request) {
	var body struct{ Key, Stage string }
	json.NewDecoder(r.Body).Decode(&body)
	ref := "github:" + body.Key
	s.mu.Lock()
	var wk *devrev.Work
	for _, x := range s.works {
		if x.ExternalRef == ref && (wk == nil || x.ID < wk.ID) {
			wk = x
		}
	}
	if wk == nil {
		s.mu.Unlock()
		writeJSON(w, 404, map[string]string{"message": "no work item for " + body.Key + " yet"})
		return
	}
	if wk.Stage.Name == body.Stage {
		s.mu.Unlock()
		writeJSON(w, 200, map[string]any{"changed": false})
		return
	}
	old := *wk
	wk.Stage.Name, wk.ModifiedDate = body.Stage, s.now()
	if dvStatus(old.Stage.Name) != dvStatus(wk.Stage.Name) {
		s.audit = append(s.audit, Write{At: wk.ModifiedDate, Side: "devrev", Actor: "user", Key: body.Key, Field: "status", Status: dvStatus(wk.Stage.Name)})
	}
	out := *wk
	s.mu.Unlock()
	s.devrevWebhook("work_updated", &out, &old)
	writeJSON(w, 200, map[string]any{"changed": true})
}

// Truth is the full state of both systems plus the audit log.
type Truth struct {
	Issues []TruthIssue  `json:"issues"`
	Works  []devrev.Work `json:"works"`
	Audit  []Write       `json:"audit"`
	Stats  Stats         `json:"stats"`
}

type TruthIssue struct {
	Key    string `json:"key"`
	PR     bool   `json:"pr"`
	Title  string `json:"title"`
	Status string `json:"status"`
}

func (s *Sim) truth(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	t := Truth{Audit: append([]Write{}, s.audit...), Stats: s.Stats()}
	for n := 1; n < s.next; n++ {
		i := s.issues[n]
		t.Issues = append(t.Issues, TruthIssue{Key: s.key(n), PR: i.IsPR(), Title: i.Title, Status: ghStatus(i)})
	}
	for _, wk := range s.works {
		c := *wk
		c.ExternalRef = strings.TrimSpace(c.ExternalRef)
		t.Works = append(t.Works, c)
	}
	s.mu.Unlock()
	writeJSON(w, 200, t)
}
