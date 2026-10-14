// Package github is the slice of the GitHub REST API the sync needs: read and
// close/reopen issues and pull requests, list them for reconciliation, and
// verify webhook signatures.
package github

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ThatDeparted2061/devrev-github-sync/internal/retry"
)

// Issue is a GitHub issue or pull request as the issues API returns it.
type Issue struct {
	Number      int       `json:"number"`
	Title       string    `json:"title"`
	Body        string    `json:"body"`
	State       string    `json:"state"`        // open | closed
	StateReason string    `json:"state_reason"` // completed | not_planned | duplicate | reopened
	UpdatedAt   time.Time `json:"updated_at"`
	HTMLURL     string    `json:"html_url"`
	PullRequest *struct {
		MergedAt *time.Time `json:"merged_at"`
	} `json:"pull_request,omitempty"`
}

func (i Issue) IsPR() bool { return i.PullRequest != nil }

// Merged reports whether a pull request was merged.
func (i Issue) Merged() bool { return i.PullRequest != nil && i.PullRequest.MergedAt != nil }

// Client talks to one repository. Calls are single attempts: the sync's
// decisions must rest on fresh reads, so a failed call fails the whole sync,
// which is retried from scratch. Only List (used by the reconciler) retries.
type Client struct {
	rc                *retry.Client
	base, owner, repo string
}

func New(base, token, owner, repo string, observe func(api string, status int)) *Client {
	return &Client{
		base: strings.TrimRight(base, "/"), owner: owner, repo: repo,
		rc: &retry.Client{
			API: "github", HTTP: &http.Client{Timeout: 15 * time.Second}, Attempts: 1, Base: 200 * time.Millisecond, Observe: observe,
			Headers: func(r *http.Request) {
				r.Header.Set("Authorization", "Bearer "+token)
				r.Header.Set("Accept", "application/vnd.github+json")
				r.Header.Set("X-GitHub-Api-Version", "2022-11-28")
			},
		},
	}
}

// Key is the object key used everywhere: owner/repo#number.
func (c *Client) Key(number int) string { return Key(c.owner+"/"+c.repo, number) }

func Key(fullName string, number int) string { return fullName + "#" + strconv.Itoa(number) }

// ParseKey splits owner/repo#number.
func ParseKey(key string) (fullName string, number int, err error) {
	fullName, n, ok := strings.Cut(key, "#")
	if !ok {
		return "", 0, fmt.Errorf("bad key %q", key)
	}
	number, err = strconv.Atoi(n)
	return fullName, number, err
}

func (c *Client) Repo() string { return c.owner + "/" + c.repo }

func (c *Client) Get(ctx context.Context, number int) (*Issue, error) {
	var i Issue
	err := c.rc.Do(ctx, http.MethodGet, fmt.Sprintf("%s/repos/%s/%s/issues/%d", c.base, c.owner, c.repo, number), nil, &i)
	return &i, err
}

// SetState opens or closes an issue or pull request. reason is completed,
// not_planned or duplicate when closing; empty when reopening.
func (c *Client) SetState(ctx context.Context, number int, state, reason string) (*Issue, error) {
	body := map[string]string{"state": state}
	if reason != "" {
		body["state_reason"] = reason
	}
	var i Issue
	err := c.rc.Do(ctx, http.MethodPatch, fmt.Sprintf("%s/repos/%s/%s/issues/%d", c.base, c.owner, c.repo, number), body, &i)
	return &i, err
}

// List returns every issue and pull request updated at or after since
// (zero = all), oldest update first.
func (c *Client) List(ctx context.Context, since time.Time) ([]Issue, error) {
	var all []Issue
	for page := 1; ; page++ {
		q := url.Values{"state": {"all"}, "per_page": {"100"}, "page": {strconv.Itoa(page)}, "sort": {"updated"}, "direction": {"asc"}}
		if !since.IsZero() {
			q.Set("since", since.UTC().Format(time.RFC3339))
		}
		var batch []Issue
		if err := c.rc.Retrying(4).Do(ctx, http.MethodGet, fmt.Sprintf("%s/repos/%s/%s/issues?%s", c.base, c.owner, c.repo, q.Encode()), nil, &batch); err != nil {
			return nil, err
		}
		all = append(all, batch...)
		if len(batch) < 100 {
			return all, nil
		}
	}
}

// Delivery is the part of an issues or pull_request webhook the sync uses.
type Delivery struct {
	Action     string `json:"action"`
	Issue      *Issue `json:"issue"`
	PR         *Issue `json:"pull_request"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}

// Object returns the issue or PR the delivery is about.
func (d Delivery) Object() *Issue {
	if d.Issue != nil {
		return d.Issue
	}
	return d.PR
}

// ParseDelivery reads an issues/pull_request payload.
func ParseDelivery(body []byte) (Delivery, error) {
	var d Delivery
	if err := json.Unmarshal(body, &d); err != nil {
		return d, err
	}
	if d.Object() == nil || d.Repository.FullName == "" {
		return d, fmt.Errorf("payload has no issue or pull_request")
	}
	return d, nil
}

// VerifySignature checks X-Hub-Signature-256 ("sha256=<hex hmac>").
func VerifySignature(secret, body []byte, header string) bool {
	got, ok := strings.CutPrefix(header, "sha256=")
	if !ok {
		return false
	}
	sig, err := hex.DecodeString(got)
	if err != nil {
		return false
	}
	m := hmac.New(sha256.New, secret)
	m.Write(body)
	return hmac.Equal(sig, m.Sum(nil))
}

// Sign returns the X-Hub-Signature-256 value for body.
func Sign(secret, body []byte) string {
	m := hmac.New(sha256.New, secret)
	m.Write(body)
	return "sha256=" + hex.EncodeToString(m.Sum(nil))
}

// StatusChangedAt returns when the issue was last closed or reopened, from
// the issue events API. The sync only asks when both sides changed status
// since the last sync and it needs to know which change came last.
func (c *Client) StatusChangedAt(ctx context.Context, number int) (time.Time, error) {
	var last time.Time
	for page := 1; ; page++ {
		var events []struct {
			Event     string    `json:"event"`
			CreatedAt time.Time `json:"created_at"`
		}
		u := fmt.Sprintf("%s/repos/%s/%s/issues/%d/events?per_page=100&page=%d", c.base, c.owner, c.repo, number, page)
		if err := c.rc.Do(ctx, http.MethodGet, u, nil, &events); err != nil {
			return time.Time{}, err
		}
		for _, e := range events {
			if (e.Event == "closed" || e.Event == "reopened" || e.Event == "merged") && e.CreatedAt.After(last) {
				last = e.CreatedAt
			}
		}
		if len(events) < 100 {
			return last, nil
		}
	}
}
