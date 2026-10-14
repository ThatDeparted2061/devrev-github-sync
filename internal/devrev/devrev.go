// Package devrev is the slice of the DevRev public API the sync needs
// (works.create/get/list/update) plus webhook verification.
// API reference: https://developer.devrev.ai/public/api-reference
package devrev

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ThatDeparted2061/devrev-github-sync/internal/retry"
)

type Stage struct {
	Name string `json:"name"`
}

// Work is a DevRev work item (the sync creates type "issue").
type Work struct {
	ID           string    `json:"id"`
	DisplayID    string    `json:"display_id,omitempty"`
	Type         string    `json:"type"`
	Title        string    `json:"title"`
	Body         string    `json:"body,omitempty"`
	Stage        Stage     `json:"stage"`
	ExternalRef  string    `json:"external_ref,omitempty"`
	Tags         []Tag     `json:"tags,omitempty"`
	ModifiedDate time.Time `json:"modified_date"`
}

type Tag struct {
	Tag struct {
		Name string `json:"name"`
	} `json:"tag"`
}

// Client talks to one DevRev org.
type Client struct {
	rc         *retry.Client
	base       string
	Part, User string // applies_to_part and owned_by for new work items
}

func New(base, token, part, user string, observe func(api string, status int)) *Client {
	return &Client{
		base: strings.TrimRight(base, "/"), Part: part, User: user,
		rc: &retry.Client{
			API: "devrev", HTTP: &http.Client{Timeout: 15 * time.Second}, Attempts: 3, Base: 200 * time.Millisecond, Observe: observe,
			Headers: func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token) },
		},
	}
}

type workEnvelope struct {
	Work Work `json:"work"`
}

// Create makes an issue. It is never retried here: if the response is lost
// after DevRev created the item, the caller's next attempt finds it by
// external_ref instead of creating it twice. external_ref is also unique per
// work type in DevRev, a second line of defence.
func (c *Client) Create(ctx context.Context, title, body, stage, externalRef string) (*Work, error) {
	var out workEnvelope
	err := c.rc.DoOnce(ctx, http.MethodPost, c.base+"/works.create", map[string]any{
		"type": "issue", "title": title, "body": body, "applies_to_part": c.Part, "owned_by": []string{c.User},
		"external_ref": externalRef, "stage": Stage{Name: stage},
	}, &out)
	return &out.Work, err
}

func (c *Client) Get(ctx context.Context, id string) (*Work, error) {
	var out workEnvelope
	err := c.rc.Do(ctx, http.MethodGet, c.base+"/works.get?"+url.Values{"id": {id}}.Encode(), nil, &out)
	return &out.Work, err
}

// FindByExternalRef returns the issue with this external_ref, or nil.
func (c *Client) FindByExternalRef(ctx context.Context, ref string) (*Work, error) {
	var out struct {
		Works []Work `json:"works"`
	}
	q := url.Values{"external_ref": {ref}, "type": {"issue"}}
	if err := c.rc.Do(ctx, http.MethodGet, c.base+"/works.list?"+q.Encode(), nil, &out); err != nil {
		return nil, err
	}
	for _, w := range out.Works {
		if w.ExternalRef == ref {
			return &w, nil
		}
	}
	return nil, nil
}

// Update sets the title, body and stage. Empty values are left unchanged.
func (c *Client) Update(ctx context.Context, id, title, body, stage string) (*Work, error) {
	req := map[string]any{"id": id}
	if title != "" {
		req["title"] = title
	}
	if body != "" {
		req["body"] = body
	}
	if stage != "" {
		req["stage"] = Stage{Name: stage}
	}
	var out workEnvelope
	err := c.rc.Do(ctx, http.MethodPost, c.base+"/works.update", req, &out)
	return &out.Work, err
}

// List returns every issue in the org, for reconciliation.
func (c *Client) List(ctx context.Context) ([]Work, error) {
	var all []Work
	cursor := ""
	for {
		q := url.Values{"type": {"issue"}, "limit": {"100"}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		var out struct {
			Works      []Work `json:"works"`
			NextCursor string `json:"next_cursor"`
		}
		if err := c.rc.Do(ctx, http.MethodGet, c.base+"/works.list?"+q.Encode(), nil, &out); err != nil {
			return nil, err
		}
		all = append(all, out.Works...)
		if out.NextCursor == "" || len(out.Works) == 0 {
			return all, nil
		}
		cursor = out.NextCursor
	}
}

// Event is a DevRev webhook delivery.
type Event struct {
	ID          string    `json:"id"`
	Type        string    `json:"type"` // verify | work_created | work_updated | work_deleted ...
	Timestamp   time.Time `json:"timestamp"`
	WorkCreated *struct {
		Work Work `json:"work"`
	} `json:"work_created,omitempty"`
	WorkUpdated *struct {
		Work    Work  `json:"work"`
		OldWork *Work `json:"old_work,omitempty"`
	} `json:"work_updated,omitempty"`
	Verify *struct {
		Challenge string `json:"challenge"`
	} `json:"verify,omitempty"`
}

// Work returns the work item the event is about, or nil.
func (e Event) Work() *Work {
	switch {
	case e.WorkUpdated != nil:
		return &e.WorkUpdated.Work
	case e.WorkCreated != nil:
		return &e.WorkCreated.Work
	}
	return nil
}

func ParseEvent(body []byte) (Event, error) {
	var e Event
	err := json.Unmarshal(body, &e)
	return e, err
}

// VerifySignature checks X-DevRev-Signature: HMAC-SHA256 of the raw body with
// the webhook secret. DevRev's docs do not pin the encoding, so both hex and
// base64 are accepted.
func VerifySignature(secret, body []byte, header string) bool {
	m := hmac.New(sha256.New, secret)
	m.Write(body)
	want := m.Sum(nil)
	if sig, err := hex.DecodeString(header); err == nil && hmac.Equal(sig, want) {
		return true
	}
	sig, err := base64.StdEncoding.DecodeString(header)
	return err == nil && hmac.Equal(sig, want)
}

// Sign returns the hex X-DevRev-Signature for body.
func Sign(secret, body []byte) string {
	m := hmac.New(sha256.New, secret)
	m.Write(body)
	return hex.EncodeToString(m.Sum(nil))
}
