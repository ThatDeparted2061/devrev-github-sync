// Package retry holds the backoff policy and an HTTP helper that retries
// throttled and failed calls.
package retry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

// Backoff is base * 2^attempt, capped, with full jitter (a random delay
// between half and all of it) so retries from many workers spread out.
func Backoff(attempt int, base, max time.Duration) time.Duration {
	d := base << min(attempt, 30)
	if d <= 0 || d > max {
		d = max
	}
	return d/2 + rand.N(d/2+1)
}

// StatusError is a non-2xx API answer.
type StatusError struct {
	API        string
	Code       int
	Body       string
	RetryAfter time.Duration
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("%s: HTTP %d: %s", e.API, e.Code, e.Body)
}

// Retryable reports whether the same call may succeed later.
func (e *StatusError) Retryable() bool {
	return e.Code == http.StatusTooManyRequests || e.Code >= 500 ||
		(e.Code == http.StatusForbidden && e.RetryAfter > 0) // GitHub secondary rate limit
}

// Client sends JSON requests and retries 429/5xx and network errors a few
// times with backoff, honouring Retry-After. Longer outages are left to the
// event queue's own retries.
type Client struct {
	API      string
	HTTP     *http.Client
	Attempts int
	Base     time.Duration
	Headers  func(*http.Request)
	// Observe, if set, is told the outcome of every attempt.
	Observe func(api string, status int)
}

// Do sends method url with body (nil for none) and decodes a 2xx answer into out.
func (c *Client) Do(ctx context.Context, method, url string, body, out any) error {
	var raw []byte
	if body != nil {
		var err error
		if raw, err = json.Marshal(body); err != nil {
			return err
		}
	}
	var last error
	for attempt := 0; attempt < max(1, c.Attempts); attempt++ {
		if attempt > 0 {
			wait := Backoff(attempt-1, c.Base, 5*time.Second)
			if se, ok := last.(*StatusError); ok && se.RetryAfter > 0 {
				wait = min(se.RetryAfter, 10*time.Second)
			}
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		last = c.once(ctx, method, url, raw, out)
		if last == nil {
			return nil
		}
		if se, ok := last.(*StatusError); ok && !se.Retryable() {
			return last
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return last
}

func (c *Client) once(ctx context.Context, method, url string, raw []byte, out any) error {
	var rd io.Reader
	if raw != nil {
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rd)
	if err != nil {
		return err
	}
	if raw != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if c.Headers != nil {
		c.Headers(req)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		if c.Observe != nil {
			c.Observe(c.API, 0)
		}
		return fmt.Errorf("%s: %w", c.API, err)
	}
	defer resp.Body.Close()
	if c.Observe != nil {
		c.Observe(c.API, resp.StatusCode)
	}
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		se := &StatusError{API: c.API, Code: resp.StatusCode, Body: string(bytes.TrimSpace(b))}
		if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			se.RetryAfter = time.Duration(s) * time.Second
		}
		return se
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
