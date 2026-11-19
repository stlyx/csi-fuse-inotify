// Package backend reads bounded, versioned snapshots over HTTPS long polling.
package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"time"
)

const MaxSize = 4 << 20

var validID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

func ValidID(id string) bool { return validID.MatchString(id) }

type Snapshot struct {
	Version string
	Data    []byte
}

type Client struct {
	base *url.URL
	http *http.Client
}

func New(endpoint string) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("backend must be an HTTPS base URL")
	}
	return &Client{base: u, http: &http.Client{
		Timeout:       40 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

// Get returns nil on a heartbeat. Errors contain neither body nor backend headers.
func (c *Client) Get(ctx context.Context, id, previous string) (*Snapshot, error) {
	if !ValidID(id) {
		return nil, errors.New("invalid volume ID")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base.JoinPath("volumes", id).String(), nil)
	if err != nil {
		return nil, errors.New("invalid backend request")
	}
	req.Header.Set("Cache-Control", "no-store")
	if previous != "" {
		req.Header.Set("If-None-Match", previous)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, errors.New("backend connection failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotModified && previous != "" {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("backend HTTP status %d", resp.StatusCode)
	}
	version := resp.Header.Get("ETag")
	if len(version) < 2 || len(version) > 256 || version[0] != '"' || version[len(version)-1] != '"' {
		return nil, errors.New("missing or invalid strong ETag")
	}
	for i := 1; i < len(version)-1; i++ {
		if version[i] < 0x21 || version[i] == '"' || version[i] == 0x7f {
			return nil, errors.New("invalid ETag")
		}
	}
	if resp.ContentLength > MaxSize {
		return nil, errors.New("snapshot too large")
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxSize+1))
	if err != nil {
		return nil, errors.New("incomplete snapshot")
	}
	if len(data) > MaxSize {
		return nil, errors.New("snapshot too large")
	}
	if version == previous {
		return nil, nil
	}
	return &Snapshot{Version: version, Data: data}, nil
}
