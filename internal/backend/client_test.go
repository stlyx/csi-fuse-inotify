package backend

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestConditionalGet(t *testing.T) {
	change := make(chan struct{})
	waiting := make(chan struct{})
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/volumes/test" || r.Header.Get("Cache-Control") != "no-store" {
			t.Error("unexpected request")
		}
		switch r.Header.Get("If-None-Match") {
		case "":
			w.Header().Set("ETag", `"1"`)
			io.WriteString(w, "first")
		case `"1"`:
			close(waiting)
			select {
			case <-change:
			case <-r.Context().Done():
				return
			}
			w.Header().Set("ETag", `"2"`)
		case `"2"`:
			w.WriteHeader(http.StatusNotModified)
		default:
			t.Error("unexpected version")
		}
	}))
	defer srv.Close()
	c, err := New(srv.URL + "/api")
	if err != nil {
		t.Fatal(err)
	}
	c.http.Transport = srv.Client().Transport
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	first, err := c.Get(ctx, "test", "")
	if err != nil || first.Version != `"1"` || string(first.Data) != "first" {
		t.Fatalf("initial snapshot: %v", err)
	}
	go func() {
		select {
		case <-waiting:
			close(change)
		case <-ctx.Done():
		}
	}()
	next, err := c.Get(ctx, "test", first.Version)
	if err != nil || next.Version != `"2"` || len(next.Data) != 0 {
		t.Fatalf("empty revision: %v", err)
	}
	next, err = c.Get(ctx, "test", next.Version)
	if err != nil || next != nil {
		t.Fatalf("heartbeat: %v", err)
	}
}

func TestRejectInvalidResponses(t *testing.T) {
	const marker = "private-response-body"
	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"missing version", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, marker) }},
		{"backend failure", func(w http.ResponseWriter, r *http.Request) { http.Error(w, marker, 500) }},
		{"oversize chunked", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("ETag", `"3"`)
			w.(http.Flusher).Flush()
			block := make([]byte, 65536)
			for n := 0; n <= MaxSize; n += len(block) {
				if _, err := w.Write(block); err != nil {
					return
				}
			}
		}},
		{"truncated body", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("ETag", `"4"`)
			w.Header().Set("Content-Length", "2048")
			io.WriteString(w, marker)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewTLSServer(tc.handler)
			defer srv.Close()
			c, err := New(srv.URL)
			if err != nil {
				t.Fatal(err)
			}
			c.http.Transport = srv.Client().Transport
			got, err := c.Get(context.Background(), "test", "")
			if got != nil || err == nil || strings.Contains(err.Error(), marker) {
				t.Fatal("expected a sanitized failure")
			}
		})
	}
}
