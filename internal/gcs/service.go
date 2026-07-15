// Package gcs provides the Cloud Storage emulator for localgcp. It embeds
// fsouza/fake-gcs-server as a library and hosts its HTTP handler on localgcp's
// configured GCS port.
//
// Object-change notifications are a first-class fake-gcs-server feature: buckets
// carry notificationConfigs (the standard GCS JSON API), and matching object
// events are published to Pub/Sub. fake-gcs-server publishes via the Google
// Pub/Sub client library, so it targets whatever PUBSUB_EMULATOR_HOST points at
// — in localgcp that is the orchestrated Pub/Sub emulator. main() sets that env
// var before services start.
package gcs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
)

// pubsubResourcePrefix is the Cloud Storage JSON API's canonical topic prefix.
// Standard clients (Go storage client, gcloud, terraform) send notification
// topics as "//pubsub.googleapis.com/projects/{p}/topics/{t}", but
// fake-gcs-server's publisher only understands the bare "projects/{p}/topics/{t}"
// form. normalizeNotificationTopics bridges the two.
const pubsubResourcePrefix = "//pubsub.googleapis.com/"

// Service adapts fake-gcs-server to localgcp's server.Service interface.
type Service struct {
	dataDir string
	quiet   bool
	logger  *log.Logger
}

// New creates the Cloud Storage service. When dataDir is set, objects persist to
// dataDir/gcs via fake-gcs-server's filesystem backend; otherwise storage is
// in-memory.
func New(dataDir string, quiet bool) *Service {
	return &Service{
		dataDir: dataDir,
		quiet:   quiet,
		logger:  log.New(os.Stderr, "[gcs] ", log.LstdFlags),
	}
}

func (s *Service) Name() string { return "Cloud Storage" }

func (s *Service) Start(ctx context.Context, addr string) error {
	opts := fakestorage.Options{
		Scheme:     "http",
		NoListener: true, // we host the handler on our own listener below
	}
	if s.dataDir != "" {
		root := filepath.Join(s.dataDir, "gcs")
		if err := os.MkdirAll(root, 0o755); err != nil {
			return fmt.Errorf("gcs: create storage root: %w", err)
		}
		opts.StorageRoot = root
	}
	// fake-gcs-server uses Writer both for request logging and for reporting
	// notification publish errors. Keep it quiet in CI.
	if s.quiet {
		opts.Writer = io.Discard
	} else {
		opts.Writer = os.Stderr
	}

	fs, err := fakestorage.NewServerWithOptions(opts)
	if err != nil {
		return fmt.Errorf("gcs: init fake-gcs-server: %w", err)
	}
	defer fs.Stop()

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("gcs: listen on %s: %w", addr, err)
	}

	srv := &http.Server{Handler: normalizeNotificationTopics(fs.HTTPHandler())}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	if err := srv.Serve(ln); err != http.ErrServerClosed {
		return err
	}
	return nil
}

// normalizeNotificationTopics wraps the fake-gcs-server handler to reconcile the
// notification topic format between standard GCS clients and fake-gcs-server.
//
// On the way in (POST/PATCH notificationConfigs) it strips the
// "//pubsub.googleapis.com/" prefix so fake-gcs-server's publisher can resolve
// the topic. On the way out (GET/POST responses) it re-adds the prefix so
// clients parse the topic project/ID back correctly. Both rewrites are guarded
// by prefix checks, so they are no-ops if the input is already in the expected
// form (and thus safe if fake-gcs-server fixes this upstream).
func normalizeNotificationTopics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/notificationConfigs") {
			next.ServeHTTP(w, r)
			return
		}

		if (r.Method == http.MethodPost || r.Method == http.MethodPatch) && r.Body != nil {
			if body, err := io.ReadAll(r.Body); err == nil {
				r.Body.Close()
				body = rewriteTopics(body, stripPubsubPrefix)
				r.Body = io.NopCloser(bytes.NewReader(body))
				r.ContentLength = int64(len(body))
			}
		}

		rec := &bufferedResponse{header: http.Header{}, status: http.StatusOK, body: &bytes.Buffer{}}
		next.ServeHTTP(rec, r)

		out := rec.body.Bytes()
		if strings.Contains(rec.header.Get("Content-Type"), "application/json") {
			out = rewriteTopics(out, addPubsubPrefix)
		}
		for k, vals := range rec.header {
			w.Header()[k] = vals
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(out)))
		w.WriteHeader(rec.status)
		w.Write(out)
	})
}

func stripPubsubPrefix(topic string) string {
	return strings.TrimPrefix(topic, pubsubResourcePrefix)
}

func addPubsubPrefix(topic string) string {
	if topic == "" || strings.HasPrefix(topic, pubsubResourcePrefix) {
		return topic
	}
	if strings.HasPrefix(topic, "projects/") {
		return pubsubResourcePrefix + topic
	}
	return topic
}

// rewriteTopics applies fn to the "topic" field of a notificationConfig JSON
// document, whether it is a single resource or a {"items":[...]} list. If the
// body is not JSON it is returned unchanged.
func rewriteTopics(body []byte, fn func(string) string) []byte {
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		return body
	}
	changed := rewriteTopicField(doc, fn)
	if items, ok := doc["items"].([]any); ok {
		for _, it := range items {
			if m, ok := it.(map[string]any); ok {
				changed = rewriteTopicField(m, fn) || changed
			}
		}
	}
	if !changed {
		return body
	}
	if out, err := json.Marshal(doc); err == nil {
		return out
	}
	return body
}

func rewriteTopicField(m map[string]any, fn func(string) string) bool {
	if t, ok := m["topic"].(string); ok {
		if nt := fn(t); nt != t {
			m["topic"] = nt
			return true
		}
	}
	return false
}

// bufferedResponse captures a handler's response so the body can be rewritten
// before it is sent to the client.
type bufferedResponse struct {
	header      http.Header
	status      int
	body        *bytes.Buffer
	wroteHeader bool
}

func (b *bufferedResponse) Header() http.Header { return b.header }

func (b *bufferedResponse) WriteHeader(status int) {
	if !b.wroteHeader {
		b.status = status
		b.wroteHeader = true
	}
}

func (b *bufferedResponse) Write(p []byte) (int, error) {
	b.wroteHeader = true
	return b.body.Write(p)
}
