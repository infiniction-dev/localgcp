package gcs

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// startTestServer boots the embedded fake-gcs-server on an ephemeral port.
func startTestServer(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	svc := New("", true) // in-memory, quiet
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go svc.Start(ctx, fmt.Sprintf(":%d", port))

	base := fmt.Sprintf("http://localhost:%d", port)
	for i := 0; i < 100; i++ {
		resp, err := http.Get(base + "/storage/v1/b?project=test")
		if err == nil {
			resp.Body.Close()
			return base
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("gcs server did not start")
	return ""
}

// TestEmbeddedGCSLifecycle verifies the fake-gcs-server embed serves the GCS
// JSON API through localgcp's own listener: create bucket, upload, download.
func TestEmbeddedGCSLifecycle(t *testing.T) {
	base := startTestServer(t)

	// Create bucket.
	resp, err := http.Post(base+"/storage/v1/b?project=test", "application/json",
		strings.NewReader(`{"name":"my-bucket"}`))
	if err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("create bucket status %d: %s", resp.StatusCode, body)
	}
	resp.Body.Close()

	// Upload an object (simple/media upload).
	upURL := base + "/upload/storage/v1/b/my-bucket/o?uploadType=media&name=hello.txt"
	resp, err = http.Post(upURL, "text/plain", strings.NewReader("hello localgcp"))
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("upload status %d: %s", resp.StatusCode, body)
	}
	resp.Body.Close()

	// Download it back.
	resp, err = http.Get(base + "/storage/v1/b/my-bucket/o/hello.txt?alt=media")
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "hello localgcp" {
		t.Fatalf("download mismatch: %q", string(body))
	}
}
