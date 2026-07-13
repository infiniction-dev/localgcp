package job

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"cloud.google.com/go/run/apiv2/runpb"
)

func TestLoadSeedAndAutoRegister(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "jobs.yaml")
	yaml := `
jobs:
  - name: report
    project: demo
    region: us-central1
    image: demo-worker:local
    command: ["/app/worker"]
    env:
      STAGE: dev
    tasks: 3
    parallelism: 2
    maxRetries: 1
`
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}

	seeds, err := LoadSeed(path)
	if err != nil {
		t.Fatalf("LoadSeed: %v", err)
	}
	if len(seeds) != 1 {
		t.Fatalf("expected 1 seed, got %d", len(seeds))
	}

	// A server built with the seed auto-registers the job (no CreateJob call).
	srv := New(&fakeRunner{}, nil, seeds)
	got, err := srv.GetJob(context.Background(), &runpb.GetJobRequest{
		Name: "projects/demo/locations/us-central1/jobs/report",
	})
	if err != nil {
		t.Fatalf("expected seeded job to be registered: %v", err)
	}
	tmpl := got.GetTemplate()
	if tmpl.GetTaskCount() != 3 || tmpl.GetParallelism() != 2 {
		t.Fatalf("wrong template: tasks=%d parallelism=%d", tmpl.GetTaskCount(), tmpl.GetParallelism())
	}
	c := tmpl.GetTemplate().GetContainers()[0]
	if c.GetImage() != "demo-worker:local" || len(c.GetCommand()) != 1 {
		t.Fatalf("wrong container: image=%s command=%v", c.GetImage(), c.GetCommand())
	}
	if tmpl.GetTemplate().GetMaxRetries() != 1 {
		t.Fatalf("expected maxRetries=1, got %d", tmpl.GetTemplate().GetMaxRetries())
	}
}

func TestLoadSeedDefaultsAndValidation(t *testing.T) {
	if seeds, err := LoadSeed(""); err != nil || seeds != nil {
		t.Fatalf("empty path should return (nil,nil), got %v %v", seeds, err)
	}

	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.yaml")
	os.WriteFile(bad, []byte("jobs:\n  - name: x\n"), 0o644) // missing image
	if _, err := LoadSeed(bad); err == nil {
		t.Fatal("expected error for job missing image")
	}
}
