package job

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/run/apiv2/runpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// fakeRunner records task runs and decides each task's exit code, so execution
// paths can be tested without Docker.
type fakeRunner struct {
	mu        sync.Mutex
	total     int
	active    int
	maxActive int
	delay     time.Duration
	env       []string                            // Env seen on the most recent RunTask
	command   []string                            // Command seen on the most recent RunTask
	decide    func(idx, attempt int) (int, error) // nil => success
}

func (f *fakeRunner) RunTask(ctx context.Context, spec TaskSpec) (int, error) {
	idx := envInt(spec.Env, "CLOUD_RUN_TASK_INDEX")
	attempt := envInt(spec.Env, "CLOUD_RUN_TASK_ATTEMPT")

	f.mu.Lock()
	f.total++
	f.active++
	if f.active > f.maxActive {
		f.maxActive = f.active
	}
	f.env = spec.Env
	f.command = spec.Command
	f.mu.Unlock()
	defer func() { f.mu.Lock(); f.active--; f.mu.Unlock() }()

	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return -1, ctx.Err()
		}
	}
	if f.decide != nil {
		return f.decide(idx, attempt)
	}
	return 0, nil
}

func envInt(env []string, key string) int {
	for _, kv := range env {
		if strings.HasPrefix(kv, key+"=") {
			n, _ := strconv.Atoi(strings.TrimPrefix(kv, key+"="))
			return n
		}
	}
	return -1
}

func testClients(t *testing.T, runner Runner) (runpb.JobsClient, runpb.ExecutionsClient, runpb.TasksClient, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	New(runner, nil).Register(srv)
	go srv.Serve(ln)

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}
	cleanup := func() { conn.Close(); srv.Stop() }
	return runpb.NewJobsClient(conn), runpb.NewExecutionsClient(conn), runpb.NewTasksClient(conn), cleanup
}

const parent = "projects/test/locations/us-central1"

func createJob(t *testing.T, jobs runpb.JobsClient, id string, taskCount, parallelism, maxRetries int32) *runpb.Job {
	t.Helper()
	op, err := jobs.CreateJob(context.Background(), &runpb.CreateJobRequest{
		Parent: parent, JobId: id,
		Job: &runpb.Job{Template: &runpb.ExecutionTemplate{
			TaskCount:   taskCount,
			Parallelism: parallelism,
			Template: &runpb.TaskTemplate{
				Retries:    &runpb.TaskTemplate_MaxRetries{MaxRetries: maxRetries},
				Containers: []*runpb.Container{{Image: "gcr.io/test/job:latest"}},
			},
		}},
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	var job runpb.Job
	if err := op.GetResponse().UnmarshalTo(&job); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return &job
}

// waitExec polls GetExecution until its Completed condition reaches a terminal
// state, then returns the final execution.
func waitExec(t *testing.T, execs runpb.ExecutionsClient, name string) *runpb.Execution {
	t.Helper()
	for i := 0; i < 200; i++ {
		e, err := execs.GetExecution(context.Background(), &runpb.GetExecutionRequest{Name: name})
		if err != nil {
			t.Fatalf("GetExecution: %v", err)
		}
		if st := e.GetConditions()[0].GetState(); st == runpb.Condition_CONDITION_SUCCEEDED || st == runpb.Condition_CONDITION_FAILED {
			return e
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("execution %s did not complete", name)
	return nil
}

func runJob(t *testing.T, jobs runpb.JobsClient, name string) *runpb.Execution {
	t.Helper()
	op, err := jobs.RunJob(context.Background(), &runpb.RunJobRequest{Name: name})
	if err != nil {
		t.Fatalf("RunJob: %v", err)
	}
	var exec runpb.Execution
	if err := op.GetResponse().UnmarshalTo(&exec); err != nil {
		t.Fatalf("unmarshal exec: %v", err)
	}
	return &exec
}

func TestJobCRUD(t *testing.T) {
	jobs, _, _, cleanup := testClients(t, &fakeRunner{})
	defer cleanup()
	ctx := context.Background()

	created := createJob(t, jobs, "my-job", 1, 1, 0)
	if created.Name != parent+"/jobs/my-job" {
		t.Fatalf("wrong name: %s", created.Name)
	}
	if created.GetTerminalCondition().GetState() != runpb.Condition_CONDITION_SUCCEEDED {
		t.Fatal("expected job Ready=SUCCEEDED")
	}

	got, err := jobs.GetJob(ctx, &runpb.GetJobRequest{Name: created.Name})
	if err != nil || got.Name != created.Name {
		t.Fatalf("GetJob: %v", err)
	}

	list, err := jobs.ListJobs(ctx, &runpb.ListJobsRequest{Parent: parent})
	if err != nil || len(list.Jobs) != 1 {
		t.Fatalf("ListJobs: %v n=%d", err, len(list.GetJobs()))
	}

	if _, err := jobs.DeleteJob(ctx, &runpb.DeleteJobRequest{Name: created.Name}); err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}
	if _, err := jobs.GetJob(ctx, &runpb.GetJobRequest{Name: created.Name}); status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound after delete, got %v", err)
	}
}

func TestDuplicateJobFails(t *testing.T) {
	jobs, _, _, cleanup := testClients(t, &fakeRunner{})
	defer cleanup()
	createJob(t, jobs, "dup", 1, 1, 0)
	_, err := jobs.CreateJob(context.Background(), &runpb.CreateJobRequest{Parent: parent, JobId: "dup", Job: &runpb.Job{}})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}
}

func TestRunJobSucceeds(t *testing.T) {
	fr := &fakeRunner{}
	jobs, execs, tasks, cleanup := testClients(t, fr)
	defer cleanup()
	ctx := context.Background()

	job := createJob(t, jobs, "batch", 3, 3, 0)
	started := runJob(t, jobs, job.Name)
	if started.RunningCount != 3 || started.GetConditions()[0].GetState() != runpb.Condition_CONDITION_RECONCILING {
		t.Fatalf("expected initial RUNNING with running=3, got running=%d state=%s", started.RunningCount, started.GetConditions()[0].GetState())
	}

	done := waitExec(t, execs, started.Name)
	if done.SucceededCount != 3 || done.FailedCount != 0 || done.RunningCount != 0 {
		t.Fatalf("counts: succeeded=%d failed=%d running=%d", done.SucceededCount, done.FailedCount, done.RunningCount)
	}
	if done.GetConditions()[0].GetState() != runpb.Condition_CONDITION_SUCCEEDED {
		t.Fatal("expected Completed=SUCCEEDED")
	}
	if fr.total != 3 {
		t.Fatalf("expected 3 task runs, got %d", fr.total)
	}

	tl, err := tasks.ListTasks(ctx, &runpb.ListTasksRequest{Parent: started.Name})
	if err != nil || len(tl.Tasks) != 3 {
		t.Fatalf("ListTasks: %v n=%d", err, len(tl.GetTasks()))
	}
	for i, task := range tl.Tasks {
		if task.Index != int32(i) || task.GetConditions()[0].GetState() != runpb.Condition_CONDITION_SUCCEEDED {
			t.Fatalf("task %d not succeeded", i)
		}
	}

	after, _ := jobs.GetJob(ctx, &runpb.GetJobRequest{Name: job.Name})
	if after.ExecutionCount != 1 ||
		after.GetLatestCreatedExecution().GetCompletionStatus() != runpb.ExecutionReference_EXECUTION_SUCCEEDED {
		t.Fatalf("job not updated: count=%d status=%s", after.ExecutionCount, after.GetLatestCreatedExecution().GetCompletionStatus())
	}
}

func TestRunJobFailure(t *testing.T) {
	fr := &fakeRunner{decide: func(idx, attempt int) (int, error) { return 1, nil }} // always exit 1
	jobs, execs, _, cleanup := testClients(t, fr)
	defer cleanup()

	job := createJob(t, jobs, "failing", 2, 2, 0)
	started := runJob(t, jobs, job.Name)
	done := waitExec(t, execs, started.Name)
	if done.FailedCount != 2 || done.SucceededCount != 0 {
		t.Fatalf("expected 2 failed, got failed=%d succeeded=%d", done.FailedCount, done.SucceededCount)
	}
	if done.GetConditions()[0].GetState() != runpb.Condition_CONDITION_FAILED {
		t.Fatal("expected Completed=FAILED")
	}
	after, _ := jobs.GetJob(context.Background(), &runpb.GetJobRequest{Name: job.Name})
	if after.GetLatestCreatedExecution().GetCompletionStatus() != runpb.ExecutionReference_EXECUTION_FAILED {
		t.Fatal("expected job latest EXECUTION_FAILED")
	}
}

func TestRunJobRetriesThenSucceeds(t *testing.T) {
	// Fail attempts 0 and 1, succeed on attempt 2.
	fr := &fakeRunner{decide: func(idx, attempt int) (int, error) {
		if attempt < 2 {
			return 1, nil
		}
		return 0, nil
	}}
	jobs, execs, tasks, cleanup := testClients(t, fr)
	defer cleanup()

	job := createJob(t, jobs, "retry", 1, 1, 2) // max_retries=2
	started := runJob(t, jobs, job.Name)
	done := waitExec(t, execs, started.Name)
	if done.SucceededCount != 1 || done.FailedCount != 0 {
		t.Fatalf("expected success after retries, got succeeded=%d failed=%d", done.SucceededCount, done.FailedCount)
	}
	if fr.total != 3 {
		t.Fatalf("expected 3 attempts, got %d", fr.total)
	}
	tl, _ := tasks.ListTasks(context.Background(), &runpb.ListTasksRequest{Parent: started.Name})
	if tl.Tasks[0].Retried != 2 {
		t.Fatalf("expected retried=2, got %d", tl.Tasks[0].Retried)
	}
}

func TestParallelismBounded(t *testing.T) {
	fr := &fakeRunner{delay: 40 * time.Millisecond} // hold tasks so they overlap
	jobs, execs, _, cleanup := testClients(t, fr)
	defer cleanup()

	job := createJob(t, jobs, "parallel", 4, 2, 0) // 4 tasks, parallelism 2
	started := runJob(t, jobs, job.Name)
	waitExec(t, execs, started.Name)
	if fr.maxActive != 2 {
		t.Fatalf("expected max 2 concurrent tasks, got %d", fr.maxActive)
	}
}

func TestRunJobOverridesEnvAndArgs(t *testing.T) {
	fr := &fakeRunner{}
	jobs, execs, _, cleanup := testClients(t, fr)
	defer cleanup()
	ctx := context.Background()

	// Job defined with a base env var and base args.
	op, err := jobs.CreateJob(ctx, &runpb.CreateJobRequest{
		Parent: parent, JobId: "ovr",
		Job: &runpb.Job{Template: &runpb.ExecutionTemplate{
			Template: &runpb.TaskTemplate{Containers: []*runpb.Container{{
				Image: "img",
				Args:  []string{"base-arg"},
				Env:   []*runpb.EnvVar{{Name: "BASE", Values: &runpb.EnvVar_Value{Value: "1"}}},
			}}},
		}},
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	var job runpb.Job
	if err := op.GetResponse().UnmarshalTo(&job); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Per-dispatch overrides: extra env + replacement args.
	runOp, err := jobs.RunJob(ctx, &runpb.RunJobRequest{
		Name: job.Name,
		Overrides: &runpb.RunJobRequest_Overrides{
			ContainerOverrides: []*runpb.RunJobRequest_Overrides_ContainerOverride{{
				Env: []*runpb.EnvVar{
					{Name: "OUTBOX_MESSAGE_ID", Values: &runpb.EnvVar_Value{Value: "m1"}},
				},
				Args: []string{"run-arg"},
			}},
		},
	})
	if err != nil {
		t.Fatalf("RunJob: %v", err)
	}
	var started runpb.Execution
	runOp.GetResponse().UnmarshalTo(&started)
	waitExec(t, execs, started.Name)

	fr.mu.Lock()
	env, command := fr.env, fr.command
	fr.mu.Unlock()

	if !hasEnv(env, "BASE=1") {
		t.Fatalf("job base env dropped: %v", env)
	}
	if !hasEnv(env, "OUTBOX_MESSAGE_ID=m1") {
		t.Fatalf("override env not applied: %v", env)
	}
	if !hasEnv(env, "CLOUD_RUN_TASK_INDEX=0") {
		t.Fatalf("task env missing: %v", env)
	}
	if len(command) != 1 || command[0] != "run-arg" {
		t.Fatalf("override args not applied: %v", command)
	}
}

func TestRunJobOverridesTaskCount(t *testing.T) {
	fr := &fakeRunner{}
	jobs, execs, _, cleanup := testClients(t, fr)
	defer cleanup()

	job := createJob(t, jobs, "count", 1, 1, 0) // job defines 1 task
	runOp, err := jobs.RunJob(context.Background(), &runpb.RunJobRequest{
		Name:      job.Name,
		Overrides: &runpb.RunJobRequest_Overrides{TaskCount: 4},
	})
	if err != nil {
		t.Fatalf("RunJob: %v", err)
	}
	var started runpb.Execution
	runOp.GetResponse().UnmarshalTo(&started)
	if started.TaskCount != 4 {
		t.Fatalf("override task_count not applied: got %d", started.TaskCount)
	}
	done := waitExec(t, execs, started.Name)
	if done.SucceededCount != 4 {
		t.Fatalf("expected 4 tasks run, got %d", done.SucceededCount)
	}
}

func hasEnv(env []string, kv string) bool {
	for _, e := range env {
		if e == kv {
			return true
		}
	}
	return false
}

func TestNotFoundErrors(t *testing.T) {
	jobs, execs, tasks, cleanup := testClients(t, &fakeRunner{})
	defer cleanup()
	ctx := context.Background()
	missing := parent + "/jobs/nope"

	if _, err := jobs.GetJob(ctx, &runpb.GetJobRequest{Name: missing}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetJob: %v", err)
	}
	if _, err := jobs.RunJob(ctx, &runpb.RunJobRequest{Name: missing}); status.Code(err) != codes.NotFound {
		t.Fatalf("RunJob: %v", err)
	}
	if _, err := execs.GetExecution(ctx, &runpb.GetExecutionRequest{Name: missing + "/executions/x"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetExecution: %v", err)
	}
	if _, err := tasks.GetTask(ctx, &runpb.GetTaskRequest{Name: missing + "/executions/x/tasks/0"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetTask: %v", err)
	}
}

func TestValidateEnv(t *testing.T) {
	if err := validateEnv([]string{"A=1", "B=hello"}); err != nil {
		t.Fatalf("small env should pass: %v", err)
	}
	// Single oversized variable.
	big := "PAYLOAD=" + strings.Repeat("x", maxEnvVarBytes)
	if err := validateEnv([]string{big}); err == nil {
		t.Fatal("expected error for oversized single var")
	}
	// Many vars exceeding the total.
	var many []string
	for i := 0; i < 12; i++ {
		many = append(many, "V"+strings.Repeat("a", 100*1024)) // ~100 KiB each
	}
	if err := validateEnv(many); err == nil {
		t.Fatal("expected error for oversized total env")
	}
}

func TestRunJobRejectsOversizedOverrideEnv(t *testing.T) {
	jobs, _, _, cleanup := testClients(t, &fakeRunner{})
	defer cleanup()

	job := createJob(t, jobs, "big", 1, 1, 0)
	// 200 KiB payload in a per-dispatch override env (well under gRPC's 4 MiB,
	// so the request reaches the server and hits env validation).
	_, err := jobs.RunJob(context.Background(), &runpb.RunJobRequest{
		Name: job.Name,
		Overrides: &runpb.RunJobRequest_Overrides{
			ContainerOverrides: []*runpb.RunJobRequest_Overrides_ContainerOverride{{
				Env: []*runpb.EnvVar{{Name: "OUTBOX_PAYLOAD", Values: &runpb.EnvVar_Value{Value: strings.Repeat("x", 200*1024)}}},
			}},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for oversized override env, got %v", err)
	}
}

func TestCreateJobRejectsOversizedEnv(t *testing.T) {
	jobs, _, _, cleanup := testClients(t, &fakeRunner{})
	defer cleanup()

	_, err := jobs.CreateJob(context.Background(), &runpb.CreateJobRequest{
		Parent: parent, JobId: "bigdef",
		Job: &runpb.Job{Template: &runpb.ExecutionTemplate{Template: &runpb.TaskTemplate{
			Containers: []*runpb.Container{{
				Image: "img",
				Env:   []*runpb.EnvVar{{Name: "PAYLOAD", Values: &runpb.EnvVar_Value{Value: strings.Repeat("x", 200*1024)}}},
			}},
		}}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for oversized job env, got %v", err)
	}
}
