package job

import (
	"context"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"sync"

	"cloud.google.com/go/run/apiv2/runpb"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Store is the in-memory store for Cloud Run Jobs, Executions and Tasks.
//
// CreateJob only records the job definition (image + config). RunJob triggers an
// Execution that actually runs the job's container image to completion via the
// Runner: one container per task, honouring parallelism and max_retries. The
// Execution starts in a RUNNING state and transitions to SUCCEEDED/FAILED
// asynchronously, matching real Cloud Run — clients poll GetExecution to observe
// completion (which is what `gcloud run jobs execute --wait` does).
type Store struct {
	mu         sync.RWMutex
	jobs       map[string]*runpb.Job
	executions map[string]*runpb.Execution
	tasks      map[string]*runpb.Task
	cancels    map[string]context.CancelFunc // execution name -> cancel
	seq        int
	runner     Runner
	logger     *log.Logger
}

func NewStore(runner Runner, logger *log.Logger) *Store {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &Store{
		jobs:       make(map[string]*runpb.Job),
		executions: make(map[string]*runpb.Execution),
		tasks:      make(map[string]*runpb.Task),
		cancels:    make(map[string]context.CancelFunc),
		runner:     runner,
		logger:     logger,
	}
}

// ── Jobs ──────────────────────────────────────────────────────────────────────

func (s *Store) CreateJob(name string, job *runpb.Job) (*runpb.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[name]; exists {
		return nil, fmt.Errorf("already exists")
	}
	if job == nil {
		job = &runpb.Job{}
	}

	s.seq++
	now := timestamppb.Now()
	job.Name = name
	job.CreateTime = now
	job.UpdateTime = now
	job.Uid = fmt.Sprintf("uid-%d", s.seq)
	job.Generation = 1
	job.ExecutionCount = 0
	job.Reconciling = false
	job.TerminalCondition = &runpb.Condition{
		Type:  "Ready",
		State: runpb.Condition_CONDITION_SUCCEEDED,
	}

	s.jobs[name] = job
	return clone(job), nil
}

func (s *Store) GetJob(name string) (*runpb.Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[name]
	if !ok {
		return nil, false
	}
	return clone(job), true
}

// ListJobs returns the jobs under parent, newest first.
func (s *Store) ListJobs(parent string) []*runpb.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := parent + "/jobs/"
	var result []*runpb.Job
	for name, job := range s.jobs {
		if strings.HasPrefix(name, prefix) {
			result = append(result, clone(job))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].GetCreateTime().AsTime().After(result[j].GetCreateTime().AsTime())
	})
	return result
}

func (s *Store) UpdateJob(name string, job *runpb.Job) (*runpb.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.jobs[name]
	if !ok {
		return nil, fmt.Errorf("not found")
	}

	job.Name = existing.Name
	job.Uid = existing.Uid
	job.CreateTime = existing.CreateTime
	job.UpdateTime = timestamppb.Now()
	job.Generation = existing.Generation + 1
	job.ExecutionCount = existing.ExecutionCount
	job.LatestCreatedExecution = existing.LatestCreatedExecution
	job.TerminalCondition = existing.TerminalCondition

	s.jobs[name] = job
	return clone(job), nil
}

func (s *Store) DeleteJob(name string) (*runpb.Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[name]
	if !ok {
		return nil, false
	}
	delete(s.jobs, name)
	return clone(job), true
}

// ── RunJob (asynchronous execution) ──────────────────────────────────────────

// RunJob creates a RUNNING Execution (and its Tasks) for the named job and
// launches the containers in the background. It returns a snapshot of the
// freshly created (still running) Execution.
func (s *Store) RunJob(jobName string) (*runpb.Execution, error) {
	s.mu.Lock()

	job, ok := s.jobs[jobName]
	if !ok {
		s.mu.Unlock()
		return nil, fmt.Errorf("not found")
	}

	s.seq++
	now := timestamppb.Now()

	taskCount, parallelism, maxRetries := int32(1), int32(1), int32(0)
	var taskTemplate *runpb.TaskTemplate
	if tmpl := job.GetTemplate(); tmpl != nil {
		if tmpl.GetTaskCount() > 0 {
			taskCount = tmpl.GetTaskCount()
		}
		if tmpl.GetParallelism() > 0 {
			parallelism = tmpl.GetParallelism()
		}
		if inner := tmpl.GetTemplate(); inner != nil {
			taskTemplate = inner
			maxRetries = inner.GetMaxRetries()
		}
	}

	execID := fmt.Sprintf("%s-%s", lastSegment(jobName), nameSuffix(s.seq))
	execName := jobName + "/executions/" + execID

	exec := &runpb.Execution{
		Name:               execName,
		Uid:                fmt.Sprintf("uid-%d", s.seq),
		Generation:         1,
		CreateTime:         now,
		StartTime:          now,
		UpdateTime:         now,
		Job:                jobName,
		Parallelism:        parallelism,
		TaskCount:          taskCount,
		Template:           taskTemplate,
		Reconciling:        true,
		RunningCount:       taskCount,
		ObservedGeneration: 1,
		LogUri:             fmt.Sprintf("https://console.cloud.google.com/run/jobs/executions/details/%s/logs", execID),
		Conditions: []*runpb.Condition{{
			Type:               "Completed",
			State:              runpb.Condition_CONDITION_RECONCILING,
			LastTransitionTime: now,
		}},
	}
	s.executions[execName] = exec

	var containers []*runpb.Container
	if taskTemplate != nil {
		containers = taskTemplate.GetContainers()
	}
	for i := int32(0); i < taskCount; i++ {
		s.seq++
		taskName := fmt.Sprintf("%s/tasks/%s-%d", execName, execID, i)
		s.tasks[taskName] = &runpb.Task{
			Name:       taskName,
			Uid:        fmt.Sprintf("uid-%d", s.seq),
			Generation: 1,
			CreateTime: now,
			UpdateTime: now,
			Job:        jobName,
			Execution:  execName,
			Containers: containers,
			Index:      i,
			MaxRetries: maxRetries,
			Conditions: []*runpb.Condition{{
				Type:               "Completed",
				State:              runpb.Condition_CONDITION_RECONCILING,
				LastTransitionTime: now,
			}},
		}
	}

	job.ExecutionCount++
	job.LatestCreatedExecution = &runpb.ExecutionReference{
		Name:             execName,
		CreateTime:       now,
		CompletionStatus: runpb.ExecutionReference_EXECUTION_RUNNING,
	}
	job.UpdateTime = now

	ctx, cancel := context.WithCancel(context.Background())
	s.cancels[execName] = cancel
	snapshot := clone(exec)
	s.mu.Unlock()

	go s.execute(ctx, execName, execID, containers, taskCount, parallelism, maxRetries)

	return snapshot, nil
}

func (s *Store) GetExecution(name string) (*runpb.Execution, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	exec, ok := s.executions[name]
	if !ok {
		return nil, false
	}
	return clone(exec), true
}

// ListExecutions returns executions under a job (parent), newest first.
func (s *Store) ListExecutions(parent string) []*runpb.Execution {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := parent + "/executions/"
	var result []*runpb.Execution
	for name, exec := range s.executions {
		if strings.HasPrefix(name, prefix) {
			result = append(result, clone(exec))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].GetCreateTime().AsTime().After(result[j].GetCreateTime().AsTime())
	})
	return result
}

func (s *Store) DeleteExecution(name string) (*runpb.Execution, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	exec, ok := s.executions[name]
	if !ok {
		return nil, false
	}
	if cancel := s.cancels[name]; cancel != nil {
		cancel()
		delete(s.cancels, name)
	}
	delete(s.executions, name)
	taskPrefix := name + "/tasks/"
	for tn := range s.tasks {
		if strings.HasPrefix(tn, taskPrefix) {
			delete(s.tasks, tn)
		}
	}
	return clone(exec), true
}

// CancelExecution requests cancellation of a running execution. Running tasks
// receive a cancelled context; the execution is finalised as cancelled.
func (s *Store) CancelExecution(name string) (*runpb.Execution, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	exec, ok := s.executions[name]
	if !ok {
		return nil, false
	}
	if cancel := s.cancels[name]; cancel != nil {
		cancel()
	}
	return clone(exec), true
}

func (s *Store) GetTask(name string) (*runpb.Task, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	task, ok := s.tasks[name]
	if !ok {
		return nil, false
	}
	return clone(task), true
}

// ListTasks returns tasks under an execution (parent), ordered by index.
func (s *Store) ListTasks(parent string) []*runpb.Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := parent + "/tasks/"
	var result []*runpb.Task
	for name, task := range s.tasks {
		if strings.HasPrefix(name, prefix) {
			result = append(result, clone(task))
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].GetIndex() < result[j].GetIndex() })
	return result
}

// ── execution orchestration ──────────────────────────────────────────────────

// execute runs every task of an execution to completion, bounded by parallelism,
// then finalises the execution's terminal state.
func (s *Store) execute(ctx context.Context, execName, execID string, containers []*runpb.Container, taskCount, parallelism, maxRetries int32) {
	if parallelism < 1 {
		parallelism = 1
	}
	sem := make(chan struct{}, parallelism)
	var wg sync.WaitGroup
	for i := int32(0); i < taskCount; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int32) {
			defer wg.Done()
			defer func() { <-sem }()
			s.runTask(ctx, execName, execID, idx, containers, taskCount, maxRetries)
		}(i)
	}
	wg.Wait()
	s.finalize(execName)
}

// runTask runs a single task, retrying up to maxRetries, and records the outcome.
func (s *Store) runTask(ctx context.Context, execName, execID string, idx int32, containers []*runpb.Container, taskCount, maxRetries int32) {
	taskName := fmt.Sprintf("%s/tasks/%s-%d", execName, execID, idx)
	s.touchTaskStart(taskName)

	var image string
	var command, env []string
	if len(containers) > 0 {
		c := containers[0]
		image = c.GetImage()
		command = append(append([]string{}, c.GetCommand()...), c.GetArgs()...)
		for _, e := range c.GetEnv() {
			env = append(env, e.GetName()+"="+e.GetValue())
		}
	}

	var exitCode int
	var runErr error
	var attempt int32
	for attempt = 0; attempt <= maxRetries; attempt++ {
		spec := TaskSpec{
			Name:    fmt.Sprintf("localgcp-job-%s-%d-%d", execID, idx, attempt),
			Image:   image,
			Command: command,
			Env: append(env,
				fmt.Sprintf("CLOUD_RUN_TASK_INDEX=%d", idx),
				fmt.Sprintf("CLOUD_RUN_TASK_COUNT=%d", taskCount),
				fmt.Sprintf("CLOUD_RUN_TASK_ATTEMPT=%d", attempt),
			),
		}
		exitCode, runErr = s.runner.RunTask(ctx, spec)
		if ctx.Err() != nil {
			s.finishTask(execName, taskName, taskOutcome{cancelled: true, attempt: attempt})
			return
		}
		if runErr == nil && exitCode == 0 {
			s.finishTask(execName, taskName, taskOutcome{attempt: attempt})
			return
		}
		s.logger.Printf("task %s attempt %d: exit=%d err=%v", taskName, attempt, exitCode, runErr)
	}
	s.finishTask(execName, taskName, taskOutcome{failed: true, exitCode: exitCode, err: runErr, attempt: maxRetries})
}

type taskOutcome struct {
	failed    bool
	cancelled bool
	exitCode  int
	err       error
	attempt   int32
}

func (s *Store) touchTaskStart(taskName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.tasks[taskName]; ok {
		now := timestamppb.Now()
		t.StartTime = now
		t.UpdateTime = now
	}
}

func (s *Store) finishTask(execName, taskName string, o taskOutcome) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := timestamppb.Now()
	if t, ok := s.tasks[taskName]; ok {
		t.CompletionTime = now
		t.UpdateTime = now
		t.Retried = o.attempt
		state := runpb.Condition_CONDITION_SUCCEEDED
		if o.failed || o.cancelled {
			state = runpb.Condition_CONDITION_FAILED
		}
		t.Conditions = []*runpb.Condition{{Type: "Completed", State: state, LastTransitionTime: now}}
		msg := &statuspb.Status{Code: 0}
		if o.err != nil {
			msg = &statuspb.Status{Code: 2, Message: o.err.Error()}
		}
		t.LastAttemptResult = &runpb.TaskAttemptResult{Status: msg, ExitCode: int32(o.exitCode)}
	}

	exec, ok := s.executions[execName]
	if !ok {
		return
	}
	if exec.RunningCount > 0 {
		exec.RunningCount--
	}
	switch {
	case o.cancelled:
		exec.CancelledCount++
	case o.failed:
		exec.FailedCount++
	default:
		exec.SucceededCount++
	}
	if o.attempt > 0 {
		exec.RetriedCount += o.attempt
	}
	exec.UpdateTime = now
}

// finalize sets the execution's terminal state once all tasks have finished.
func (s *Store) finalize(execName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if cancel := s.cancels[execName]; cancel != nil {
		cancel()
		delete(s.cancels, execName)
	}

	exec, ok := s.executions[execName]
	if !ok {
		return
	}
	now := timestamppb.Now()
	exec.CompletionTime = now
	exec.UpdateTime = now
	exec.Reconciling = false
	exec.RunningCount = 0

	state := runpb.Condition_CONDITION_SUCCEEDED
	completion := runpb.ExecutionReference_EXECUTION_SUCCEEDED
	msg := ""
	switch {
	case exec.CancelledCount > 0:
		state, completion, msg = runpb.Condition_CONDITION_FAILED, runpb.ExecutionReference_EXECUTION_CANCELLED, "Execution cancelled"
	case exec.FailedCount > 0:
		state, completion, msg = runpb.Condition_CONDITION_FAILED, runpb.ExecutionReference_EXECUTION_FAILED, "One or more tasks failed"
	}
	exec.Conditions = []*runpb.Condition{{Type: "Completed", State: state, Message: msg, LastTransitionTime: now}}

	if job, ok := s.jobs[exec.Job]; ok && job.GetLatestCreatedExecution().GetName() == execName {
		job.LatestCreatedExecution.CompletionTime = now
		job.LatestCreatedExecution.CompletionStatus = completion
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

func clone[T proto.Message](m T) T { return proto.Clone(m).(T) }

func lastSegment(name string) string {
	parts := strings.Split(name, "/")
	return parts[len(parts)-1]
}

// nameSuffix renders n as a short, stable, lowercase-alphanumeric suffix so
// generated execution names resemble real Cloud Run names (e.g. "myjob-abc12").
func nameSuffix(n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 5)
	for i := range b {
		b[i] = alphabet[n%len(alphabet)]
		n /= len(alphabet)
	}
	return string(b)
}
