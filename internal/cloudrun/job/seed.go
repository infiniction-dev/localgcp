package job

import (
	"fmt"
	"os"

	"cloud.google.com/go/run/apiv2/runpb"
	"gopkg.in/yaml.v3"
)

// SeedJob declares a job to auto-register at startup (see --jobs). It maps the
// common Cloud Run Job fields to a runpb.Job so a job can be defined
// declaratively instead of via a CreateJob call.
type SeedJob struct {
	Name        string            `yaml:"name"`
	Project     string            `yaml:"project"`
	Region      string            `yaml:"region"`
	Image       string            `yaml:"image"`
	Command     []string          `yaml:"command"`
	Args        []string          `yaml:"args"`
	Env         map[string]string `yaml:"env"`
	Tasks       int32             `yaml:"tasks"`
	Parallelism int32             `yaml:"parallelism"`
	MaxRetries  int32             `yaml:"maxRetries"`
}

type seedFile struct {
	Jobs []SeedJob `yaml:"jobs"`
}

// LoadSeed reads a YAML seed file. An empty path returns no jobs.
func LoadSeed(path string) ([]SeedJob, error) {
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read jobs file: %w", err)
	}
	var f seedFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse jobs file %s: %w", path, err)
	}
	for i, j := range f.Jobs {
		if j.Name == "" || j.Image == "" {
			return nil, fmt.Errorf("jobs[%d]: name and image are required", i)
		}
	}
	return f.Jobs, nil
}

func (sj SeedJob) fullName() string {
	project := orDefault(sj.Project, "local")
	region := orDefault(sj.Region, "us-central1")
	return fmt.Sprintf("projects/%s/locations/%s/jobs/%s", project, region, sj.Name)
}

func (sj SeedJob) toJob() *runpb.Job {
	var env []*runpb.EnvVar
	for k, v := range sj.Env {
		env = append(env, &runpb.EnvVar{Name: k, Values: &runpb.EnvVar_Value{Value: v}})
	}
	tasks := sj.Tasks
	if tasks < 1 {
		tasks = 1
	}
	parallelism := sj.Parallelism
	if parallelism < 1 {
		parallelism = 1
	}
	return &runpb.Job{Template: &runpb.ExecutionTemplate{
		TaskCount:   tasks,
		Parallelism: parallelism,
		Template: &runpb.TaskTemplate{
			Retries: &runpb.TaskTemplate_MaxRetries{MaxRetries: sj.MaxRetries},
			Containers: []*runpb.Container{{
				Image:   sj.Image,
				Command: sj.Command,
				Args:    sj.Args,
				Env:     env,
			}},
		},
	}}
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
