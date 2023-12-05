package job

import (
	. "github.com/go-playground/pkg/v5/values/option"
	"github.com/google/uuid"
	"time"
)

// New defines all information needed to enqueue or requeue a job.
type New[P, S any] struct {
	// Queue is used to differentiate different job types that can be picked up by job runners/workers.
	//
	// The maximum size is 1024 characters.
	Queue string `json:"queue"`

	// ID is the unique job ID which is also CAN be used to ensure the Job is unique within a `queue`.
	//
	// The maximum size is 1024 characters.
	ID string `json:"id"`

	// Timeout denotes the duration, in seconds, after a job has started processing or since the last
	// heartbeat request occurred before considering the job failed and being put back into the
	// queue.
	//
	// The value is required to be >= 0.
	Timeout int32 `json:"timeout"`

	// MaxRetries determines how many times the job can be retried, due to timeouts, before being considered
	// permanently failed. Infinite retries are supported when specifying None.
	//
	// The value is required to be >= 0 when set.
	MaxRetries Option[int16] `json:"max_retries"`

	// Payload is the immutable raw JSON payload that the job runner will receive and used to execute the Job.
	Payload P `json:"payload"`

	/// State is the mutable raw JSON state payload that the job runner will receive, update and use to track job progress.
	State Option[S] `json:"state"`

	/// RunAt indicates the time that a job is eligible to be run. Defaults to now if not specified.
	RunAt Option[time.Time] `json:"run_at"`
}

// Existing defines all information about an existing Job.
type Existing[P, S any] struct {
	// Queue is used to differentiate different job types that can be picked up by job runners/workers.
	//
	// The maximum size is 1024 characters.
	Queue string `json:"queue"`

	// ID is the unique job ID which is also CAN be used to ensure the Job is unique within a `queue`.
	//
	// The maximum size is 1024 characters.
	ID string `json:"id"`

	// Timeout denotes the duration, in seconds, after a job has started processing or since the last
	// heartbeat request occurred before considering the job failed and being put back into the
	// queue.
	//
	// The value is required to be >= 0.
	Timeout int32 `json:"timeout"`

	// MaxRetries determines how many times the job can be retried, due to timeouts, before being considered
	// permanently failed. Infinite retries are supported when specifying None.
	//
	// The value is required to be >= 0 when set.
	MaxRetries Option[int16] `json:"max_retries"`

	// RetriesRemaining specifies how many more times the Job can be retried before being considered permanently failed and deleted
	//
	// The value will be >= 0 when set.
	RetriesRemaining Option[int16] `json:"retries_remaining"`

	// Payload is the immutable raw JSON payload that the job runner will receive and used to execute the Job.
	Payload P `json:"payload"`

	/// State is the mutable raw JSON state payload that the job runner will receive, update and use to track job progress.
	State Option[S] `json:"state"`

	// RunID is the current Jobs unique `run_id`. When there is a value here it signifies that the job is
	// currently in-flight being processed.
	RunID Option[uuid.UUID] `json:"run_id"`

	// RunAt indicates the time that a job is/was eligible to be run. Defaults to now if not specified.
	RunAt time.Time `json:"run_at"`

	// UpdatedAt indicates the last time the job was updated either through enqueue, requeue or
	// heartbeat.
	UpdatedAt time.Time `json:"updated_at"`

	// CreatedAt indicates the time the job was originally created.
	CreatedAt time.Time `json:"created_at"`
}
