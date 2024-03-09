package client

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	. "github.com/go-playground/pkg/v5/values/option"
	"github.com/google/uuid"
	"github.com/relay-io/relay-sdk-go/core/job"
	"github.com/stretchr/testify/require"
)

var baseURL = "http://127.0.0.1:8080"

func init() {
	if url := os.Getenv("RELAY_URL"); url != "" {
		baseURL = url
	}
}

func TestOneShotJob(t *testing.T) {
	assert := require.New(t)
	ctx := context.Background()
	client := NewBuilder[int, int](baseURL).Build()
	now := time.Now().UTC().Truncate(time.Microsecond)
	j := job.New[int, int]{
		Queue:      uuid.NewString(),
		ID:         uuid.NewString(),
		Timeout:    30,
		MaxRetries: None[int16](),
		Payload:    3,
		State:      None[int](),
		RunAt:      Some(now),
	}

	err := client.Enqueue(ctx, job.Unique, []job.New[int, int]{j})
	assert.NoError(err)

	exists, err := client.Exists(ctx, j.Queue, j.ID)
	assert.NoError(err)
	assert.True(exists)

	maybeJob, err := client.Get(ctx, j.Queue, j.ID)
	assert.NoError(err)
	assert.True(maybeJob.IsSome())

	existing := maybeJob.Unwrap()
	expected := job.Existing[int, int]{
		Queue:            j.Queue,
		ID:               j.ID,
		Timeout:          j.Timeout,
		MaxRetries:       None[int16](),
		RetriesRemaining: None[int16](),
		Payload:          3,
		State:            None[int](),
		RunID:            None[uuid.UUID](),
		RunAt:            now,
		UpdatedAt:        now,
		CreatedAt:        now,
	}
	assert.True(existing.UpdatedAt.After(expected.UpdatedAt) || existing.UpdatedAt.Equal(expected.UpdatedAt))
	assert.True(existing.CreatedAt.After(expected.CreatedAt) || existing.CreatedAt.Equal(expected.CreatedAt))
	existing.UpdatedAt = expected.UpdatedAt
	existing.CreatedAt = expected.CreatedAt
	assert.Equal(expected, existing)

	jobs, err := client.Next(ctx, j.Queue, 10)
	assert.NoError(err)
	assert.Equal(1, len(jobs))

	job := jobs[0]
	assert.True(job.RunID.IsSome())
	assert.True(job.UpdatedAt.After(expected.UpdatedAt) || job.UpdatedAt.Equal(expected.UpdatedAt))
	job.UpdatedAt = expected.UpdatedAt
	job.CreatedAt = expected.CreatedAt
	runID := job.RunID
	job.RunID = None[uuid.UUID]()
	assert.Equal(expected, job)
	job.RunID = runID

	err = client.Complete(ctx, job.Queue, job.ID, job.RunID.Unwrap())
	assert.NoError(err)

	exists, err = client.Exists(ctx, job.Queue, job.ID)
	assert.NoError(err)
	assert.False(exists)
}

func TestPoll(t *testing.T) {
	assert := require.New(t)
	ctx := context.Background()
	client := NewBuilder[int, int](baseURL).Build()
	now := time.Now().UTC().Truncate(time.Microsecond)
	j := job.New[int, int]{
		Queue:      uuid.NewString(),
		ID:         uuid.NewString(),
		Timeout:    30,
		MaxRetries: None[int16](),
		Payload:    3,
		State:      None[int](),
		RunAt:      Some(now),
	}

	err := client.Enqueue(ctx, job.Unique, []job.New[int, int]{j})
	assert.NoError(err)

	exists, err := client.Exists(ctx, j.Queue, j.ID)
	assert.NoError(err)
	assert.True(exists)

	maybeJob, err := client.Get(ctx, j.Queue, j.ID)
	assert.NoError(err)
	assert.True(maybeJob.IsSome())

	existing := maybeJob.Unwrap()
	expected := job.Existing[int, int]{
		Queue:            j.Queue,
		ID:               j.ID,
		Timeout:          j.Timeout,
		MaxRetries:       None[int16](),
		RetriesRemaining: None[int16](),
		Payload:          3,
		State:            None[int](),
		RunID:            None[uuid.UUID](),
		RunAt:            now,
		UpdatedAt:        now,
		CreatedAt:        now,
	}
	assert.True(existing.UpdatedAt.After(expected.UpdatedAt) || existing.UpdatedAt.Equal(expected.UpdatedAt))
	assert.True(existing.CreatedAt.After(expected.CreatedAt) || existing.CreatedAt.Equal(expected.CreatedAt))
	existing.UpdatedAt = expected.UpdatedAt
	existing.CreatedAt = expected.CreatedAt
	assert.Equal(expected, existing)

	pollCtx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	jobs, err := client.Poll(pollCtx, j.Queue, 10)
	assert.NoError(err)
	cancel()
	assert.Equal(1, len(jobs))

	job := jobs[0]
	assert.True(job.RunID.IsSome())
	assert.True(job.UpdatedAt.After(expected.UpdatedAt) || job.UpdatedAt.Equal(expected.UpdatedAt))
	job.UpdatedAt = expected.UpdatedAt
	job.CreatedAt = expected.CreatedAt
	runID := job.RunID
	job.RunID = None[uuid.UUID]()
	assert.Equal(expected, job)
	job.RunID = runID

	err = client.Complete(ctx, job.Queue, job.ID, job.RunID.Unwrap())
	assert.NoError(err)

	exists, err = client.Exists(ctx, job.Queue, job.ID)
	assert.NoError(err)
	assert.False(exists)
}

func TestEnqueueModes(t *testing.T) {
	assert := require.New(t)
	ctx := context.Background()
	client := NewBuilder[int, int](baseURL).Build()
	now := time.Now().UTC().Truncate(time.Microsecond)
	j := job.New[int, int]{
		Queue:      uuid.NewString(),
		ID:         uuid.NewString(),
		Timeout:    30,
		MaxRetries: None[int16](),
		Payload:    3,
		State:      None[int](),
		RunAt:      Some(now),
	}

	err := client.Enqueue(ctx, job.Unique, []job.New[int, int]{j})
	assert.NoError(err)

	err = client.Enqueue(ctx, job.Unique, []job.New[int, int]{j})
	assert.Error(err)
	assert.True(errors.As(err, &job.ErrAlreadyExists{}))

	err = client.Enqueue(ctx, job.Ignore, []job.New[int, int]{j})
	assert.NoError(err)

	j.Payload = 4
	err = client.Enqueue(ctx, job.Replace, []job.New[int, int]{j})
	assert.NoError(err)

	exists, err := client.Exists(ctx, j.Queue, j.ID)
	assert.NoError(err)
	assert.True(exists)

	maybeJob, err := client.Get(ctx, j.Queue, j.ID)
	assert.NoError(err)
	assert.True(maybeJob.IsSome())

	existing := maybeJob.Unwrap()
	expected := job.Existing[int, int]{
		Queue:            j.Queue,
		ID:               j.ID,
		Timeout:          j.Timeout,
		MaxRetries:       None[int16](),
		RetriesRemaining: None[int16](),
		Payload:          4,
		State:            None[int](),
		RunID:            None[uuid.UUID](),
		RunAt:            now,
		UpdatedAt:        now,
		CreatedAt:        now,
	}
	assert.True(existing.UpdatedAt.After(expected.UpdatedAt) || existing.UpdatedAt.Equal(expected.UpdatedAt))
	assert.True(existing.CreatedAt.After(expected.CreatedAt) || existing.CreatedAt.Equal(expected.CreatedAt))
	existing.UpdatedAt = expected.UpdatedAt
	existing.CreatedAt = expected.CreatedAt
	assert.Equal(expected, existing)

	err = client.Delete(ctx, j.Queue, j.ID)
	assert.NoError(err)

	exists, err = client.Exists(ctx, j.Queue, j.ID)
	assert.NoError(err)
	assert.False(exists)
}

func TestHeartbeat(t *testing.T) {
	assert := require.New(t)
	ctx := context.Background()
	client := NewBuilder[int, int](baseURL).Build()
	now := time.Now().UTC().Truncate(time.Microsecond)
	j := job.New[int, int]{
		Queue:      uuid.NewString(),
		ID:         uuid.NewString(),
		Timeout:    30,
		MaxRetries: None[int16](),
		Payload:    3,
		State:      None[int](),
		RunAt:      Some(now),
	}

	err := client.Enqueue(ctx, job.Unique, []job.New[int, int]{j})
	assert.NoError(err)

	exists, err := client.Exists(ctx, j.Queue, j.ID)
	assert.NoError(err)
	assert.True(exists)

	maybeJob, err := client.Get(ctx, j.Queue, j.ID)
	assert.NoError(err)
	assert.True(maybeJob.IsSome())

	existing := maybeJob.Unwrap()
	expected := job.Existing[int, int]{
		Queue:            j.Queue,
		ID:               j.ID,
		Timeout:          j.Timeout,
		MaxRetries:       None[int16](),
		RetriesRemaining: None[int16](),
		Payload:          3,
		State:            None[int](),
		RunID:            None[uuid.UUID](),
		RunAt:            now,
		UpdatedAt:        now,
		CreatedAt:        now,
	}
	assert.True(existing.UpdatedAt.After(expected.UpdatedAt) || existing.UpdatedAt.Equal(expected.UpdatedAt))
	assert.True(existing.CreatedAt.After(expected.CreatedAt) || existing.CreatedAt.Equal(expected.CreatedAt))
	existing.UpdatedAt = expected.UpdatedAt
	existing.CreatedAt = expected.CreatedAt
	assert.Equal(expected, existing)

	jobs, err := client.Next(ctx, j.Queue, 10)
	assert.NoError(err)
	assert.Equal(1, len(jobs))

	job0 := jobs[0]
	assert.True(job0.RunID.IsSome())
	assert.True(job0.UpdatedAt.After(expected.UpdatedAt) || job0.UpdatedAt.Equal(expected.UpdatedAt))
	job0.UpdatedAt = expected.UpdatedAt
	job0.CreatedAt = expected.CreatedAt
	runID := job0.RunID
	job0.RunID = None[uuid.UUID]()
	assert.Equal(expected, job0)
	job0.RunID = runID

	err = client.Heartbeat(ctx, job0.Queue, job0.ID, job0.RunID.Unwrap(), Some(30))
	assert.NoError(err)

	maybeJob, err = client.Get(ctx, j.Queue, j.ID)
	assert.NoError(err)
	assert.True(maybeJob.IsSome())

	existing = maybeJob.Unwrap()
	expected = job.Existing[int, int]{
		Queue:            j.Queue,
		ID:               j.ID,
		Timeout:          j.Timeout,
		MaxRetries:       None[int16](),
		RetriesRemaining: None[int16](),
		Payload:          3,
		State:            Some(30),
		RunID:            job0.RunID,
		RunAt:            now,
		UpdatedAt:        now,
		CreatedAt:        now,
	}
	assert.True(existing.UpdatedAt.After(expected.UpdatedAt) || existing.UpdatedAt.Equal(expected.UpdatedAt))
	assert.True(existing.CreatedAt.After(expected.CreatedAt) || existing.CreatedAt.Equal(expected.CreatedAt))
	existing.UpdatedAt = expected.UpdatedAt
	existing.CreatedAt = expected.CreatedAt
	assert.Equal(expected, existing)

	err = client.Complete(ctx, job0.Queue, job0.ID, job0.RunID.Unwrap())
	assert.NoError(err)

	exists, err = client.Exists(ctx, job0.Queue, job0.ID)
	assert.NoError(err)
	assert.False(exists)
}

func TestRequeue(t *testing.T) {
	assert := require.New(t)
	ctx := context.Background()
	client := NewBuilder[int, int](baseURL).Build()
	now := time.Now().UTC().Truncate(time.Microsecond)
	j := job.New[int, int]{
		Queue:      uuid.NewString(),
		ID:         uuid.NewString(),
		Timeout:    30,
		MaxRetries: None[int16](),
		Payload:    3,
		State:      None[int](),
		RunAt:      Some(now),
	}

	err := client.Enqueue(ctx, job.Unique, []job.New[int, int]{j})
	assert.NoError(err)

	exists, err := client.Exists(ctx, j.Queue, j.ID)
	assert.NoError(err)
	assert.True(exists)

	expected := job.Existing[int, int]{
		Queue:            j.Queue,
		ID:               j.ID,
		Timeout:          j.Timeout,
		MaxRetries:       None[int16](),
		RetriesRemaining: None[int16](),
		Payload:          3,
		State:            None[int](),
		RunID:            None[uuid.UUID](),
		RunAt:            now,
		UpdatedAt:        now,
		CreatedAt:        now,
	}

	jobs, err := client.Next(ctx, j.Queue, 10)
	assert.NoError(err)
	assert.Equal(1, len(jobs))

	job0 := jobs[0]
	assert.True(job0.RunID.IsSome())
	assert.True(job0.UpdatedAt.After(expected.UpdatedAt) || job0.UpdatedAt.Equal(expected.UpdatedAt))
	job0.UpdatedAt = expected.UpdatedAt
	job0.CreatedAt = expected.CreatedAt
	runID := job0.RunID
	job0.RunID = None[uuid.UUID]()
	assert.Equal(expected, job0)
	job0.RunID = runID

	expected.Payload = 5
	j.Payload = expected.Payload
	err = client.Requeue(ctx, job.Unique, job0.Queue, job0.ID, job0.RunID.Unwrap(), []job.New[int, int]{j})
	assert.NoError(err)

	maybeJob, err := client.Get(ctx, j.Queue, j.ID)
	assert.NoError(err)
	assert.True(maybeJob.IsSome())

	expected.RunID = None[uuid.UUID]()
	existing := maybeJob.Unwrap()
	assert.True(existing.RunID.IsNone())
	assert.True(existing.UpdatedAt.After(job0.UpdatedAt))
	assert.True(existing.CreatedAt.After(job0.CreatedAt))
	existing.UpdatedAt = expected.UpdatedAt
	existing.CreatedAt = expected.CreatedAt
	assert.Equal(expected, existing)

	err = client.Complete(ctx, job0.Queue, job0.ID, job0.RunID.Unwrap())
	assert.NoError(err)
}

func TestRequeueModes(t *testing.T) {
	assert := require.New(t)
	ctx := context.Background()
	client := NewBuilder[int, int](baseURL).Build()
	now := time.Now().UTC().Truncate(time.Microsecond)
	j := job.New[int, int]{
		Queue:      uuid.NewString(),
		ID:         uuid.NewString(),
		Timeout:    30,
		MaxRetries: None[int16](),
		Payload:    3,
		State:      None[int](),
		RunAt:      Some(now),
	}
	j2 := job.New[int, int]{
		Queue:      uuid.NewString(),
		ID:         uuid.NewString(),
		Timeout:    30,
		MaxRetries: None[int16](),
		Payload:    4,
		State:      None[int](),
		RunAt:      Some(now),
	}

	err := client.Enqueue(ctx, job.Unique, []job.New[int, int]{j, j2})
	assert.NoError(err)

	jobs, err := client.Next(ctx, j.Queue, 10)
	assert.NoError(err)
	assert.Equal(1, len(jobs))

	job0 := jobs[0]
	err = client.Requeue(ctx, job.Unique, job0.Queue, job0.ID, job0.RunID.Unwrap(), []job.New[int, int]{j2})
	assert.Error(err)
	assert.True(errors.As(err, &job.ErrAlreadyExists{}))

	j2.Payload = 5
	err = client.Requeue(ctx, job.Ignore, job0.Queue, job0.ID, job0.RunID.Unwrap(), []job.New[int, int]{j2})
	assert.NoError(err)

	maybeJob, err := client.Get(ctx, j2.Queue, j2.ID)
	assert.NoError(err)
	assert.True(maybeJob.IsSome())

	existing := maybeJob.Unwrap()
	assert.Equal(4, existing.Payload)

	err = client.Enqueue(ctx, job.Ignore, []job.New[int, int]{j, j2})
	assert.NoError(err)

	jobs, err = client.Next(ctx, j.Queue, 10)
	assert.NoError(err)
	assert.Equal(1, len(jobs))

	job0 = jobs[0]

	err = client.Requeue(ctx, job.Replace, job0.Queue, job0.ID, job0.RunID.Unwrap(), []job.New[int, int]{j2})
	assert.NoError(err)

	maybeJob, err = client.Get(ctx, j2.Queue, j2.ID)
	assert.NoError(err)
	assert.True(maybeJob.IsSome())

	existing = maybeJob.Unwrap()
	assert.Equal(j2.Payload, existing.Payload)
}
