package client

import (
	"context"
	"sync"
	"testing"
	"time"

	. "github.com/go-playground/pkg/v5/values/option"
	"github.com/google/uuid"
	"github.com/relay-io/relay-sdk-go/core/job"
	"github.com/stretchr/testify/require"
)

func TestPoller(t *testing.T) {
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
		Queue:      j.Queue,
		ID:         uuid.NewString(),
		Timeout:    30,
		MaxRetries: None[int16](),
		Payload:    4,
		State:      None[int](),
		RunAt:      Some(now),
	}

	err := client.Enqueue(ctx, job.Unique, []job.New[int, int]{j, j2})
	assert.NoError(err)

	mr := new(mockRunner)
	poller := client.Poller(j.Queue, mr).NumWorkers(1).Build()

	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	go func() {
		assert.NoError(poller.Start(ctx))
	}()

	for {
		if mr.Len() == 2 {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
	assert.Equal(2, len(mr.helpers))
	assert.Equal(j.Queue, mr.helpers[0].job.Queue)
	assert.Equal(j.ID, mr.helpers[0].job.ID)
	assert.Equal(j2.Queue, mr.helpers[1].job.Queue)
	assert.Equal(j2.ID, mr.helpers[1].job.ID)

	jobH1 := mr.helpers[0]
	jobH2 := mr.helpers[1]

	err = jobH2.Complete(ctx)
	assert.NoError(err)

	exists, err := jobH2.Exists(ctx)
	assert.NoError(err)
	assert.False(exists)

	err = jobH1.Heartbeat(ctx, Some(5))
	assert.NoError(err)

	maybeExisting, err := jobH1.InnerClient().Get(ctx, jobH1.Job().Queue, jobH1.Job().ID)
	assert.NoError(err)
	assert.True(maybeExisting.IsSome())

	existing := maybeExisting.Unwrap()
	assert.Equal(Some(5), existing.State)
	assert.Equal(jobH1.Job().RunID, existing.RunID)

	j.State = Some(6)
	err = jobH1.Requeue(ctx, job.Unique, []job.New[int, int]{j})
	assert.NoError(err)

	maybeExisting, err = jobH1.InnerClient().Get(ctx, jobH1.Job().Queue, jobH1.Job().ID)
	assert.NoError(err)
	assert.True(maybeExisting.IsSome())

	existing = maybeExisting.Unwrap()
	assert.Equal(6, existing.State.Unwrap())
	assert.True(existing.RunID.IsNone())

	err = jobH1.Delete(ctx)
	assert.NoError(err)

	exists, err = jobH1.Exists(ctx)
	assert.NoError(err)
	assert.False(exists)
}

var _ Runner[int, int] = (*mockRunner)(nil)

type mockRunner struct {
	helpers []JobHelper[int, int]
	m       sync.Mutex
}

func (m *mockRunner) Run(ctx context.Context, helper JobHelper[int, int]) {
	m.m.Lock()
	defer m.m.Unlock()
	m.helpers = append(m.helpers, helper)
}

func (m *mockRunner) Len() int {
	m.m.Lock()
	l := len(m.helpers)
	m.m.Unlock()
	return l
}
