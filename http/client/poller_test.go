package client

import (
	"context"
	. "github.com/go-playground/pkg/v5/values/option"
	"github.com/google/uuid"
	"github.com/relay-io/relay-sdk-go/core/job"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
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
	assert.Equal(2, len(mr.jobs))
	assert.Equal(j.Queue, mr.jobs[0].Queue)
	assert.Equal(j.ID, mr.jobs[0].ID)
	assert.Equal(j2.Queue, mr.jobs[1].Queue)
	assert.Equal(j2.ID, mr.jobs[1].ID)
}

var _ Runner[int, int] = (*mockRunner)(nil)

type mockRunner struct {
	jobs []job.Existing[int, int]
	m    sync.Mutex
}

func (m *mockRunner) Run(ctx context.Context, helper JobHelper[int, int]) {
	m.m.Lock()
	defer m.m.Unlock()
	m.jobs = append(m.jobs, helper.job)
}

func (m *mockRunner) Len() int {
	m.m.Lock()
	l := len(m.jobs)
	m.m.Unlock()
	return l
}
