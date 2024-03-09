package client

import (
	"context"
	"runtime"
	"sync"

	"github.com/go-playground/errors/v5"
	typesext "github.com/go-playground/pkg/v5/types"
	valuesext "github.com/go-playground/pkg/v5/values"
	. "github.com/go-playground/pkg/v5/values/option"
	"github.com/relay-io/relay-sdk-go/core/job"
)

// Runner is an interface used by the `Poller` to execute a `Job` after fetching it to be processed.
type Runner[P, S any] interface {
	Run(ctx context.Context, helper JobHelper[P, S])
}

// JobHelper is used to provide a `Existing` job and Relay client instance to the `Runner` for processing.
//
// It contains helper functions allowing abstraction away complexity of interacting with Relay.
type JobHelper[P, S any] struct {
	client *Client[P, S]
	job    job.Existing[P, S]
}

// Job returns the `Existing` job to be processed.
func (h JobHelper[P, S]) Job() job.Existing[P, S] {
	return h.job
}

// InnerClient returns a reference to the inner Relay client instance for interacting with Relay when
// needing to do things the helper functions for the inner job don't apply to such as spawning
// one-off jobs not related to the existing running job in any way.
//
// It is rare to need the inner client and helper functions should be preferred in most cases.
func (h JobHelper[P, S]) InnerClient() *Client[P, S] {
	return h.client
}

// Complete completes/deletes this in-flight `Existing` job.
//
// # Panics
//
// If the `Existing` job doesn't have a `run_id` set.
//
// # Errors
//
// Will return `Err` on:
// - An unrecoverable network error.
// - The `Existing` job doesn't exist.
func (h JobHelper[P, S]) Complete(ctx context.Context) error {
	return h.client.Complete(ctx, h.job.Queue, h.job.ID, h.job.RunID.Unwrap())
}

// Delete removes/delete the `Existing` job.
//
// # Errors
//
// Will return `Err` on:
// - an unrecoverable network error.
func (h JobHelper[P, S]) Delete(ctx context.Context) error {
	return h.client.Delete(ctx, h.job.Queue, h.job.ID)
}

// Exists returns if the `Existing` job still exists.
//
// # Errors
//
// Will return `Err` on an unrecoverable network error.
func (h JobHelper[P, S]) Exists(ctx context.Context) (bool, error) {
	return h.client.Exists(ctx, h.job.Queue, h.job.ID)
}

// Heartbeat sends a heartbeat request to this in-flight `Existing` job indicating it is still processing, resetting
// the timeout. Optionally you can update the `Existing` jobs state during the same request.
//
// # Panics
//
// If the `Existing` job doesn't have a `run_id` set.
//
// # Errors
//
// Will return `Err` on:
// - An unrecoverable network error.
// - If the `Existing` job doesn't exist.
func (h JobHelper[P, S]) Heartbeat(ctx context.Context, state Option[S]) error {
	return h.client.Heartbeat(ctx, h.job.Queue, h.job.ID, h.job.RunID.Unwrap(), state)
}

// Requeue re-queues this in-flight `Existing` job to be run again or spawn a new set of jobs
// atomically.
//
// The `Existing` jobs queue, id and `run_id` must match an existing in-flight Job.
// This is primarily used to schedule a new/the next run of a singleton job. This provides the
// ability for self-perpetuating scheduled jobs in an atomic manner.
//
// Reschedule also allows you to change the jobs `queue` and `id` during the reschedule.
// This is allowed to facilitate advancing a job through a distributed pipeline/state
// machine atomically if that is more appropriate than advancing using the jobs state alone.
//
// The mode will be used to determine the behaviour if a conflicting record already exists,
// just like when enqueuing jobs.
//
// If the `Existing` job no longer exists or is not in-flight, this will return without error and will
// not enqueue any jobs.
//
// # Panics
//
// If the `Existing` job doesn't have a `run_id` set.
//
// # Errors
//
// Will return `Err` on:
// - An unrecoverable network error.
// - If one of the `Existing` jobs exists when mode is unique.
func (h JobHelper[P, S]) Requeue(ctx context.Context, mode job.EnqueueMode, jobs []job.New[P, S]) error {
	return h.client.Requeue(ctx, mode, h.job.Queue, h.job.ID, h.job.RunID.Unwrap(), jobs)
}

// PollBuilder is used to configure and build Poller for use.
type PollBuilder[P, S any, R Runner[P, S]] struct {
	client  *Client[P, S]
	workers int
	queue   string
	runner  R
}

func newPollBuilder[P, S any, R Runner[P, S]](c *Client[P, S], queue string, runner R) *PollBuilder[P, S, R] {
	return &PollBuilder[P, S, R]{
		client:  c,
		workers: runtime.NumCPU(),
		queue:   queue,
		runner:  runner,
	}
}

// NumWorkers sets the number of backend async workers indicating the maximum number of in-flight
// `Job`s.
//
// Default is number of logical cores on the machine.
func (b *PollBuilder[P, S, R]) NumWorkers(n int) *PollBuilder[P, S, R] {
	b.workers = n
	return b
}

// Build creates a new `Poller` using the Builders configuration.
func (b *PollBuilder[P, S, R]) Build() *Poller[P, S, R] {
	return &Poller[P, S, R]{
		client: b.client,
		sem:    make(chan typesext.Nothing, b.workers),
		queue:  b.queue,
		runner: b.runner,
	}
}

// Poller is used to abstract away polling and running multiple `Job`s calling the provided `Runner` method(s).
type Poller[P, S any, R Runner[P, S]] struct {
	client *Client[P, S]
	sem    chan typesext.Nothing
	queue  string
	runner R
}

// Start begins polling for jobs and calling provided `Runner`.
func (p *Poller[P, S, R]) Start(ctx context.Context) (err error) {

	wg := new(sync.WaitGroup)
	ch := make(chan JobHelper[P, S], cap(p.sem))

	for i := 0; i < cap(ch); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.worker(ctx, ch)
		}()
	}
	err = p.poller(ctx, ch)

	close(ch)
	wg.Wait() // wait for all workers to finish
	close(p.sem)
	return
}

func (p *Poller[P, S, R]) worker(ctx context.Context, ch <-chan JobHelper[P, S]) {
	for helper := range ch {
		p.process(ctx, helper)
	}
}

func (p *Poller[P, S, R]) process(ctx context.Context, helper JobHelper[P, S]) {
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		select {
		case <-p.sem:
		default:
			panic("application is out-of-sync, more Jobs running than were requested")
		}
	}()
	p.runner.Run(ctx, helper)
}

func (p *Poller[P, S, R]) poller(ctx context.Context, ch chan<- JobHelper[P, S]) error {
	var numJobs uint
	for {
		if numJobs == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case p.sem <- valuesext.Nothing:
				numJobs++
			}
		}

		// attempt to maximize number of Jobs to try and pull multiple in one request.
	FOR:
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case p.sem <- valuesext.Nothing:
				numJobs++
			default:
				break FOR
			}
		}

		jobs, err := p.client.Poll(ctx, p.queue, numJobs)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return errors.Wrap(err, "failed to fetch next Job")
		}

		for _, j := range jobs {
			ch <- JobHelper[P, S]{
				client: p.client,
				job:    j,
			}
		}
		numJobs -= uint(len(jobs))
	}
}
