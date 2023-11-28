package client

import (
	"context"
	"github.com/go-playground/errors/v5"
	typesext "github.com/go-playground/pkg/v5/types"
	valuesext "github.com/go-playground/pkg/v5/values"
	"github.com/relay-io/relay-sdk-go/core/job"
	"sync"
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

// PollBuilder is used to configure and build Poller for use.
type PollBuilder[P, S any, R Runner[P, S]] struct {
	client  *Client[P, S]
	workers int
	queue   string
	runner  R
}

func newPollBuilder[P, S any, R Runner[P, S]](c *Client[P, S], queue string, runner R) *PollBuilder[P, S, R] {
	return &PollBuilder[P, S, R]{
		workers: 10,
		queue:   queue,
		runner:  runner,
	}
}

// NumWorkers sets the number of backend async workers indicating the maximum number of in-flight
// `Job`s.
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

		for _, job := range jobs {
			ch <- JobHelper[P, S]{
				client: p.client,
				job:    job,
			}
		}
		numJobs -= uint(len(jobs))
	}
}
