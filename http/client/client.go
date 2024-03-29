package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-playground/backoff-sys"
	"github.com/go-playground/errors/v5"
	bytesext "github.com/go-playground/pkg/v5/bytes"
	errorsext "github.com/go-playground/pkg/v5/errors"
	httpext "github.com/go-playground/pkg/v5/net/http"
	unsafeext "github.com/go-playground/pkg/v5/unsafe"
	. "github.com/go-playground/pkg/v5/values/option"
	"github.com/google/uuid"
	"github.com/relay-io/relay-sdk-go/core/job"
	"io"
	"net/http"
	"net/url"
	"time"
)

// Builder is used to build a Relay Client for use.
type Builder[P, S any] struct {
	baseURL         string
	pollBackoff     backoff.Exponential
	retryBackoff    backoff.Exponential
	maxRetries      Option[uint]
	client          *http.Client
	maxBytes        int64
	customOnRetryFn Option[errorsext.OnRetryFn[error]]
}

// NewBuilder creates a new Builder for use with sane defaults.
func NewBuilder[P, S any](baseURL string) *Builder[P, S] {
	return &Builder[P, S]{
		baseURL:      baseURL,
		pollBackoff:  backoff.NewExponential().Interval(200 * time.Millisecond).Jitter(25 * time.Millisecond).Max(time.Second).Init(),
		retryBackoff: backoff.NewExponential().Interval(100 * time.Millisecond).Jitter(25 * time.Millisecond).Max(time.Second).Init(),
		maxRetries:   None[uint](),
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		maxBytes: 5 * bytesext.MiB,
	}
}

// PollBackoff sets the backoff used when polling for Jobs.
func (b *Builder[P, S]) PollBackoff(bo backoff.Exponential) *Builder[P, S] {
	b.pollBackoff = bo
	return b
}

// RetryBackoff sets the backoff used when retrying failed requests.
func (b *Builder[P, S]) RetryBackoff(bo backoff.Exponential) *Builder[P, S] {
	b.retryBackoff = bo
	return b
}

// MaxRetries sets the maximum number of retries to attempt before giving up.
func (b *Builder[P, S]) MaxRetries(maxRetries uint) *Builder[P, S] {
	b.maxRetries = Some(maxRetries)
	return b
}

// Client sets the http.Client to use for all requests.
func (b *Builder[P, S]) Client(client *http.Client) *Builder[P, S] {
	b.client = client
	return b
}

// MaxBytes sets the maximum number of bytes to accept for an HTTP response.
//
// see bytesext.Bytes for easily working with bytes.
func (b *Builder[P, S]) MaxBytes(maxBytes int64) *Builder[P, S] {
	b.maxBytes = maxBytes
	return b
}

// OnRetry sets an optional OnRetryFn to call when requests are being retried.
//
// When set this function will be called from within the default OnRetryFn within this client which
// handles retry backoff.
func (b *Builder[P, S]) OnRetry(fn errorsext.OnRetryFn[error]) *Builder[P, S] {
	b.customOnRetryFn = Some(fn)
	return b
}

// Build creates a new Relay Client for use.
func (b *Builder[P, S]) Build() *Client[P, S] {
	return &Client[P, S]{
		baseURL:         b.baseURL,
		pollBackoff:     b.pollBackoff,
		retryBackoff:    b.retryBackoff,
		maxRetries:      b.maxRetries,
		client:          b.client,
		maxBytes:        b.maxBytes,
		customOnRetryFn: b.customOnRetryFn,
	}
}

// Client is the Relay low-level http client for interacting with the Relay HTTP API.
type Client[P, S any] struct {
	baseURL         string
	pollBackoff     backoff.Exponential
	retryBackoff    backoff.Exponential
	maxRetries      Option[uint]
	client          *http.Client
	maxBytes        int64
	customOnRetryFn Option[errorsext.OnRetryFn[error]]
}

// Enqueue enqueues a batch of one or more `New` jobs for processing using the provided `EnqueueMode`.
//
// # Errors
//
// Will return `Err` on an unrecoverable network error.
func (c *Client[P, S]) Enqueue(ctx context.Context, mode job.EnqueueMode, jobs []job.New[P, S]) (err error) {
	b, err := json.Marshal(jobs)
	if err != nil {
		return errors.Wrap(err, "failed to marshal jobs for enqueueing")
	}

	url := fmt.Sprintf("%s/v2/queues/jobs?mode=%s", c.baseURL, mode.String())
	fn := func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
		if err != nil {
			return nil, errors.Wrap(err, "failed to create enqueue request")
		}
		req.Header.Set(httpext.ContentType, httpext.ApplicationJSON)
		return req, nil
	}
	result := httpext.DoRetryableResponse(ctx, c.onRetryRetryFn, httpext.IsRetryableStatusCode, c.client, fn)
	if result.IsErr() {
		return errors.Wrap(result.Err(), "failed to make enqueue jobs request")
	}
	resp := result.Unwrap()
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		return nil
	case http.StatusConflict:
		b, _ := io.ReadAll(resp.Body)
		return job.ErrAlreadyExists{Err: errors.New(unsafeext.BytesToString(b))}
	default:
		b, _ := io.ReadAll(resp.Body)
		return job.ErrRequest{
			Message:     unsafeext.BytesToString(b),
			StatusCode:  Some(resp.StatusCode),
			IsRetryable: false,
		}
	}
}

// Get attempts to return an `Existing` job.
//
// # Errors
//
// Will return `Err` on:
// - an unrecoverable network error.
// - if the `Job` doesn't exist.
func (c *Client[P, S]) Get(ctx context.Context, queue, jobID string) (Option[job.Existing[P, S]], error) {
	url := fmt.Sprintf("%s/v2/queues/%s/jobs/%s", c.baseURL, url.QueryEscape(queue), url.QueryEscape(jobID))
	fn := func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create get request")
		}
		return req, nil
	}
	result := httpext.DoRetryable[job.Existing[P, S]](ctx, errorsext.IsRetryableHTTP, c.onRetryRetryFn, httpext.IsRetryableStatusCode, c.client, http.StatusOK, c.maxBytes, fn)
	if result.IsErr() {
		var e httpext.ErrUnexpectedResponse
		if errors.As(result.Err(), &e) && e.Response.StatusCode == http.StatusNotFound {
			return None[job.Existing[P, S]](), job.ErrNotFound{Err: errors.Wrap(result.Err(), "failed to get job")}
		}
		return None[job.Existing[P, S]](), errors.Wrap(result.Err(), "failed to fetch job")
	}
	return Some[job.Existing[P, S]](result.Unwrap()), nil
}

// Exists returns if a `Existing` job exists.
//
// # Errors
//
// Will return `Err` on an unrecoverable network error.\
func (c *Client[P, S]) Exists(ctx context.Context, queue, jobID string) (bool, error) {
	url := fmt.Sprintf("%s/v2/queues/%s/jobs/%s", c.baseURL, url.QueryEscape(queue), url.QueryEscape(jobID))
	fn := func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create get request")
		}
		return req, nil
	}
	result := httpext.DoRetryableResponse(ctx, c.onRetryRetryFn, httpext.IsRetryableStatusCode, c.client, fn)
	if result.IsErr() {
		return false, errors.Wrap(result.Err(), "failed to fetch job")
	}
	resp := result.Unwrap()
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		b, _ := io.ReadAll(resp.Body)
		return false, job.ErrRequest{
			Message:     unsafeext.BytesToString(b),
			StatusCode:  Some(resp.StatusCode),
			IsRetryable: false,
		}
	}
}

// Delete deletes an `Existing` job.
//
// # Errors
//
// Will return `Err` on:
// - an unrecoverable network error.
func (c *Client[P, S]) Delete(ctx context.Context, queue, jobID string) error {
	url := fmt.Sprintf("%s/v2/queues/%s/jobs/%s", c.baseURL, url.QueryEscape(queue), url.QueryEscape(jobID))
	fn := func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create delete request")
		}
		return req, nil
	}
	result := httpext.DoRetryableResponse(ctx, c.onRetryRetryFn, httpext.IsRetryableStatusCode, c.client, fn)
	if result.IsErr() {
		return errors.Wrap(result.Err(), "failed to make delete job request")
	}
	resp := result.Unwrap()
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	default:
		b, _ := io.ReadAll(resp.Body)
		return job.ErrRequest{
			Message:     unsafeext.BytesToString(b),
			StatusCode:  Some(resp.StatusCode),
			IsRetryable: false,
		}
	}
}

// Complete deletes an in-flight `Existing` job.
//
// # Errors
//
// Will return `Err` on:
// - an unrecoverable network error.
// - The `Existing` job doesn't exist.
func (c *Client[P, S]) Complete(ctx context.Context, queue, jobID string, runID uuid.UUID) error {
	url := fmt.Sprintf("%s/v2/queues/%s/jobs/%s/run-id/%s", c.baseURL, url.QueryEscape(queue), url.QueryEscape(jobID), runID)
	fn := func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create complete request")
		}
		return req, nil
	}
	result := httpext.DoRetryableResponse(ctx, c.onRetryRetryFn, httpext.IsRetryableStatusCode, c.client, fn)
	if result.IsErr() {
		return errors.Wrap(result.Err(), "failed to make complete job request")
	}
	resp := result.Unwrap()
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	default:
		b, _ := io.ReadAll(resp.Body)
		return job.ErrRequest{
			Message:     unsafeext.BytesToString(b),
			StatusCode:  Some(resp.StatusCode),
			IsRetryable: false,
		}
	}
}

// Heartbeat sends a heartbeat request to an in-flight `Existing` job indicating it is still processing, resetting
// the timeout. Optionally you can update the `Existing` jobs state during the same request.
//
// # Errors
//
// Will return `Err` on:
// - an unrecoverable network error.
// - if the `Existing` job doesn't exist.
func (c *Client[P, S]) Heartbeat(ctx context.Context, queue, jobID string, runID uuid.UUID, state Option[S]) (err error) {
	var b []byte
	if state.IsSome() {
		b, err = json.Marshal(state)
		if err != nil {
			return errors.Wrap(err, "failed to marshal state for heartbeat")
		}
	}
	url := fmt.Sprintf("%s/v2/queues/%s/jobs/%s/run-id/%s", c.baseURL, url.QueryEscape(queue), url.QueryEscape(jobID), runID)
	fn := func(ctx context.Context) (req *http.Request, err error) {
		if state.IsSome() {
			req, err = http.NewRequestWithContext(ctx, http.MethodPatch, url, bytes.NewReader(b))
		} else {
			req, err = http.NewRequestWithContext(ctx, http.MethodPatch, url, nil)
		}
		if err != nil {
			return nil, errors.Wrap(err, "failed to create heartbeat request")
		}
		req.Header.Set(httpext.ContentType, httpext.ApplicationJSON)
		return req, nil
	}
	result := httpext.DoRetryableResponse(ctx, c.onRetryRetryFn, httpext.IsRetryableStatusCode, c.client, fn)
	if result.IsErr() {
		return errors.Wrap(result.Err(), "failed to make heartbeat job request")
	}
	resp := result.Unwrap()
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		return nil
	case http.StatusNotFound:
		return job.ErrNotFound{Err: errors.Wrap(result.Err(), "failed to heartbeat job")}
	default:
		b, _ := io.ReadAll(resp.Body)
		return job.ErrRequest{
			Message:     unsafeext.BytesToString(b),
			StatusCode:  Some(resp.StatusCode),
			IsRetryable: false,
		}
	}
}

// Requeue re-enqueues an existing in-flight `Existing` job to be run again or spawn a new set of jobs
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
// # Errors
//
// Will return `Err` on:
// - an unrecoverable network error.
// - if one of the `Existing` jobs exists when mode is unique.
func (c *Client[P, S]) Requeue(ctx context.Context, mode job.EnqueueMode, queue, jobID string, runID uuid.UUID, jobs []job.New[P, S]) (err error) {
	b, err := json.Marshal(jobs)
	if err != nil {
		return errors.Wrap(err, "failed to marshal jobs for requeue")
	}
	url := fmt.Sprintf("%s/v2/queues/%s/jobs/%s/run-id/%s?mode=%s", c.baseURL, url.QueryEscape(queue), url.QueryEscape(jobID), runID, mode.String())
	fn := func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(b))
		if err != nil {
			return nil, errors.Wrap(err, "failed to create requeue request")
		}
		req.Header.Set(httpext.ContentType, httpext.ApplicationJSON)
		return req, nil
	}
	result := httpext.DoRetryableResponse(ctx, c.onRetryRetryFn, httpext.IsRetryableStatusCode, c.client, fn)
	if result.IsErr() {
		return errors.Wrap(result.Err(), "failed to make requeue job request")
	}
	resp := result.Unwrap()
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		return nil
	case http.StatusConflict:
		b, _ := io.ReadAll(resp.Body)
		return job.ErrAlreadyExists{Err: errors.New(unsafeext.BytesToString(b))}
	default:
		b, _ := io.ReadAll(resp.Body)
		return job.ErrRequest{
			Message:     unsafeext.BytesToString(b),
			StatusCode:  Some(resp.StatusCode),
			IsRetryable: false,
		}
	}
}

// Next attempts to retrieve the next `Existing` job(s) for processing.
//
// # Errors
//
// Will return `Err` on:
// - an unrecoverable network error.
// - no `Existing` jobs currently exists.
func (c *Client[P, S]) Next(ctx context.Context, queue string, numJobs uint) ([]job.Existing[P, S], error) {
	url := fmt.Sprintf("%s/v2/queues/%s/jobs?num_jobs=%d", c.baseURL, url.QueryEscape(queue), numJobs)
	fn := func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create next request")
		}
		// TODO: Test Accept Encoding Gzip also with Rust Client
		return req, nil
	}
	result := httpext.DoRetryable[[]job.Existing[P, S]](ctx, errorsext.IsRetryableHTTP, c.onRetryRetryFn, httpext.IsRetryableStatusCode, c.client, http.StatusOK, c.maxBytes, fn)
	if result.IsErr() {
		var e httpext.ErrUnexpectedResponse
		if errors.As(result.Err(), &e) && e.Response.StatusCode == http.StatusNoContent {
			return nil, nil
		}
		return nil, errors.Wrap(result.Err(), "failed to fetch next jobs")
	}
	return result.Unwrap(), nil
}

// Poll polls the Relay server until a `Job` becomes available.
//
// # Errors
//
// Will return `Err` on an unrecoverable network error.
func (c *Client[P, S]) Poll(ctx context.Context, queue string, numJobs uint) ([]job.Existing[P, S], error) {
	var attempt int
	for {
		jobs, err := c.Next(ctx, queue, numJobs)
		if err != nil {
			return nil, errors.Wrap(err, "failed to polling for next jobs")
		}
		if len(jobs) > 0 {
			return jobs, nil
		}
		if err := c.pollBackoff.Sleep(ctx, attempt); err != nil {
			// can only happen if context cancelled or timed out
			return nil, errors.Wrap(err, "failed to sleep before next poll attempt")
		}
		attempt++
	}
}

// Poller Creates a new poller that will handle asynchronously polling and distributing `Existing`
// jobs to be processed by calling the supplied `Runner`.
func (c *Client[P, S]) Poller(queue string, runner Runner[P, S]) *PollBuilder[P, S, Runner[P, S]] {
	return newPollBuilder(c, queue, runner)
}

func (c *Client[P, S]) onRetryRetryFn(ctx context.Context, origErr error, retryReason string, attempt int) Option[error] {
	if err := c.retryBackoff.Sleep(ctx, attempt); err != nil {
		if errors.Is(origErr, backoff.ErrMaxAttemptsReached) {
			return Some(origErr)
		}
		if c.customOnRetryFn.IsSome() {
			return c.customOnRetryFn.Unwrap()(ctx, origErr, retryReason, attempt)
		}
		return Some(err)
	}
	return None[error]()
}
