package job

import (
	. "github.com/go-playground/pkg/v5/values/option"
)

// ErrRequest represents and HTTP request error.
type ErrRequest struct {
	Message     string
	StatusCode  Option[int]
	IsRetryable bool
}

// Error returns the error message associated with the HTTP request.
func (e ErrRequest) Error() string {
	return e.Message
}

// Temporary returns if the error is temporary and can be retried.
func (e ErrRequest) Temporary() bool {
	return e.IsRetryable
}

// ErrAlreadyExists represents an error when a job already exists.
type ErrAlreadyExists struct {
	Err error
}

// Error returns the error message associated with the HTTP request.
func (e ErrAlreadyExists) Error() string {
	return e.Err.Error()
}

// Unwrap returns the underlying error.
func (e ErrAlreadyExists) Unwrap() error {
	return e.Err
}

// ErrNotFound represents an error when a job is not found.
type ErrNotFound struct {
	Err error
}

// Error returns the error message associated with the HTTP request.
func (e ErrNotFound) Error() string {
	return e.Err.Error()
}

// Unwrap returns the underlying error.
func (e ErrNotFound) Unwrap() error {
	return e.Err
}
