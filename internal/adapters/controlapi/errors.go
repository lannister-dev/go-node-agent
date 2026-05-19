package controlapi

import "fmt"

type RetryableError struct {
	Status     int
	Underlying error
	Body       string
}

func (e *RetryableError) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("control-api: retryable status=%d: %v", e.Status, e.Underlying)
	}
	return fmt.Sprintf("control-api: retryable: %v", e.Underlying)
}

func (e *RetryableError) Unwrap() error { return e.Underlying }

type NonRetryableError struct {
	Status     int
	Underlying error
	Body       string
}

func (e *NonRetryableError) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("control-api: non-retryable status=%d: %v", e.Status, e.Underlying)
	}
	return fmt.Sprintf("control-api: non-retryable: %v", e.Underlying)
}

func (e *NonRetryableError) Unwrap() error { return e.Underlying }

func (e *NonRetryableError) IsConflict() bool { return e.Status == 409 }
