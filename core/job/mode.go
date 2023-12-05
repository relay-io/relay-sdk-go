package job

// EnqueueMode is a custom enqueue mode that determines the behaviour of the enqueue function.
type EnqueueMode uint8

const (
	// Unique ensures the job is unique by job ID and will return an error id any Job already exists.
	Unique EnqueueMode = iota
	// Ignore will silently do nothing if the job that already exists.
	Ignore
	// Replace will replace the `Existing` job with the new `New` job changing the job to be immediately no longer in-flight.
	Replace
)

// String returns the string representation of the EnqueueMode.
func (m EnqueueMode) String() string {
	switch m {
	case Unique:
		return "unique"
	case Ignore:
		return "ignore"
	case Replace:
		return "replace"
	}
	return ""
}
