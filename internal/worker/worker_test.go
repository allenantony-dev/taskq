package worker

import (
	"errors"
	"fmt"
	"taskq/internal/queue"
	"testing"
	"time"
)

func TestBackoffForClimbsThenClamps(t *testing.T) {
	cases := []struct {
		attempts int64
		want     time.Duration
	}{
		{1, time.Second},
		{2, 10 * time.Second},
		{3, time.Minute},
		{4, 5 * time.Minute},
		// max_attempts can exceed the table, and the last delay then repeats.
		{5, 5 * time.Minute},
		{99, 5 * time.Minute},
	}

	for _, c := range cases {
		if got := backoffFor(c.attempts); got != c.want {
			t.Errorf("backoffFor(%d) = %s, want %s", c.attempts, got, c.want)
		}
	}
}

// max_attempts = 5 must mean exactly five attempts: attempts is incremented on
// claim, so it is the fifth failure that ends the job.
func TestIsFinal(t *testing.T) {
	boom := errors.New("boom")

	cases := []struct {
		name     string
		attempts int64
		err      error
		want     bool
	}{
		{"first of five", 1, boom, false},
		{"one attempt left", 4, boom, false},
		{"last attempt used", 5, boom, true},
		{"past the limit", 6, boom, true},
		{"permanent failure on the first attempt", 1, fmt.Errorf("bad payload: %w", ErrPermanent), true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			task := queue.Task{Attempts: c.attempts, MaxAttempts: 5}
			if got := isFinal(task, c.err); got != c.want {
				t.Errorf("isFinal(attempts=%d, %v) = %v, want %v", c.attempts, c.err, got, c.want)
			}
		})
	}
}
