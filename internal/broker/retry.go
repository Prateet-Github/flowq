package broker

import (
	"context"
	_ "embed"

	"encoding/json"

	"fmt"
	"time"

	"github.com/Prateet-Github/flowq/internal/job"
)

func (b *RedisBroker) ScheduleRetry(
	ctx context.Context,
	j *job.Job,
	delay time.Duration,
) error {
	data, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal retry job: %w", err)
	}

	delayMs := delay / time.Millisecond
	if delay%time.Millisecond > 0 {
		delayMs++
	} else if delay%time.Millisecond < 0 {
		delayMs--
	}

	result, err := retryScript.Run(
		ctx,
		b.client,
		[]string{
			"{q}:active",
			"{q}:scheduled",
		},
		j.ID,
		"{q}:job:",
		data,
		j.LeaseToken,
		delayMs,
	).Int()

	if err != nil {
		return fmt.Errorf("schedule retry for job %s: %w", j.ID, err)
	}

	if result == 0 {
		return fmt.Errorf(
			"cannot schedule retry for job %s: lease expired or ownership lost",
			j.ID,
		)
	}

	return nil
}

func (b *RedisBroker) PromoteRetries(
	ctx context.Context,
	limit int,
) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("retry promotion limit must be positive")
	}

	result, err := promoteRetriesScript.Run(
		ctx,
		b.client,
		[]string{"{q}:scheduled"},
		"{q}:job:",
		limit,
	).Int()

	if err != nil {
		return 0, fmt.Errorf("promote due retries: %w", err)
	}

	return result, nil
}
