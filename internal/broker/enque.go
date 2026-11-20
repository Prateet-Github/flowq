package broker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Prateet-Github/flowq/internal/job"
)

func (b *RedisBroker) Enqueue(ctx context.Context, j *job.Job) error {
	if err := j.Validate(); err != nil {
		return err
	}

	data, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	key := fmt.Sprintf("{q}:pending:%s", j.Queue)

	if err := b.client.RPush(ctx, key, data).Err(); err != nil {
		return fmt.Errorf("enqueue job: %w", err)
	}

	return nil
}
