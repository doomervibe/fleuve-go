package delay

import (
	"context"
	"time"

	"github.com/robfig/cron/v3"
)

func NextCronFire(expr, tz string) *time.Time {
	if expr == "" {
		return nil
	}

	var loc *time.Location = time.UTC
	if tz != "" {
		var err error
		loc, err = time.LoadLocation(tz)
		if err != nil {
			loc = time.UTC
		}
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(expr)
	if err != nil {
		return nil
	}

	next := schedule.Next(time.Now().In(loc))
	return &next
}

func NextCronFires(expr, tz string, n int) []time.Time {
	if expr == "" {
		return nil
	}

	var loc *time.Location = time.UTC
	if tz != "" {
		var err error
		loc, err = time.LoadLocation(tz)
		if err != nil {
			loc = time.UTC
		}
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(expr)
	if err != nil {
		return nil
	}

	result := make([]time.Time, 0, n)
	now := time.Now().In(loc)
	for i := 0; i < n; i++ {
		now = schedule.Next(now)
		result = append(result, now)
	}
	return result
}

type DelayScheduler interface {
	Start(ctx context.Context) error
	Stop() error
	RegisterDelay(ctx context.Context, workflowID, delayID string, delayUntil time.Time, eventVersion int64, nextCmd []byte, cronExpr, tz string) error
	RemoveDelay(ctx context.Context, workflowID, delayID string) error
}
