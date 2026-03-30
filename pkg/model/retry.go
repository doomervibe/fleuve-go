package model

import "time"

type RetryPolicy struct {
	MaxRetries      int           `json:"max_retries"`
	BackoffStrategy string        `json:"backoff_strategy"` // "exponential" or "linear"
	BackoffFactor   float64       `json:"backoff_factor"`
	BackoffMax      time.Duration `json:"backoff_max"`
	BackoffMin      time.Duration `json:"backoff_min"`
	BackoffJitter   float64       `json:"backoff_jitter"`
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxRetries:      3,
		BackoffStrategy: "exponential",
		BackoffFactor:   2,
		BackoffMax:      60 * time.Second,
		BackoffMin:      1 * time.Second,
		BackoffJitter:   0.5,
	}
}

func (r RetryPolicy) CalculateDelay(retryCount int) time.Duration {
	var delay time.Duration
	switch r.BackoffStrategy {
	case "exponential":
		delay = time.Duration(float64(r.BackoffMin) * float64(uint64(1)<<uint(retryCount)))
		if delay > r.BackoffMax {
			delay = r.BackoffMax
		}
	case "linear":
		delay = r.BackoffMin * time.Duration(retryCount+1)
		if delay > r.BackoffMax {
			delay = r.BackoffMax
		}
	default:
		delay = r.BackoffMin
	}
	return delay
}
