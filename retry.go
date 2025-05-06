package rlock

import "time"

type RetryStrategy interface {
	Next() (time.Duration, bool)
}
