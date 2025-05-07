package rlock

import "time"

type RetryStrategy interface {
	Next() (time.Duration, bool)
}

var _ RetryStrategy = (*FixRetryStrategy)(nil)

type FixRetryStrategy struct {
	internal time.Duration
	cnt      int
	max      int
}

func (f *FixRetryStrategy) Next() (time.Duration, bool) {
	f.cnt++
	return f.internal, f.max >= f.cnt
}
