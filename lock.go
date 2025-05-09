package rlock

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

var (
	//go:embed script/lua/lock.lua
	luaLock string
	//go:embed script/lua/unlock.lua
	luaUnlock string
	//go:embed script/lua/refresh.lua
	luaRefresh string

	ErrFaildToPreemptLock = errors.New("rlock: failed to preempt lock")
	ErrLockNotHold        = errors.New("rlock: lock not hold")
)

type Client struct {
	g      singleflight.Group
	client redis.Cmdable
	valuer func() string
}

func NewClient(client redis.Cmdable) *Client {
	return &Client{
		client: client,
		valuer: func() string {
			return uuid.NewString()
		},
	}
}

func (c *Client) SingleflightLock(ctx context.Context, key string, expiration time.Duration, retry RetryStrategy, timeout time.Duration) (*Lock, error) {
	for {
		flag := false
		result := c.g.DoChan(key, func() (any, error) {
			flag = true
			return c.Lock(ctx, key, expiration, retry, timeout)
		})
		select {
		case res := <-result:
			if flag {
				c.g.Forget(key)
				if res.Err != nil {
					return nil, res.Err
				}
				return res.Val.(*Lock), nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Lock get lock
func (c *Client) Lock(ctx context.Context, key string, expiration time.Duration, retry RetryStrategy, timeout time.Duration) (*Lock, error) {
	val := c.valuer()
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		lctx, cancel := context.WithTimeout(ctx, timeout)

		res, err := c.client.Eval(lctx, luaLock, []string{key}, val).Result()
		cancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}

		if res == "OK" {
			l := newLock(c.client, key, val, expiration)
			return l, nil
		}

		interval, ok := retry.Next()

		if !ok {
			if err != nil {
				err = fmt.Errorf("rlock: last retry fail: %w", err)
			} else {
				err = fmt.Errorf("rlock: preempt lock fail")
			}
			return nil, err
		}

		if timer == nil {
			timer = time.NewTimer(interval)
		} else {
			timer.Reset(interval)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}

}

// TryLock try to get lock
func (c *Client) TryLock(ctx context.Context, key string, expiration time.Duration) (*Lock, error) {
	val := c.valuer()
	ok, err := c.client.SetNX(ctx, key, val, expiration).Result()
	if err != nil {
		return nil, err
	}
	if !ok {
		// the lock is already held by someone else
		return nil, ErrFaildToPreemptLock
	}
	return newLock(c.client, key, val, expiration), nil
}

type Lock struct {
	client           redis.Cmdable
	key              string
	value            string
	expiration       time.Duration
	unlock           chan struct{}
	signalUnlockOnce sync.Once
}

func newLock(client redis.Cmdable, key string, value string, expiration time.Duration) *Lock {
	return &Lock{
		client:     client,
		key:        key,
		value:      value,
		expiration: expiration,
		unlock:     make(chan struct{}, 1),
	}
}

func (l *Lock) AutoRefresh(interval time.Duration, timeout time.Duration) error {
	timer := time.NewTicker(interval)
	ch := make(chan struct{}, 1)
	defer func() {
		timer.Stop()
		close(ch)
	}()

	for {
		select {
		case <-timer.C:
			lctx, cancel := context.WithTimeout(context.Background(), timeout)
			err := l.Refresh(lctx)
			cancel()
			if errors.Is(err, context.DeadlineExceeded) {
				select {
				case ch <- struct{}{}:
				default:
				}
				continue
			}
			if err != nil {
				return err
			}
		case <-ch:
			lctx, cancel := context.WithTimeout(context.Background(), timeout)
			err := l.Refresh(lctx)
			cancel()
			if errors.Is(err, context.DeadlineExceeded) {
				select {
				case ch <- struct{}{}:
				default:
				}
				continue
			}
			if err != nil {
				return err
			}
		case <-l.unlock:
			return nil
		}
	}
}

func (l *Lock) Refresh(ctx context.Context) error {
	res, err := l.client.Eval(ctx, luaRefresh, []string{l.key}, l.value, l.expiration.Seconds()).Int64()
	if errors.Is(err, redis.Nil) {
		return ErrLockNotHold
	}
	if err != nil {
		return err
	}
	if res != 1 {
		return ErrLockNotHold
	}
	return nil
}

func (l *Lock) Unlock(ctx context.Context) error {
	res, err := l.client.Eval(ctx, luaUnlock, []string{l.key}, l.value).Int64()
	defer func() {
		l.signalUnlockOnce.Do(func() {
			l.unlock <- struct{}{}
			close(l.unlock)
		})
	}()
	if errors.Is(err, redis.Nil) {
		return ErrLockNotHold
	}
	if err != nil {
		return err
	}
	if res != 1 {
		return ErrLockNotHold
	}
	return nil
}
