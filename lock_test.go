package rlock

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/huangyul/go-redis-lock/mocks"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

//go:generate mockgen -package=mocks -destination=./mocks/redis_mock.go github.com/redis/go-redis/v9 Cmdable

func TestClient_Lock(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name       string
		beforeMock func() redis.Cmdable
		key        string
		expiration time.Duration
		retry      RetryStrategy
		timeout    time.Duration
		wantErr    string
	}{
		{
			name: "locked",
			beforeMock: func() redis.Cmdable {
				client := mocks.NewMockCmdable(ctrl)
				res := redis.NewCmd(context.Background())
				res.SetVal("OK")
				client.EXPECT().Eval(gomock.Any(), luaLock, []string{"lock-key"}, gomock.Any()).Return(res)
				return client
			},
			key:        "lock-key",
			expiration: time.Minute,
			retry: &FixRetryStrategy{
				internal: time.Second,
				max:      1,
			},
			timeout: time.Second,
		},
		{
			name: "network error",
			beforeMock: func() redis.Cmdable {
				client := mocks.NewMockCmdable(ctrl)
				res := redis.NewCmd(context.Background(), nil)
				res.SetErr(errors.New("network error"))
				client.EXPECT().Eval(gomock.Any(), luaLock, []string{"lock-key"}, gomock.Any()).Return(res)
				return client
			},
			key:        "lock-key",
			expiration: time.Minute,
			retry: &FixRetryStrategy{
				internal: time.Second,
				max:      1,
			},
			timeout: time.Second,
			wantErr: "network error",
		},
		{
			name: "retry over time",
			beforeMock: func() redis.Cmdable {
				client := mocks.NewMockCmdable(ctrl)
				res := redis.NewCmd(context.Background(), nil)
				res.SetErr(context.DeadlineExceeded)
				client.EXPECT().Eval(gomock.Any(), luaLock, []string{"lock-key"}, gomock.Any()).Times(3).Return(res)
				return client
			},
			key:        "lock-key",
			expiration: time.Minute,
			retry: &FixRetryStrategy{
				internal: time.Second,
				max:      2,
			},
			timeout: time.Second,
			wantErr: fmt.Errorf("rlock: last retry fail: %w", context.DeadlineExceeded).Error(),
		},
		{
			name: "retry over times-lock holded",
			beforeMock: func() redis.Cmdable {
				client := mocks.NewMockCmdable(ctrl)
				res := redis.NewCmd(context.Background(), nil)
				client.EXPECT().Eval(gomock.Any(), luaLock, []string{"lock-key"}, gomock.Any()).Times(3).Return(res)
				return client
			},
			key:        "lock-key",
			expiration: time.Minute,
			retry: &FixRetryStrategy{
				internal: time.Second,
				max:      2,
			},
			timeout: time.Second,
			wantErr: "rlock: preempt lock fail",
		},
		{
			name: "retry and success",
			beforeMock: func() redis.Cmdable {
				client := mocks.NewMockCmdable(ctrl)
				res := redis.NewCmd(context.Background(), nil)
				res.SetErr(context.DeadlineExceeded)
				client.EXPECT().Eval(gomock.Any(), luaLock, []string{"lock-key"}, gomock.Any()).Times(2).Return(res)
				res1 := redis.NewCmd(context.Background(), nil)
				res1.SetVal("OK")
				client.EXPECT().Eval(gomock.Any(), luaLock, []string{"lock-key"}, gomock.Any()).Return(res1)
				return client
			},
			key:        "lock-key",
			expiration: time.Minute,
			retry: &FixRetryStrategy{
				internal: time.Second,
				max:      3,
			},
			timeout: time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdsClient := tt.beforeMock()
			client := NewClient(rdsClient)
			lctx, cancel := context.WithTimeout(context.Background(), tt.expiration)
			defer cancel()
			l, err := client.Lock(lctx, tt.key, tt.expiration, tt.retry, tt.timeout)
			if err != nil {
				assert.EqualError(t, err, tt.wantErr)
				return
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, rdsClient, l.client)
			assert.Equal(t, tt.key, l.key)
			assert.Equal(t, tt.expiration, l.expiration)
			assert.NotEmpty(t, l.value)
		})
	}
}

func TestClient_TryLock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	key := "trylock-key"
	timeout := time.Minute
	expiration := time.Second
	tests := []struct {
		name    string
		mock    func() redis.Cmdable
		wantErr string
	}{
		{
			name: "try lock success",
			mock: func() redis.Cmdable {
				m := mocks.NewMockCmdable(ctrl)
				res := redis.NewBoolCmd(context.Background(), nil)
				res.SetVal(true)
				m.EXPECT().SetNX(gomock.Any(), key, gomock.Any(), expiration).Return(res)
				return m
			},
		},
		{
			name: "try lock fail",
			mock: func() redis.Cmdable {
				m := mocks.NewMockCmdable(ctrl)
				res := redis.NewBoolCmd(context.Background(), nil)
				res.SetVal(false)
				m.EXPECT().SetNX(gomock.Any(), key, gomock.Any(), expiration).Return(res)
				return m
			},
			wantErr: "rlock: failed to preempt lock",
		},
		{
			name: "redis error",
			mock: func() redis.Cmdable {
				m := mocks.NewMockCmdable(ctrl)
				res := redis.NewBoolCmd(context.Background(), nil)
				res.SetErr(errors.New("redis error"))
				m.EXPECT().SetNX(gomock.Any(), key, gomock.Any(), expiration).Return(res)
				return m
			},
			wantErr: "redis error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockRds := tt.mock()
			client := NewClient(mockRds)
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			l, err := client.TryLock(ctx, key, expiration)
			if err != nil {
				assert.EqualError(t, err, tt.wantErr)
				return
			}
			assert.Equal(t, mockRds, l.client)
			assert.Equal(t, key, l.key)
		})
	}

}

func TestLock_AutoRefresh(t *testing.T) {
	key := "auto_reresh"
	val := "123"
	inter := float64(60)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name       string
		lock       func() *Lock
		interval   time.Duration
		timeout    time.Duration
		unlockTime time.Duration
		wantErr    error
	}{
		{
			name:       "refresh success",
			interval:   time.Millisecond * 100,
			timeout:    time.Second * 2,
			unlockTime: time.Second * 1,
			lock: func() *Lock {
				mock := mocks.NewMockCmdable(ctrl)
				res1 := redis.NewCmd(context.Background())
				res1.SetVal(int64(1))
				mock.EXPECT().Eval(gomock.Any(), luaRefresh, []string{key}, val, inter).AnyTimes().Return(res1)
				res2 := redis.NewCmd(context.Background())
				res2.SetVal(int64(1))
				mock.EXPECT().Eval(gomock.Any(), luaUnlock, []string{key}, val).Return(res2)
				l := &Lock{
					client:     mock,
					key:        key,
					value:      val,
					expiration: time.Minute,
					unlock:     make(chan struct{}, 1),
				}
				return l
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lock := tt.lock()
			go func() {
				time.Sleep(tt.unlockTime)
				err := lock.Unlock(context.Background())
				assert.NoError(t, err)
			}()
			err := lock.AutoRefresh(tt.interval, tt.timeout)
			assert.Equal(t, tt.wantErr, err)
		})
	}

}
