package xredis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const (
	luaObtainLock = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
if redis.call("SET", KEYS[1], ARGV[1], "NX", "PX", ARGV[2]) then
  return 1
end
return 0`
	luaReleaseLock = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end`
	luaRefreshLock = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
  return 0
end`
)

// LockOnRedisError 是加锁时 Redis 报错的处理方式。
type LockOnRedisError int8

const (
	// LockOnRedisErrorStrict 把错误返回给调用方。
	LockOnRedisErrorStrict LockOnRedisError = 0
	// LockOnRedisErrorTreatAsBusy 把 Redis 异常当成没抢到锁。
	LockOnRedisErrorTreatAsBusy LockOnRedisError = 1
)

// RetryStrategy 决定抢锁失败后多久再试。NextBackoff 返回 0 表示停止，对齐 redislock。
type RetryStrategy interface {
	NextBackoff() time.Duration
}

type linearBackoff time.Duration

// LinearBackoff 按固定间隔重试，直到 context 结束或外层 LimitRetry 用完次数。
func LinearBackoff(backoff time.Duration) RetryStrategy {
	return linearBackoff(backoff)
}

func (b linearBackoff) NextBackoff() time.Duration {
	if b <= 0 {
		return 0
	}
	return time.Duration(b)
}

type limitedRetry struct {
	s RetryStrategy
	n atomic.Int64
}

// LimitRetry 最多重试 n 次。n 是失败后的重试次数，不含第一次尝试。
// 返回的策略自己保存剩余次数，不要并发传给多个 Obtain。
func LimitRetry(s RetryStrategy, n int) RetryStrategy {
	r := &limitedRetry{s: s}
	if n > 0 {
		r.n.Store(int64(n))
	}
	return r
}

func (r *limitedRetry) NextBackoff() time.Duration {
	if r.s == nil {
		return 0
	}
	for {
		cur := r.n.Load()
		if cur <= 0 {
			return 0
		}
		if r.n.CompareAndSwap(cur, cur-1) {
			return r.s.NextBackoff()
		}
	}
}

type noRetry struct{}

func (noRetry) NextBackoff() time.Duration { return 0 }

// LockOptions 是 Obtain 的可选参数。nil 表示只抢一次，不自动续期。
type LockOptions struct {
	// RetryStrategy 抢锁失败后的重试策略。nil 表示不重试。
	RetryStrategy RetryStrategy
	// AutoRefresh 持有期间自动续期，Release 时停止。
	AutoRefresh bool
	// RefreshInterval 自动续期间隔。0 表示租约的三分之一。
	RefreshInterval time.Duration
	// OnRedisError 加锁过程 Redis 报错时的策略。
	OnRedisError LockOnRedisError
}

// Lock 表示已经拿到的锁。用完调用 Release。
type Lock struct {
	c        *Client
	key      string
	token    string
	ttl      time.Duration
	cancel   context.CancelFunc
	stopOnce sync.Once
	mu       sync.Mutex
}

// Obtain 获取锁，签名对齐 redislock.Obtain。
// 没抢到返回 ErrNotObtained。context 有截止时间时，会在截止前按 RetryStrategy 重试。
// context 没有截止时间时，最多等待一个 ttl，避免重试策略一直循环。
func (c *Client) Obtain(ctx context.Context, key string, ttl time.Duration, opt *LockOptions) (*Lock, error) {
	full, err := c.prepare(ctx, key)
	if err != nil {
		return nil, err
	}
	if ttl <= 0 {
		return nil, ErrInvalidTTL
	}
	if opt == nil {
		opt = &LockOptions{}
	}
	retry := opt.RetryStrategy
	if retry == nil {
		retry = noRetry{}
	}

	waitCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, ttl)
		defer cancel()
	}

	token := uuid.NewString()
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for {
		res, err := c.cli.Eval(waitCtx, luaObtainLock, []string{full}, token, ttlMillis(ttl)).Result()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if waitCtx.Err() != nil {
				return nil, ErrNotObtained
			}
			if opt.OnRedisError == LockOnRedisErrorTreatAsBusy {
				c.warnf("redis lock obtain error, treat as busy: key=%s err=%v", full, err)
				return nil, ErrNotObtained
			}
			return nil, fmt.Errorf("xredis: obtain lock %s: %w", key, err)
		}
		if n, ok := luaInt(res); ok && n == 1 {
			lock := &Lock{c: c, key: full, token: token, ttl: ttl}
			if opt.AutoRefresh {
				lock.startRefresh(refreshInterval(opt.RefreshInterval, ttl))
			}
			return lock, nil
		}

		backoff := retry.NextBackoff()
		if backoff < 1 {
			return nil, ErrNotObtained
		}
		if timer == nil {
			timer = time.NewTimer(backoff)
		} else {
			timer.Reset(backoff)
		}
		select {
		case <-waitCtx.Done():
			if err = ctx.Err(); err != nil {
				return nil, err
			}
			return nil, ErrNotObtained
		case <-timer.C:
		}
	}
}

// WithLock 获取锁后执行 fn，返回前自动 Release。没抢到返回 ErrNotObtained。
// fn 成功但释放失败时，返回释放错误。
func (c *Client) WithLock(ctx context.Context, key string, ttl time.Duration, opt *LockOptions, fn func() error) (err error) {
	if fn == nil {
		return errors.New("xredis: lock function is nil")
	}
	lock, err := c.Obtain(ctx, key, ttl, opt)
	if err != nil {
		return err
	}
	defer func() {
		if rerr := lock.Release(context.Background()); rerr != nil && err == nil {
			err = rerr
		}
	}()
	err = fn()
	return err
}

// Key 返回锁使用的真实 key。
func (l *Lock) Key() string {
	if l == nil {
		return ""
	}
	return l.key
}

// Token 返回本锁的持有者标识。
func (l *Lock) Token() string {
	if l == nil {
		return ""
	}
	return l.token
}

// Refresh 续期。锁已经不是自己的时返回 ErrLockNotHeld。
func (l *Lock) Refresh(ctx context.Context, ttl time.Duration) error {
	if l == nil || l.c == nil || l.c.cli == nil {
		return ErrLockNotHeld
	}
	l.mu.Lock()
	if ttl <= 0 {
		ttl = l.ttl
	}
	cli, key, token := l.c.cli, l.key, l.token
	l.mu.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}
	res, err := cli.Eval(ctx, luaRefreshLock, []string{key}, token, ttlMillis(ttl)).Result()
	if err != nil {
		return fmt.Errorf("xredis: refresh lock: %w", err)
	}
	n, ok := luaInt(res)
	if !ok || n == 0 {
		return ErrLockNotHeld
	}
	l.mu.Lock()
	l.ttl = ttl
	l.mu.Unlock()
	return nil
}

// Release 停止自动续期，并只删除自己持有的锁。
func (l *Lock) Release(ctx context.Context) error {
	if l == nil || l.c == nil || l.c.cli == nil {
		return ErrLockNotHeld
	}
	l.stopRefresh()
	l.mu.Lock()
	cli, key, token := l.c.cli, l.key, l.token
	l.mu.Unlock()
	if ctx == nil || ctx.Err() != nil {
		ctx = context.Background()
	}
	res, err := cli.Eval(ctx, luaReleaseLock, []string{key}, token).Result()
	if err != nil {
		return fmt.Errorf("xredis: release lock: %w", err)
	}
	n, ok := luaInt(res)
	if ok && n == 0 {
		return ErrLockNotHeld
	}
	return nil
}

func (l *Lock) startRefresh(interval time.Duration) {
	if interval <= 0 {
		interval = time.Millisecond
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel
	ttl := l.ttl
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := l.Refresh(ctx, ttl)
				if err == nil {
					continue
				}
				if errors.Is(err, context.Canceled) || errors.Is(err, ErrLockNotHeld) {
					return
				}
				// 网络抖动时继续续期。锁已经不属于自己时，上面的 ErrLockNotHeld 会停掉。
				l.c.warnf("redis lock refresh failed: key=%s err=%v", l.key, err)
			}
		}
	}()
}

func (l *Lock) stopRefresh() {
	l.stopOnce.Do(func() {
		if l.cancel != nil {
			l.cancel()
		}
	})
}

func refreshInterval(interval, ttl time.Duration) time.Duration {
	if interval <= 0 || interval >= ttl {
		interval = ttl / 3
	}
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	return interval
}

func ttlMillis(ttl time.Duration) int64 {
	ms := ttl.Milliseconds()
	if ms < 1 {
		return 1
	}
	return ms
}

func luaInt(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case float64:
		return int64(n), true
	default:
		return 0, false
	}
}
