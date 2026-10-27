package xredis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// Set 按 JSON 写入缓存，签名对齐 go-redis 的 Set。ttl 为 0 表示不过期。
func (c *Client) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	full, err := c.prepare(ctx, key)
	if err != nil {
		return err
	}
	if ttl < 0 {
		return ErrInvalidTTL
	}
	buf, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("xredis: marshal %s: %w", key, err)
	}
	if err = c.cli.Set(ctx, full, buf, ttl).Err(); err != nil {
		return fmt.Errorf("xredis: set %s: %w", key, err)
	}
	return nil
}

// Get 读取 Set 写入的 JSON，并填进 dest。dest 必须是非空指针。
// key 不存在时返回 ErrCacheMiss。
func (c *Client) Get(ctx context.Context, key string, dest any) error {
	if err := checkDest(dest); err != nil {
		return err
	}
	full, err := c.prepare(ctx, key)
	if err != nil {
		return err
	}
	buf, err := c.cli.Get(ctx, full).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return ErrCacheMiss
		}
		return fmt.Errorf("xredis: get %s: %w", key, err)
	}
	return decode(key, dest, buf)
}

// SetNX 仅在 key 不存在时写入 JSON。写入成功返回 true。可用于防重复提交。
func (c *Client) SetNX(ctx context.Context, key string, value any, ttl time.Duration) (bool, error) {
	full, err := c.prepare(ctx, key)
	if err != nil {
		return false, err
	}
	if ttl < 0 {
		return false, ErrInvalidTTL
	}
	buf, err := json.Marshal(value)
	if err != nil {
		return false, fmt.Errorf("xredis: marshal %s: %w", key, err)
	}
	ok, err := c.cli.SetNX(ctx, full, buf, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("xredis: setnx %s: %w", key, err)
	}
	return ok, nil
}

// Del 删除一个或多个业务 key，返回实际删除的数量。
func (c *Client) Del(ctx context.Context, keys ...string) (int64, error) {
	if c == nil || c.cli == nil {
		return 0, ErrNotInitialized
	}
	if ctx == nil {
		return 0, errors.New("xredis: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if len(keys) == 0 {
		return 0, ErrEmptyKey
	}
	var deleted int64
	for _, key := range keys {
		item, err := c.fullKey(key)
		if err != nil {
			return deleted, err
		}
		// 集群模式下多个 key 可能不在同一个槽，逐个删除才能在单机和集群上表现一致。
		n, err := c.cli.Del(ctx, item).Result()
		if err != nil {
			return deleted, fmt.Errorf("xredis: del %s: %w", key, err)
		}
		deleted += n
	}
	return deleted, nil
}

// Exists 判断业务 key 是否存在。
func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	full, err := c.prepare(ctx, key)
	if err != nil {
		return false, err
	}
	n, err := c.cli.Exists(ctx, full).Result()
	if err != nil {
		return false, fmt.Errorf("xredis: exists %s: %w", key, err)
	}
	return n > 0, nil
}

// Expire 给已存在的 key 设置过期时间。ttl 必须大于 0。
func (c *Client) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	full, err := c.prepare(ctx, key)
	if err != nil {
		return false, err
	}
	if ttl <= 0 {
		return false, ErrInvalidTTL
	}
	ok, err := c.cli.Expire(ctx, full, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("xredis: expire %s: %w", key, err)
	}
	return ok, nil
}

// TTL 返回剩余生存时间。key 不存在返回 ErrCacheMiss；没有过期时间时返回 -1s。
func (c *Client) TTL(ctx context.Context, key string) (time.Duration, error) {
	full, err := c.prepare(ctx, key)
	if err != nil {
		return 0, err
	}
	d, err := c.cli.TTL(ctx, full).Result()
	if err != nil {
		return 0, fmt.Errorf("xredis: ttl %s: %w", key, err)
	}
	if d == -2*time.Second {
		return 0, ErrCacheMiss
	}
	return d, nil
}

// Incr 将 key 的整数值加 1。这个 key 不要再用 Set 写入 JSON。
func (c *Client) Incr(ctx context.Context, key string) (int64, error) {
	return c.IncrBy(ctx, key, 1)
}

// IncrBy 将 key 的整数值加上 n。
func (c *Client) IncrBy(ctx context.Context, key string, n int64) (int64, error) {
	full, err := c.prepare(ctx, key)
	if err != nil {
		return 0, err
	}
	val, err := c.cli.IncrBy(ctx, full, n).Result()
	if err != nil {
		return 0, fmt.Errorf("xredis: incr %s: %w", key, err)
	}
	return val, nil
}

// Once 先读缓存，没有则执行 do 并写入。同一个 key 并发回源时 do 只会执行一次。
// 行为对齐 go-redis/cache 的 Once。dest 必须是非空指针。
func (c *Client) Once(ctx context.Context, key string, dest any, ttl time.Duration, do func(context.Context) (any, error)) error {
	if err := checkDest(dest); err != nil {
		return err
	}
	err := c.Get(ctx, key, dest)
	if err == nil {
		return nil
	}
	if !errors.Is(err, ErrCacheMiss) {
		return err
	}
	buf, err := c.loadAndStore(ctx, key, ttl, do)
	if err != nil {
		return err
	}
	return decode(key, dest, buf)
}

func (c *Client) loadAndStore(ctx context.Context, key string, ttl time.Duration, do func(context.Context) (any, error)) ([]byte, error) {
	full, err := c.prepare(ctx, key)
	if err != nil {
		return nil, err
	}
	if ttl < 0 {
		return nil, ErrInvalidTTL
	}
	if do == nil {
		return nil, errors.New("xredis: do function is nil")
	}

	base := context.WithoutCancel(ctx)
	ch := c.group.DoChan(full, func() (any, error) {
		raw, err := c.cli.Get(base, full).Bytes()
		if err == nil {
			return raw, nil
		}
		if !errors.Is(err, redis.Nil) {
			return nil, err
		}
		val, err := callDo(do, base)
		if err != nil {
			return nil, err
		}
		buf, err := json.Marshal(val)
		if err != nil {
			return nil, fmt.Errorf("xredis: marshal %s: %w", key, err)
		}
		if err = c.cli.Set(base, full, buf, ttl).Err(); err != nil {
			return nil, fmt.Errorf("xredis: set %s: %w", key, err)
		}
		return buf, nil
	})

	select {
	case res := <-ch:
		return flightBytes(res)
	case <-ctx.Done():
	}
	select {
	case res := <-ch:
		return flightBytes(res)
	default:
		return nil, ctx.Err()
	}
}

func flightBytes(res singleflight.Result) ([]byte, error) {
	if res.Err != nil {
		return nil, res.Err
	}
	buf, ok := res.Val.([]byte)
	if !ok {
		return nil, errors.New("xredis: empty cache value")
	}
	return buf, nil
}

func callDo(do func(context.Context) (any, error), ctx context.Context) (val any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("xredis: do panicked: %v", r)
		}
	}()
	return do(ctx)
}

func decode(key string, dest any, buf []byte) error {
	resetDest(dest)
	if err := json.Unmarshal(buf, dest); err != nil {
		return fmt.Errorf("xredis: unmarshal %s: %w", key, err)
	}
	return nil
}

// resetDest 先清掉旧值，避免复用同一个结构体时残留上次的字段。
func resetDest(dest any) {
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return
	}
	elem := v.Elem()
	elem.Set(reflect.Zero(elem.Type()))
}

func checkDest(dest any) error {
	if dest == nil {
		return errors.New("xredis: dest is nil")
	}
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return errors.New("xredis: dest must be a non-nil pointer")
	}
	return nil
}
