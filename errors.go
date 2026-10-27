package xredis

import "errors"

var (
	// ErrNotInitialized 客户端是 nil，或还没有通过 New 创建。
	ErrNotInitialized = errors.New("xredis: not initialized")
	// ErrCacheMiss key 不存在。业务可用 errors.Is 判断后回源。
	ErrCacheMiss = errors.New("xredis: cache miss")
	// ErrEmptyKey 业务 key 为空。
	ErrEmptyKey = errors.New("xredis: key is empty")
	// ErrInvalidTTL ttl 不合法。Set 允许 0（永不过期），Expire 和 Obtain 必须大于 0。
	ErrInvalidTTL = errors.New("xredis: invalid ttl")
	// ErrNotObtained 没有抢到锁。对齐 redislock.ErrNotObtained。
	ErrNotObtained = errors.New("xredis: lock not obtained")
	// ErrLockNotHeld 续期或释放时锁已经不是自己的，不会删除别人的锁。
	ErrLockNotHeld = errors.New("xredis: lock not held")
)
