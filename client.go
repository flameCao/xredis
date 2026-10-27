package xredis

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// Logger 接收组件内部告警，例如锁续期失败。可用 zap、slog 包一层接进来。
type Logger interface {
	Warnf(format string, args ...any)
}

type stdLogger struct{}

func (stdLogger) Warnf(format string, args ...any) {
	log.Printf(format, args...)
}

// noCopy 阻止复制 Client。复制后 singleflight 和 Close 都会出错，go vet 能检查到。
type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// Client 是缓存和分布式锁的入口。用法和 go-redis 一样，New 之后保存这个实例。
type Client struct {
	noCopy    noCopy
	cli       redis.UniversalClient
	prefix    string
	owned     bool
	logger    atomic.Value // Logger
	group     singleflight.Group
	closeOnce sync.Once
	closeErr  error
}

// New 创建客户端并完成连通性检查。一个进程可以创建多个，用来连接不同的 Redis。
func New(cfg Config) (*Client, error) {
	cfg = cfg.withDefaults()
	cli, err := cfg.newRedis()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.PingTimeout)
	defer cancel()
	if err = cli.Ping(ctx).Err(); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("xredis: connect redis: %w", err)
	}
	c := &Client{
		cli:    cli,
		prefix: normalizePrefix(cfg.KeyPrefix),
		owned:  true,
	}
	c.SetLogger(cfg.Logger)
	return c, nil
}

// NewFromClient 复用已经创建好的 go-redis 客户端。
// 连接不是这里创建的，Close 不会关掉它。
func NewFromClient(cli redis.UniversalClient, keyPrefix string) (*Client, error) {
	if cli == nil {
		return nil, errors.New("xredis: redis client is nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cli.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("xredis: connect redis: %w", err)
	}
	c := &Client{
		cli:    cli,
		prefix: normalizePrefix(keyPrefix),
		owned:  false,
	}
	c.SetLogger(nil)
	return c, nil
}

// SetLogger 替换告警日志。传 nil 时回到标准日志。请在开始处理业务前调用。
func (c *Client) SetLogger(logger Logger) {
	if c == nil {
		return
	}
	if logger == nil {
		logger = stdLogger{}
	}
	c.logger.Store(logger)
}

// Close 关闭由 New 创建的连接。重复调用是安全的。
func (c *Client) Close() error {
	if c == nil || !c.owned || c.cli == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.closeErr = c.cli.Close()
	})
	return c.closeErr
}

// Raw 返回底层 go-redis 客户端，只给这里没有覆盖的命令使用。
func (c *Client) Raw() redis.UniversalClient {
	if c == nil {
		return nil
	}
	return c.cli
}

// FullKey 返回加上前缀后的真实 key，排查数据时用。
func (c *Client) FullKey(key string) (string, error) {
	return c.fullKey(key)
}

// Key 用冒号拼接业务 key，不包含全局前缀。
func Key(parts ...string) string {
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return strings.Join(out, ":")
}

func normalizePrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return ""
	}
	if !strings.HasSuffix(prefix, ":") {
		prefix += ":"
	}
	return prefix
}

func (c *Client) fullKey(key string) (string, error) {
	if c == nil || c.cli == nil {
		return "", ErrNotInitialized
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return "", ErrEmptyKey
	}
	return c.prefix + key, nil
}

func (c *Client) warnf(format string, args ...any) {
	c.currentLogger().Warnf(format, args...)
}

func (c *Client) currentLogger() Logger {
	if c != nil {
		if lg, ok := c.logger.Load().(Logger); ok && lg != nil {
			return lg
		}
	}
	return stdLogger{}
}

func (c *Client) prepare(ctx context.Context, key string) (string, error) {
	full, err := c.fullKey(key)
	if err != nil {
		return "", err
	}
	if ctx == nil {
		return "", errors.New("xredis: context is nil")
	}
	if err = ctx.Err(); err != nil {
		return "", err
	}
	return full, nil
}
