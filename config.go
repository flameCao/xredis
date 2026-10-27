package xredis

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config 是交给 New 的全部连接参数。
// 只填 Addr 就是单机；Addrs 填多个是集群；MasterName 有值就是哨兵。
// 超时、连接池不填会使用默认值，业务启动时通常只需要地址、密码、库号和前缀。
//
// 嵌进项目配置即可，例如：
//
//	type AppConfig struct {
//		Redis xredis.Config `json:"redis" yaml:"redis" mapstructure:"redis"`
//	}
type Config struct {
	// Addr 单机地址，格式 host:port。也支持 redis:// 或 rediss:// 连接串。
	Addr string `json:"addr" yaml:"addr" mapstructure:"addr"`
	// Addrs 集群节点或哨兵地址。和 Addr 二选一。
	Addrs []string `json:"addrs" yaml:"addrs" mapstructure:"addrs"`
	// Cluster 只有一个配置地址、但实际是集群时打开（例如云厂商的配置端点）。
	Cluster bool `json:"cluster" yaml:"cluster" mapstructure:"cluster"`
	// MasterName 哨兵监控的主节点名字。填写后按哨兵模式连接。
	MasterName string `json:"master_name" yaml:"master_name" mapstructure:"master_name"`
	// SentinelUsername 哨兵 ACL 用户名。
	SentinelUsername string `json:"sentinel_username" yaml:"sentinel_username" mapstructure:"sentinel_username"`
	// SentinelPassword 哨兵密码。
	SentinelPassword string `json:"sentinel_password" yaml:"sentinel_password" mapstructure:"sentinel_password"`

	Username string `json:"username" yaml:"username" mapstructure:"username"`
	Password string `json:"password" yaml:"password" mapstructure:"password"`
	// DB 逻辑库。集群模式会忽略这个字段。
	DB int `json:"db" yaml:"db" mapstructure:"db"`

	// KeyPrefix 自动加在所有业务 key 前面，用来隔开项目。不带冒号也会补上。
	KeyPrefix string `json:"key_prefix" yaml:"key_prefix" mapstructure:"key_prefix"`
	// ClientName 显示在 Redis CLIENT LIST 里，默认 xredis。
	ClientName string `json:"client_name" yaml:"client_name" mapstructure:"client_name"`

	// PoolSize 连接池大小，默认 20。
	PoolSize int `json:"pool_size" yaml:"pool_size" mapstructure:"pool_size"`
	// MinIdleConns 最小空闲连接，默认 5，且不会超过 PoolSize。
	MinIdleConns int `json:"min_idle_conns" yaml:"min_idle_conns" mapstructure:"min_idle_conns"`
	// MaxRetries 失败重试次数。0 表示沿用 go-redis 的默认值（3 次）。设为 -1 表示不重试。
	MaxRetries int `json:"max_retries" yaml:"max_retries" mapstructure:"max_retries"`

	// DialTimeout 建立连接超时，默认 5 秒。
	DialTimeout time.Duration `json:"dial_timeout" yaml:"dial_timeout" mapstructure:"dial_timeout"`
	// ReadTimeout 读超时，默认 3 秒。
	ReadTimeout time.Duration `json:"read_timeout" yaml:"read_timeout" mapstructure:"read_timeout"`
	// WriteTimeout 写超时，默认 3 秒。
	WriteTimeout time.Duration `json:"write_timeout" yaml:"write_timeout" mapstructure:"write_timeout"`
	// PingTimeout 启动时 Ping 的超时，默认 3 秒。连不上会让 New 失败。
	PingTimeout time.Duration `json:"ping_timeout" yaml:"ping_timeout" mapstructure:"ping_timeout"`

	// Logger 可选。不填时告警写到标准日志。这个字段不参与配置文件反序列化。
	Logger Logger `json:"-" yaml:"-" mapstructure:"-"`
}

func (cfg Config) withDefaults() Config {
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 3 * time.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 3 * time.Second
	}
	if cfg.PingTimeout <= 0 {
		cfg.PingTimeout = 3 * time.Second
	}
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = 20
	}
	if cfg.MinIdleConns <= 0 {
		cfg.MinIdleConns = 5
	}
	if cfg.MinIdleConns > cfg.PoolSize {
		cfg.MinIdleConns = cfg.PoolSize
	}
	if strings.TrimSpace(cfg.ClientName) == "" {
		cfg.ClientName = "xredis"
	}
	cfg.Addr = strings.TrimSpace(cfg.Addr)
	cfg.MasterName = strings.TrimSpace(cfg.MasterName)
	cfg.KeyPrefix = strings.TrimSpace(cfg.KeyPrefix)
	return cfg
}

func (cfg Config) validate() error {
	if cfg.DB < 0 {
		return errors.New("xredis: db must be greater than or equal to 0")
	}
	if cfg.Addr != "" && len(cfg.Addrs) > 0 {
		return errors.New("xredis: set only one of addr and addrs")
	}
	if cfg.MasterName != "" && cfg.Cluster {
		return errors.New("xredis: master_name and cluster cannot both be set")
	}
	if cfg.Addr == "" && len(cfg.addrs()) == 0 {
		return errors.New("xredis: addr is required")
	}
	return nil
}

func (cfg Config) addrs() []string {
	out := make([]string, 0, len(cfg.Addrs))
	for _, addr := range cfg.Addrs {
		addr = strings.TrimSpace(addr)
		if addr != "" {
			out = append(out, addr)
		}
	}
	return out
}

func (cfg Config) newRedis() (redis.UniversalClient, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if strings.Contains(cfg.Addr, "://") {
		opt, err := redis.ParseURL(cfg.Addr)
		if err != nil {
			return nil, fmt.Errorf("xredis: parse addr: %w", err)
		}
		applyStandalone(opt, cfg)
		return redis.NewClient(opt), nil
	}

	addrs := cfg.addrs()
	if cfg.Addr != "" {
		addrs = []string{cfg.Addr}
	}
	return redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:                 addrs,
		ClientName:            cfg.ClientName,
		DB:                    cfg.DB,
		Username:              cfg.Username,
		Password:              cfg.Password,
		SentinelUsername:      cfg.SentinelUsername,
		SentinelPassword:      cfg.SentinelPassword,
		MasterName:            cfg.MasterName,
		IsClusterMode:         cfg.Cluster,
		DialTimeout:           cfg.DialTimeout,
		ReadTimeout:           cfg.ReadTimeout,
		WriteTimeout:          cfg.WriteTimeout,
		PoolSize:              cfg.PoolSize,
		MinIdleConns:          cfg.MinIdleConns,
		MaxRetries:            cfg.MaxRetries,
		ContextTimeoutEnabled: true,
	}), nil
}

func applyStandalone(opt *redis.Options, cfg Config) {
	opt.DialTimeout = cfg.DialTimeout
	opt.ReadTimeout = cfg.ReadTimeout
	opt.WriteTimeout = cfg.WriteTimeout
	opt.PoolSize = cfg.PoolSize
	opt.MinIdleConns = cfg.MinIdleConns
	opt.MaxRetries = cfg.MaxRetries
	opt.ClientName = cfg.ClientName
	opt.ContextTimeoutEnabled = true
	if cfg.Username != "" {
		opt.Username = cfg.Username
	}
	if cfg.Password != "" {
		opt.Password = cfg.Password
	}
	if cfg.DB != 0 {
		opt.DB = cfg.DB
	}
}
