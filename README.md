# xredis

开箱即用的 Redis 缓存和分布式锁。`New` 得到一个客户端，业务代码只调用这个实例的方法。

缓存方法对齐 [go-redis](https://github.com/redis/go-redis) 和 [go-redis/cache](https://github.com/go-redis/cache) 的 `Once`。分布式锁对齐 [redislock](https://github.com/bsm/redislock) 的 `Obtain` / `Release`。

## 配置怎么引进来

把 `xredis.Config` 嵌进项目自己的配置，随 json、yaml 或 viper 一起加载：

```yaml
redis:
  addr: 127.0.0.1:6379
  password: ""
  db: 0
  key_prefix: myapp
```

```go
type AppConfig struct {
    Redis xredis.Config `yaml:"redis" mapstructure:"redis"`
}

func main() {
    cfg := loadAppConfig()
    rdb, err := xredis.New(cfg.Redis)
    if err != nil {
        panic(err)
    }
    defer rdb.Close()

    r := gin.Default()
    r.Run(":8080")
}
```

不走配置文件时，直接填结构体：

```go
rdb, err := xredis.New(xredis.Config{
    Addr:      "127.0.0.1:6379",
    Password:  "secret",
    DB:        0,
    KeyPrefix: "myapp",
})
```

常用字段只有这些：

| 字段 | 作用 |
| --- | --- |
| `Addr` | 单机地址 `host:port`，也支持 `redis://`、`rediss://` |
| `Addrs` | 多个地址。不填 `MasterName` 时按集群连接 |
| `MasterName` | 哨兵主节点名，填写后按哨兵连接 |
| `Password` / `Username` | 认证 |
| `DB` | 逻辑库，集群模式会忽略 |
| `KeyPrefix` | 自动加到所有业务 key 前，用来隔开项目 |

超时和连接池有默认值（连接 5 秒、读写 3 秒、池大小 20）。`New` 会 Ping，连不上直接返回错误。

项目里已经有 go-redis 客户端时：

```go
rdb, err := xredis.NewFromClient(existingClient, "myapp")
```

要同时连两台 Redis，就 `New` 两次，各自保存一个 `*xredis.Client`。

锁续期失败会打一条告警。要接到项目自己的日志时，设置 `Config.Logger`，或在开始处理请求前调用 `rdb.SetLogger`。

## 缓存

结构体字段需要可导出，才会写进 JSON。字符串也用 `Set` / `Get`，会按 JSON 读写。

```go
var user User
err := rdb.Once(ctx, xredis.Key("user", userID), &user, 10*time.Minute, func(ctx context.Context) (any, error) {
    return userRepo.Find(ctx, userID)
})
```

`Once` 先读缓存，没有再回源并写入。同一个 key 同时打进来，回源函数只会执行一次。

直接读写：

```go
if err := rdb.Set(ctx, xredis.Key("user", userID), user, 10*time.Minute); err != nil {
    return err
}

var user User
err := rdb.Get(ctx, xredis.Key("user", userID), &user)
if errors.Is(err, xredis.ErrCacheMiss) {
    // 自己回源
}
```

验证码、token：

```go
_ = rdb.Set(ctx, "sms:"+phone, code, 5*time.Minute)
var code string
err := rdb.Get(ctx, "sms:"+phone, &code)
```

防重复提交用 `SetNX`，时间窗口内只有第一次返回 true。计数用 `Incr`，这个 key 不要再 `Set` 成 JSON。

## 分布式锁

```go
err := rdb.WithLock(ctx, xredis.Key("order", orderID), 10*time.Second, &xredis.LockOptions{
    AutoRefresh: true,
}, func() error {
    return createOrder(ctx, orderID)
})
if errors.Is(err, xredis.ErrNotObtained) {
    return errors.New("please retry later")
}
```

`AutoRefresh` 会在业务没跑完时自动续期，函数返回后自动解锁。解锁带 token 校验，不会删掉别人的锁。锁和缓存不要使用同一个 key。

需要自己控制释放时，用法和 redislock 相同：

```go
lock, err := rdb.Obtain(ctx, xredis.Key("order", orderID), 10*time.Second, &xredis.LockOptions{
    AutoRefresh: true,
})
if errors.Is(err, xredis.ErrNotObtained) {
    return errors.New("please retry later")
}
if err != nil {
    return err
}
defer lock.Release(ctx)
```

在一段时间内重试：给 context 加截止时间，并传入 `LinearBackoff`。

```go
ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
defer cancel()

err := rdb.WithLock(ctx, xredis.Key("stock", sku), 10*time.Second, &xredis.LockOptions{
    AutoRefresh:   true,
    RetryStrategy: xredis.LinearBackoff(100 * time.Millisecond),
}, func() error {
    return deductStock(ctx, sku)
})
```

只重试固定次数时用 `xredis.LimitRetry(xredis.LinearBackoff(100*time.Millisecond), 5)`。不传 `RetryStrategy` 就只抢一次。context 没有截止时间时，最多等待一个租约时长，避免重试停不下来。同一个 `LimitRetry` 不要同时交给多个 `Obtain`。

## 业务侧不用关心的事

- 连接池、超时、启动探测
- key 前缀
- JSON 序列化
- 缓存击穿时的并发回源
- 锁的 token、Lua 释放和自动续期

底层命令不够用时再拿 `rdb.Raw()`。常规缓存和锁走上面的方法。
