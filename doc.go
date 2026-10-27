// Package xredis 提供开箱即用的 Redis 缓存和分布式锁。
//
// 用法对齐 go-redis：New 得到客户端，之后只调用这个实例的方法。
//
//	rdb, err := xredis.New(xredis.Config{
//		Addr:      "127.0.0.1:6379",
//		Password:  "secret",
//		DB:        0,
//		KeyPrefix: "myapp",
//	})
//	if err != nil {
//		panic(err)
//	}
//	defer rdb.Close()
//
// 配置可以直接嵌进项目自己的配置结构，用 json、yaml 或 viper 反序列化后交给 New。
package xredis
