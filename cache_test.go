package xredis

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type cacheUser struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func TestCacheRoundTrip(t *testing.T) {
	c, mr := newMini(t, "app")
	ctx := context.Background()
	want := cacheUser{ID: 7, Name: "ann"}

	if err := c.Set(ctx, Key("user", "7"), want, time.Minute); err != nil {
		t.Fatal(err)
	}
	if !mr.Exists("app:user:7") {
		t.Fatal("prefixed key was not written")
	}

	var got cacheUser
	if err := c.Get(ctx, "user:7", &got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("got %+v", got)
	}

	n, err := c.Del(ctx, "user:7")
	if err != nil || n != 1 {
		t.Fatalf("del n=%d err=%v", n, err)
	}
	err = c.Get(ctx, "user:7", &got)
	if !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("miss err=%v", err)
	}
}

func TestOnceSingleflight(t *testing.T) {
	c, _ := newMini(t, "app")
	ctx := context.Background()
	var calls atomic.Int32
	start := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			var name string
			err := c.Once(ctx, "profile", &name, time.Minute, func(context.Context) (any, error) {
				calls.Add(1)
				time.Sleep(80 * time.Millisecond)
				return "ann", nil
			})
			if err != nil {
				t.Errorf("once: %v", err)
				return
			}
			if name != "ann" {
				t.Errorf("name=%s", name)
			}
		}()
	}
	close(start)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("load calls=%d", calls.Load())
	}

	var cached string
	err := c.Once(ctx, "profile", &cached, time.Minute, func(context.Context) (any, error) {
		calls.Add(1)
		return "other", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if cached != "ann" || calls.Load() != 1 {
		t.Fatalf("cached=%s calls=%d", cached, calls.Load())
	}
}

func TestOnceFillsDest(t *testing.T) {
	c, _ := newMini(t, "")
	ctx := context.Background()
	var got cacheUser
	err := c.Once(ctx, "user:1", &got, time.Minute, func(context.Context) (any, error) {
		return cacheUser{ID: 1, Name: "ann"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "ann" || got.ID != 1 {
		t.Fatalf("%+v", got)
	}
}

func TestStringAndCounter(t *testing.T) {
	c, _ := newMini(t, "app")
	ctx := context.Background()
	if err := c.Set(ctx, "sms:1", "9527", time.Minute); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := c.Get(ctx, "sms:1", &got); err != nil || got != "9527" {
		t.Fatalf("got=%s err=%v", got, err)
	}
	n, err := c.Incr(ctx, "visit")
	if err != nil || n != 1 {
		t.Fatalf("incr=%d err=%v", n, err)
	}
	n, err = c.IncrBy(ctx, "visit", 4)
	if err != nil || n != 5 {
		t.Fatalf("incrby=%d err=%v", n, err)
	}
}

func TestCacheRejectsBadInput(t *testing.T) {
	c, _ := newMini(t, "")
	ctx := context.Background()
	if err := c.Set(ctx, "", "a", time.Second); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("empty key: %v", err)
	}
	if err := c.Set(ctx, "a", "b", -time.Second); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("ttl: %v", err)
	}
	if err := c.Get(ctx, "a", nil); err == nil {
		t.Fatal("nil dest")
	}
	if err := c.Get(ctx, "a", cacheUser{}); err == nil {
		t.Fatal("non-pointer dest")
	}
	var nilClient *Client
	if err := nilClient.Set(ctx, "a", "b", time.Second); !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("nil client: %v", err)
	}
}

func TestGetClearsStaleFields(t *testing.T) {
	c, _ := newMini(t, "")
	ctx := context.Background()
	type profile struct {
		ID   int    `json:"id"`
		Name string `json:"name,omitempty"`
	}
	if err := c.Set(ctx, "p", profile{ID: 2}, time.Minute); err != nil {
		t.Fatal(err)
	}
	got := profile{ID: 1, Name: "old"}
	if err := c.Get(ctx, "p", &got); err != nil {
		t.Fatal(err)
	}
	if got.ID != 2 || got.Name != "" {
		t.Fatalf("%+v", got)
	}
}

func TestDelEachKey(t *testing.T) {
	c, _ := newMini(t, "app")
	ctx := context.Background()
	if err := c.Set(ctx, "a", "1", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := c.Set(ctx, "b", "2", time.Minute); err != nil {
		t.Fatal(err)
	}
	n, err := c.Del(ctx, "a", "b", "missing")
	if err != nil || n != 2 {
		t.Fatalf("n=%d err=%v", n, err)
	}
}

func TestOncePanicDoesNotCrash(t *testing.T) {
	c, _ := newMini(t, "")
	var got string
	err := c.Once(context.Background(), "k", &got, time.Minute, func(context.Context) (any, error) {
		panic("bad loader")
	})
	if err == nil || got != "" {
		t.Fatalf("err=%v got=%q", err, got)
	}
}

func TestConfigJSON(t *testing.T) {
	raw := []byte(`{"addr":"127.0.0.1:6379","password":"p","db":2,"key_prefix":"app"}`)
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != "127.0.0.1:6379" || cfg.Password != "p" || cfg.DB != 2 || cfg.KeyPrefix != "app" {
		t.Fatalf("%+v", cfg)
	}
}

func TestNewRejectsConfig(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("empty config")
	}
	if _, err := New(Config{Addr: "127.0.0.1:6379", Addrs: []string{"127.0.0.1:6380"}}); err == nil {
		t.Fatal("both addr and addrs")
	}
	if _, err := New(Config{DB: -1, Addr: "127.0.0.1:6379"}); err == nil {
		t.Fatal("negative db")
	}

	mr := newMiniServer(t)
	addr := mr.Addr()
	mr.Close()
	_, err := New(Config{
		Addr:         addr,
		DialTimeout:  200 * time.Millisecond,
		ReadTimeout:  200 * time.Millisecond,
		WriteTimeout: 200 * time.Millisecond,
		PingTimeout:  time.Second,
	})
	if err == nil {
		t.Fatal("closed redis should fail")
	}
}
