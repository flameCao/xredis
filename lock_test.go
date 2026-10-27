package xredis

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLockMutualExclusion(t *testing.T) {
	c, mr := newMini(t, "lk")
	ctx := context.Background()

	lock, err := c.Obtain(ctx, Key("order", "8"), 30*time.Second, &LockOptions{AutoRefresh: true})
	if err != nil {
		t.Fatal(err)
	}
	if lock.Token() == "" || lock.Key() != "lk:order:8" {
		t.Fatalf("key=%s token=%s", lock.Key(), lock.Token())
	}
	if !mr.Exists("lk:order:8") {
		t.Fatal("lock key missing")
	}

	ran := false
	err = c.WithLock(ctx, "order:8", 30*time.Second, nil, func() error {
		ran = true
		return nil
	})
	if !errors.Is(err, ErrNotObtained) || ran {
		t.Fatalf("second lock ran=%v err=%v", ran, err)
	}

	if err = lock.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if mr.Exists("lk:order:8") {
		t.Fatal("lock was not released")
	}
	if err = lock.Release(ctx); !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("second release: %v", err)
	}

	err = c.WithLock(ctx, "order:8", 30*time.Second, nil, func() error {
		ran = true
		return errors.New("biz failed")
	})
	if !ran || err == nil || err.Error() != "biz failed" {
		t.Fatalf("with lock ran=%v err=%v", ran, err)
	}
	if mr.Exists("lk:order:8") {
		t.Fatal("lock leaked after business error")
	}
}

func TestLockDoesNotDeleteForeignToken(t *testing.T) {
	c, mr := newMini(t, "lk")
	ctx := context.Background()
	lock, err := c.Obtain(ctx, "job", 30*time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	mr.Set("lk:job", "someone-else")
	if err = lock.Release(ctx); !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("release err=%v", err)
	}
	if !mr.Exists("lk:job") {
		t.Fatal("foreign lock was deleted")
	}
	got, err := mr.Get("lk:job")
	if err != nil || got != "someone-else" {
		t.Fatalf("value=%s err=%v", got, err)
	}
}

func TestAutoRefreshKeepsLock(t *testing.T) {
	c, mr := newMini(t, "lk")
	ctx := context.Background()
	lock, err := c.Obtain(ctx, "job", 300*time.Millisecond, &LockOptions{
		AutoRefresh:     true,
		RefreshInterval: 80 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release(context.Background())

	time.Sleep(200 * time.Millisecond)
	mr.FastForward(250 * time.Millisecond)
	if !mr.Exists("lk:job") {
		t.Fatal("auto refresh did not renew the lock")
	}
}

func TestWithLockReleasesOnPanic(t *testing.T) {
	c, mr := newMini(t, "lk")
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
		if mr.Exists("lk:job") {
			t.Fatal("lock not released after panic")
		}
	}()
	_ = c.WithLock(context.Background(), "job", 30*time.Second, nil, func() error {
		panic("boom")
	})
}

func TestSetNX(t *testing.T) {
	c, _ := newMini(t, "")
	ctx := context.Background()
	ok, err := c.SetNX(ctx, "submit:1", "1", time.Second)
	if err != nil || !ok {
		t.Fatalf("first ok=%v err=%v", ok, err)
	}
	ok, err = c.SetNX(ctx, "submit:1", "1", time.Second)
	if err != nil || ok {
		t.Fatalf("second ok=%v err=%v", ok, err)
	}
	if _, err = c.Obtain(ctx, "job", 0, nil); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("zero ttl: %v", err)
	}
}

func TestLimitRetryStops(t *testing.T) {
	c, _ := newMini(t, "lk")
	ctx := context.Background()
	if _, err := c.Obtain(ctx, "job", 30*time.Second, nil); err != nil {
		t.Fatal(err)
	}
	_, err := c.Obtain(ctx, "job", 30*time.Second, &LockOptions{
		RetryStrategy: LimitRetry(LinearBackoff(20*time.Millisecond), 2),
	})
	if !errors.Is(err, ErrNotObtained) {
		t.Fatalf("err=%v", err)
	}
}
