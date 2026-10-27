package xredis

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func newMini(t *testing.T, prefix string) (*Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	c, err := New(Config{
		Addr:         mr.Addr(),
		KeyPrefix:    prefix,
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		PingTimeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, mr
}

func newMiniServer(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	return miniredis.RunT(t)
}
