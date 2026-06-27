package swvfsdaemon

import "testing"

func TestDeviceBufferPoolReusesSizedBuffers(t *testing.T) {
	pool := NewDeviceBufferPool(1024)
	first, pooled := pool.Get(512)
	if pooled {
		t.Fatal("first allocation should be a miss")
	}
	if len(first) != 1024 {
		t.Fatalf("first len = %d, want pooled size", len(first))
	}
	first[0] = 7
	pool.Put(first)

	second, pooled := pool.Get(512)
	if !pooled {
		t.Fatal("second allocation should reuse the pool")
	}
	if len(second) != 512 {
		t.Fatalf("second len = %d, want requested size", len(second))
	}
	if cap(second) < 1024 {
		t.Fatalf("second cap = %d, want at least pooled size", cap(second))
	}
}

func TestDeviceBufferPoolBypassesOversizedRequests(t *testing.T) {
	pool := NewDeviceBufferPool(1024)
	buf, pooled := pool.Get(2048)
	if pooled {
		t.Fatal("oversized allocation should bypass the pool")
	}
	if len(buf) != 2048 {
		t.Fatalf("oversized len = %d, want requested size", len(buf))
	}
}
