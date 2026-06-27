package swvfsdaemon

import (
	"fmt"
	"os"
	"strconv"
	"sync"

	"seaweedfs-rdma-sidecar/pkg/swvfsproto"
)

const (
	defaultKernelMRPoolMaxIdle  = 64
	defaultKernelMRPoolMaxBytes = 64 << 20
	defaultKernelMRPoolMinBytes = 256 << 10
)

type KernelMRPoolConfig struct {
	MaxIdle  int
	MaxBytes uint64
	MinBytes uint32
}

type KernelMRPoolSession struct {
	SessionID uint64
	Capacity  uint32
}

type KernelMRPool struct {
	Control RDMATestMRControl
	Stats   *Stats

	maxIdle  int
	maxBytes uint64
	minBytes uint32

	mu        sync.Mutex
	idle      []KernelMRPoolSession
	active    map[uint64]KernelMRPoolSession
	idleBytes uint64
}

func NewKernelMRPoolFromEnv(control RDMATestMRControl, stats *Stats) *KernelMRPool {
	cfg := KernelMRPoolConfig{
		MaxIdle:  envInt("SWVFS_RDMA_MR_POOL_MAX_IDLE", defaultKernelMRPoolMaxIdle),
		MaxBytes: envUint64("SWVFS_RDMA_MR_POOL_MAX_BYTES", defaultKernelMRPoolMaxBytes),
		MinBytes: uint32(envUint64("SWVFS_RDMA_MR_POOL_MIN_BYTES", defaultKernelMRPoolMinBytes)),
	}
	return NewKernelMRPool(control, stats, cfg)
}

func NewKernelMRPool(control RDMATestMRControl, stats *Stats, cfg KernelMRPoolConfig) *KernelMRPool {
	if control == nil {
		return nil
	}
	if cfg.MaxIdle < 0 {
		cfg.MaxIdle = 0
	}
	if cfg.MaxBytes == 0 {
		cfg.MaxIdle = 0
	}
	if cfg.MinBytes == 0 {
		cfg.MinBytes = defaultKernelMRPoolMinBytes
	}
	if cfg.MinBytes > swvfsproto.RDMAIOMax {
		cfg.MinBytes = swvfsproto.RDMAIOMax
	}
	return &KernelMRPool{
		Control:  control,
		Stats:    stats,
		maxIdle:  cfg.MaxIdle,
		maxBytes: cfg.MaxBytes,
		minBytes: cfg.MinBytes,
		active:   make(map[uint64]KernelMRPoolSession),
	}
}

func (p *KernelMRPool) Acquire(length uint32) (KernelMRPoolSession, error) {
	if p == nil || p.Control == nil {
		return KernelMRPoolSession{}, ErrnoError{Errno: ErrnoNoSys, Msg: "kernel MR pool is not configured"}
	}
	if length == 0 || length > swvfsproto.RDMAIOMax {
		return KernelMRPoolSession{}, fmt.Errorf("invalid kernel MR pool length %d", length)
	}
	capacity := p.roundCapacity(length)
	p.Stats.Inc("rdma_mr_pool_acquire_requests")
	p.Stats.Add("rdma_mr_pool_acquire_bytes", uint64(length))

	p.mu.Lock()
	best := -1
	for i, session := range p.idle {
		if session.Capacity < length {
			continue
		}
		if best < 0 || session.Capacity < p.idle[best].Capacity {
			best = i
		}
	}
	if best >= 0 {
		session := p.idle[best]
		p.idle = append(p.idle[:best], p.idle[best+1:]...)
		p.idleBytes -= uint64(session.Capacity)
		p.active[session.SessionID] = session
		p.mu.Unlock()
		p.Stats.Inc("rdma_mr_pool_hits")
		p.Stats.Add("rdma_mr_pool_hit_bytes", uint64(session.Capacity))
		return session, nil
	}
	p.mu.Unlock()

	mr, err := p.Control.TestMRAlloc(capacity, 0)
	if err != nil {
		p.Stats.Inc("rdma_mr_pool_alloc_errors")
		return KernelMRPoolSession{}, err
	}
	if mr.SessionID == 0 {
		p.Stats.Inc("rdma_mr_pool_alloc_errors")
		return KernelMRPoolSession{}, fmt.Errorf("kernel MR allocation returned no session id")
	}
	session := KernelMRPoolSession{SessionID: mr.SessionID, Capacity: mr.Length}
	if session.Capacity == 0 {
		session.Capacity = capacity
	}
	if session.Capacity < length {
		_ = p.Control.TestMRFree(session.SessionID)
		p.Stats.Inc("rdma_mr_pool_alloc_short")
		return KernelMRPoolSession{}, fmt.Errorf("kernel MR allocation length %d is smaller than requested %d", session.Capacity, length)
	}
	p.mu.Lock()
	p.active[session.SessionID] = session
	p.mu.Unlock()
	p.Stats.Inc("rdma_mr_pool_misses")
	p.Stats.Add("rdma_mr_pool_alloc_bytes", uint64(session.Capacity))
	return session, nil
}

func (p *KernelMRPool) Release(sessionID uint64) error {
	if p == nil || sessionID == 0 {
		return nil
	}
	p.mu.Lock()
	session, ok := p.active[sessionID]
	if ok {
		delete(p.active, sessionID)
	}
	if !ok {
		p.mu.Unlock()
		p.Stats.Inc("rdma_mr_pool_release_unknown")
		return nil
	}
	if p.maxIdle > 0 &&
		len(p.idle) < p.maxIdle &&
		p.idleBytes+uint64(session.Capacity) <= p.maxBytes {
		p.idle = append(p.idle, session)
		p.idleBytes += uint64(session.Capacity)
		p.mu.Unlock()
		p.Stats.Inc("rdma_mr_pool_reused")
		p.Stats.Add("rdma_mr_pool_reused_bytes", uint64(session.Capacity))
		return nil
	}
	p.mu.Unlock()
	p.Stats.Inc("rdma_mr_pool_release_free")
	return p.Control.TestMRFree(sessionID)
}

func (p *KernelMRPool) Discard(sessionID uint64) error {
	if p == nil || sessionID == 0 {
		return nil
	}
	p.mu.Lock()
	if _, ok := p.active[sessionID]; ok {
		delete(p.active, sessionID)
		p.mu.Unlock()
		p.Stats.Inc("rdma_mr_pool_discard_active")
		return p.Control.TestMRFree(sessionID)
	}
	for i, session := range p.idle {
		if session.SessionID != sessionID {
			continue
		}
		p.idle = append(p.idle[:i], p.idle[i+1:]...)
		p.idleBytes -= uint64(session.Capacity)
		p.mu.Unlock()
		p.Stats.Inc("rdma_mr_pool_discard_idle")
		return p.Control.TestMRFree(sessionID)
	}
	p.mu.Unlock()
	p.Stats.Inc("rdma_mr_pool_discard_unknown")
	return nil
}

func (p *KernelMRPool) Close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	sessions := make([]KernelMRPoolSession, 0, len(p.idle)+len(p.active))
	sessions = append(sessions, p.idle...)
	for _, session := range p.active {
		sessions = append(sessions, session)
	}
	p.idle = nil
	p.active = make(map[uint64]KernelMRPoolSession)
	p.idleBytes = 0
	p.mu.Unlock()
	for _, session := range sessions {
		_ = p.Control.TestMRFree(session.SessionID)
	}
}

func (p *KernelMRPool) roundCapacity(length uint32) uint32 {
	capacity := p.minBytes
	if capacity == 0 {
		capacity = defaultKernelMRPoolMinBytes
	}
	for capacity < length && capacity < swvfsproto.RDMAIOMax {
		capacity <<= 1
	}
	if capacity < length {
		capacity = length
	}
	if capacity > swvfsproto.RDMAIOMax {
		capacity = swvfsproto.RDMAIOMax
	}
	return capacity
}

func envInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return value
}

func envUint64(name string, fallback uint64) uint64 {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return fallback
	}
	return value
}
