package weed_server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

var (
	ErrVolumeRdmaReadNotConfigured = errors.New("native RDMA read exporter is not configured")
	ErrVolumeRdmaReadTooLarge      = errors.New("native RDMA read request is too large")
	ErrVolumeRdmaReadNotExportable = errors.New("native RDMA read buffer is not exportable")
)

const (
	defaultVolumeRdmaReadMaxSize    = 4 << 20
	defaultVolumeRdmaReadLeaseTTL   = 30 * time.Second
	defaultVolumeRdmaReadBufferSize = 1 << 20
)

type VolumeRdmaReadRegistrar interface {
	RegisterReadBuffer(context.Context, []byte) (VolumeRdmaRegisteredBuffer, error)
}

type VolumeRdmaRegisteredBuffer interface {
	Descriptor() VolumeRdmaDataDesc
	Release(context.Context) error
}

type VolumeRdmaReadExporterConfig struct {
	MaxSize        uint64
	LeaseTTL       time.Duration
	ReadBufferSize int
}

type volumeRdmaNeedleReader interface {
	ReadVolumeNeedleDataInto(needle.VolumeId, *needle.Needle, *storage.ReadOption, io.Writer, int64, int64) error
}

type VolumeStoreRdmaReadExporter struct {
	store     volumeRdmaNeedleReader
	registrar VolumeRdmaReadRegistrar
	cfg       VolumeRdmaReadExporterConfig

	mu            sync.Mutex
	nextSessionID uint64
	leases        map[uint64]volumeRdmaReadBufferLease
}

type volumeRdmaReadBufferLease struct {
	buffer    VolumeRdmaRegisteredBuffer
	createdAt time.Time
	expiresAt time.Time
}

func NewVolumeStoreRdmaReadExporter(store *storage.Store, registrar VolumeRdmaReadRegistrar, cfg VolumeRdmaReadExporterConfig) *VolumeStoreRdmaReadExporter {
	var reader volumeRdmaNeedleReader
	if store != nil {
		reader = store
	}
	return newVolumeStoreRdmaReadExporter(reader, registrar, cfg)
}

func newVolumeStoreRdmaReadExporter(store volumeRdmaNeedleReader, registrar VolumeRdmaReadRegistrar, cfg VolumeRdmaReadExporterConfig) *VolumeStoreRdmaReadExporter {
	if cfg.MaxSize == 0 {
		cfg.MaxSize = defaultVolumeRdmaReadMaxSize
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = defaultVolumeRdmaReadLeaseTTL
	}
	if cfg.ReadBufferSize <= 0 {
		cfg.ReadBufferSize = defaultVolumeRdmaReadBufferSize
	}
	return &VolumeStoreRdmaReadExporter{
		store:     store,
		registrar: registrar,
		cfg:       cfg,
		leases:    make(map[uint64]volumeRdmaReadBufferLease),
	}
}

func (vs *VolumeServer) SetRdmaReadExporter(exporter VolumeRdmaReadExporter) {
	if vs == nil {
		return
	}
	vs.rdmaReadExporter = exporter
}

func (e *VolumeStoreRdmaReadExporter) PrepareRead(ctx context.Context, req VolumeRdmaReadRequest) (*VolumeRdmaReadLease, error) {
	if e == nil || e.store == nil || e.registrar == nil {
		return nil, ErrVolumeRdmaReadNotConfigured
	}
	if req.VolumeID == 0 || req.NeedleID == 0 || req.Size == 0 {
		return nil, fmt.Errorf("volume_id, needle_id, and size are required")
	}
	if req.Size > e.cfg.MaxSize || req.Size > math.MaxInt32 {
		return nil, fmt.Errorf("%w: requested=%d max=%d", ErrVolumeRdmaReadTooLarge, req.Size, e.cfg.MaxSize)
	}
	if req.Offset > math.MaxInt64 || req.Size > math.MaxInt64 || req.Offset+req.Size < req.Offset {
		return nil, fmt.Errorf("invalid read range offset=%d size=%d", req.Offset, req.Size)
	}

	data, err := e.readNeedleRange(ctx, req)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("native RDMA read produced no data")
	}

	registered, err := e.registrar.RegisterReadBuffer(ctx, data)
	if err != nil {
		return nil, err
	}
	desc := registered.Descriptor()
	if desc.RemoteAddr == 0 {
		_ = registered.Release(ctx)
		return nil, ErrVolumeRdmaReadNotExportable
	}
	if desc.Length == 0 || uint64(desc.Length) < uint64(len(data)) {
		_ = registered.Release(ctx)
		return nil, fmt.Errorf("%w: descriptor length=%d data=%d", ErrVolumeRdmaReadNotExportable, desc.Length, len(data))
	}
	desc.Length = uint32(len(data))

	sessionID := e.trackLease(registered)
	if sessionID == 0 {
		_ = registered.Release(ctx)
		return nil, fmt.Errorf("failed to allocate native RDMA read lease")
	}
	e.scheduleLeaseExpiry(sessionID)
	desc.Reserved[0] = sessionID

	return &VolumeRdmaReadLease{
		Desc:      desc,
		SessionID: sessionID,
	}, nil
}

func (e *VolumeStoreRdmaReadExporter) ReleaseRead(ctx context.Context, sessionID uint64) error {
	if e == nil {
		return ErrVolumeRdmaReadNotConfigured
	}
	lease, ok := e.popLease(sessionID)
	if !ok {
		return nil
	}
	return lease.buffer.Release(ctx)
}

func (e *VolumeStoreRdmaReadExporter) readNeedleRange(ctx context.Context, req VolumeRdmaReadRequest) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	n := &needle.Needle{
		Id:     types.Uint64ToNeedleId(req.NeedleID),
		Cookie: types.Uint32ToCookie(req.Cookie),
	}
	readOption := &storage.ReadOption{ReadBufferSize: e.cfg.ReadBufferSize}
	buf := bytes.NewBuffer(make([]byte, 0, int(req.Size)))
	if err := e.store.ReadVolumeNeedleDataInto(
		needle.VolumeId(req.VolumeID),
		n,
		readOption,
		buf,
		int64(req.Offset),
		int64(req.Size),
	); err != nil {
		return nil, err
	}
	if n.Cookie != types.Uint32ToCookie(req.Cookie) {
		return nil, fmt.Errorf("cookie mismatch for needle %d: got %08x, want %08x", req.NeedleID, uint32(n.Cookie), req.Cookie)
	}
	return buf.Bytes(), nil
}

func (e *VolumeStoreRdmaReadExporter) trackLease(buffer VolumeRdmaRegisteredBuffer) uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()

	for {
		e.nextSessionID++
		if e.nextSessionID != 0 {
			break
		}
	}
	sessionID := e.nextSessionID
	now := time.Now()
	e.leases[sessionID] = volumeRdmaReadBufferLease{
		buffer:    buffer,
		createdAt: now,
		expiresAt: now.Add(e.cfg.LeaseTTL),
	}
	return sessionID
}

func (e *VolumeStoreRdmaReadExporter) popLease(sessionID uint64) (volumeRdmaReadBufferLease, bool) {
	if sessionID == 0 {
		return volumeRdmaReadBufferLease{}, false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	lease, ok := e.leases[sessionID]
	if ok {
		delete(e.leases, sessionID)
	}
	return lease, ok
}

func (e *VolumeStoreRdmaReadExporter) scheduleLeaseExpiry(sessionID uint64) {
	ttl := e.cfg.LeaseTTL
	if ttl <= 0 {
		ttl = defaultVolumeRdmaReadLeaseTTL
	}
	time.AfterFunc(ttl, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = e.ReleaseRead(ctx, sessionID)
	})
}
