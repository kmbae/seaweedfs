package swvfsfiler

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"seaweedfs-rdma-sidecar/pkg/swvfsdaemon"
	"seaweedfs-rdma-sidecar/pkg/swvfsproto"
)

type NativeVolumeWriteDescriptorClient struct {
	Client       *http.Client
	Control      swvfsdaemon.RDMAPeerConnectorControl
	PeerManager  *swvfsdaemon.VolumeNativePeerManager
	Timeout      time.Duration
	ServiceLevel uint32
	Stats        *swvfsdaemon.Stats

	peerMu sync.Mutex
	peer   *swvfsdaemon.VolumeNativePeerManager
}

type nativeVolumeWriteKey struct {
	Path   string
	Offset uint64
	Size   uint64
}

type nativeVolumeWriteLease struct {
	Key          nativeVolumeWriteKey
	VolumeServer string
	FileID       string
	VolumeID     uint32
	NeedleID     uint64
	Cookie       uint32
	SessionID    uint64
	Created      time.Time
}

func (b *Backend) prepareWriteNativeRDMA(ctx context.Context, fullPath string, offset, size uint64) (*swvfsproto.RDMADataDesc, *swvfsproto.Attr, error) {
	if b == nil || b.Store == nil || b.NativeWriteDescriptor == nil {
		return nil, nil, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoSys, Msg: "native volume rdma write descriptor backend is not configured"}
	}
	fullPath = cleanFullPath(fullPath)
	if size == 0 {
		return nil, nil, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoSys, Msg: "native volume rdma write descriptor requires a non-empty write"}
	}
	if size > swvfsproto.RDMAIOMax {
		return nil, nil, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoTooLarge, Msg: "native volume rdma write exceeds kernel RDMA IO max"}
	}

	fileID, volumeServer, err := b.Store.AssignVolume(ctx, fullPath, size)
	if err != nil {
		return nil, nil, err
	}
	volumeID, needleID, cookie, err := ParseFileID(fileID)
	if err != nil {
		return nil, nil, err
	}
	desc, sessionID, err := b.NativeWriteDescriptor.PrepareNeedleWriteRDMA(ctx, nativeVolumeWriteLease{
		Key: nativeVolumeWriteKey{
			Path:   fullPath,
			Offset: offset,
			Size:   size,
		},
		VolumeServer: volumeServer,
		FileID:       fileID,
		VolumeID:     volumeID,
		NeedleID:     needleID,
		Cookie:       cookie,
	})
	if err != nil {
		return nil, nil, err
	}
	if desc == nil || desc.RemoteAddr == 0 || desc.RKey == 0 || desc.Length == 0 || sessionID == 0 {
		return nil, nil, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoSys, Msg: "native volume rdma write descriptor is not exportable"}
	}
	if uint64(desc.Length) < size {
		_ = b.NativeWriteDescriptor.AbortNeedleWriteRDMA(context.Background(), volumeServer, sessionID)
		return nil, nil, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoIO, Msg: "native volume rdma write descriptor is smaller than write size"}
	}
	desc.Length = uint32(size)
	desc.Reserved[0] = sessionID

	lease := nativeVolumeWriteLease{
		Key: nativeVolumeWriteKey{
			Path:   fullPath,
			Offset: offset,
			Size:   size,
		},
		VolumeServer: volumeServer,
		FileID:       fileID,
		VolumeID:     volumeID,
		NeedleID:     needleID,
		Cookie:       cookie,
		SessionID:    sessionID,
		Created:      time.Now(),
	}
	replaced := b.trackNativeWriteLease(lease)
	if replaced.SessionID != 0 {
		_ = b.NativeWriteDescriptor.AbortNeedleWriteRDMA(context.Background(), replaced.VolumeServer, replaced.SessionID)
		b.NativeWriteDescriptor.Stats.Inc("volume_native_rdma_write_desc_replaced")
	}
	b.NativeWriteDescriptor.Stats.Inc("volume_native_rdma_write_desc_success")
	b.NativeWriteDescriptor.Stats.Add("volume_native_rdma_write_desc_bytes", size)
	return desc, nil, nil
}

func (b *Backend) commitWriteNativeRDMA(ctx context.Context, fullPath string, offset, size uint64) (*swvfsproto.Attr, error) {
	if b == nil || b.NativeWriteDescriptor == nil {
		return nil, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoSys, Msg: "native volume rdma write descriptor backend is not configured"}
	}
	key := nativeVolumeWriteKey{Path: cleanFullPath(fullPath), Offset: offset, Size: size}
	lease, ok := b.popNativeWriteLease(key)
	if !ok {
		return nil, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoSys, Msg: "native volume rdma write commit has no tracked descriptor"}
	}
	resp, err := b.NativeWriteDescriptor.CommitNeedleWriteRDMA(ctx, lease)
	if err != nil {
		b.NativeWriteDescriptor.Stats.Inc("volume_native_rdma_write_commit_errors")
		return nil, err
	}
	fileID := resp.FileID
	if fileID == "" {
		fileID = lease.FileID
	}
	attr, err := b.appendChunkEntry(ctx, lease.Key.Path, lease.Key.Offset, lease.Key.Size, defaultRegularMode, 0, 0, fileID)
	if err != nil {
		return nil, err
	}
	b.NativeWriteDescriptor.Stats.Inc("volume_native_rdma_write_commit_success")
	b.NativeWriteDescriptor.Stats.Add("volume_native_rdma_write_commit_bytes", lease.Key.Size)
	return attr, nil
}

func (b *Backend) trackNativeWriteLease(lease nativeVolumeWriteLease) nativeVolumeWriteLease {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.nativeWriteLeases == nil {
		b.nativeWriteLeases = make(map[nativeVolumeWriteKey]nativeVolumeWriteLease)
	}
	replaced := b.nativeWriteLeases[lease.Key]
	b.nativeWriteLeases[lease.Key] = lease
	return replaced
}

func (b *Backend) popNativeWriteLease(key nativeVolumeWriteKey) (nativeVolumeWriteLease, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	lease, ok := b.nativeWriteLeases[key]
	if ok {
		delete(b.nativeWriteLeases, key)
	}
	return lease, ok
}

func (c *NativeVolumeWriteDescriptorClient) PrepareNeedleWriteRDMA(ctx context.Context, lease nativeVolumeWriteLease) (*swvfsproto.RDMADataDesc, uint64, error) {
	if c == nil {
		return nil, 0, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoSys, Msg: "native volume rdma write descriptor client is not configured"}
	}
	c.Stats.Inc("volume_native_rdma_write_desc_requests")
	c.Stats.Add("volume_native_rdma_write_desc_requested_bytes", lease.Key.Size)
	start := time.Now()
	defer func() {
		c.Stats.Observe("volume_native_rdma_write_desc", time.Since(start))
	}()
	if lease.VolumeServer == "" {
		return nil, 0, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoSys, Msg: "native volume rdma write descriptor requires a volume server"}
	}
	if lease.Key.Size == 0 {
		return nil, 0, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoSys, Msg: "native volume rdma write descriptor requires a non-empty write"}
	}
	timeout := c.timeout()
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	peer, err := c.ensureVolumeNativePeer(attemptCtx, lease.VolumeServer)
	if err != nil {
		c.Stats.Inc("volume_native_rdma_write_peer_connect_errors")
		return nil, 0, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoSys, Msg: fmt.Sprintf("native volume RDMA write peer handshake failed: %v", err)}
	}
	desc, sessionID, err := swvfsdaemon.PostVolumeNativeWriteDesc(attemptCtx, c.Client, lease.VolumeServer, swvfsdaemon.VolumeRDMAWriteDescRequest{
		ConnectionID: peer.VolumeConnectionID,
		FileID:       lease.FileID,
		VolumeID:     lease.VolumeID,
		NeedleID:     lease.NeedleID,
		Cookie:       lease.Cookie,
		Size:         lease.Key.Size,
	})
	if err != nil {
		c.Stats.Inc("volume_native_rdma_write_desc_post_errors")
		return nil, 0, err
	}
	if desc != nil && peer.VolumeConnectionID != 0 {
		desc.Reserved[1] = peer.VolumeConnectionID
	}
	return desc, sessionID, nil
}

func (c *NativeVolumeWriteDescriptorClient) CommitNeedleWriteRDMA(ctx context.Context, lease nativeVolumeWriteLease) (swvfsdaemon.VolumeRDMAWriteResponse, error) {
	if c == nil {
		return swvfsdaemon.VolumeRDMAWriteResponse{}, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoSys, Msg: "native volume rdma write descriptor client is not configured"}
	}
	c.Stats.Inc("volume_native_rdma_write_commit_requests")
	c.Stats.Add("volume_native_rdma_write_commit_requested_bytes", lease.Key.Size)
	start := time.Now()
	defer func() {
		c.Stats.Observe("volume_native_rdma_write_commit", time.Since(start))
	}()
	timeout := c.timeout()
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return swvfsdaemon.PostVolumeNativeWriteCommit(attemptCtx, c.Client, lease.VolumeServer, swvfsdaemon.VolumeRDMAWriteCommitRequest{
		SessionID: lease.SessionID,
		FileID:    lease.FileID,
		VolumeID:  lease.VolumeID,
		NeedleID:  lease.NeedleID,
		Cookie:    lease.Cookie,
		Size:      lease.Key.Size,
	})
}

func (c *NativeVolumeWriteDescriptorClient) AbortNeedleWriteRDMA(ctx context.Context, volumeServer string, sessionID uint64) error {
	if c == nil || volumeServer == "" || sessionID == 0 {
		return nil
	}
	timeout := c.timeout()
	abortCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := swvfsdaemon.PostVolumeNativeWriteAbort(abortCtx, c.Client, volumeServer, sessionID); err != nil {
		c.Stats.Inc("volume_native_rdma_write_abort_errors")
		return err
	}
	c.Stats.Inc("volume_native_rdma_write_abort_success")
	return nil
}

func (c *NativeVolumeWriteDescriptorClient) ensureVolumeNativePeer(ctx context.Context, volumeServer string) (swvfsdaemon.VolumeNativePeer, error) {
	manager := c.volumeNativePeerManager()
	if manager == nil {
		return swvfsdaemon.VolumeNativePeer{}, fmt.Errorf("kernel RDMA control is not configured")
	}
	peer, err := manager.Ensure(ctx, volumeServer)
	if err == nil && peer.VolumeConnectionID != 0 {
		c.Stats.Inc("volume_native_rdma_write_peer_connect_success")
	}
	return peer, err
}

func (c *NativeVolumeWriteDescriptorClient) volumeNativePeerManager() *swvfsdaemon.VolumeNativePeerManager {
	if c == nil {
		return nil
	}
	if c.PeerManager != nil {
		return c.PeerManager
	}
	if c.Control == nil {
		return nil
	}
	c.peerMu.Lock()
	defer c.peerMu.Unlock()
	if c.peer == nil {
		c.peer = &swvfsdaemon.VolumeNativePeerManager{
			Client:       c.Client,
			Control:      c.Control,
			ServiceLevel: c.ServiceLevel,
			Stats:        c.Stats,
		}
	}
	return c.peer
}

func (c *NativeVolumeWriteDescriptorClient) timeout() time.Duration {
	if c == nil || c.Timeout <= 0 {
		return 5 * time.Second
	}
	return c.Timeout
}
