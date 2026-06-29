package swvfsdaemon

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"seaweedfs-rdma-sidecar/pkg/swvfsproto"
)

type rdmaWriteDescriptorKey struct {
	Path   string
	Offset uint64
	Size   uint64
}

type rdmaWriteDescriptorLease struct {
	PeerURL   string
	SessionID uint64
	Key       rdmaWriteDescriptorKey
	Created   time.Time
}

type KernelMRWriteStager struct {
	Control RDMATestMRControl
	Local   RDMALocalProvider
	Writer  FileBackend
	Pool    *KernelMRPool
	Stats   *Stats

	mu        sync.Mutex
	pending   map[rdmaWriteDescriptorKey]KernelMRPoolSession
	bySession map[uint64]rdmaWriteDescriptorKey
}

func (s *KernelMRWriteStager) PrepareWriteRDMA(ctx context.Context, path string, offset, size uint64) (*swvfsproto.RDMADataDesc, *swvfsproto.Attr, error) {
	_ = ctx
	start := time.Now()
	if s != nil {
		s.Stats.Inc("rdma_write_stager_prepare_requests")
		s.Stats.Add("rdma_write_stager_prepare_bytes", size)
		defer func() {
			s.Stats.Observe("rdma_write_stager_prepare", time.Since(start))
		}()
	}
	if s == nil || s.Control == nil || s.Writer == nil {
		return nil, nil, ErrnoError{Errno: ErrnoNoSys, Msg: "rdma write descriptor stager is not configured"}
	}
	if size == 0 || size > swvfsproto.RDMAIOMax {
		return nil, nil, ErrnoError{Errno: ErrnoNoSys, Msg: "rdma write descriptor request exceeds kernel RDMA IO max"}
	}
	if err := s.requireConnected(); err != nil {
		s.Stats.Inc("rdma_write_stager_local_not_connected")
		return nil, nil, err
	}
	session, err := s.acquireMR(uint32(size))
	if err != nil {
		s.Stats.Inc("rdma_write_stager_mr_alloc_errors")
		return nil, nil, err
	}
	mr, err := s.Control.TestMRInfo(session.SessionID)
	if err != nil {
		s.discardMR(session.SessionID)
		s.Stats.Inc("rdma_write_stager_mr_info_errors")
		return nil, nil, err
	}
	if !mr.Allocated() || !mr.Registered() || mr.RemoteAddr == 0 || mr.RKey == 0 {
		s.discardMR(session.SessionID)
		s.Stats.Inc("rdma_write_stager_mr_not_exportable")
		return nil, nil, fmt.Errorf("kernel RDMA write MR is not exportable: flags=0x%x addr=%#x rkey=%#x", mr.Flags, mr.RemoteAddr, mr.RKey)
	}

	key := rdmaWriteDescriptorKey{Path: path, Offset: offset, Size: size}
	var replaced KernelMRPoolSession
	s.mu.Lock()
	if s.pending == nil {
		s.pending = make(map[rdmaWriteDescriptorKey]KernelMRPoolSession)
	}
	if s.bySession == nil {
		s.bySession = make(map[uint64]rdmaWriteDescriptorKey)
	}
	if old, ok := s.pending[key]; ok {
		replaced = old
		delete(s.bySession, old.SessionID)
	}
	s.pending[key] = session
	s.bySession[session.SessionID] = key
	s.mu.Unlock()
	if replaced.SessionID != 0 {
		s.discardMR(replaced.SessionID)
		s.Stats.Inc("rdma_write_stager_prepare_replaced")
	}

	desc := swvfsproto.RDMADataDesc{
		RemoteAddr: mr.RemoteAddr,
		RKey:       mr.RKey,
		Length:     uint32(size),
	}
	desc.SetLeaseID(session.SessionID)
	s.Stats.Inc("rdma_write_stager_prepare_success")
	return &desc, nil, nil
}

func (s *KernelMRWriteStager) CommitWriteRDMA(ctx context.Context, path string, offset, size uint64) (*swvfsproto.Attr, error) {
	return s.CommitWriteRDMASession(ctx, 0, path, offset, size)
}

func (s *KernelMRWriteStager) CommitWriteRDMASession(ctx context.Context, sessionID uint64, path string, offset, size uint64) (*swvfsproto.Attr, error) {
	start := time.Now()
	if s != nil {
		s.Stats.Inc("rdma_write_stager_commit_requests")
		s.Stats.Add("rdma_write_stager_commit_bytes", size)
		defer func() {
			s.Stats.Observe("rdma_write_stager_commit", time.Since(start))
		}()
	}
	if s == nil || s.Control == nil || s.Writer == nil {
		return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "rdma write descriptor stager is not configured"}
	}
	if size == 0 || size > swvfsproto.RDMAIOMax {
		return nil, ErrnoError{Errno: ErrnoInval, Msg: "invalid rdma write descriptor commit size"}
	}
	session, ok := s.popPending(sessionID, rdmaWriteDescriptorKey{Path: path, Offset: offset, Size: size})
	if !ok {
		s.Stats.Inc("rdma_write_stager_commit_unknown")
		return nil, ErrnoError{Errno: ErrnoIO, Msg: "rdma write descriptor commit has no matching prepare"}
	}

	data, _, err := s.Control.TestMRRead(session.SessionID, uint32(size))
	if err != nil {
		s.discardMR(session.SessionID)
		s.Stats.Inc("rdma_write_stager_mr_read_errors")
		return nil, err
	}
	if uint64(len(data)) != size {
		s.discardMR(session.SessionID)
		s.Stats.Inc("rdma_write_stager_mr_read_short")
		return nil, fmt.Errorf("rdma write descriptor read returned %d bytes, want %d", len(data), size)
	}
	s.Stats.Add("rdma_write_stager_mr_read_bytes", uint64(len(data)))
	if err := s.releaseMR(session.SessionID); err != nil {
		s.Stats.Inc("rdma_write_stager_release_errors")
		return nil, err
	}
	attr, err := s.Writer.WriteFile(ctx, path, offset, data, 0, 0, 0, true)
	if err != nil {
		s.Stats.Inc("rdma_write_stager_backend_errors")
		return nil, err
	}
	s.Stats.Inc("rdma_write_stager_flush_deferred")
	s.Stats.Inc("rdma_write_stager_commit_success")
	return attr, nil
}

func (s *KernelMRWriteStager) FlushFile(ctx context.Context, path string) (*swvfsproto.Attr, error) {
	if s == nil || s.Writer == nil {
		return nil, nil
	}
	if flusher, ok := s.Writer.(interface {
		FlushFile(context.Context, string) (*swvfsproto.Attr, error)
	}); ok {
		attr, err := flusher.FlushFile(ctx, path)
		if err != nil {
			s.Stats.Inc("rdma_write_stager_flush_errors")
			return nil, err
		}
		s.Stats.Inc("rdma_write_stager_flush_success")
		return attr, nil
	}
	return nil, nil
}

func (s *KernelMRWriteStager) AbortWriteRDMASession(ctx context.Context, sessionID uint64) error {
	_ = ctx
	if s == nil || sessionID == 0 {
		return nil
	}
	session, ok := s.popPending(sessionID, rdmaWriteDescriptorKey{})
	if !ok {
		s.Stats.Inc("rdma_write_stager_abort_unknown")
		return nil
	}
	s.Stats.Inc("rdma_write_stager_abort_requests")
	return s.discardMR(session.SessionID)
}

func (s *KernelMRWriteStager) popPending(sessionID uint64, key rdmaWriteDescriptorKey) (KernelMRPoolSession, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sessionID != 0 {
		storedKey, ok := s.bySession[sessionID]
		if !ok {
			return KernelMRPoolSession{}, false
		}
		session := s.pending[storedKey]
		delete(s.bySession, sessionID)
		delete(s.pending, storedKey)
		return session, true
	}
	session, ok := s.pending[key]
	if !ok {
		return KernelMRPoolSession{}, false
	}
	delete(s.pending, key)
	delete(s.bySession, session.SessionID)
	return session, true
}

func (s *KernelMRWriteStager) requireConnected() error {
	if s == nil || s.Local == nil {
		return nil
	}
	info, err := s.Local.GetLocal()
	if err != nil {
		return err
	}
	if !info.EndpointReady() || !info.Connected() {
		return ErrnoError{Errno: ErrnoNoSys, Msg: "local kernel RDMA endpoint is not connected"}
	}
	return nil
}

func (s *KernelMRWriteStager) acquireMR(length uint32) (KernelMRPoolSession, error) {
	if s != nil && s.Pool != nil {
		return s.Pool.Acquire(length)
	}
	alloc, err := s.Control.TestMRAlloc(length, 0)
	if err != nil {
		return KernelMRPoolSession{}, err
	}
	return KernelMRPoolSession{SessionID: alloc.SessionID, Capacity: alloc.Length}, nil
}

func (s *KernelMRWriteStager) releaseMR(sessionID uint64) error {
	if sessionID == 0 || s == nil {
		return nil
	}
	if s.Pool != nil {
		return s.Pool.Release(sessionID)
	}
	return s.Control.TestMRFree(sessionID)
}

func (s *KernelMRWriteStager) discardMR(sessionID uint64) error {
	if sessionID == 0 || s == nil {
		return nil
	}
	if s.Pool != nil {
		return s.Pool.Discard(sessionID)
	}
	return s.Control.TestMRFree(sessionID)
}

type RemoteRDMAWriteDescriptorClient struct {
	Control    RDMALocalProvider
	Peers      []string
	Client     *http.Client
	Timeout    time.Duration
	AbortDelay time.Duration
	Stats      *Stats

	mu          sync.Mutex
	nextLeaseID uint64
	leases      map[uint64]rdmaWriteDescriptorLease
	byKey       map[rdmaWriteDescriptorKey]uint64
	dirty       map[string]map[string]struct{}
}

func (c *RemoteRDMAWriteDescriptorClient) PrepareWriteRDMA(ctx context.Context, path string, offset, size uint64) (*swvfsproto.RDMADataDesc, *swvfsproto.Attr, error) {
	if c == nil || c.Control == nil || len(c.Peers) == 0 {
		return nil, nil, ErrnoError{Errno: ErrnoNoSys, Msg: "remote rdma write descriptor client is not configured"}
	}
	start := time.Now()
	c.Stats.Inc("rdma_write_desc_client_prepare_requests")
	c.Stats.Add("rdma_write_desc_client_prepare_bytes", size)
	defer func() {
		c.Stats.Observe("rdma_write_desc_client_prepare", time.Since(start))
	}()
	if size == 0 || size > swvfsproto.RDMAIOMax {
		return nil, nil, ErrnoError{Errno: ErrnoNoSys, Msg: "remote rdma write descriptor request exceeds kernel RDMA IO max"}
	}
	timeout := c.timeout()
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	peerURL, err := c.selectPeerURL(attemptCtx)
	if err != nil {
		c.Stats.Inc("rdma_write_desc_client_peer_errors")
		return nil, nil, err
	}
	desc, attr, sessionID, err := PostRDMAPeerWritePrepare(attemptCtx, c.Client, peerURL, path, offset, size)
	if err != nil {
		c.Stats.Inc("rdma_write_desc_client_prepare_post_errors")
		return nil, nil, ErrnoError{Errno: ErrnoNoSys, Msg: "remote rdma write descriptor unavailable: " + err.Error()}
	}
	if sessionID == 0 {
		c.Stats.Inc("rdma_write_desc_client_prepare_missing_session")
		return nil, nil, ErrnoError{Errno: ErrnoNoSys, Msg: "remote rdma write descriptor returned no session id"}
	}
	leaseID := c.trackLease(rdmaWriteDescriptorLease{
		PeerURL:   peerURL,
		SessionID: sessionID,
		Key:       rdmaWriteDescriptorKey{Path: path, Offset: offset, Size: size},
		Created:   time.Now(),
	})
	if leaseID != 0 {
		desc.SetLeaseID(leaseID)
		c.scheduleAbort(leaseID)
	}
	c.Stats.Inc("rdma_write_desc_client_prepare_success")
	return desc, attr, nil
}

func (c *RemoteRDMAWriteDescriptorClient) CommitWriteRDMA(ctx context.Context, path string, offset, size uint64) (*swvfsproto.Attr, error) {
	if c == nil {
		return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "remote rdma write descriptor client is not configured"}
	}
	start := time.Now()
	c.Stats.Inc("rdma_write_desc_client_commit_requests")
	c.Stats.Add("rdma_write_desc_client_commit_bytes", size)
	defer func() {
		c.Stats.Observe("rdma_write_desc_client_commit", time.Since(start))
	}()
	key := rdmaWriteDescriptorKey{Path: path, Offset: offset, Size: size}
	lease, ok := c.popLeaseByKey(key)
	if !ok {
		c.Stats.Inc("rdma_write_desc_client_commit_unknown")
		return nil, ErrnoError{Errno: ErrnoIO, Msg: "rdma write commit has no tracked remote descriptor"}
	}
	timeout := c.timeout()
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	attr, err := PostRDMAPeerWriteCommit(attemptCtx, c.Client, lease.PeerURL, lease.SessionID, path, offset, size)
	if err != nil {
		c.Stats.Inc("rdma_write_desc_client_commit_post_errors")
		abortCtx, abortCancel := context.WithTimeout(context.Background(), timeout)
		_ = PostRDMAPeerWriteAbort(abortCtx, c.Client, lease.PeerURL, lease.SessionID)
		abortCancel()
		return nil, err
	}
	c.trackDirty(path, lease.PeerURL)
	c.Stats.Inc("rdma_write_desc_client_commit_success")
	return attr, nil
}

func (c *RemoteRDMAWriteDescriptorClient) FlushFile(ctx context.Context, path string) (*swvfsproto.Attr, error) {
	if c == nil || path == "" {
		return nil, nil
	}
	peers := c.popDirty(path)
	if len(peers) == 0 {
		return nil, nil
	}
	c.Stats.Inc("rdma_write_desc_client_flush_requests")
	var attr *swvfsproto.Attr
	for _, peerURL := range peers {
		timeout := c.timeout()
		attemptCtx, cancel := context.WithTimeout(ctx, timeout)
		peerAttr, err := PostRDMAPeerWriteFlush(attemptCtx, c.Client, peerURL, path)
		cancel()
		if err != nil {
			c.Stats.Inc("rdma_write_desc_client_flush_post_errors")
			c.trackDirty(path, peerURL)
			return attr, err
		}
		if peerAttr != nil {
			attr = peerAttr
		}
		c.Stats.Inc("rdma_write_desc_client_flush_peer_success")
	}
	c.Stats.Inc("rdma_write_desc_client_flush_success")
	return attr, nil
}

func (c *RemoteRDMAWriteDescriptorClient) selectPeerURL(ctx context.Context) (string, error) {
	localInfo, err := c.Control.GetLocal()
	if err != nil {
		return "", err
	}
	local := RDMALocalEndpointFromInfo(localInfo)
	if !local.ReadyForConnect() || !local.QPConnected {
		return "", ErrnoError{Errno: ErrnoNoSys, Msg: "local kernel RDMA endpoint is not connected"}
	}
	urls := ExpandRDMAPeerURLs(ctx, c.Peers)
	peers := make([]RDMALocalEndpoint, 0, len(urls))
	peerURLs := make(map[string]string, len(urls))
	for _, peerURL := range urls {
		endpoint, err := FetchRDMAPeerEndpoint(ctx, c.Client, peerURL)
		if err != nil || !endpoint.ReadyForConnect() {
			c.Stats.Inc("rdma_write_desc_client_peer_unready")
			continue
		}
		peers = append(peers, endpoint)
		peerURLs[endpoint.PeerKey()] = peerURL
	}
	selected, ok := SelectRDMAPairedPeer(local, peers)
	if !ok {
		return "", ErrnoError{Errno: ErrnoNoSys, Msg: "no paired RDMA peer is available for write descriptor"}
	}
	peerURL := peerURLs[selected.PeerKey()]
	if peerURL == "" {
		return "", ErrnoError{Errno: ErrnoNoSys, Msg: "selected RDMA peer URL is unavailable"}
	}
	return peerURL, nil
}

func (c *RemoteRDMAWriteDescriptorClient) trackLease(lease rdmaWriteDescriptorLease) uint64 {
	if c == nil || lease.SessionID == 0 || lease.PeerURL == "" {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leases == nil {
		c.leases = make(map[uint64]rdmaWriteDescriptorLease)
	}
	if c.byKey == nil {
		c.byKey = make(map[rdmaWriteDescriptorKey]uint64)
	}
	if oldID, ok := c.byKey[lease.Key]; ok {
		delete(c.leases, oldID)
	}
	c.nextLeaseID++
	if c.nextLeaseID == 0 {
		c.nextLeaseID++
	}
	leaseID := c.nextLeaseID
	c.leases[leaseID] = lease
	c.byKey[lease.Key] = leaseID
	c.Stats.Inc("rdma_write_desc_client_leases_created")
	return leaseID
}

func (c *RemoteRDMAWriteDescriptorClient) trackDirty(path, peerURL string) {
	if c == nil || path == "" || peerURL == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dirty == nil {
		c.dirty = make(map[string]map[string]struct{})
	}
	peers := c.dirty[path]
	if peers == nil {
		peers = make(map[string]struct{})
		c.dirty[path] = peers
	}
	peers[peerURL] = struct{}{}
}

func (c *RemoteRDMAWriteDescriptorClient) popDirty(path string) []string {
	if c == nil || path == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	peers := c.dirty[path]
	if len(peers) == 0 {
		return nil
	}
	out := make([]string, 0, len(peers))
	for peerURL := range peers {
		out = append(out, peerURL)
	}
	delete(c.dirty, path)
	sort.Strings(out)
	return out
}

func (c *RemoteRDMAWriteDescriptorClient) popLease(leaseID uint64) (rdmaWriteDescriptorLease, bool) {
	if c == nil || leaseID == 0 {
		return rdmaWriteDescriptorLease{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	lease, ok := c.leases[leaseID]
	if ok {
		delete(c.leases, leaseID)
		delete(c.byKey, lease.Key)
	}
	return lease, ok
}

func (c *RemoteRDMAWriteDescriptorClient) popLeaseByKey(key rdmaWriteDescriptorKey) (rdmaWriteDescriptorLease, bool) {
	if c == nil {
		return rdmaWriteDescriptorLease{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	leaseID, ok := c.byKey[key]
	if !ok {
		return rdmaWriteDescriptorLease{}, false
	}
	lease, ok := c.leases[leaseID]
	if ok {
		delete(c.leases, leaseID)
		delete(c.byKey, key)
	}
	return lease, ok
}

func (c *RemoteRDMAWriteDescriptorClient) scheduleAbort(leaseID uint64) {
	if c == nil || leaseID == 0 {
		return
	}
	delay := c.AbortDelay
	if delay <= 0 {
		delay = 30 * time.Second
	}
	time.AfterFunc(delay, func() {
		lease, ok := c.popLease(leaseID)
		if !ok {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout())
		defer cancel()
		if err := PostRDMAPeerWriteAbort(ctx, c.Client, lease.PeerURL, lease.SessionID); err != nil {
			c.Stats.Inc("rdma_write_desc_client_abort_errors")
			return
		}
		c.Stats.Inc("rdma_write_desc_client_abort_success")
	})
}

func (c *RemoteRDMAWriteDescriptorClient) timeout() time.Duration {
	if c == nil || c.Timeout <= 0 {
		return 5 * time.Second
	}
	return c.Timeout
}
