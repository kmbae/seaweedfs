package swvfsdaemon

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"seaweedfs-rdma-sidecar/pkg/swvfsproto"
)

type RDMATestMRControl interface {
	TestMRAlloc(length uint32, pattern uint32) (swvfsproto.RDMATestMR, error)
	TestMRInfo(sessionID uint64) (swvfsproto.RDMATestMR, error)
	TestMRWrite(sessionID uint64, data []byte) (swvfsproto.RDMATestMR, error)
	TestMRFree(sessionID uint64) error
}

type RDMAReadDescriptorLease struct {
	Desc      swvfsproto.RDMADataDesc
	Attr      *swvfsproto.Attr
	SessionID uint64
}

type RDMAReadDescriptorStager interface {
	StageReadRDMA(ctx context.Context, path string, offset, size uint64) (*RDMAReadDescriptorLease, error)
	ReleaseReadRDMA(ctx context.Context, sessionID uint64) error
}

type KernelMRReadStager struct {
	Control RDMATestMRControl
	Reader  FileBackend
	Stats   *Stats
}

func (s *KernelMRReadStager) StageReadRDMA(ctx context.Context, path string, offset, size uint64) (*RDMAReadDescriptorLease, error) {
	start := time.Now()
	if s != nil {
		s.Stats.Inc("rdma_stager_read_requests")
		s.Stats.Add("rdma_stager_read_requested_bytes", size)
		defer func() {
			s.Stats.Observe("rdma_stager_read", time.Since(start))
		}()
	}
	if s == nil || s.Control == nil || s.Reader == nil {
		return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "rdma read descriptor stager is not configured"}
	}
	if size > swvfsproto.RDMAIOMax {
		return nil, ErrnoError{Errno: ErrnoTooLarge, Msg: "rdma read descriptor request exceeds kernel RDMA IO max"}
	}
	data, attr, err := s.Reader.ReadFile(ctx, path, offset, size, false)
	if err != nil {
		s.Stats.Inc("rdma_stager_file_read_errors")
		return nil, err
	}
	s.Stats.Add("rdma_stager_file_read_bytes", uint64(len(data)))
	if len(data) == 0 {
		s.Stats.Inc("rdma_stager_empty_reads")
		return &RDMAReadDescriptorLease{Attr: attr}, nil
	}
	if len(data) > swvfsproto.RDMAIOMax {
		return nil, ErrnoError{Errno: ErrnoTooLarge, Msg: "rdma read descriptor payload exceeds kernel RDMA IO max"}
	}
	alloc, err := s.Control.TestMRAlloc(uint32(len(data)), 0)
	if err != nil {
		s.Stats.Inc("rdma_stager_mr_alloc_errors")
		return nil, err
	}
	sessionID := alloc.SessionID
	if sessionID == 0 {
		return nil, fmt.Errorf("kernel RDMA test MR allocation returned no session id")
	}
	if _, err := s.Control.TestMRWrite(sessionID, data); err != nil {
		_ = s.Control.TestMRFree(sessionID)
		s.Stats.Inc("rdma_stager_mr_write_errors")
		return nil, err
	}
	mr, err := s.Control.TestMRInfo(sessionID)
	if err != nil {
		_ = s.Control.TestMRFree(sessionID)
		s.Stats.Inc("rdma_stager_mr_info_errors")
		return nil, err
	}
	if !mr.Allocated() || !mr.Registered() || mr.RemoteAddr == 0 || mr.RKey == 0 {
		_ = s.Control.TestMRFree(sessionID)
		s.Stats.Inc("rdma_stager_mr_not_exportable")
		return nil, fmt.Errorf("kernel RDMA test MR is not exportable: flags=0x%x addr=%#x rkey=%#x", mr.Flags, mr.RemoteAddr, mr.RKey)
	}
	s.Stats.Inc("rdma_stager_desc_success")
	s.Stats.Add("rdma_stager_desc_bytes", uint64(len(data)))
	return &RDMAReadDescriptorLease{
		Desc: swvfsproto.RDMADataDesc{
			RemoteAddr: mr.RemoteAddr,
			RKey:       mr.RKey,
			Length:     uint32(len(data)),
			Reserved:   [4]uint64{sessionID},
		},
		Attr:      attr,
		SessionID: sessionID,
	}, nil
}

func (s *KernelMRReadStager) ReleaseReadRDMA(ctx context.Context, sessionID uint64) error {
	_ = ctx
	if sessionID == 0 {
		return nil
	}
	if s == nil || s.Control == nil {
		return ErrnoError{Errno: ErrnoNoSys, Msg: "rdma read descriptor stager is not configured"}
	}
	s.Stats.Inc("rdma_stager_release_requests")
	err := s.Control.TestMRFree(sessionID)
	if err != nil {
		s.Stats.Inc("rdma_stager_release_errors")
		return err
	}
	s.Stats.Inc("rdma_stager_release_success")
	return nil
}

type RemoteRDMAReadDescriptorClient struct {
	Control      RDMALocalProvider
	Peers        []string
	Client       *http.Client
	Timeout      time.Duration
	ReleaseDelay time.Duration
	Stats        *Stats

	mu          sync.Mutex
	nextLeaseID uint64
	leases      map[uint64]remoteRDMAReadLease
}

type remoteRDMAReadLease struct {
	PeerURL   string
	SessionID uint64
	Created   time.Time
}

func (c *RemoteRDMAReadDescriptorClient) ReadFileRDMA(ctx context.Context, path string, offset, size uint64) (*swvfsproto.RDMADataDesc, *swvfsproto.Attr, error) {
	if c == nil || c.Control == nil || len(c.Peers) == 0 {
		return nil, nil, ErrnoError{Errno: ErrnoNoSys, Msg: "remote rdma read descriptor client is not configured"}
	}
	start := time.Now()
	c.Stats.Inc("rdma_read_desc_client_requests")
	c.Stats.Add("rdma_read_desc_client_requested_bytes", size)
	defer func() {
		c.Stats.Observe("rdma_read_desc_client", time.Since(start))
	}()
	if size > swvfsproto.RDMAIOMax {
		return nil, nil, ErrnoError{Errno: ErrnoTooLarge, Msg: "remote rdma read descriptor request exceeds kernel RDMA IO max"}
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	localInfo, err := c.Control.GetLocal()
	if err != nil {
		c.Stats.Inc("rdma_read_desc_client_local_errors")
		return nil, nil, err
	}
	local := RDMALocalEndpointFromInfo(localInfo)
	if !local.ReadyForConnect() || !local.QPConnected {
		c.Stats.Inc("rdma_read_desc_client_local_not_connected")
		return nil, nil, ErrnoError{Errno: ErrnoNoSys, Msg: "local kernel RDMA endpoint is not connected"}
	}

	urls := ExpandRDMAPeerURLs(attemptCtx, c.Peers)
	peers := make([]RDMALocalEndpoint, 0, len(urls))
	peerURLs := make(map[string]string, len(urls))
	for _, peerURL := range urls {
		endpoint, err := FetchRDMAPeerEndpoint(attemptCtx, c.Client, peerURL)
		if err != nil || !endpoint.ReadyForConnect() {
			c.Stats.Inc("rdma_read_desc_client_peer_unready")
			continue
		}
		peers = append(peers, endpoint)
		peerURLs[endpoint.PeerKey()] = peerURL
	}
	selected, ok := SelectRDMAPairedPeer(local, peers)
	if !ok {
		c.Stats.Inc("rdma_read_desc_client_no_paired_peer")
		return nil, nil, ErrnoError{Errno: ErrnoNoSys, Msg: "no paired RDMA peer is available for read descriptor"}
	}
	peerURL := peerURLs[selected.PeerKey()]
	if peerURL == "" {
		c.Stats.Inc("rdma_read_desc_client_peer_url_missing")
		return nil, nil, ErrnoError{Errno: ErrnoNoSys, Msg: "selected RDMA peer URL is unavailable"}
	}
	desc, attr, sessionID, err := PostRDMAPeerReadDesc(attemptCtx, c.Client, peerURL, path, offset, size)
	if err != nil {
		c.Stats.Inc("rdma_read_desc_client_post_errors")
		return nil, nil, ErrnoError{Errno: ErrnoNoSys, Msg: "remote rdma read descriptor unavailable: " + err.Error()}
	}
	leaseID := c.trackLease(peerURL, sessionID)
	if leaseID != 0 {
		desc.Reserved[0] = leaseID
		c.scheduleRelease(leaseID)
	}
	c.Stats.Inc("rdma_read_desc_client_success")
	c.Stats.Add("rdma_read_desc_client_desc_bytes", uint64(desc.Length))
	return desc, attr, nil
}

func (c *RemoteRDMAReadDescriptorClient) trackLease(peerURL string, sessionID uint64) uint64 {
	if c == nil || sessionID == 0 || peerURL == "" {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leases == nil {
		c.leases = make(map[uint64]remoteRDMAReadLease)
	}
	c.nextLeaseID++
	if c.nextLeaseID == 0 {
		c.nextLeaseID++
	}
	leaseID := c.nextLeaseID
	c.leases[leaseID] = remoteRDMAReadLease{
		PeerURL:   peerURL,
		SessionID: sessionID,
		Created:   time.Now(),
	}
	c.Stats.Inc("rdma_read_desc_client_leases_created")
	return leaseID
}

func (c *RemoteRDMAReadDescriptorClient) popLease(leaseID uint64) (remoteRDMAReadLease, bool) {
	if c == nil || leaseID == 0 {
		return remoteRDMAReadLease{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	lease, ok := c.leases[leaseID]
	if ok {
		delete(c.leases, leaseID)
		c.Stats.Inc("rdma_read_desc_client_leases_popped")
	}
	return lease, ok
}

func (c *RemoteRDMAReadDescriptorClient) ReleaseReadDescriptor(ctx context.Context, leaseID uint64, status int32, bytes uint64) error {
	_ = status
	_ = bytes
	if c == nil {
		return nil
	}
	c.Stats.Inc("rdma_read_desc_client_release_requests")
	lease, ok := c.popLease(leaseID)
	if !ok {
		c.Stats.Inc("rdma_read_desc_client_release_unknown")
		return nil
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	releaseCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := PostRDMAPeerReleaseDesc(releaseCtx, c.Client, lease.PeerURL, lease.SessionID); err != nil {
		c.Stats.Inc("rdma_read_desc_client_release_errors")
		return err
	}
	c.Stats.Inc("rdma_read_desc_client_release_success")
	return nil
}

func (c *RemoteRDMAReadDescriptorClient) scheduleRelease(leaseID uint64) {
	if leaseID == 0 {
		return
	}
	delay := c.ReleaseDelay
	if delay <= 0 {
		delay = 30 * time.Second
	}
	time.AfterFunc(delay, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.ReleaseReadDescriptor(ctx, leaseID, 0, 0)
	})
}
