package swvfsdaemon

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"seaweedfs-rdma-sidecar/pkg/swvfsproto"
)

type RDMATestMRControl interface {
	TestMRAlloc(length uint32, pattern uint32) (swvfsproto.RDMATestMR, error)
	TestMRInfo() (swvfsproto.RDMATestMR, error)
	TestMRWrite(data []byte) (swvfsproto.RDMATestMR, error)
}

type RDMAReadDescriptorStager interface {
	StageReadRDMA(ctx context.Context, path string, offset, size uint64) (*swvfsproto.RDMADataDesc, *swvfsproto.Attr, error)
}

type KernelMRReadStager struct {
	Control RDMATestMRControl
	Reader  FileBackend
}

func (s *KernelMRReadStager) StageReadRDMA(ctx context.Context, path string, offset, size uint64) (*swvfsproto.RDMADataDesc, *swvfsproto.Attr, error) {
	if s == nil || s.Control == nil || s.Reader == nil {
		return nil, nil, ErrnoError{Errno: ErrnoNoSys, Msg: "rdma read descriptor stager is not configured"}
	}
	if size > swvfsproto.RDMAIOMax {
		return nil, nil, ErrnoError{Errno: ErrnoTooLarge, Msg: "rdma read descriptor request exceeds kernel RDMA IO max"}
	}
	data, attr, err := s.Reader.ReadFile(ctx, path, offset, size, false)
	if err != nil {
		return nil, nil, err
	}
	if len(data) == 0 {
		return &swvfsproto.RDMADataDesc{}, attr, nil
	}
	if len(data) > swvfsproto.RDMAIOMax {
		return nil, nil, ErrnoError{Errno: ErrnoTooLarge, Msg: "rdma read descriptor payload exceeds kernel RDMA IO max"}
	}
	if _, err := s.Control.TestMRAlloc(uint32(len(data)), 0); err != nil {
		return nil, nil, err
	}
	if _, err := s.Control.TestMRWrite(data); err != nil {
		return nil, nil, err
	}
	mr, err := s.Control.TestMRInfo()
	if err != nil {
		return nil, nil, err
	}
	if !mr.Allocated() || !mr.Registered() || mr.RemoteAddr == 0 || mr.RKey == 0 {
		return nil, nil, fmt.Errorf("kernel RDMA test MR is not exportable: flags=0x%x addr=%#x rkey=%#x", mr.Flags, mr.RemoteAddr, mr.RKey)
	}
	return &swvfsproto.RDMADataDesc{
		RemoteAddr: mr.RemoteAddr,
		RKey:       mr.RKey,
		Length:     uint32(len(data)),
	}, attr, nil
}

type RemoteRDMAReadDescriptorClient struct {
	Control RDMALocalProvider
	Peers   []string
	Client  *http.Client
	Timeout time.Duration
}

func (c *RemoteRDMAReadDescriptorClient) ReadFileRDMA(ctx context.Context, path string, offset, size uint64) (*swvfsproto.RDMADataDesc, *swvfsproto.Attr, error) {
	if c == nil || c.Control == nil || len(c.Peers) == 0 {
		return nil, nil, ErrnoError{Errno: ErrnoNoSys, Msg: "remote rdma read descriptor client is not configured"}
	}
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
		return nil, nil, err
	}
	local := RDMALocalEndpointFromInfo(localInfo)
	if !local.ReadyForConnect() || !local.QPConnected {
		return nil, nil, ErrnoError{Errno: ErrnoNoSys, Msg: "local kernel RDMA endpoint is not connected"}
	}

	urls := ExpandRDMAPeerURLs(attemptCtx, c.Peers)
	peers := make([]RDMALocalEndpoint, 0, len(urls))
	peerURLs := make(map[string]string, len(urls))
	for _, peerURL := range urls {
		endpoint, err := FetchRDMAPeerEndpoint(attemptCtx, c.Client, peerURL)
		if err != nil || !endpoint.ReadyForConnect() {
			continue
		}
		peers = append(peers, endpoint)
		peerURLs[endpoint.PeerKey()] = peerURL
	}
	selected, ok := SelectRDMAPairedPeer(local, peers)
	if !ok {
		return nil, nil, ErrnoError{Errno: ErrnoNoSys, Msg: "no paired RDMA peer is available for read descriptor"}
	}
	peerURL := peerURLs[selected.PeerKey()]
	if peerURL == "" {
		return nil, nil, ErrnoError{Errno: ErrnoNoSys, Msg: "selected RDMA peer URL is unavailable"}
	}
	desc, attr, err := PostRDMAPeerReadDesc(attemptCtx, c.Client, peerURL, path, offset, size)
	if err != nil {
		return nil, nil, ErrnoError{Errno: ErrnoNoSys, Msg: "remote rdma read descriptor unavailable: " + err.Error()}
	}
	return desc, attr, nil
}
