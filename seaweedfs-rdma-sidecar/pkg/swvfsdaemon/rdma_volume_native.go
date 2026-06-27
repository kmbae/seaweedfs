package swvfsdaemon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"seaweedfs-rdma-sidecar/pkg/swvfsproto"
)

const (
	VolumeRDMAReadDescPath    = "/rdma/native/read-desc"
	VolumeRDMAReleaseDescPath = "/rdma/native/release-desc"
	nativeReadLeaseBit        = uint64(1) << 63
)

type NeedleReadDescriptorRequest struct {
	FileID       string
	VolumeID     uint32
	NeedleID     uint64
	Cookie       uint32
	VolumeServer string
	RDMAServer   string
	Offset       uint64
	Size         uint64
}

type NeedleReadDescriptorBackend interface {
	ReadNeedleRDMA(context.Context, NeedleReadDescriptorRequest) (*swvfsproto.RDMADataDesc, *swvfsproto.Attr, error)
}

type VolumeRDMAReadDescRequest struct {
	FileID   string `json:"file_id"`
	VolumeID uint32 `json:"volume_id"`
	NeedleID uint64 `json:"needle_id"`
	Cookie   uint32 `json:"cookie"`
	Offset   uint64 `json:"offset"`
	Size     uint64 `json:"size"`
}

type VolumeRDMAReadDescResponse struct {
	Desc      swvfsproto.RDMADataDesc `json:"desc"`
	Attr      *swvfsproto.Attr        `json:"attr,omitempty"`
	SessionID uint64                  `json:"session_id,omitempty"`
}

type VolumeRDMAReleaseDescRequest struct {
	SessionID uint64 `json:"session_id"`
}

type VolumeNativeRDMAReadDescriptorClient struct {
	Client       *http.Client
	Timeout      time.Duration
	ReleaseDelay time.Duration
	Stats        *Stats

	mu          sync.Mutex
	nextLeaseID uint64
	leases      map[uint64]volumeNativeReadLease
}

type volumeNativeReadLease struct {
	VolumeServer string
	SessionID    uint64
	Created      time.Time
}

func MarkNativeReadLease(leaseID uint64) uint64 {
	if leaseID == 0 {
		return 0
	}
	return leaseID | nativeReadLeaseBit
}

func IsNativeReadLease(leaseID uint64) bool {
	return leaseID&nativeReadLeaseBit != 0
}

func UnmarkNativeReadLease(leaseID uint64) uint64 {
	return leaseID &^ nativeReadLeaseBit
}

func (c *VolumeNativeRDMAReadDescriptorClient) ReadNeedleRDMA(ctx context.Context, req NeedleReadDescriptorRequest) (*swvfsproto.RDMADataDesc, *swvfsproto.Attr, error) {
	if c == nil {
		return nil, nil, ErrnoError{Errno: ErrnoNoSys, Msg: "native volume rdma descriptor client is not configured"}
	}
	c.Stats.Inc("volume_native_rdma_read_desc_requests")
	c.Stats.Add("volume_native_rdma_read_desc_requested_bytes", req.Size)
	start := time.Now()
	defer func() {
		c.Stats.Observe("volume_native_rdma_read_desc", time.Since(start))
	}()
	if req.VolumeServer == "" {
		c.Stats.Inc("volume_native_rdma_read_desc_no_volume_server")
		return nil, nil, ErrnoError{Errno: ErrnoNoSys, Msg: "native volume rdma descriptor requires a volume server"}
	}
	if req.Size == 0 {
		c.Stats.Inc("volume_native_rdma_read_desc_empty")
		return nil, nil, ErrnoError{Errno: ErrnoNoSys, Msg: "native volume rdma descriptor requires a non-empty read"}
	}
	if req.Size > swvfsproto.RDMAIOMax {
		c.Stats.Inc("volume_native_rdma_read_desc_too_large")
		return nil, nil, ErrnoError{Errno: ErrnoTooLarge, Msg: "native volume rdma read exceeds kernel RDMA IO max"}
	}

	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	desc, attr, sessionID, err := PostVolumeNativeReadDesc(attemptCtx, c.Client, req.VolumeServer, VolumeRDMAReadDescRequest{
		FileID:   req.FileID,
		VolumeID: req.VolumeID,
		NeedleID: req.NeedleID,
		Cookie:   req.Cookie,
		Offset:   req.Offset,
		Size:     req.Size,
	})
	if err != nil {
		c.Stats.Inc("volume_native_rdma_read_desc_errors")
		return nil, nil, err
	}
	if desc == nil || desc.RemoteAddr == 0 || desc.RKey == 0 || desc.Length == 0 {
		c.Stats.Inc("volume_native_rdma_read_desc_invalid")
		return nil, nil, ErrnoError{Errno: ErrnoNoSys, Msg: "native volume rdma descriptor is not exportable"}
	}
	if uint64(desc.Length) > req.Size {
		c.Stats.Inc("volume_native_rdma_read_desc_oversized")
		return nil, nil, ErrnoError{Errno: ErrnoIO, Msg: "native volume rdma descriptor length exceeds request size"}
	}
	if sessionID != 0 {
		leaseID := c.trackLease(req.VolumeServer, sessionID)
		if leaseID != 0 {
			desc.Reserved[0] = MarkNativeReadLease(leaseID)
			c.scheduleRelease(leaseID)
		}
	}
	c.Stats.Inc("volume_native_rdma_read_desc_success")
	c.Stats.Add("volume_native_rdma_read_desc_bytes", uint64(desc.Length))
	return desc, attr, nil
}

func PostVolumeNativeReadDesc(ctx context.Context, client *http.Client, rawURL string, reqBody VolumeRDMAReadDescRequest) (*swvfsproto.RDMADataDesc, *swvfsproto.Attr, uint64, error) {
	reqURL, err := normalizeVolumeNativeURL(rawURL, VolumeRDMAReadDescPath)
	if err != nil {
		return nil, nil, 0, err
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, nil, 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(body))
	if err != nil {
		return nil, nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient(client).Do(req)
	if err != nil {
		return nil, nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, nil, 0, volumeNativeHTTPError(reqURL, resp.StatusCode, resp.Status)
	}
	var out VolumeRDMAReadDescResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, nil, 0, err
	}
	return &out.Desc, out.Attr, out.SessionID, nil
}

func PostVolumeNativeReleaseDesc(ctx context.Context, client *http.Client, rawURL string, sessionID uint64) error {
	reqURL, err := normalizeVolumeNativeURL(rawURL, VolumeRDMAReleaseDescPath)
	if err != nil {
		return err
	}
	body, err := json.Marshal(VolumeRDMAReleaseDescRequest{SessionID: sessionID})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient(client).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return volumeNativeHTTPError(reqURL, resp.StatusCode, resp.Status)
	}
	return nil
}

func volumeNativeHTTPError(reqURL string, statusCode int, statusText string) error {
	msg := fmt.Sprintf("POST %s returned %s", reqURL, statusText)
	switch statusCode {
	case http.StatusNotFound, http.StatusNotImplemented, http.StatusServiceUnavailable:
		return ErrnoError{Errno: ErrnoNoSys, Msg: msg}
	case http.StatusRequestEntityTooLarge:
		return ErrnoError{Errno: ErrnoTooLarge, Msg: msg}
	default:
		return fmt.Errorf("%s", msg)
	}
}

func normalizeVolumeNativeURL(raw string, targetPath string) (string, error) {
	if raw == "" {
		return "", fmt.Errorf("empty volume server URL")
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if u.Scheme == "" {
		u.Scheme = "http"
	}
	u.Path = targetPath
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}

func (c *VolumeNativeRDMAReadDescriptorClient) trackLease(volumeServer string, sessionID uint64) uint64 {
	if c == nil || volumeServer == "" || sessionID == 0 {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leases == nil {
		c.leases = make(map[uint64]volumeNativeReadLease)
	}
	c.nextLeaseID++
	if c.nextLeaseID == 0 || IsNativeReadLease(c.nextLeaseID) {
		c.nextLeaseID = 1
	}
	leaseID := c.nextLeaseID
	c.leases[leaseID] = volumeNativeReadLease{
		VolumeServer: volumeServer,
		SessionID:    sessionID,
		Created:      time.Now(),
	}
	c.Stats.Inc("volume_native_rdma_read_desc_leases_created")
	return leaseID
}

func (c *VolumeNativeRDMAReadDescriptorClient) popLease(leaseID uint64) (volumeNativeReadLease, bool) {
	if c == nil || leaseID == 0 {
		return volumeNativeReadLease{}, false
	}
	leaseID = UnmarkNativeReadLease(leaseID)
	c.mu.Lock()
	defer c.mu.Unlock()
	lease, ok := c.leases[leaseID]
	if ok {
		delete(c.leases, leaseID)
		c.Stats.Inc("volume_native_rdma_read_desc_leases_popped")
	}
	return lease, ok
}

func (c *VolumeNativeRDMAReadDescriptorClient) ReleaseReadDescriptor(ctx context.Context, leaseID uint64, status int32, bytes uint64) error {
	_ = status
	_ = bytes
	if c == nil {
		return nil
	}
	c.Stats.Inc("volume_native_rdma_read_desc_release_requests")
	lease, ok := c.popLease(leaseID)
	if !ok {
		c.Stats.Inc("volume_native_rdma_read_desc_release_unknown")
		return nil
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	releaseCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := PostVolumeNativeReleaseDesc(releaseCtx, c.Client, lease.VolumeServer, lease.SessionID); err != nil {
		c.Stats.Inc("volume_native_rdma_read_desc_release_errors")
		return err
	}
	c.Stats.Inc("volume_native_rdma_read_desc_release_success")
	return nil
}

func (c *VolumeNativeRDMAReadDescriptorClient) scheduleRelease(leaseID uint64) {
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
		_ = c.ReleaseReadDescriptor(ctx, MarkNativeReadLease(leaseID), 0, 0)
	})
}
