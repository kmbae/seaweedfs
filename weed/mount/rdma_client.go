package mount

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

const (
	rdmaMountModeSidecar = "sidecar"
	rdmaMountModeNative  = "native"
	maxInt               = int(^uint(0) >> 1)
	maxUint32            = uint64(^uint32(0))
)

// RDMAMountClient provides RDMA acceleration for SeaweedFS mount operations
type RDMAMountClient struct {
	sidecarAddr   string
	mode          string
	httpClient    *http.Client
	maxConcurrent int
	timeout       time.Duration
	semaphore     chan struct{}

	// Volume lookup
	lookupFileIdFn wdclient.LookupFileIdFunctionType

	nativeRequester    weed_server.VolumeRdmaRequesterEndpoint
	nativeServiceLevel uint32
	nativeConnMu       sync.Mutex
	nativeConnections  map[string]*rdmaNativeConnection
	nativeNextConnID   atomic.Uint64

	// Read statistics
	totalRequests   atomic.Int64
	successfulReads atomic.Int64
	failedReads     atomic.Int64
	totalBytesRead  atomic.Int64
	totalLatencyNs  atomic.Int64

	nativeReadRequests   atomic.Int64
	nativeReadSuccesses  atomic.Int64
	nativeReadFailures   atomic.Int64
	nativeReadBytes      atomic.Int64
	nativeReadLatencyNs  atomic.Int64
	sidecarReadRequests  atomic.Int64
	sidecarReadSuccesses atomic.Int64
	sidecarReadFailures  atomic.Int64
	sidecarReadBytes     atomic.Int64
	sidecarReadLatencyNs atomic.Int64
	nonRdmaReadResponses atomic.Int64
	nonRdmaReadBytes     atomic.Int64
	rdmaReadBytes        atomic.Int64

	fallbackReadRequests  atomic.Int64
	fallbackReadSuccesses atomic.Int64
	fallbackReadFailures  atomic.Int64
	fallbackReadBytes     atomic.Int64

	// Write statistics
	totalWriteRequests  atomic.Int64
	successfulWrites    atomic.Int64
	failedWrites        atomic.Int64
	totalBytesWritten   atomic.Int64
	totalWriteLatencyNs atomic.Int64
}

type rdmaNativeConnection struct {
	cacheKey              string
	volumeServer          string
	providerConnectionID  uint64
	requesterConnectionID uint64
	connectedAt           time.Time
}

type volumeRdmaReadDescResponse struct {
	Desc         weed_server.VolumeRdmaDataDesc `json:"desc"`
	ConnectionID uint64                         `json:"connection_id,omitempty"`
	SessionID    uint64                         `json:"session_id,omitempty"`
}

type volumeRdmaReadDescBatchResult struct {
	Index        int                            `json:"index"`
	FileID       string                         `json:"file_id,omitempty"`
	Size         uint64                         `json:"size,omitempty"`
	Desc         weed_server.VolumeRdmaDataDesc `json:"desc,omitempty"`
	ConnectionID uint64                         `json:"connection_id,omitempty"`
	SessionID    uint64                         `json:"session_id,omitempty"`
	Status       int32                          `json:"status"`
	Error        string                         `json:"error,omitempty"`
}

type volumeRdmaReadDescBatchResponse struct {
	Results []volumeRdmaReadDescBatchResult `json:"results"`
}

type volumeRdmaReleaseDescRequest struct {
	SessionID uint64 `json:"session_id"`
}

type volumeRdmaReleaseDescBatchRequest struct {
	SessionIDs []uint64 `json:"session_ids"`
}

type volumeRdmaReleaseDescBatchResult struct {
	SessionID uint64 `json:"session_id"`
	Released  bool   `json:"released"`
	Status    int32  `json:"status"`
	Error     string `json:"error,omitempty"`
}

type volumeRdmaReleaseDescBatchResponse struct {
	Results []volumeRdmaReleaseDescBatchResult `json:"results"`
}

type RDMANeedleReadRequest struct {
	FileID string
	Offset uint64
	Size   uint64
	Dst    io.Writer
}

// RDMAReadRequest represents a request to read data via RDMA
type RDMAReadRequest struct {
	VolumeID uint32 `json:"volume_id"`
	NeedleID uint64 `json:"needle_id"`
	Cookie   uint32 `json:"cookie"`
	Offset   uint64 `json:"offset"`
	Size     uint64 `json:"size"`
}

// RDMAReadResponse represents the response from an RDMA read operation
type RDMAReadResponse struct {
	Success   bool   `json:"success"`
	IsRDMA    bool   `json:"is_rdma"`
	Source    string `json:"source"`
	Duration  string `json:"duration"`
	DataSize  int    `json:"data_size"`
	SessionID string `json:"session_id,omitempty"`
	ErrorMsg  string `json:"error,omitempty"`

	// Zero-copy optimization fields
	UseTempFile bool   `json:"use_temp_file"`
	TempFile    string `json:"temp_file"`
}

// RDMAWriteResponse represents the response from an RDMA write operation
type RDMAWriteResponse struct {
	Success bool   `json:"success"`
	IsRDMA  bool   `json:"is_rdma"`
	Source  string `json:"source"`
	FileID  string `json:"file_id"`
	Size    int    `json:"size"`
}

// RDMAHealthResponse represents the health status of the RDMA sidecar
type RDMAHealthResponse struct {
	Status string `json:"status"`
	RDMA   struct {
		Enabled   bool `json:"enabled"`
		Connected bool `json:"connected"`
	} `json:"rdma"`
	Timestamp string `json:"timestamp"`
}

// NewRDMAMountClient creates a new RDMA client for mount operations
func NewRDMAMountClient(sidecarAddr string, lookupFileIdFn wdclient.LookupFileIdFunctionType, maxConcurrent int, timeoutMs int) (*RDMAMountClient, error) {
	client := newRDMAMountClientBase(rdmaMountModeSidecar, sidecarAddr, lookupFileIdFn, maxConcurrent, timeoutMs)

	// Test connectivity and RDMA availability
	if err := client.healthCheck(); err != nil {
		return nil, fmt.Errorf("RDMA sidecar health check failed: %w", err)
	}

	glog.Infof("RDMA mount client initialized: mode=%s, sidecar=%s, maxConcurrent=%d, timeout=%v",
		client.mode, sidecarAddr, maxConcurrent, client.timeout)

	return client, nil
}

// NewNativeRDMAMountClient creates an in-process native RDMA requester for mount reads.
func NewNativeRDMAMountClient(lookupFileIdFn wdclient.LookupFileIdFunctionType, maxConcurrent int, timeoutMs int, device string, port uint32, gidIndex uint32, serviceLevel uint32) (*RDMAMountClient, error) {
	if serviceLevel > 15 {
		return nil, fmt.Errorf("RDMA service level must be 0..15")
	}
	endpoint, _, err := weed_server.NewEmbeddedVolumeRdmaTransport(weed_server.VolumeRdmaEmbeddedConfig{
		Device:   strings.TrimSpace(device),
		Port:     port,
		GIDIndex: gidIndex,
	})
	if err != nil {
		return nil, fmt.Errorf("native RDMA mount requester init failed: %w", err)
	}
	requester, ok := endpoint.(weed_server.VolumeRdmaRequesterEndpoint)
	if !ok {
		if closer, closeOK := endpoint.(interface{ Close() error }); closeOK {
			_ = closer.Close()
		}
		return nil, fmt.Errorf("native RDMA endpoint %T does not support requester reads", endpoint)
	}

	client := newRDMAMountClientBase(rdmaMountModeNative, "", lookupFileIdFn, maxConcurrent, timeoutMs)
	client.nativeRequester = requester
	client.nativeServiceLevel = serviceLevel

	glog.Infof("RDMA mount client initialized: mode=%s, device=%s, port=%d, gidIndex=%d, serviceLevel=%d, maxConcurrent=%d, timeout=%v",
		client.mode, strings.TrimSpace(device), port, gidIndex, serviceLevel, client.maxConcurrent, client.timeout)

	return client, nil
}

func newRDMAMountClientBase(mode string, sidecarAddr string, lookupFileIdFn wdclient.LookupFileIdFunctionType, maxConcurrent int, timeoutMs int) *RDMAMountClient {
	if maxConcurrent <= 0 {
		maxConcurrent = 64
	}
	if timeoutMs <= 0 {
		timeoutMs = 5000
	}
	mode = strings.TrimSpace(mode)
	if mode == "" {
		mode = rdmaMountModeSidecar
	}
	return &RDMAMountClient{
		sidecarAddr:   sidecarAddr,
		mode:          mode,
		maxConcurrent: maxConcurrent,
		timeout:       time.Duration(timeoutMs) * time.Millisecond,
		httpClient: &http.Client{
			Timeout: time.Duration(timeoutMs) * time.Millisecond,
		},
		semaphore:         make(chan struct{}, maxConcurrent),
		lookupFileIdFn:    lookupFileIdFn,
		nativeConnections: make(map[string]*rdmaNativeConnection),
	}
}

func (c *RDMAMountClient) normalizedMode() string {
	if c.mode == rdmaMountModeNative {
		return rdmaMountModeNative
	}
	return rdmaMountModeSidecar
}

// lookupVolumeLocationByFileID finds the best volume server for a given file ID
func (c *RDMAMountClient) lookupVolumeLocationByFileID(ctx context.Context, fileID string) (string, error) {
	glog.V(4).Infof("Looking up volume location for file ID %s", fileID)

	targetUrls, err := c.lookupFileIdFn(ctx, fileID)
	if err != nil {
		return "", fmt.Errorf("failed to lookup volume for file %s: %w", fileID, err)
	}

	if len(targetUrls) == 0 {
		return "", fmt.Errorf("no locations found for file %s", fileID)
	}

	// Choose the first URL and extract the server address
	targetUrl := targetUrls[0]
	// Extract server address from URL like "http://server:port/fileId"
	parts := strings.Split(targetUrl, "/")
	if len(parts) < 3 {
		return "", fmt.Errorf("invalid target URL format: %s", targetUrl)
	}
	bestAddress := fmt.Sprintf("http://%s", parts[2])

	glog.V(4).Infof("File %s located at %s", fileID, bestAddress)
	return bestAddress, nil
}

// healthCheck verifies that the RDMA sidecar is available and functioning
func (c *RDMAMountClient) healthCheck() error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("http://%s/health", c.sidecarAddr), nil)
	if err != nil {
		return fmt.Errorf("failed to create health check request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("health check request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check failed with status: %s", resp.Status)
	}

	// Parse health response
	var health RDMAHealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		return fmt.Errorf("failed to parse health response: %w", err)
	}

	if health.Status != "healthy" {
		return fmt.Errorf("sidecar reports unhealthy status: %s", health.Status)
	}

	if !health.RDMA.Enabled {
		return fmt.Errorf("RDMA is not enabled on sidecar")
	}

	if !health.RDMA.Connected {
		glog.Warningf("RDMA sidecar is healthy but not connected to RDMA engine")
	}

	return nil
}

// ReadNeedle reads data from a specific needle using RDMA acceleration
func (c *RDMAMountClient) ReadNeedle(ctx context.Context, fileID string, offset, size uint64) ([]byte, bool, error) {
	var buf bytes.Buffer
	if size <= uint64(maxInt) {
		buf.Grow(int(size))
	}
	_, isRDMA, err := c.ReadNeedleTo(ctx, fileID, offset, size, &buf)
	if err != nil {
		return nil, false, err
	}
	return buf.Bytes(), isRDMA, nil
}

// ReadNeedleTo streams data from a specific needle into dst using RDMA acceleration.
func (c *RDMAMountClient) ReadNeedleTo(ctx context.Context, fileID string, offset, size uint64, dst io.Writer) (int64, bool, error) {
	if dst == nil {
		return 0, false, fmt.Errorf("RDMA read requires destination writer")
	}
	// Acquire semaphore for concurrency control
	select {
	case c.semaphore <- struct{}{}:
		defer func() { <-c.semaphore }()
	case <-ctx.Done():
		return 0, false, ctx.Err()
	}

	c.totalRequests.Add(1)
	startTime := time.Now()

	// Lookup volume location using file ID directly
	volumeServer, err := c.lookupVolumeLocationByFileID(ctx, fileID)
	if err != nil {
		c.failedReads.Add(1)
		return 0, false, fmt.Errorf("failed to lookup volume for file %s: %w", fileID, err)
	}

	var written int64
	var isRDMA bool
	switch c.mode {
	case rdmaMountModeNative:
		written, err = c.readNeedleNativeTo(ctx, volumeServer, fileID, offset, size, dst)
		isRDMA = err == nil
	default:
		written, isRDMA, err = c.readNeedleSidecarTo(ctx, volumeServer, fileID, offset, size, dst)
	}

	duration := time.Since(startTime)
	c.totalLatencyNs.Add(duration.Nanoseconds())
	c.recordReadAttempt(c.normalizedMode(), written, isRDMA, duration, err)
	if err != nil {
		c.failedReads.Add(1)
		return written, false, err
	}

	c.successfulReads.Add(1)
	c.totalBytesRead.Add(written)

	glog.V(4).Infof("RDMA read completed: mode=%s, fileID=%s, offset=%d, requested=%d, read=%d, duration=%v, rdma=%v, volumeServer=%s",
		c.mode, fileID, offset, size, written, duration, isRDMA, volumeServer)

	return written, isRDMA, nil
}

// ReadNeedlesTo streams multiple needles into their destination writers. Native
// mode uses one descriptor batch per volume server to avoid one HTTP round trip
// per FUSE-sized read.
func (c *RDMAMountClient) ReadNeedlesTo(ctx context.Context, reads []RDMANeedleReadRequest) (int64, bool, error) {
	if len(reads) == 0 {
		return 0, true, nil
	}
	for i, read := range reads {
		if read.Dst == nil {
			return 0, false, fmt.Errorf("RDMA batch read entry %d requires destination writer", i)
		}
	}

	select {
	case c.semaphore <- struct{}{}:
		defer func() { <-c.semaphore }()
	case <-ctx.Done():
		return 0, false, ctx.Err()
	}

	c.totalRequests.Add(int64(len(reads)))
	startTime := time.Now()

	var written int64
	var isRDMA bool
	var err error
	switch c.mode {
	case rdmaMountModeNative:
		written, err = c.readNeedlesNativeTo(ctx, reads)
		isRDMA = err == nil
	default:
		written, isRDMA, err = c.readNeedlesSidecarTo(ctx, reads)
	}

	duration := time.Since(startTime)
	c.totalLatencyNs.Add(duration.Nanoseconds())
	for range reads {
		c.recordReadAttempt(c.normalizedMode(), 0, isRDMA, duration, err)
	}
	if err != nil {
		c.failedReads.Add(int64(len(reads)))
		return written, false, err
	}
	c.successfulReads.Add(int64(len(reads)))
	c.totalBytesRead.Add(written)
	if isRDMA && written > 0 {
		switch c.normalizedMode() {
		case rdmaMountModeNative:
			c.nativeReadBytes.Add(written)
			c.rdmaReadBytes.Add(written)
		case rdmaMountModeSidecar:
			c.sidecarReadBytes.Add(written)
			c.rdmaReadBytes.Add(written)
		}
	} else if written > 0 {
		c.nonRdmaReadBytes.Add(written)
	}

	glog.V(4).Infof("RDMA batch read completed: mode=%s, entries=%d, read=%d, duration=%v, rdma=%v",
		c.mode, len(reads), written, duration, isRDMA)

	return written, isRDMA, nil
}

func (c *RDMAMountClient) recordReadAttempt(mode string, written int64, isRDMA bool, duration time.Duration, err error) {
	switch mode {
	case rdmaMountModeNative:
		c.nativeReadRequests.Add(1)
		c.nativeReadLatencyNs.Add(duration.Nanoseconds())
		if err != nil {
			c.nativeReadFailures.Add(1)
			return
		}
		c.nativeReadSuccesses.Add(1)
		if written > 0 {
			c.nativeReadBytes.Add(written)
			c.rdmaReadBytes.Add(written)
		}
	default:
		c.sidecarReadRequests.Add(1)
		c.sidecarReadLatencyNs.Add(duration.Nanoseconds())
		if err != nil {
			c.sidecarReadFailures.Add(1)
			return
		}
		if isRDMA {
			c.sidecarReadSuccesses.Add(1)
			if written > 0 {
				c.sidecarReadBytes.Add(written)
				c.rdmaReadBytes.Add(written)
			}
			return
		}
		c.nonRdmaReadResponses.Add(1)
		if written > 0 {
			c.nonRdmaReadBytes.Add(written)
		}
	}
}

func (c *RDMAMountClient) RecordFallbackRead(bytesRead int64, err error) {
	c.fallbackReadRequests.Add(1)
	if bytesRead > 0 {
		c.fallbackReadBytes.Add(bytesRead)
	}
	if err != nil && err != io.EOF {
		c.fallbackReadFailures.Add(1)
		return
	}
	c.fallbackReadSuccesses.Add(1)
}

func (c *RDMAMountClient) readNeedleSidecarTo(ctx context.Context, volumeServer string, fileID string, offset, size uint64, dst io.Writer) (int64, bool, error) {
	if strings.TrimSpace(c.sidecarAddr) == "" {
		return 0, false, fmt.Errorf("RDMA sidecar address is required")
	}

	// Prepare request URL with file_id parameter (simpler than individual components)
	reqURL := fmt.Sprintf("http://%s/read?file_id=%s&offset=%d&size=%d&volume_server=%s",
		c.sidecarAddr, url.QueryEscape(fileID), offset, size, url.QueryEscape(volumeServer))

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return 0, false, fmt.Errorf("failed to create RDMA request: %w", err)
	}

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, false, fmt.Errorf("RDMA request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, false, fmt.Errorf("RDMA read failed with status %s: %s", resp.Status, string(body))
	}

	// Check if response indicates RDMA was used
	isRDMA := resp.Header.Get("X-RDMA-Used") == "true"

	// Check for zero-copy temp file optimization
	tempFilePath := resp.Header.Get("X-Temp-File")
	useTempFile := resp.Header.Get("X-Use-Temp-File") == "true"

	if useTempFile && tempFilePath != "" {
		// Zero-copy path: read from temp file (page cache)
		glog.V(4).Infof("🔥 Using zero-copy temp file: %s", tempFilePath)

		n, err := c.copyFromTempFile(tempFilePath, dst)
		if err != nil {
			glog.V(2).Infof("Zero-copy failed, falling back to HTTP body: %v", err)
			// Fall back to reading HTTP body
			n, err = io.Copy(dst, resp.Body)
		}
		go c.cleanupTempFile(tempFilePath)
		if err != nil {
			return n, false, fmt.Errorf("failed to read RDMA response: %w", err)
		}
		return n, isRDMA, nil
	}

	n, err := io.Copy(dst, resp.Body)
	if err != nil {
		return n, false, fmt.Errorf("failed to read RDMA response: %w", err)
	}
	return n, isRDMA, nil
}

func (c *RDMAMountClient) readNeedlesSidecarTo(ctx context.Context, reads []RDMANeedleReadRequest) (int64, bool, error) {
	var total int64
	allRDMA := true
	for _, read := range reads {
		if read.Size == 0 {
			continue
		}
		volumeServer, err := c.lookupVolumeLocationByFileID(ctx, read.FileID)
		if err != nil {
			return total, false, fmt.Errorf("failed to lookup volume for file %s: %w", read.FileID, err)
		}
		n, isRDMA, err := c.readNeedleSidecarTo(ctx, volumeServer, read.FileID, read.Offset, read.Size, read.Dst)
		total += n
		if err != nil {
			return total, false, err
		}
		if !isRDMA {
			allRDMA = false
		}
	}
	return total, allRDMA, nil
}

type rdmaNeedleReadForVolume struct {
	index int
	read  RDMANeedleReadRequest
}

func (c *RDMAMountClient) readNeedlesNativeTo(ctx context.Context, reads []RDMANeedleReadRequest) (int64, error) {
	if c.nativeRequester == nil {
		return 0, fmt.Errorf("native RDMA requester is not configured")
	}

	groups := make(map[string][]rdmaNeedleReadForVolume)
	for i, read := range reads {
		if read.Size == 0 {
			continue
		}
		if read.Size > maxUint32 {
			return 0, fmt.Errorf("native RDMA read is too large for one descriptor: %d", read.Size)
		}
		volumeServer, err := c.lookupVolumeLocationByFileID(ctx, read.FileID)
		if err != nil {
			return 0, fmt.Errorf("failed to lookup volume for file %s: %w", read.FileID, err)
		}
		groups[volumeServer] = append(groups[volumeServer], rdmaNeedleReadForVolume{
			index: i,
			read:  read,
		})
	}

	var total int64
	for volumeServer, group := range groups {
		n, err := c.readNeedlesNativeForVolumeTo(ctx, volumeServer, group)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (c *RDMAMountClient) readNeedlesNativeForVolumeTo(ctx context.Context, volumeServer string, group []rdmaNeedleReadForVolume) (int64, error) {
	if len(group) == 0 {
		return 0, nil
	}
	if len(group) == 1 {
		read := group[0].read
		return c.readNeedleNativeTo(ctx, volumeServer, read.FileID, read.Offset, read.Size, read.Dst)
	}

	entries := make([]weed_server.VolumeRdmaReadRequest, len(group))
	conns := make([]*rdmaNativeConnection, len(group))
	parallelism := c.nativeReadBatchParallelism(len(group))
	for i, item := range group {
		conn, err := c.ensureNativeConnectionSlot(ctx, volumeServer, i%parallelism)
		if err != nil {
			return 0, err
		}
		conns[i] = conn

		fid, err := needle.ParseFileIdFromString(item.read.FileID)
		if err != nil {
			return 0, fmt.Errorf("parse file id %s: %w", item.read.FileID, err)
		}
		entries[i] = weed_server.VolumeRdmaReadRequest{
			ConnectionID: conn.providerConnectionID,
			FileID:       item.read.FileID,
			VolumeID:     uint32(fid.VolumeId),
			NeedleID:     uint64(fid.Key),
			Cookie:       uint32(fid.Cookie),
			Offset:       item.read.Offset,
			Size:         item.read.Size,
		}
	}

	var resp volumeRdmaReadDescBatchResponse
	if err := c.doNativeJSON(ctx, http.MethodPost, volumeServer, weed_server.VolumeRdmaNativeReadDescBatchPath, nil, weed_server.VolumeRdmaReadDescBatchRequest{Entries: entries}, &resp); err != nil {
		glog.V(2).Infof("native RDMA read descriptor batch failed for %s, using single descriptors: %v", volumeServer, err)
		var total int64
		for _, item := range group {
			read := item.read
			n, singleErr := c.readNeedleNativeTo(ctx, volumeServer, read.FileID, read.Offset, read.Size, read.Dst)
			total += n
			if singleErr != nil {
				return total, singleErr
			}
		}
		return total, nil
	}
	if len(resp.Results) != len(group) {
		return 0, fmt.Errorf("native RDMA read descriptor batch returned %d results for %d entries", len(resp.Results), len(group))
	}

	descs := make([]weed_server.VolumeRdmaDataDesc, len(group))
	sessionIDs := make([]uint64, 0, len(group))
	var validationErr error
	for _, result := range resp.Results {
		if result.Index < 0 || result.Index >= len(group) {
			validationErr = fmt.Errorf("native RDMA read descriptor batch returned invalid index %d", result.Index)
			continue
		}
		if result.SessionID != 0 {
			sessionIDs = append(sessionIDs, result.SessionID)
		}
		if result.Status < http.StatusOK || result.Status >= http.StatusMultipleChoices {
			if validationErr == nil {
				validationErr = fmt.Errorf("native RDMA read descriptor batch entry %d failed with status %d: %s", result.Index, result.Status, result.Error)
			}
			continue
		}
		read := group[result.Index].read
		if result.SessionID == 0 {
			if validationErr == nil {
				validationErr = fmt.Errorf("native RDMA read descriptor batch entry %d missing session_id", result.Index)
			}
			continue
		}
		if result.Desc.RemoteAddr == 0 || result.Desc.RKey == 0 || result.Desc.Length == 0 {
			if validationErr == nil {
				validationErr = fmt.Errorf("native RDMA read descriptor batch entry %d is not exportable", result.Index)
			}
			continue
		}
		if uint64(result.Desc.Length) < read.Size {
			if validationErr == nil {
				validationErr = fmt.Errorf("native RDMA read descriptor batch entry %d length %d is smaller than requested size %d", result.Index, result.Desc.Length, read.Size)
			}
			continue
		}
		result.Desc.Length = uint32(read.Size)
		descs[result.Index] = result.Desc
	}
	if validationErr != nil {
		_ = c.releaseNativeReadDescs(context.Background(), volumeServer, sessionIDs)
		return 0, validationErr
	}

	total, readErr := c.readNativeBatchDescsParallel(ctx, volumeServer, group, conns, descs, parallelism)

	releaseErr := c.releaseNativeReadDescs(context.Background(), volumeServer, sessionIDs)
	if readErr != nil {
		return total, readErr
	}
	if releaseErr != nil {
		glog.V(2).Infof("native RDMA read descriptor batch release failed for %s sessions=%v: %v", volumeServer, sessionIDs, releaseErr)
	}
	return total, nil
}

func (c *RDMAMountClient) nativeReadBatchParallelism(entries int) int {
	if entries <= 1 {
		return 1
	}
	parallelism := c.maxConcurrent
	if parallelism <= 0 {
		parallelism = 1
	}
	if parallelism > entries {
		parallelism = entries
	}
	return parallelism
}

func (c *RDMAMountClient) readNativeBatchDescsParallel(ctx context.Context, volumeServer string, group []rdmaNeedleReadForVolume, conns []*rdmaNativeConnection, descs []weed_server.VolumeRdmaDataDesc, parallelism int) (int64, error) {
	if parallelism <= 1 || len(group) <= 1 {
		var total int64
		for i, item := range group {
			counter := &countingWriter{w: item.read.Dst}
			if err := c.nativeRequester.ReadRemoteToFor(ctx, conns[i].requesterConnectionID, descs[i], c.timeout, counter); err != nil {
				c.dropNativeConnection(volumeServer, conns[i])
				return total + counter.n, fmt.Errorf("native RDMA READ failed: %w", err)
			}
			total += counter.n
			if counter.n != int64(item.read.Size) {
				return total, fmt.Errorf("native RDMA READ copied %d bytes, expected %d", counter.n, item.read.Size)
			}
		}
		return total, nil
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type readResult struct {
		n   int64
		err error
	}
	results := make([]readResult, len(group))
	sem := make(chan struct{}, parallelism)
	var wg sync.WaitGroup
	for i, item := range group {
		i, item := i, item
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-readCtx.Done():
				results[i].err = readCtx.Err()
				return
			}

			counter := &countingWriter{w: item.read.Dst}
			if err := c.nativeRequester.ReadRemoteToFor(readCtx, conns[i].requesterConnectionID, descs[i], c.timeout, counter); err != nil {
				c.dropNativeConnection(volumeServer, conns[i])
				results[i] = readResult{n: counter.n, err: fmt.Errorf("native RDMA READ failed: %w", err)}
				cancel()
				return
			}
			if counter.n != int64(item.read.Size) {
				results[i] = readResult{n: counter.n, err: fmt.Errorf("native RDMA READ copied %d bytes, expected %d", counter.n, item.read.Size)}
				cancel()
				return
			}
			results[i] = readResult{n: counter.n}
		}()
	}
	wg.Wait()

	var total int64
	for _, result := range results {
		total += result.n
		if result.err != nil {
			return total, result.err
		}
	}
	return total, nil
}

func (c *RDMAMountClient) readNeedleNativeTo(ctx context.Context, volumeServer string, fileID string, offset, size uint64, dst io.Writer) (int64, error) {
	if c.nativeRequester == nil {
		return 0, fmt.Errorf("native RDMA requester is not configured")
	}
	if size == 0 {
		return 0, nil
	}
	if size > maxUint32 {
		return 0, fmt.Errorf("native RDMA read is too large for one descriptor: %d", size)
	}

	fid, err := needle.ParseFileIdFromString(fileID)
	if err != nil {
		return 0, fmt.Errorf("parse file id %s: %w", fileID, err)
	}

	conn, err := c.ensureNativeConnection(ctx, volumeServer)
	if err != nil {
		return 0, err
	}

	var readDesc volumeRdmaReadDescResponse
	err = c.doNativeJSON(ctx, http.MethodPost, volumeServer, weed_server.VolumeRdmaNativeReadDescPath, nil, weed_server.VolumeRdmaReadRequest{
		ConnectionID: conn.providerConnectionID,
		FileID:       fileID,
		VolumeID:     uint32(fid.VolumeId),
		NeedleID:     uint64(fid.Key),
		Cookie:       uint32(fid.Cookie),
		Offset:       offset,
		Size:         size,
	}, &readDesc)
	if err != nil {
		return 0, fmt.Errorf("native RDMA read descriptor failed: %w", err)
	}
	if readDesc.SessionID == 0 {
		return 0, fmt.Errorf("native RDMA read descriptor missing session_id")
	}
	if readDesc.Desc.RemoteAddr == 0 || readDesc.Desc.RKey == 0 || readDesc.Desc.Length == 0 {
		_ = c.releaseNativeReadDesc(context.Background(), volumeServer, readDesc.SessionID)
		return 0, fmt.Errorf("native RDMA read descriptor is not exportable")
	}
	if uint64(readDesc.Desc.Length) < size {
		_ = c.releaseNativeReadDesc(context.Background(), volumeServer, readDesc.SessionID)
		return 0, fmt.Errorf("native RDMA read descriptor length %d is smaller than requested size %d", readDesc.Desc.Length, size)
	}
	readDesc.Desc.Length = uint32(size)

	counter := &countingWriter{w: dst}
	readErr := c.nativeRequester.ReadRemoteToFor(ctx, conn.requesterConnectionID, readDesc.Desc, c.timeout, counter)
	releaseErr := c.releaseNativeReadDesc(context.Background(), volumeServer, readDesc.SessionID)
	if readErr != nil {
		c.dropNativeConnection(volumeServer, conn)
		return counter.n, fmt.Errorf("native RDMA READ failed: %w", readErr)
	}
	if releaseErr != nil {
		glog.V(2).Infof("native RDMA read descriptor release failed for %s session=%d: %v", volumeServer, readDesc.SessionID, releaseErr)
	}
	if counter.n != int64(size) {
		return counter.n, fmt.Errorf("native RDMA READ copied %d bytes, expected %d", counter.n, size)
	}
	return counter.n, nil
}

func (c *RDMAMountClient) ensureNativeConnection(ctx context.Context, volumeServer string) (*rdmaNativeConnection, error) {
	return c.ensureNativeConnectionSlot(ctx, volumeServer, 0)
}

func (c *RDMAMountClient) ensureNativeConnectionSlot(ctx context.Context, volumeServer string, slot int) (*rdmaNativeConnection, error) {
	c.nativeConnMu.Lock()
	defer c.nativeConnMu.Unlock()

	cacheKey := nativeConnectionCacheKey(volumeServer, slot)
	if conn := c.nativeConnections[cacheKey]; conn != nil {
		return conn, nil
	}

	connectionID := c.nextNativeConnectionID()
	providerLocal, err := c.getNativeLocalEndpoint(ctx, volumeServer, connectionID)
	if err != nil {
		return nil, fmt.Errorf("native RDMA provider local endpoint failed: %w", err)
	}
	providerConnectionID := connectionID
	if providerLocal.ConnectionID != 0 {
		providerConnectionID = providerLocal.ConnectionID
	}

	requesterLocal, requesterConnectionID, err := c.nativeRequester.RequesterLocalEndpointFor(ctx, connectionID)
	if err != nil {
		return nil, fmt.Errorf("native RDMA requester local endpoint failed: %w", err)
	}
	if requesterConnectionID == 0 {
		requesterConnectionID = connectionID
	}
	if !requesterLocal.ReadyForConnect() {
		return nil, fmt.Errorf("native RDMA requester endpoint is not ready")
	}

	if err := c.connectNativeProvider(ctx, volumeServer, providerConnectionID, requesterLocal); err != nil {
		c.dropNativeConnectionLocked(cacheKey, requesterConnectionID, providerConnectionID)
		return nil, fmt.Errorf("native RDMA provider connect failed: %w", err)
	}
	providerRemote, err := providerLocal.RemoteInfo(c.nativeServiceLevel)
	if err != nil {
		c.dropNativeConnectionLocked(cacheKey, requesterConnectionID, providerConnectionID)
		return nil, fmt.Errorf("native RDMA provider endpoint metadata failed: %w", err)
	}
	if err := c.nativeRequester.RequesterConnectEndpointFor(ctx, requesterConnectionID, providerRemote); err != nil {
		c.dropNativeConnectionLocked(cacheKey, requesterConnectionID, providerConnectionID)
		return nil, fmt.Errorf("native RDMA requester connect failed: %w", err)
	}

	conn := &rdmaNativeConnection{
		cacheKey:              cacheKey,
		volumeServer:          volumeServer,
		providerConnectionID:  providerConnectionID,
		requesterConnectionID: requesterConnectionID,
		connectedAt:           time.Now(),
	}
	c.nativeConnections[cacheKey] = conn
	glog.Infof("native RDMA mount connected: volumeServer=%s providerConnectionID=%d requesterConnectionID=%d", volumeServer, providerConnectionID, requesterConnectionID)
	return conn, nil
}

func nativeConnectionCacheKey(volumeServer string, slot int) string {
	if slot <= 0 {
		return volumeServer
	}
	return fmt.Sprintf("%s#%d", volumeServer, slot)
}

func (c *RDMAMountClient) nextNativeConnectionID() uint64 {
	id := c.nativeNextConnID.Add(1)
	if id == 0 {
		id = c.nativeNextConnID.Add(1)
	}
	return id
}

func (c *RDMAMountClient) getNativeLocalEndpoint(ctx context.Context, volumeServer string, connectionID uint64) (weed_server.VolumeRdmaEndpointInfo, error) {
	var endpoint weed_server.VolumeRdmaEndpointInfo
	q := url.Values{}
	if connectionID != 0 {
		q.Set("connection_id", fmt.Sprintf("%d", connectionID))
	}
	if err := c.doNativeJSON(ctx, http.MethodGet, volumeServer, weed_server.VolumeRdmaNativeLocalPath, q, nil, &endpoint); err != nil {
		return endpoint, err
	}
	if !endpoint.ReadyForConnect() {
		return endpoint, fmt.Errorf("native RDMA provider endpoint is not ready")
	}
	return endpoint, nil
}

func (c *RDMAMountClient) connectNativeProvider(ctx context.Context, volumeServer string, connectionID uint64, requesterLocal weed_server.VolumeRdmaEndpointInfo) error {
	q := url.Values{}
	if connectionID != 0 {
		q.Set("connection_id", fmt.Sprintf("%d", connectionID))
	}
	if c.nativeServiceLevel != 0 {
		q.Set("sl", fmt.Sprintf("%d", c.nativeServiceLevel))
	}
	var resp map[string]bool
	return c.doNativeJSON(ctx, http.MethodPost, volumeServer, weed_server.VolumeRdmaNativeConnectPath, q, requesterLocal, &resp)
}

func (c *RDMAMountClient) releaseNativeReadDesc(ctx context.Context, volumeServer string, sessionID uint64) error {
	if sessionID == 0 {
		return nil
	}
	releaseCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var resp map[string]bool
	return c.doNativeJSON(releaseCtx, http.MethodPost, volumeServer, weed_server.VolumeRdmaNativeReleaseDescPath, nil, volumeRdmaReleaseDescRequest{SessionID: sessionID}, &resp)
}

func (c *RDMAMountClient) releaseNativeReadDescs(ctx context.Context, volumeServer string, sessionIDs []uint64) error {
	filtered := sessionIDs[:0]
	for _, sessionID := range sessionIDs {
		if sessionID != 0 {
			filtered = append(filtered, sessionID)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	if len(filtered) == 1 {
		return c.releaseNativeReadDesc(ctx, volumeServer, filtered[0])
	}

	releaseCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	var resp volumeRdmaReleaseDescBatchResponse
	err := c.doNativeJSON(releaseCtx, http.MethodPost, volumeServer, weed_server.VolumeRdmaNativeReleaseDescBatchPath, nil, volumeRdmaReleaseDescBatchRequest{SessionIDs: filtered}, &resp)
	if err != nil {
		for _, sessionID := range filtered {
			_ = c.releaseNativeReadDesc(context.Background(), volumeServer, sessionID)
		}
		return err
	}
	if len(resp.Results) != len(filtered) {
		return fmt.Errorf("native RDMA release descriptor batch returned %d results for %d sessions", len(resp.Results), len(filtered))
	}
	for _, result := range resp.Results {
		if result.Status < http.StatusOK || result.Status >= http.StatusMultipleChoices || !result.Released {
			return fmt.Errorf("native RDMA release descriptor batch session %d failed with status %d: %s", result.SessionID, result.Status, result.Error)
		}
	}
	return nil
}

func (c *RDMAMountClient) doNativeJSON(ctx context.Context, method string, volumeServer string, endpointPath string, query url.Values, body interface{}, out interface{}) error {
	base := strings.TrimRight(volumeServer, "/")
	reqURL := base + endpointPath
	if len(query) != 0 {
		reqURL += "?" + query.Encode()
	}

	var bodyReader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return err
		}
		bodyReader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, reqURL, bodyReader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		payload, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s %s failed with status %s: %s", method, endpointPath, resp.Status, strings.TrimSpace(string(payload)))
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode %s response: %w", endpointPath, err)
	}
	return nil
}

func (c *RDMAMountClient) dropNativeConnection(volumeServer string, conn *rdmaNativeConnection) {
	c.nativeConnMu.Lock()
	defer c.nativeConnMu.Unlock()
	if conn == nil {
		delete(c.nativeConnections, volumeServer)
		return
	}
	c.dropNativeConnectionLocked(conn.cacheKey, conn.requesterConnectionID, conn.providerConnectionID)
}

func (c *RDMAMountClient) dropNativeConnectionLocked(cacheKey string, requesterConnectionID uint64, providerConnectionID uint64) {
	if existing := c.nativeConnections[cacheKey]; existing != nil {
		if requesterConnectionID == 0 || existing.requesterConnectionID == requesterConnectionID {
			delete(c.nativeConnections, cacheKey)
		}
	}
	glog.V(2).Infof("native RDMA mount connection dropped: key=%s providerConnectionID=%d requesterConnectionID=%d", cacheKey, providerConnectionID, requesterConnectionID)
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}

// WriteNeedle writes data to a volume server via the RDMA sidecar
func (c *RDMAMountClient) WriteNeedle(ctx context.Context, fileID string, data []byte, volumeServer string) (*RDMAWriteResponse, error) {
	select {
	case c.semaphore <- struct{}{}:
		defer func() { <-c.semaphore }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	c.totalWriteRequests.Add(1)
	startTime := time.Now()

	if volumeServer == "" {
		var err error
		volumeServer, err = c.lookupVolumeLocationByFileID(ctx, fileID)
		if err != nil {
			c.failedWrites.Add(1)
			return nil, fmt.Errorf("failed to lookup volume for file %s: %w", fileID, err)
		}
	}

	reqURL := fmt.Sprintf("http://%s/write?file_id=%s&volume_server=%s",
		c.sidecarAddr,
		url.QueryEscape(fileID),
		url.QueryEscape(volumeServer))

	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, bytes.NewReader(data))
	if err != nil {
		c.failedWrites.Add(1)
		return nil, fmt.Errorf("failed to create RDMA write request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.failedWrites.Add(1)
		return nil, fmt.Errorf("RDMA write request failed: %w", err)
	}
	defer resp.Body.Close()

	duration := time.Since(startTime)
	c.totalWriteLatencyNs.Add(duration.Nanoseconds())

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		c.failedWrites.Add(1)
		return nil, fmt.Errorf("failed to read write response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		c.failedWrites.Add(1)
		return nil, fmt.Errorf("RDMA write failed with status %s: %s", resp.Status, string(body))
	}

	var writeResp RDMAWriteResponse
	if err := json.Unmarshal(body, &writeResp); err != nil {
		c.failedWrites.Add(1)
		return nil, fmt.Errorf("failed to parse write response: %w", err)
	}

	c.successfulWrites.Add(1)
	c.totalBytesWritten.Add(int64(len(data)))

	glog.V(4).Infof("RDMA write completed: fileID=%s, size=%d, duration=%v, rdma=%v, source=%s",
		fileID, len(data), duration, writeResp.IsRDMA, writeResp.Source)

	return &writeResp, nil
}

// cleanupTempFile requests cleanup of a temp file from the sidecar
func (c *RDMAMountClient) cleanupTempFile(tempFilePath string) {
	if tempFilePath == "" {
		return
	}

	// Give the page cache a brief moment to be utilized before cleanup
	// This preserves the zero-copy performance window
	time.Sleep(100 * time.Millisecond)

	// Call sidecar cleanup endpoint
	cleanupURL := fmt.Sprintf("http://%s/cleanup?temp_file=%s", c.sidecarAddr, url.QueryEscape(tempFilePath))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "DELETE", cleanupURL, nil)
	if err != nil {
		glog.V(2).Infof("Failed to create cleanup request for %s: %v", tempFilePath, err)
		return
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		glog.V(2).Infof("Failed to cleanup temp file %s: %v", tempFilePath, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		glog.V(4).Infof("🧹 Temp file cleaned up: %s", tempFilePath)
	} else {
		glog.V(2).Infof("Cleanup failed for %s: status %s", tempFilePath, resp.Status)
	}
}

// GetStats returns current RDMA client statistics
func (c *RDMAMountClient) GetStats() map[string]interface{} {
	totalRequests := c.totalRequests.Load()
	successfulReads := c.successfulReads.Load()
	failedReads := c.failedReads.Load()
	totalBytesRead := c.totalBytesRead.Load()
	totalLatencyNs := c.totalLatencyNs.Load()
	nativeReadRequests := c.nativeReadRequests.Load()
	nativeReadLatencyNs := c.nativeReadLatencyNs.Load()
	sidecarReadRequests := c.sidecarReadRequests.Load()
	sidecarReadLatencyNs := c.sidecarReadLatencyNs.Load()
	totalWriteRequests := c.totalWriteRequests.Load()
	successfulWrites := c.successfulWrites.Load()
	failedWrites := c.failedWrites.Load()
	totalBytesWritten := c.totalBytesWritten.Load()
	totalWriteLatencyNs := c.totalWriteLatencyNs.Load()
	c.nativeConnMu.Lock()
	nativeConnections := len(c.nativeConnections)
	c.nativeConnMu.Unlock()

	readSuccessRate := float64(0)
	avgReadLatencyNs := int64(0)
	if totalRequests > 0 {
		readSuccessRate = float64(successfulReads) / float64(totalRequests) * 100
		avgReadLatencyNs = totalLatencyNs / totalRequests
	}
	avgNativeReadLatencyNs := int64(0)
	if nativeReadRequests > 0 {
		avgNativeReadLatencyNs = nativeReadLatencyNs / nativeReadRequests
	}
	avgSidecarReadLatencyNs := int64(0)
	if sidecarReadRequests > 0 {
		avgSidecarReadLatencyNs = sidecarReadLatencyNs / sidecarReadRequests
	}

	writeSuccessRate := float64(0)
	avgWriteLatencyNs := int64(0)
	if totalWriteRequests > 0 {
		writeSuccessRate = float64(successfulWrites) / float64(totalWriteRequests) * 100
		avgWriteLatencyNs = totalWriteLatencyNs / totalWriteRequests
	}

	return map[string]interface{}{
		"mode":                    c.mode,
		"sidecar_addr":            c.sidecarAddr,
		"max_concurrent":          c.maxConcurrent,
		"timeout_ms":              int(c.timeout / time.Millisecond),
		"native_connections":      nativeConnections,
		"total_read_requests":     totalRequests,
		"successful_reads":        successfulReads,
		"failed_reads":            failedReads,
		"read_success_rate_pct":   fmt.Sprintf("%.1f", readSuccessRate),
		"total_bytes_read":        totalBytesRead,
		"rdma_bytes_read":         c.rdmaReadBytes.Load(),
		"avg_read_latency_ms":     fmt.Sprintf("%.3f", float64(avgReadLatencyNs)/1000000),
		"native_read_requests":    nativeReadRequests,
		"native_read_successes":   c.nativeReadSuccesses.Load(),
		"native_read_failures":    c.nativeReadFailures.Load(),
		"native_read_bytes":       c.nativeReadBytes.Load(),
		"native_avg_read_ms":      fmt.Sprintf("%.3f", float64(avgNativeReadLatencyNs)/1000000),
		"sidecar_read_requests":   sidecarReadRequests,
		"sidecar_read_successes":  c.sidecarReadSuccesses.Load(),
		"sidecar_read_failures":   c.sidecarReadFailures.Load(),
		"sidecar_read_bytes":      c.sidecarReadBytes.Load(),
		"sidecar_avg_read_ms":     fmt.Sprintf("%.3f", float64(avgSidecarReadLatencyNs)/1000000),
		"non_rdma_read_responses": c.nonRdmaReadResponses.Load(),
		"non_rdma_read_bytes":     c.nonRdmaReadBytes.Load(),
		"fallback_read_requests":  c.fallbackReadRequests.Load(),
		"fallback_read_successes": c.fallbackReadSuccesses.Load(),
		"fallback_read_failures":  c.fallbackReadFailures.Load(),
		"fallback_read_bytes":     c.fallbackReadBytes.Load(),
		"total_write_requests":    totalWriteRequests,
		"successful_writes":       successfulWrites,
		"failed_writes":           failedWrites,
		"write_success_rate_pct":  fmt.Sprintf("%.1f", writeSuccessRate),
		"total_bytes_written":     totalBytesWritten,
		"avg_write_latency_ms":    fmt.Sprintf("%.3f", float64(avgWriteLatencyNs)/1000000),
	}
}

// Close shuts down the RDMA client and releases resources
func (c *RDMAMountClient) Close() error {
	// No need to close semaphore channel; closing it may cause panics if goroutines are still using it.
	// The semaphore will be garbage collected when the client is no longer referenced.

	// Log final statistics
	stats := c.GetStats()
	glog.Infof("RDMA mount client closing: %+v", stats)

	if closer, ok := c.nativeRequester.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

// IsHealthy checks if the RDMA sidecar is currently healthy
func (c *RDMAMountClient) IsHealthy() bool {
	if c.mode == rdmaMountModeNative {
		if c.nativeRequester == nil {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		local, _, err := c.nativeRequester.RequesterLocalEndpointFor(ctx, 0)
		return err == nil && local.ReadyForConnect()
	}
	err := c.healthCheck()
	return err == nil
}

// copyFromTempFile performs zero-copy-ish read from temp file using page cache
func (c *RDMAMountClient) copyFromTempFile(tempFilePath string, dst io.Writer) (int64, error) {
	if tempFilePath == "" {
		return 0, fmt.Errorf("empty temp file path")
	}

	// Open temp file for reading
	file, err := os.Open(tempFilePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open temp file %s: %w", tempFilePath, err)
	}
	defer file.Close()

	// Copy from temp file (this should be served from page cache)
	n, err := io.Copy(dst, file)
	if err != nil {
		return n, fmt.Errorf("failed to read from temp file: %w", err)
	}

	glog.V(4).Infof("🔥 Zero-copy read: %d bytes from temp file %s", n, tempFilePath)

	return n, nil
}
