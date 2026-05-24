// Package seaweedfs provides SeaweedFS-specific RDMA integration
package seaweedfs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"seaweedfs-rdma-sidecar/pkg/rdma"
	"seaweedfs-rdma-sidecar/pkg/volumeread"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/sirupsen/logrus"
)

// SeaweedFSRDMAClient provides SeaweedFS-specific RDMA operations
type SeaweedFSRDMAClient struct {
	rdmaClient      *rdma.Client
	localReader     *volumeread.Reader
	logger          *logrus.Logger
	volumeServerURL string
	enabled         bool

	// Zero-copy optimization
	tempDir     string
	useZeroCopy bool
}

// Config holds configuration for the SeaweedFS RDMA client
type Config struct {
	RDMASocketPath  string
	VolumeServerURL string
	Enabled         bool
	DefaultTimeout  time.Duration
	Logger          *logrus.Logger

	// Zero-copy optimization
	TempDir     string // Directory for temp files (default: /tmp/rdma-cache)
	UseZeroCopy bool   // Enable zero-copy via temp files

	// Connection pooling options
	EnablePooling  bool          // Enable RDMA connection pooling (default: true)
	MaxConnections int           // Max connections in pool (default: 10)
	MaxIdleTime    time.Duration // Max idle time before connection cleanup (default: 5min)

	// Local volume directory for colocated sidecar (shared PVC with volume server).
	VolumeDataDir    string
	VolumeIdxDir     string
	VolumeCollection string
}

// NeedleReadRequest represents a SeaweedFS needle read request
type NeedleReadRequest struct {
	VolumeID     uint32
	NeedleID     uint64
	Cookie       uint32
	Offset       uint64
	Size         uint64
	VolumeServer string // Override volume server URL for this request
}

// NeedleReadResponse represents the result of a needle read
type NeedleReadResponse struct {
	Data      []byte
	IsRDMA    bool
	Latency   time.Duration
	Source    string // "rdma" or "http"
	SessionID string

	// Zero-copy optimization fields
	TempFilePath string // Path to temp file with data (for zero-copy)
	UseTempFile  bool   // Whether to use temp file instead of Data
}

// NewSeaweedFSRDMAClient creates a new SeaweedFS RDMA client
func NewSeaweedFSRDMAClient(config *Config) (*SeaweedFSRDMAClient, error) {
	if config.Logger == nil {
		config.Logger = logrus.New()
		config.Logger.SetLevel(logrus.InfoLevel)
	}

	var rdmaClient *rdma.Client
	if config.Enabled && config.RDMASocketPath != "" {
		rdmaConfig := &rdma.Config{
			EngineSocketPath: config.RDMASocketPath,
			DefaultTimeout:   config.DefaultTimeout,
			Logger:           config.Logger,
			EnablePooling:    config.EnablePooling,
			MaxConnections:   config.MaxConnections,
			MaxIdleTime:      config.MaxIdleTime,
		}
		rdmaClient = rdma.NewClient(rdmaConfig)
	}

	// Setup temp directory for zero-copy optimization
	tempDir := config.TempDir
	if tempDir == "" {
		tempDir = "/tmp/rdma-cache"
	}

	if config.UseZeroCopy {
		if err := os.MkdirAll(tempDir, 0755); err != nil {
			config.Logger.WithError(err).Warn("Failed to create temp directory, disabling zero-copy")
			config.UseZeroCopy = false
		}
	}

	client := &SeaweedFSRDMAClient{
		rdmaClient:      rdmaClient,
		logger:          config.Logger,
		volumeServerURL: config.VolumeServerURL,
		enabled:         config.Enabled,
		tempDir:         tempDir,
		useZeroCopy:     config.UseZeroCopy,
	}
	if config.VolumeDataDir != "" {
		client.localReader = volumeread.NewReader(config.VolumeDataDir, config.VolumeIdxDir, config.VolumeCollection)
	}
	return client, nil
}

// Start initializes the RDMA client connection
func (c *SeaweedFSRDMAClient) Start(ctx context.Context) error {
	if !c.enabled || c.rdmaClient == nil {
		c.logger.Info("🔄 RDMA disabled, using HTTP fallback only")
		return nil
	}

	c.logger.Info("🚀 Starting SeaweedFS RDMA client...")

	if err := c.rdmaClient.Connect(ctx); err != nil {
		c.logger.WithError(err).Error("❌ Failed to connect to RDMA engine")
		return fmt.Errorf("failed to connect to RDMA engine: %w", err)
	}

	c.logger.Info("✅ SeaweedFS RDMA client started successfully")
	return nil
}

// Stop shuts down the RDMA client
func (c *SeaweedFSRDMAClient) Stop() {
	if c.localReader != nil {
		c.localReader.Close()
	}
	if c.rdmaClient != nil {
		c.rdmaClient.Disconnect()
		c.logger.Info("🔌 SeaweedFS RDMA client stopped")
	}
}

// IsEnabled returns true if RDMA is enabled and available
func (c *SeaweedFSRDMAClient) IsEnabled() bool {
	return c.enabled && c.rdmaClient != nil && c.rdmaClient.IsConnected()
}

// ReadNeedle reads a needle using RDMA fast path or HTTP fallback
func (c *SeaweedFSRDMAClient) ReadNeedle(ctx context.Context, req *NeedleReadRequest) (*NeedleReadResponse, error) {
	start := time.Now()
	var rdmaErr error

	// Try RDMA fast path first
	if c.IsEnabled() {
		c.logger.WithFields(logrus.Fields{
			"volume_id": req.VolumeID,
			"needle_id": req.NeedleID,
			"offset":    req.Offset,
			"size":      req.Size,
		}).Debug("🚀 Attempting RDMA fast path")

		rdmaReq := &rdma.ReadRequest{
			VolumeID: req.VolumeID,
			NeedleID: req.NeedleID,
			Cookie:   req.Cookie,
			Offset:   req.Offset,
			Size:     req.Size,
		}

		_, err := c.rdmaClient.Read(ctx, rdmaReq)
		if err != nil {
			c.logger.WithError(err).Warn("⚠️  RDMA read failed, falling back to HTTP")
			rdmaErr = err
		} else {
			data, source, fetchErr := c.fetchNeedleData(ctx, req)
			if fetchErr != nil {
				c.logger.WithError(fetchErr).Warn("⚠️  RDMA session ok but data fetch failed, falling back to HTTP")
				rdmaErr = fetchErr
			} else {
				isRealRDMA := c.rdmaClient.IsRealRdma()
				c.logger.WithFields(logrus.Fields{
					"volume_id":  req.VolumeID,
					"needle_id":  req.NeedleID,
					"source":     source,
					"real_rdma":  isRealRDMA,
					"latency":    time.Since(start),
					"bytes_read": len(data),
				}).Info("🚀 RDMA fast path successful")

				if c.useZeroCopy && len(data) > 64*1024 {
					tempFilePath, err := c.writeToTempFile(req, data)
					if err != nil {
						c.logger.WithError(err).Warn("Failed to write temp file, using regular response")
					} else {
						return &NeedleReadResponse{
							Data:         nil,
							IsRDMA:       isRealRDMA,
							Latency:      time.Since(start),
							Source:       source + "-zerocopy",
							TempFilePath: tempFilePath,
							UseTempFile:  true,
						}, nil
					}
				}

				return &NeedleReadResponse{
					Data:    data,
					IsRDMA:  isRealRDMA,
					Latency: time.Since(start),
					Source:  source,
				}, nil
			}
		}
	}

	// Fallback to HTTP
	c.logger.WithFields(logrus.Fields{
		"volume_id": req.VolumeID,
		"needle_id": req.NeedleID,
		"reason":    "rdma_unavailable",
	}).Debug("🌐 Using HTTP fallback")

	data, err := c.httpFallback(ctx, req)
	if err != nil {
		if rdmaErr != nil {
			return nil, fmt.Errorf("both RDMA and HTTP fallback failed: RDMA=%v, HTTP=%v", rdmaErr, err)
		}
		return nil, fmt.Errorf("HTTP fallback failed: %w", err)
	}

	return &NeedleReadResponse{
		Data:    data,
		IsRDMA:  false,
		Latency: time.Since(start),
		Source:  "http",
	}, nil
}

// ReadNeedleRange reads a specific range from a needle
func (c *SeaweedFSRDMAClient) ReadNeedleRange(ctx context.Context, volumeID uint32, needleID uint64, cookie uint32, offset, size uint64) (*NeedleReadResponse, error) {
	req := &NeedleReadRequest{
		VolumeID: volumeID,
		NeedleID: needleID,
		Cookie:   cookie,
		Offset:   offset,
		Size:     size,
	}
	return c.ReadNeedle(ctx, req)
}

// fetchNeedleData loads actual needle bytes after an RDMA session completes.
// Mock engines only coordinate the session; data always comes from local files or HTTP.
func (c *SeaweedFSRDMAClient) fetchNeedleData(ctx context.Context, req *NeedleReadRequest) ([]byte, string, error) {
	if c.localReader != nil {
		data, err := c.localReader.ReadNeedle(req.VolumeID, req.NeedleID, req.Cookie, req.Offset, req.Size)
		if err == nil {
			return data, "local-volume", nil
		}
		c.logger.WithError(err).Debug("local volume read failed, trying HTTP fallback")
	}

	data, err := c.httpFallback(ctx, req)
	if err != nil {
		return nil, "", err
	}
	return data, "http", nil
}

// httpFallback performs HTTP fallback read from SeaweedFS volume server
func (c *SeaweedFSRDMAClient) httpFallback(ctx context.Context, req *NeedleReadRequest) ([]byte, error) {
	// Use volume server from request, fallback to configured URL
	volumeServerURL := req.VolumeServer
	if volumeServerURL == "" {
		volumeServerURL = c.volumeServerURL
	}

	if volumeServerURL == "" {
		return nil, fmt.Errorf("no volume server URL provided in request or configured")
	}

	// Build URL using existing SeaweedFS file ID construction
	volumeId := needle.VolumeId(req.VolumeID)
	needleId := types.NeedleId(req.NeedleID)
	cookie := types.Cookie(req.Cookie)

	fileId := &needle.FileId{
		VolumeId: volumeId,
		Key:      needleId,
		Cookie:   cookie,
	}

	url := fmt.Sprintf("%s/%s", volumeServerURL, fileId.String())

	if req.Offset > 0 || req.Size > 0 {
		url += fmt.Sprintf("?offset=%d&size=%d", req.Offset, req.Size)
	}

	c.logger.WithField("url", url).Debug("📥 HTTP fallback request")

	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP request failed with status: %d", resp.StatusCode)
	}

	// Read response data - io.ReadAll handles context cancellation and timeouts correctly
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read HTTP response body: %w", err)
	}

	c.logger.WithFields(logrus.Fields{
		"volume_id": req.VolumeID,
		"needle_id": req.NeedleID,
		"data_size": len(data),
	}).Debug("📥 HTTP fallback successful")

	return data, nil
}

// HealthCheck verifies that the RDMA client is healthy
func (c *SeaweedFSRDMAClient) HealthCheck(ctx context.Context) error {
	if !c.enabled {
		return fmt.Errorf("RDMA is disabled")
	}

	if c.rdmaClient == nil {
		return fmt.Errorf("RDMA client not initialized")
	}

	if !c.rdmaClient.IsConnected() {
		return fmt.Errorf("RDMA client not connected")
	}

	// Try a ping to the RDMA engine
	_, err := c.rdmaClient.Ping(ctx)
	return err
}

// GetStats returns statistics about the RDMA client
func (c *SeaweedFSRDMAClient) GetStats() map[string]interface{} {
	stats := map[string]interface{}{
		"enabled":           c.enabled,
		"volume_server_url": c.volumeServerURL,
		"rdma_socket_path":  "",
	}

	if c.rdmaClient != nil {
		stats["connected"] = c.rdmaClient.IsConnected()
		// Note: Capabilities method may not be available, skip for now
	} else {
		stats["connected"] = false
		stats["error"] = "RDMA client not initialized"
	}

	return stats
}

// writeToTempFile writes RDMA data to a temp file for zero-copy optimization
func (c *SeaweedFSRDMAClient) writeToTempFile(req *NeedleReadRequest, data []byte) (string, error) {
	// Create temp file with unique name based on needle info
	fileName := fmt.Sprintf("vol%d_needle%x_cookie%d_offset%d_size%d.tmp",
		req.VolumeID, req.NeedleID, req.Cookie, req.Offset, req.Size)
	tempFilePath := filepath.Join(c.tempDir, fileName)

	// Write data to temp file (this populates the page cache)
	err := os.WriteFile(tempFilePath, data, 0644)
	if err != nil {
		return "", fmt.Errorf("failed to write temp file: %w", err)
	}

	c.logger.WithFields(logrus.Fields{
		"temp_file": tempFilePath,
		"size":      len(data),
	}).Debug("📁 Temp file written to page cache")

	return tempFilePath, nil
}

// CleanupTempFile removes a temp file (called by mount client after use)
func (c *SeaweedFSRDMAClient) CleanupTempFile(tempFilePath string) error {
	if tempFilePath == "" {
		return nil
	}

	// Validate that tempFilePath is within c.tempDir
	absTempDir, err := filepath.Abs(c.tempDir)
	if err != nil {
		return fmt.Errorf("failed to resolve temp dir: %w", err)
	}
	absFilePath, err := filepath.Abs(tempFilePath)
	if err != nil {
		return fmt.Errorf("failed to resolve temp file path: %w", err)
	}
	// Ensure absFilePath is within absTempDir
	if !strings.HasPrefix(absFilePath, absTempDir+string(os.PathSeparator)) && absFilePath != absTempDir {
		c.logger.WithField("temp_file", tempFilePath).Warn("Attempted cleanup of file outside temp dir")
		return fmt.Errorf("invalid temp file path")
	}

	err = os.Remove(absFilePath)
	if err != nil && !os.IsNotExist(err) {
		c.logger.WithError(err).WithField("temp_file", absFilePath).Warn("Failed to cleanup temp file")
		return err
	}

	c.logger.WithField("temp_file", absFilePath).Debug("🧹 Temp file cleaned up")
	return nil
}
