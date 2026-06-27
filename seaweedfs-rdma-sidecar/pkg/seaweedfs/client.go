// Package seaweedfs provides SeaweedFS-specific RDMA integration
package seaweedfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"seaweedfs-rdma-sidecar/pkg/nativeengine"
	"seaweedfs-rdma-sidecar/pkg/rdma"
	"seaweedfs-rdma-sidecar/pkg/remote"
	"seaweedfs-rdma-sidecar/pkg/swvfsdaemon"
	"seaweedfs-rdma-sidecar/pkg/swvfsproto"
	"seaweedfs-rdma-sidecar/pkg/volumeread"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// SeaweedFSRDMAClient provides SeaweedFS-specific RDMA operations
type SeaweedFSRDMAClient struct {
	rdmaClient         *rdma.Client
	nativeEngine       nativeVolumeEngine
	localReader        *volumeread.Reader
	logger             *logrus.Logger
	volumeServerURL    string
	enabled            bool
	payloadRDMA        bool
	nativeVolumeRDMA   bool
	nativeServiceLevel uint32
	remoteReadPort     uint16
	operationTimeout   time.Duration

	// Zero-copy optimization
	tempDir     string
	useZeroCopy bool

	nativeMu    sync.Mutex
	nativePeers map[string]nativeVolumePeer
}

type nativeVolumePeer struct {
	RequesterConnectionID uint64
	VolumeConnectionID    uint64
}

// Config holds configuration for the SeaweedFS RDMA client
type Config struct {
	RDMASocketPath         string
	NativeEngineSocketPath string
	VolumeServerURL        string
	Enabled                bool
	EnablePayloadRDMA      bool
	EnableNativeVolumeRDMA bool
	NativeRDMAServiceLevel uint32
	DefaultTimeout         time.Duration
	Logger                 *logrus.Logger

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

	// RemoteReadPort is the TCP port for remote needle reads (default 18515).
	RemoteReadPort uint16
}

type nativeVolumeEngine interface {
	RequesterLocal(context.Context) (swvfsdaemon.RDMALocalEndpoint, error)
	RequesterConnect(context.Context, swvfsproto.RDMARemoteInfo) error
	ReadRemote(context.Context, swvfsproto.RDMADataDesc, time.Duration) ([]byte, error)
	RequesterLocalFor(context.Context, uint64) (swvfsdaemon.RDMALocalEndpoint, uint64, error)
	RequesterConnectFor(context.Context, uint64, swvfsproto.RDMARemoteInfo) error
	ReadRemoteFor(context.Context, uint64, swvfsproto.RDMADataDesc, time.Duration) ([]byte, error)
}

// NeedleReadRequest represents a SeaweedFS needle read request
type NeedleReadRequest struct {
	VolumeID     uint32
	NeedleID     uint64
	Cookie       uint32
	Offset       uint64
	Size         uint64
	VolumeServer string // Override volume server URL for this request
	RDMAServer   string // Optional data-plane host/IP for remote RDMA engine
}

// NeedleReadResponse represents the result of a needle read
type NeedleReadResponse struct {
	Data        []byte
	IsRDMA      bool // true only when payload data used real RDMA
	Latency     time.Duration
	Source      string // high-level path, e.g. "session+http" or "http"
	SessionID   string
	SessionRDMA bool
	RealRDMA    bool
	DataSource  string

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
	var nativeEngine nativeVolumeEngine
	if config.EnableNativeVolumeRDMA && config.NativeEngineSocketPath != "" {
		nativeEngine = nativeengine.New(config.NativeEngineSocketPath, config.DefaultTimeout)
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
		rdmaClient:         rdmaClient,
		nativeEngine:       nativeEngine,
		logger:             config.Logger,
		volumeServerURL:    config.VolumeServerURL,
		enabled:            config.Enabled,
		payloadRDMA:        config.EnablePayloadRDMA,
		nativeVolumeRDMA:   config.EnableNativeVolumeRDMA,
		nativeServiceLevel: config.NativeRDMAServiceLevel,
		tempDir:            tempDir,
		useZeroCopy:        config.UseZeroCopy,
		remoteReadPort:     config.RemoteReadPort,
		operationTimeout:   config.DefaultTimeout,
		nativePeers:        make(map[string]nativeVolumePeer),
	}
	if client.remoteReadPort == 0 {
		client.remoteReadPort = remote.DefaultRemotePort
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

// RDMAClient returns the underlying RDMA client for sidecar health endpoints.
func (c *SeaweedFSRDMAClient) RDMAClient() *rdma.Client {
	return c.rdmaClient
}

// ReadNeedle reads a needle using RDMA fast path or HTTP fallback
func (c *SeaweedFSRDMAClient) ReadNeedle(ctx context.Context, req *NeedleReadRequest) (*NeedleReadResponse, error) {
	start := time.Now()
	var rdmaErr error

	if c.nativeVolumeRDMA && c.nativeEngine != nil {
		if data, sessionID, ok, err := c.readNeedleViaNativeVolumeRDMA(ctx, req); err == nil && ok {
			c.logger.WithFields(logrus.Fields{
				"volume_id":   req.VolumeID,
				"needle_id":   req.NeedleID,
				"source":      "native-volume-rdma",
				"real_rdma":   true,
				"data_source": "volume-native",
				"latency":     time.Since(start),
				"bytes_read":  len(data),
			}).Info("🚀 Native volume RDMA read path completed")
			return &NeedleReadResponse{
				Data:        data,
				IsRDMA:      true,
				Latency:     time.Since(start),
				Source:      "native-volume-rdma",
				SessionID:   sessionID,
				SessionRDMA: true,
				RealRDMA:    true,
				DataSource:  "volume-native",
			}, nil
		} else if err != nil {
			c.logger.WithError(err).Debug("native volume RDMA read path unavailable, trying configured RDMA path")
			rdmaErr = err
		}
	}

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

		if c.payloadRDMA {
			if data, source, sessionID, ok, err := c.readNeedleViaRemoteRDMA(ctx, req, rdmaReq); err == nil && ok {
				realRDMAEngine := c.rdmaClient.IsRealRdma()
				c.logger.WithFields(logrus.Fields{
					"volume_id":    req.VolumeID,
					"needle_id":    req.NeedleID,
					"source":       "session+" + source,
					"session_rdma": true,
					"real_rdma":    realRDMAEngine,
					"data_rdma":    true,
					"data_source":  source,
					"latency":      time.Since(start),
					"bytes_read":   len(data),
				}).Info("🚀 RDMA payload read path completed")
				return &NeedleReadResponse{
					Data:        data,
					IsRDMA:      true,
					Latency:     time.Since(start),
					Source:      "session+" + source,
					SessionID:   sessionID,
					SessionRDMA: true,
					RealRDMA:    realRDMAEngine,
					DataSource:  source,
				}, nil
			} else if err != nil {
				c.logger.WithError(err).Debug("RDMA payload read path unavailable, trying session/TCP path")
			}
		} else {
			c.logger.Debug("RDMA payload read path disabled, using session/TCP path")
		}

		rdmaResp, err := c.rdmaClient.Read(ctx, rdmaReq)
		if err != nil {
			c.logger.WithError(err).Warn("⚠️  RDMA read failed, falling back to HTTP")
			rdmaErr = err
		} else {
			data, source, dataRealRDMA, fetchErr := c.fetchNeedleData(ctx, req)
			if fetchErr != nil {
				c.logger.WithError(fetchErr).Warn("⚠️  RDMA session ok but data fetch failed, falling back to HTTP")
				rdmaErr = fetchErr
			} else {
				realRDMAEngine := c.rdmaClient.IsRealRdma()
				isRealRDMA := realRDMAEngine && dataRealRDMA
				rdmaSource := "session+" + source
				sessionID := ""
				if rdmaResp != nil {
					sessionID = rdmaResp.SessionID
				}
				c.logger.WithFields(logrus.Fields{
					"volume_id":    req.VolumeID,
					"needle_id":    req.NeedleID,
					"source":       rdmaSource,
					"session_rdma": true,
					"real_rdma":    realRDMAEngine,
					"data_rdma":    isRealRDMA,
					"data_source":  source,
					"latency":      time.Since(start),
					"bytes_read":   len(data),
				}).Info("🚀 RDMA session path completed")

				if isRealRDMA && c.useZeroCopy && len(data) > 64*1024 {
					tempFilePath, err := c.writeToTempFile(req, data)
					if err != nil {
						c.logger.WithError(err).Warn("Failed to write temp file, using regular response")
					} else {
						return &NeedleReadResponse{
							Data:         nil,
							IsRDMA:       isRealRDMA,
							Latency:      time.Since(start),
							Source:       rdmaSource + "-zerocopy",
							SessionID:    sessionID,
							SessionRDMA:  true,
							RealRDMA:     realRDMAEngine,
							DataSource:   source,
							TempFilePath: tempFilePath,
							UseTempFile:  true,
						}, nil
					}
				}

				return &NeedleReadResponse{
					Data:        data,
					IsRDMA:      isRealRDMA,
					Latency:     time.Since(start),
					Source:      rdmaSource,
					SessionID:   sessionID,
					SessionRDMA: true,
					RealRDMA:    realRDMAEngine,
					DataSource:  source,
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
		Data:        data,
		IsRDMA:      false,
		Latency:     time.Since(start),
		Source:      "http",
		SessionRDMA: false,
		RealRDMA:    false,
		DataSource:  "http",
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

// ReadLocalNeedle reads from the local shared volume directory without going
// through the volume server HTTP handler. It is intended for volume-sidecars.
func (c *SeaweedFSRDMAClient) ReadLocalNeedle(ctx context.Context, req *NeedleReadRequest) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.localReader == nil {
		return nil, fmt.Errorf("local volume reader is not configured")
	}
	return c.localReader.ReadNeedle(req.VolumeID, req.NeedleID, req.Cookie, req.Offset, req.Size)
}

func (c *SeaweedFSRDMAClient) readNeedleViaNativeVolumeRDMA(ctx context.Context, req *NeedleReadRequest) ([]byte, string, bool, error) {
	if c == nil || c.nativeEngine == nil {
		return nil, "", false, nil
	}
	volumeServer := req.VolumeServer
	if volumeServer == "" {
		volumeServer = c.volumeServerURL
	}
	if volumeServer == "" {
		return nil, "", false, nil
	}
	size := req.Size
	if size == 0 {
		size = 4096
	}
	if size > swvfsproto.RDMAIOMax {
		return nil, "", false, fmt.Errorf("native volume RDMA read too large: %d > %d", size, swvfsproto.RDMAIOMax)
	}

	peer, err := c.ensureNativeVolumePeer(ctx, volumeServer)
	if err != nil {
		return nil, "", false, err
	}
	desc, _, sessionID, err := swvfsdaemon.PostVolumeNativeReadDesc(ctx, nil, volumeServer, swvfsdaemon.VolumeRDMAReadDescRequest{
		ConnectionID: peer.VolumeConnectionID,
		FileID:       reqFileID(req),
		VolumeID:     req.VolumeID,
		NeedleID:     req.NeedleID,
		Cookie:       req.Cookie,
		Offset:       req.Offset,
		Size:         size,
	})
	if err != nil {
		return nil, "", false, err
	}
	if sessionID != 0 {
		defer func() {
			releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := swvfsdaemon.PostVolumeNativeReleaseDesc(releaseCtx, nil, volumeServer, sessionID); err != nil {
				c.logger.WithError(err).Warn("native volume RDMA read descriptor release failed")
			}
		}()
	}
	if desc == nil || desc.RemoteAddr == 0 || desc.Length == 0 {
		return nil, "", false, fmt.Errorf("native volume RDMA descriptor is not exportable")
	}
	if uint64(desc.Length) > size {
		return nil, "", false, fmt.Errorf("native volume RDMA descriptor length %d exceeds requested size %d", desc.Length, size)
	}

	timeout := c.nativeOperationTimeout()
	data, err := c.nativeEngine.ReadRemoteFor(ctx, peer.RequesterConnectionID, *desc, timeout)
	if err != nil {
		return nil, "", false, err
	}
	if uint64(len(data)) < uint64(desc.Length) {
		return nil, "", false, fmt.Errorf("native volume RDMA read returned %d bytes for descriptor length %d", len(data), desc.Length)
	}
	return data[:desc.Length], fmt.Sprintf("%d", sessionID), true, nil
}

func (c *SeaweedFSRDMAClient) ensureNativeVolumePeer(ctx context.Context, volumeServer string) (nativeVolumePeer, error) {
	key := nativeVolumePeerKey(volumeServer)
	c.nativeMu.Lock()
	if peer, ok := c.nativePeers[key]; ok {
		c.nativeMu.Unlock()
		return peer, nil
	}
	c.nativeMu.Unlock()

	local, requesterConnectionID, err := c.nativeEngine.RequesterLocalFor(ctx, 0)
	if err != nil {
		return nativeVolumePeer{}, err
	}
	if !local.ReadyForConnect() {
		return nativeVolumePeer{}, fmt.Errorf("native requester endpoint is not ready: qpn=%d lid=%d", local.QPNum, local.LID)
	}
	remoteEndpoint, err := swvfsdaemon.FetchVolumeNativeEndpoint(ctx, nil, volumeServer)
	if err != nil {
		return nativeVolumePeer{}, err
	}
	if !remoteEndpoint.ReadyForConnect() {
		return nativeVolumePeer{}, fmt.Errorf("native volume endpoint is not ready: qpn=%d lid=%d", remoteEndpoint.QPNum, remoteEndpoint.LID)
	}
	if err := swvfsdaemon.PostVolumeNativeConnectFor(ctx, nil, volumeServer, remoteEndpoint.ConnectionID, local, c.nativeServiceLevel); err != nil {
		return nativeVolumePeer{}, err
	}
	remoteInfo, err := remoteEndpoint.RemoteInfo(c.nativeServiceLevel)
	if err != nil {
		return nativeVolumePeer{}, err
	}
	if err := c.nativeEngine.RequesterConnectFor(ctx, requesterConnectionID, remoteInfo); err != nil {
		return nativeVolumePeer{}, err
	}

	peer := nativeVolumePeer{
		RequesterConnectionID: requesterConnectionID,
		VolumeConnectionID:    remoteEndpoint.ConnectionID,
	}
	c.nativeMu.Lock()
	c.nativePeers[key] = peer
	c.nativeMu.Unlock()
	return peer, nil
}

func (c *SeaweedFSRDMAClient) nativeOperationTimeout() time.Duration {
	if c != nil && c.operationTimeout > 0 {
		return c.operationTimeout
	}
	return 5 * time.Second
}

func nativeVolumePeerKey(volumeServer string) string {
	return strings.TrimRight(strings.TrimSpace(volumeServer), "/")
}

func reqFileID(req *NeedleReadRequest) string {
	if req == nil || req.VolumeID == 0 || req.NeedleID == 0 {
		return ""
	}
	return needle.NewFileId(needle.VolumeId(req.VolumeID), req.NeedleID, req.Cookie).String()
}

func (c *SeaweedFSRDMAClient) readNeedleViaRemoteRDMA(ctx context.Context, req *NeedleReadRequest, rdmaReq *rdma.ReadRequest) ([]byte, string, string, bool, error) {
	if c.rdmaClient == nil || !c.rdmaClient.IsRealRdma() {
		return nil, "", "", false, nil
	}

	volumeServer := req.VolumeServer
	if volumeServer == "" {
		volumeServer = c.volumeServerURL
	}
	remoteServer := req.RDMAServer
	if remoteServer == "" {
		remoteServer = volumeServer
	}
	if remote.IsLocalHost(remoteServer) {
		return nil, "", "", false, nil
	}

	worker, err := c.rdmaClient.GetWorkerAddress(ctx)
	if err != nil {
		return nil, "", "", false, err
	}
	if !worker.RealRdma || worker.WorkerAddressB64 == "" {
		return nil, "", "", false, fmt.Errorf("worker RDMA address unavailable")
	}

	startResp, err := c.rdmaClient.StartReadSession(ctx, rdmaReq)
	if err != nil {
		return nil, "", "", false, err
	}
	completed := false
	defer func() {
		if !completed {
			_, _ = c.rdmaClient.CompleteReadSession(context.Background(), startResp.SessionID, false, 0, nil)
		}
	}()

	size := req.Size
	if size == 0 {
		size = 4096
	}
	result, err := remote.ReadNeedleResult(ctx, remoteServer, c.remoteReadPort, &remote.NeedleReadRequest{
		VolumeID:         req.VolumeID,
		NeedleID:         req.NeedleID,
		Cookie:           req.Cookie,
		Offset:           req.Offset,
		Size:             size,
		WorkerAddressB64: worker.WorkerAddressB64,
		RemoteAddr:       startResp.LocalAddr,
		RemoteKeyB64:     startResp.RemoteKeyB64,
	})
	if err != nil {
		return nil, "", "", false, err
	}
	if normalizedRemoteTransport(result.Transport) != "rdma" || !result.RealRDMA {
		completed = true
		_, _ = c.rdmaClient.CompleteReadSession(ctx, startResp.SessionID, false, 0, nil)
		return result.Data, remoteReadSourceWithBacking(result.Transport, result.Source), startResp.SessionID, false, nil
	}

	bytesTransferred := result.Size
	if bytesTransferred == 0 {
		bytesTransferred = uint64(len(result.Data))
	}
	if bytesTransferred > startResp.TransferSize {
		return nil, "", "", false, fmt.Errorf("remote RDMA read returned %d bytes for %d byte buffer", bytesTransferred, startResp.TransferSize)
	}

	completeResp, err := c.rdmaClient.CompleteReadSession(ctx, startResp.SessionID, true, bytesTransferred, &startResp.ExpectedCrc)
	if err != nil {
		return nil, "", "", false, err
	}
	completed = true
	if !completeResp.Success {
		return nil, "", "", false, fmt.Errorf("RDMA read completion failed")
	}
	if len(completeResp.Data) == 0 && bytesTransferred > 0 {
		return nil, "", "", false, fmt.Errorf("RDMA read returned empty buffer")
	}
	return completeResp.Data, remoteReadSourceWithBacking(result.Transport, result.Source), startResp.SessionID, true, nil
}

// fetchNeedleData loads actual needle bytes after an RDMA session completes.
func (c *SeaweedFSRDMAClient) fetchNeedleData(ctx context.Context, req *NeedleReadRequest) ([]byte, string, bool, error) {
	volumeServer := req.VolumeServer
	if volumeServer == "" {
		volumeServer = c.volumeServerURL
	}

	if c.localReader != nil && remote.IsLocalHost(volumeServer) {
		data, err := c.localReader.ReadNeedle(req.VolumeID, req.NeedleID, req.Cookie, req.Offset, req.Size)
		if err == nil {
			return data, "local-volume", false, nil
		}
		c.logger.WithError(err).Debug("local volume read failed, trying remote/HTTP")
	}

	remoteServer := req.RDMAServer
	if remoteServer == "" {
		remoteServer = volumeServer
	}

	if !remote.IsLocalHost(remoteServer) {
		size := req.Size
		if size == 0 {
			size = 4096
		}
		result, err := remote.ReadNeedleResult(ctx, remoteServer, c.remoteReadPort, &remote.NeedleReadRequest{
			VolumeID: req.VolumeID,
			NeedleID: req.NeedleID,
			Cookie:   req.Cookie,
			Offset:   req.Offset,
			Size:     size,
		})
		if err == nil {
			return result.Data, remoteReadSourceWithBacking(result.Transport, result.Source), result.RealRDMA, nil
		}
		c.logger.WithError(err).Debug("remote sidecar read failed, trying HTTP fallback")
	}

	data, err := c.httpFallback(ctx, req)
	if err != nil {
		return nil, "", false, err
	}
	return data, "http", false, nil
}

func remoteReadSource(transport string) string {
	return "remote-" + normalizedRemoteTransport(transport)
}

func remoteReadSourceWithBacking(transport, backing string) string {
	source := remoteReadSource(transport)
	backing = strings.TrimSpace(backing)
	if backing == "" {
		return source
	}
	return source + ":" + backing
}

func remoteWriteSource(transport string) string {
	return remoteReadSource(transport) + "-write"
}

func remoteWriteSourceWithBacking(transport, backing string) string {
	source := remoteWriteSource(transport)
	backing = strings.TrimSpace(backing)
	if backing == "" {
		return source
	}
	return source + ":" + backing
}

func normalizedRemoteTransport(transport string) string {
	transport = strings.ToLower(strings.TrimSpace(transport))
	if transport == "" {
		return "tcp"
	}
	return transport
}

func volumeServerAddress(volumeServer string) (pb.ServerAddress, error) {
	volumeServer = strings.TrimSpace(volumeServer)
	if volumeServer == "" {
		return "", fmt.Errorf("empty volume server URL")
	}
	if !strings.HasPrefix(volumeServer, "http://") {
		volumeServer = "http://" + strings.TrimPrefix(volumeServer, "https://")
	}
	addr, _, err := pb.ParseUrl(volumeServer)
	if err != nil {
		return "", fmt.Errorf("parse volume server %q: %w", volumeServer, err)
	}
	return addr, nil
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

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("HTTP request failed with status: %d", resp.StatusCode)
	}

	// Read response data - io.ReadAll handles context cancellation and timeouts correctly
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read HTTP response body: %w", err)
	}
	rangeApplied := resp.StatusCode == http.StatusPartialContent || resp.Header.Get("Content-Range") != ""
	data = normalizeHTTPReadData(data, req.Offset, req.Size, rangeApplied)

	c.logger.WithFields(logrus.Fields{
		"volume_id": req.VolumeID,
		"needle_id": req.NeedleID,
		"data_size": len(data),
	}).Debug("📥 HTTP fallback successful")

	return data, nil
}

func normalizeHTTPReadData(data []byte, offset, size uint64, rangeApplied bool) []byte {
	if size == 0 || uint64(len(data)) <= size {
		return data
	}
	requested := int(size)
	if rangeApplied {
		return data[:requested]
	}
	if offset >= uint64(len(data)) {
		return nil
	}
	end := offset + size
	if end < offset || end > uint64(len(data)) {
		end = uint64(len(data))
	}
	return data[int(offset):int(end)]
}

// NeedleWriteRequest represents a SeaweedFS needle write request
type NeedleWriteRequest struct {
	VolumeID     uint32
	NeedleID     uint64
	Cookie       uint32
	Data         []byte
	VolumeServer string
	RDMAServer   string
}

// NeedleWriteResponse represents the result of a needle write
type NeedleWriteResponse struct {
	Success     bool
	IsRDMA      bool
	Source      string
	Latency     time.Duration
	FileID      string
	Size        int
	SessionRDMA bool
	RealRDMA    bool
	DataSource  string
}

// WriteNeedle writes a needle using RDMA fast path + persist, or HTTP fallback
func (c *SeaweedFSRDMAClient) WriteNeedle(ctx context.Context, req *NeedleWriteRequest) (*NeedleWriteResponse, error) {
	start := time.Now()
	var rdmaErr error

	if c.IsEnabled() {
		c.logger.WithFields(logrus.Fields{
			"volume_id": req.VolumeID,
			"needle_id": req.NeedleID,
			"data_size": len(req.Data),
		}).Debug("📝 Attempting RDMA write fast path")

		writeReq := &rdma.WriteRequest{
			VolumeID: req.VolumeID,
			NeedleID: req.NeedleID,
			Cookie:   req.Cookie,
			Data:     req.Data,
		}

		if c.payloadRDMA {
			if fileID, source, ok, err := c.writeNeedleViaRemoteRDMA(ctx, req, writeReq); err == nil && ok {
				realRDMAEngine := c.rdmaClient.IsRealRdma()
				c.logger.WithFields(logrus.Fields{
					"volume_id":     req.VolumeID,
					"needle_id":     req.NeedleID,
					"source":        "session+" + source,
					"session_rdma":  true,
					"real_rdma":     realRDMAEngine,
					"data_rdma":     true,
					"data_source":   source,
					"bytes_written": len(req.Data),
					"latency":       time.Since(start),
					"file_id":       fileID,
				}).Info("📝 RDMA payload write path completed")
				return &NeedleWriteResponse{
					Success:     true,
					IsRDMA:      true,
					Source:      "session+" + source,
					Latency:     time.Since(start),
					FileID:      fileID,
					Size:        len(req.Data),
					SessionRDMA: true,
					RealRDMA:    realRDMAEngine,
					DataSource:  source,
				}, nil
			} else if err != nil {
				c.logger.WithError(err).Debug("RDMA payload write path unavailable, trying session/TCP path")
			}
		} else {
			c.logger.Debug("RDMA payload write path disabled, using session/TCP path")
		}

		writeResp, err := c.rdmaClient.Write(ctx, writeReq)
		if err != nil {
			c.logger.WithError(err).Warn("⚠️  RDMA write failed, falling back to HTTP")
			rdmaErr = err
		} else {
			fileID, source, dataRealRDMA, persistErr := c.persistNeedleData(ctx, req)
			if persistErr != nil {
				c.logger.WithError(persistErr).Warn("⚠️  RDMA session ok but persist failed, falling back to HTTP")
				rdmaErr = persistErr
			} else {
				realRDMAEngine := c.rdmaClient.IsRealRdma()
				isRealRDMA := realRDMAEngine && dataRealRDMA
				rdmaSource := "session+" + source
				c.logger.WithFields(logrus.Fields{
					"volume_id":     req.VolumeID,
					"needle_id":     req.NeedleID,
					"source":        rdmaSource,
					"session_rdma":  true,
					"real_rdma":     realRDMAEngine,
					"data_rdma":     isRealRDMA,
					"data_source":   source,
					"bytes_written": writeResp.BytesWritten,
					"latency":       time.Since(start),
					"file_id":       fileID,
				}).Info("📝 RDMA write session path completed")

				return &NeedleWriteResponse{
					Success:     true,
					IsRDMA:      isRealRDMA,
					Source:      rdmaSource,
					Latency:     time.Since(start),
					FileID:      fileID,
					Size:        len(req.Data),
					SessionRDMA: true,
					RealRDMA:    realRDMAEngine,
					DataSource:  source,
				}, nil
			}
		}
	}

	c.logger.WithFields(logrus.Fields{
		"volume_id": req.VolumeID,
		"needle_id": req.NeedleID,
		"reason":    "rdma_unavailable",
	}).Debug("🌐 Using HTTP-only write fallback")

	fileID, err := c.httpWriteFallback(ctx, req)
	if err != nil {
		if rdmaErr != nil {
			return nil, fmt.Errorf("both RDMA and HTTP write failed: RDMA=%v, HTTP=%v", rdmaErr, err)
		}
		return nil, fmt.Errorf("HTTP write failed: %w", err)
	}

	return &NeedleWriteResponse{
		Success:     true,
		IsRDMA:      false,
		Source:      "http",
		Latency:     time.Since(start),
		FileID:      fileID,
		Size:        len(req.Data),
		SessionRDMA: false,
		RealRDMA:    false,
		DataSource:  "http",
	}, nil
}

// WriteNeedleBlobGRPC persists a raw payload via the volume server's blob gRPC
// API. The payload is encoded locally using the target volume's on-disk version,
// while the actual append and needle-map update remain inside the volume server.
func (c *SeaweedFSRDMAClient) WriteNeedleBlobGRPC(ctx context.Context, req *NeedleWriteRequest) (string, error) {
	if len(req.Data) == 0 {
		return "", fmt.Errorf("empty write payload")
	}

	version := needle.GetCurrentVersion()
	if c.localReader != nil {
		localVersion, err := c.localReader.VolumeVersion(req.VolumeID)
		if err != nil {
			return "", fmt.Errorf("local volume version: %w", err)
		}
		version = localVersion
	}

	n := &needle.Needle{
		Id:           types.NeedleId(req.NeedleID),
		Cookie:       types.Cookie(req.Cookie),
		Data:         req.Data,
		LastModified: uint64(time.Now().Unix()),
	}
	n.SetHasLastModifiedDate()
	n.Checksum = needle.NewCRC(n.Data)

	blob, size, err := needle.EncodeNeedleBlob(n, version)
	if err != nil {
		return "", fmt.Errorf("encode needle blob: %w", err)
	}

	volumeServer := req.VolumeServer
	if volumeServer == "" {
		volumeServer = c.volumeServerURL
	}
	addr, err := volumeServerAddress(volumeServer)
	if err != nil {
		return "", err
	}

	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	if err := operation.WithVolumeServerClient(false, addr, dialOption, func(client volume_server_pb.VolumeServerClient) error {
		_, err := client.WriteNeedleBlob(ctx, &volume_server_pb.WriteNeedleBlobRequest{
			VolumeId:   req.VolumeID,
			NeedleId:   req.NeedleID,
			Size:       int32(size),
			NeedleBlob: blob,
		})
		return err
	}); err != nil {
		return "", fmt.Errorf("write needle blob to %s: %w", addr, err)
	}
	if c.localReader != nil {
		c.localReader.Invalidate(req.VolumeID)
	}

	fileID := &needle.FileId{
		VolumeId: needle.VolumeId(req.VolumeID),
		Key:      types.NeedleId(req.NeedleID),
		Cookie:   types.Cookie(req.Cookie),
	}
	return fileID.String(), nil
}

func (c *SeaweedFSRDMAClient) writeNeedleViaRemoteRDMA(ctx context.Context, req *NeedleWriteRequest, writeReq *rdma.WriteRequest) (string, string, bool, error) {
	if c.rdmaClient == nil || !c.rdmaClient.IsRealRdma() {
		return "", "", false, nil
	}

	volumeServer := req.VolumeServer
	if volumeServer == "" {
		volumeServer = c.volumeServerURL
	}
	remoteServer := req.RDMAServer
	if remoteServer == "" {
		remoteServer = volumeServer
	}
	if remote.IsLocalHost(remoteServer) {
		return "", "", false, nil
	}

	worker, err := c.rdmaClient.GetWorkerAddress(ctx)
	if err != nil {
		return "", "", false, err
	}
	if !worker.RealRdma || worker.WorkerAddressB64 == "" {
		return "", "", false, fmt.Errorf("worker RDMA address unavailable")
	}

	startResp, err := c.rdmaClient.StartWriteSession(ctx, writeReq)
	if err != nil {
		return "", "", false, err
	}
	completed := false
	defer func() {
		if !completed {
			_, _ = c.rdmaClient.CompleteWriteSession(context.Background(), startResp.SessionID, false, 0, nil)
		}
	}()

	result, err := remote.WriteNeedleResult(ctx, remoteServer, c.remoteReadPort, &remote.NeedleWriteRequest{
		VolumeID:         req.VolumeID,
		NeedleID:         req.NeedleID,
		Cookie:           req.Cookie,
		Data:             []byte{},
		Size:             uint64(len(req.Data)),
		WorkerAddressB64: worker.WorkerAddressB64,
		RemoteAddr:       startResp.LocalAddr,
		RemoteKeyB64:     startResp.RemoteKeyB64,
	})
	if err != nil {
		return "", "", false, err
	}
	if normalizedRemoteTransport(result.Transport) != "rdma" || !result.RealRDMA {
		return "", "", false, fmt.Errorf("remote write did not use RDMA payload transport: %s", result.Transport)
	}

	if _, err := c.rdmaClient.CompleteWriteSession(ctx, startResp.SessionID, true, startResp.BytesBuffered, nil); err != nil {
		c.logger.WithError(err).Warn("RDMA write payload persisted but session cleanup failed")
	}
	completed = true
	return result.FileID, remoteWriteSourceWithBacking(result.Transport, result.Source), true, nil
}

// persistNeedleData submits data to the volume server after RDMA buffering.
// Returns (fileID, source, payloadUsedRDMA, error).
func (c *SeaweedFSRDMAClient) persistNeedleData(ctx context.Context, req *NeedleWriteRequest) (string, string, bool, error) {
	volumeServer := req.VolumeServer
	if volumeServer == "" {
		volumeServer = c.volumeServerURL
	}

	remoteServer := req.RDMAServer
	if remoteServer == "" {
		remoteServer = volumeServer
	}

	if !remote.IsLocalHost(remoteServer) {
		result, err := remote.WriteNeedleResult(ctx, remoteServer, c.remoteReadPort, &remote.NeedleWriteRequest{
			VolumeID: req.VolumeID,
			NeedleID: req.NeedleID,
			Cookie:   req.Cookie,
			Data:     req.Data,
		})
		if err == nil {
			return result.FileID, remoteWriteSourceWithBacking(result.Transport, result.Source), result.RealRDMA, nil
		}
		c.logger.WithError(err).Debug("remote write failed, trying HTTP volume upload")
	}

	fileID, err := c.httpVolumeUpload(ctx, volumeServer, req)
	if err != nil {
		return "", "", false, err
	}
	return fileID, "http-upload", false, nil
}

// httpVolumeUpload POSTs needle data to the volume server's file-id URL.
func (c *SeaweedFSRDMAClient) httpVolumeUpload(ctx context.Context, volumeServer string, req *NeedleWriteRequest) (string, error) {
	if volumeServer == "" {
		volumeServer = c.volumeServerURL
	}
	fileID := needle.NewFileId(needle.VolumeId(req.VolumeID), req.NeedleID, req.Cookie).String()
	uploadURL := fmt.Sprintf("%s/%s", strings.TrimRight(volumeServer, "/"), fileID)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, uploadURL, bytes.NewReader(req.Data))
	if err != nil {
		return "", fmt.Errorf("failed to create HTTP upload request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/octet-stream")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("HTTP volume upload failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		return "", fmt.Errorf("HTTP volume upload status %d: %s", resp.StatusCode, string(body))
	}
	return fileID, nil
}

// httpWriteFallback performs HTTP-only write to the volume server.
func (c *SeaweedFSRDMAClient) httpWriteFallback(ctx context.Context, req *NeedleWriteRequest) (string, error) {
	volumeServer := req.VolumeServer
	if volumeServer == "" {
		volumeServer = c.volumeServerURL
	}
	return c.httpVolumeUpload(ctx, volumeServer, req)
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
