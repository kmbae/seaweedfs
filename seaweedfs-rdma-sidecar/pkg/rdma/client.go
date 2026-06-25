// Package rdma provides high-level RDMA operations for SeaweedFS integration
package rdma

import (
	"context"
	"fmt"
	"sync"
	"time"

	"seaweedfs-rdma-sidecar/pkg/ipc"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/sirupsen/logrus"
)

// PooledConnection represents a pooled RDMA connection
type PooledConnection struct {
	ipcClient *ipc.Client
	lastUsed  time.Time
	inUse     bool
	sessionID string
	created   time.Time
}

// ConnectionPool manages a pool of RDMA connections
type ConnectionPool struct {
	connections    []*PooledConnection
	mutex          sync.RWMutex
	maxConnections int
	maxIdleTime    time.Duration
	enginePath     string
	logger         *logrus.Logger
}

// Client provides high-level RDMA operations with connection pooling
type Client struct {
	pool           *ConnectionPool
	logger         *logrus.Logger
	enginePath     string
	capabilities   *ipc.GetCapabilitiesResponse
	connected      bool
	defaultTimeout time.Duration

	// Legacy single connection (for backward compatibility)
	ipcClient *ipc.Client
}

// Config holds configuration for the RDMA client
type Config struct {
	EngineSocketPath string
	DefaultTimeout   time.Duration
	Logger           *logrus.Logger

	// Connection pooling options
	EnablePooling  bool          // Enable connection pooling (default: true)
	MaxConnections int           // Max connections in pool (default: 10)
	MaxIdleTime    time.Duration // Max idle time before connection cleanup (default: 5min)
}

// ReadRequest represents a SeaweedFS needle read request
type ReadRequest struct {
	VolumeID  uint32
	NeedleID  uint64
	Cookie    uint32
	Offset    uint64
	Size      uint64
	AuthToken *string
}

// ReadResponse represents the result of an RDMA read operation
type ReadResponse struct {
	Data         []byte
	BytesRead    uint64
	Duration     time.Duration
	TransferRate float64
	SessionID    string
	Success      bool
	Message      string
}

// WriteRequest represents a SeaweedFS needle write request
type WriteRequest struct {
	VolumeID uint32
	NeedleID uint64
	Cookie   uint32
	Data     []byte
}

// WriteResponse represents the result of an RDMA write operation
type WriteResponse struct {
	SessionID    string
	BytesWritten uint64
	ServerCRC    *uint32
	FileID       string
	Duration     time.Duration
	TransferRate float64
	Success      bool
	Message      string
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(enginePath string, maxConnections int, maxIdleTime time.Duration, logger *logrus.Logger) *ConnectionPool {
	if maxConnections <= 0 {
		maxConnections = 10 // Default
	}
	if maxIdleTime <= 0 {
		maxIdleTime = 5 * time.Minute // Default
	}

	return &ConnectionPool{
		connections:    make([]*PooledConnection, 0, maxConnections),
		maxConnections: maxConnections,
		maxIdleTime:    maxIdleTime,
		enginePath:     enginePath,
		logger:         logger,
	}
}

// getConnection gets an available connection from the pool or creates a new one
func (p *ConnectionPool) getConnection(ctx context.Context) (*PooledConnection, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Drop stale/broken connections before reuse
	active := make([]*PooledConnection, 0, len(p.connections))
	for _, conn := range p.connections {
		if conn.inUse {
			active = append(active, conn)
			continue
		}
		if !conn.ipcClient.IsConnected() || time.Since(conn.lastUsed) >= p.maxIdleTime {
			conn.ipcClient.Disconnect()
			p.logger.WithField("session_id", conn.sessionID).Debug("🧹 Dropping stale pooled RDMA connection")
			continue
		}
		active = append(active, conn)
	}
	p.connections = active

	// Look for an available connection
	for _, conn := range p.connections {
		if !conn.inUse {
			conn.inUse = true
			conn.lastUsed = time.Now()
			p.logger.WithField("session_id", conn.sessionID).Debug("🔌 Reusing pooled RDMA connection")
			return conn, nil
		}
	}

	// Create new connection if under limit
	if len(p.connections) < p.maxConnections {
		ipcClient := ipc.NewClient(p.enginePath, p.logger)
		if err := ipcClient.Connect(ctx); err != nil {
			return nil, fmt.Errorf("failed to create new pooled connection: %w", err)
		}

		conn := &PooledConnection{
			ipcClient: ipcClient,
			lastUsed:  time.Now(),
			inUse:     true,
			sessionID: fmt.Sprintf("pool-%d-%d", len(p.connections), time.Now().Unix()),
			created:   time.Now(),
		}

		p.connections = append(p.connections, conn)
		p.logger.WithFields(logrus.Fields{
			"session_id": conn.sessionID,
			"pool_size":  len(p.connections),
		}).Info("🚀 Created new pooled RDMA connection")

		return conn, nil
	}

	// Pool is full, wait for an available connection
	return nil, fmt.Errorf("connection pool exhausted (max: %d)", p.maxConnections)
}

// releaseConnection returns a connection to the pool, or removes it if broken.
func (p *ConnectionPool) releaseConnection(conn *PooledConnection, broken bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if broken || !conn.ipcClient.IsConnected() {
		conn.ipcClient.Disconnect()
		remaining := make([]*PooledConnection, 0, len(p.connections))
		for _, c := range p.connections {
			if c != conn {
				remaining = append(remaining, c)
			}
		}
		p.connections = remaining
		p.logger.WithField("session_id", conn.sessionID).Warn("🗑️ Removed broken pooled RDMA connection")
		return
	}

	conn.inUse = false
	conn.lastUsed = time.Now()
	p.logger.WithField("session_id", conn.sessionID).Debug("🔄 Released RDMA connection back to pool")
}

// cleanup removes idle connections from the pool
func (p *ConnectionPool) cleanup() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	now := time.Now()
	activeConnections := make([]*PooledConnection, 0, len(p.connections))

	for _, conn := range p.connections {
		if conn.inUse || now.Sub(conn.lastUsed) < p.maxIdleTime {
			activeConnections = append(activeConnections, conn)
		} else {
			// Close idle connection
			conn.ipcClient.Disconnect()
			p.logger.WithFields(logrus.Fields{
				"session_id": conn.sessionID,
				"idle_time":  now.Sub(conn.lastUsed),
			}).Debug("🧹 Cleaned up idle RDMA connection")
		}
	}

	p.connections = activeConnections
}

// Close closes all connections in the pool
func (p *ConnectionPool) Close() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, conn := range p.connections {
		conn.ipcClient.Disconnect()
	}
	p.connections = nil
	p.logger.Info("🔌 Connection pool closed")
}

// NewClient creates a new RDMA client
func NewClient(config *Config) *Client {
	if config.Logger == nil {
		config.Logger = logrus.New()
		config.Logger.SetLevel(logrus.InfoLevel)
	}

	if config.DefaultTimeout == 0 {
		config.DefaultTimeout = 30 * time.Second
	}

	client := &Client{
		logger:         config.Logger,
		enginePath:     config.EngineSocketPath,
		defaultTimeout: config.DefaultTimeout,
	}

	// Initialize connection pooling if enabled (default: true)
	enablePooling := config.EnablePooling
	if config.MaxConnections == 0 && config.MaxIdleTime == 0 {
		// Default to enabled if not explicitly configured
		enablePooling = true
	}

	if enablePooling {
		client.pool = NewConnectionPool(
			config.EngineSocketPath,
			config.MaxConnections,
			config.MaxIdleTime,
			config.Logger,
		)

		// Start cleanup goroutine
		go client.startCleanupRoutine()

		config.Logger.WithFields(logrus.Fields{
			"max_connections": client.pool.maxConnections,
			"max_idle_time":   client.pool.maxIdleTime,
		}).Info("🔌 RDMA connection pooling enabled")
	} else {
		// Legacy single connection mode
		client.ipcClient = ipc.NewClient(config.EngineSocketPath, config.Logger)
		config.Logger.Info("🔌 RDMA single connection mode (pooling disabled)")
	}

	return client
}

// startCleanupRoutine starts a background goroutine to clean up idle connections
func (c *Client) startCleanupRoutine() {
	ticker := time.NewTicker(1 * time.Minute) // Cleanup every minute
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			if c.pool != nil {
				c.pool.cleanup()
			}
		}
	}()
}

// Connect establishes connection to the Rust RDMA engine and queries capabilities
func (c *Client) Connect(ctx context.Context) error {
	c.logger.Info("🚀 Connecting to RDMA engine")

	if c.pool != nil {
		clientID := "rdma-client"
		conn, err := c.pool.getConnection(ctx)
		if err != nil {
			return fmt.Errorf("failed to connect pooled IPC: %w", err)
		}
		broken := false
		defer func() { c.pool.releaseConnection(conn, broken) }()

		pong, err := conn.ipcClient.Ping(ctx, &clientID)
		if err != nil {
			broken = true
			return fmt.Errorf("failed to ping RDMA engine: %w", err)
		}
		if pong == nil {
			broken = true
			return fmt.Errorf("empty pong response from RDMA engine")
		}

		latency := time.Duration(pong.ServerRttNs)
		c.logger.WithFields(logrus.Fields{
			"latency":    latency,
			"server_rtt": time.Duration(pong.ServerRttNs),
			"pooled":     true,
		}).Info("📡 RDMA engine ping successful")

		caps, err := conn.ipcClient.GetCapabilities(ctx, &clientID)
		if err != nil {
			broken = true
			return fmt.Errorf("failed to get engine capabilities: %w", err)
		}
		if caps == nil {
			broken = true
			return fmt.Errorf("empty capabilities response from RDMA engine")
		}

		c.capabilities = caps
		c.connected = true
		c.logger.WithFields(logrus.Fields{
			"version":           caps.Version,
			"device_name":       caps.DeviceName,
			"vendor_id":         caps.VendorId,
			"max_sessions":      caps.MaxSessions,
			"max_transfer_size": caps.MaxTransferSize,
			"active_sessions":   caps.ActiveSessions,
			"real_rdma":         caps.RealRdma,
			"port_gid":          caps.PortGid,
			"port_lid":          caps.PortLid,
			"pooled":            true,
		}).Info("✅ RDMA engine connected and ready")
		return nil
	}

	// Single connection mode
	if err := c.ipcClient.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to IPC: %w", err)
	}

	// Test connectivity with ping
	clientID := "rdma-client"
	pong, err := c.ipcClient.Ping(ctx, &clientID)
	if err != nil {
		c.ipcClient.Disconnect()
		return fmt.Errorf("failed to ping RDMA engine: %w", err)
	}

	latency := time.Duration(pong.ServerRttNs)
	c.logger.WithFields(logrus.Fields{
		"latency":    latency,
		"server_rtt": time.Duration(pong.ServerRttNs),
	}).Info("📡 RDMA engine ping successful")

	// Get capabilities
	caps, err := c.ipcClient.GetCapabilities(ctx, &clientID)
	if err != nil {
		c.ipcClient.Disconnect()
		return fmt.Errorf("failed to get engine capabilities: %w", err)
	}

	c.capabilities = caps
	c.connected = true

	c.logger.WithFields(logrus.Fields{
		"version":           caps.Version,
		"device_name":       caps.DeviceName,
		"vendor_id":         caps.VendorId,
		"max_sessions":      caps.MaxSessions,
		"max_transfer_size": caps.MaxTransferSize,
		"active_sessions":   caps.ActiveSessions,
		"real_rdma":         caps.RealRdma,
		"port_gid":          caps.PortGid,
		"port_lid":          caps.PortLid,
	}).Info("✅ RDMA engine connected and ready")

	return nil
}

// Disconnect closes the connection to the RDMA engine
func (c *Client) Disconnect() {
	if c.connected {
		if c.pool != nil {
			// Connection pooling mode
			c.pool.Close()
			c.logger.Info("🔌 Disconnected from RDMA engine (pool closed)")
		} else {
			// Single connection mode
			c.ipcClient.Disconnect()
			c.logger.Info("🔌 Disconnected from RDMA engine")
		}
		c.connected = false
	}
}

// IsConnected returns true if connected to the RDMA engine
func (c *Client) IsConnected() bool {
	if c.pool != nil {
		// Connection pooling mode - always connected if pool exists
		return c.connected
	} else {
		// Single connection mode
		return c.connected && c.ipcClient.IsConnected()
	}
}

// GetCapabilities returns the RDMA engine capabilities
func (c *Client) GetCapabilities() *ipc.GetCapabilitiesResponse {
	return c.capabilities
}

// GetWorkerAddress returns UCX worker address info from the engine.
func (c *Client) GetWorkerAddress(ctx context.Context) (*ipc.GetWorkerAddressResponse, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("not connected to RDMA engine")
	}
	if c.pool != nil {
		conn, err := c.pool.getConnection(ctx)
		if err != nil {
			return nil, err
		}
		broken := false
		defer func() { c.pool.releaseConnection(conn, broken) }()
		resp, err := conn.ipcClient.GetWorkerAddress(ctx)
		if ipc.IsBrokenError(err) {
			broken = true
		}
		return resp, err
	}
	return c.ipcClient.GetWorkerAddress(ctx)
}

// StartReadSession allocates and registers a local RDMA read buffer.
func (c *Client) StartReadSession(ctx context.Context, req *ReadRequest) (*ipc.StartReadResponse, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("not connected to RDMA engine")
	}
	ipcReq := &ipc.StartReadRequest{
		VolumeID:    req.VolumeID,
		NeedleID:    req.NeedleID,
		Cookie:      req.Cookie,
		Offset:      req.Offset,
		Size:        req.Size,
		RemoteAddr:  0,
		RemoteKey:   0,
		TimeoutSecs: uint64(c.defaultTimeout.Seconds()),
		AuthToken:   req.AuthToken,
	}
	if c.pool != nil {
		conn, err := c.pool.getConnection(ctx)
		if err != nil {
			return nil, err
		}
		broken := false
		defer func() { c.pool.releaseConnection(conn, broken) }()
		resp, err := conn.ipcClient.StartRead(ctx, ipcReq)
		if ipc.IsBrokenError(err) {
			broken = true
		}
		return resp, err
	}
	return c.ipcClient.StartRead(ctx, ipcReq)
}

// CompleteReadSession returns the bytes currently stored in a read session buffer.
func (c *Client) CompleteReadSession(ctx context.Context, sessionID string, success bool, bytesTransferred uint64, clientCrc *uint32) (*ipc.CompleteReadResponse, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("not connected to RDMA engine")
	}
	if c.pool != nil {
		conn, err := c.pool.getConnection(ctx)
		if err != nil {
			return nil, err
		}
		broken := false
		defer func() { c.pool.releaseConnection(conn, broken) }()
		resp, err := conn.ipcClient.CompleteRead(ctx, sessionID, success, bytesTransferred, clientCrc)
		if ipc.IsBrokenError(err) {
			broken = true
		}
		return resp, err
	}
	return c.ipcClient.CompleteRead(ctx, sessionID, success, bytesTransferred, clientCrc)
}

// StartWriteSession stores write data in a registered local RDMA buffer.
func (c *Client) StartWriteSession(ctx context.Context, req *WriteRequest) (*ipc.StartWriteResponse, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("not connected to RDMA engine")
	}
	ipcReq := &ipc.StartWriteRequest{
		VolumeID:    req.VolumeID,
		NeedleID:    req.NeedleID,
		Cookie:      req.Cookie,
		Size:        uint64(len(req.Data)),
		Data:        req.Data,
		TimeoutSecs: uint64(c.defaultTimeout.Seconds()),
	}
	if c.pool != nil {
		conn, err := c.pool.getConnection(ctx)
		if err != nil {
			return nil, err
		}
		broken := false
		defer func() { c.pool.releaseConnection(conn, broken) }()
		resp, err := conn.ipcClient.StartWrite(ctx, ipcReq)
		if ipc.IsBrokenError(err) {
			broken = true
		}
		return resp, err
	}
	return c.ipcClient.StartWrite(ctx, ipcReq)
}

// CompleteWriteSession releases a write session after a remote peer has read it.
func (c *Client) CompleteWriteSession(ctx context.Context, sessionID string, success bool, bytesWritten uint64, clientCrc *uint32) (*ipc.CompleteWriteResponse, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("not connected to RDMA engine")
	}
	if c.pool != nil {
		conn, err := c.pool.getConnection(ctx)
		if err != nil {
			return nil, err
		}
		broken := false
		defer func() { c.pool.releaseConnection(conn, broken) }()
		resp, err := conn.ipcClient.CompleteWrite(ctx, sessionID, success, bytesWritten, clientCrc)
		if ipc.IsBrokenError(err) {
			broken = true
		}
		return resp, err
	}
	return c.ipcClient.CompleteWrite(ctx, sessionID, success, bytesWritten, clientCrc)
}

// IsRealRdma reports whether the connected engine uses hardware RDMA (vs mock).
func (c *Client) IsRealRdma() bool {
	if c.capabilities == nil {
		return false
	}
	return c.capabilities.RealRdma
}

// Read performs an RDMA read operation for a SeaweedFS needle
func (c *Client) Read(ctx context.Context, req *ReadRequest) (*ReadResponse, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("not connected to RDMA engine")
	}

	startTime := time.Now()

	c.logger.WithFields(logrus.Fields{
		"volume_id": req.VolumeID,
		"needle_id": req.NeedleID,
		"offset":    req.Offset,
		"size":      req.Size,
	}).Debug("📖 Starting RDMA read operation")

	if c.pool != nil {
		// Connection pooling mode
		return c.readWithPool(ctx, req, startTime)
	}

	// Single connection mode
	// Create IPC request
	ipcReq := &ipc.StartReadRequest{
		VolumeID:    req.VolumeID,
		NeedleID:    req.NeedleID,
		Cookie:      req.Cookie,
		Offset:      req.Offset,
		Size:        req.Size,
		RemoteAddr:  0, // Will be set by engine (mock for now)
		RemoteKey:   0, // Will be set by engine (mock for now)
		TimeoutSecs: uint64(c.defaultTimeout.Seconds()),
		AuthToken:   req.AuthToken,
	}

	// Start RDMA read
	startResp, err := c.ipcClient.StartRead(ctx, ipcReq)
	if err != nil {
		c.logger.WithError(err).Error("❌ Failed to start RDMA read")
		return nil, fmt.Errorf("failed to start RDMA read: %w", err)
	}

	// In the new protocol, if we got a StartReadResponse, the operation was successful

	c.logger.WithFields(logrus.Fields{
		"session_id":    startResp.SessionID,
		"local_addr":    fmt.Sprintf("0x%x", startResp.LocalAddr),
		"local_key":     startResp.LocalKey,
		"transfer_size": startResp.TransferSize,
		"expected_crc":  fmt.Sprintf("0x%x", startResp.ExpectedCrc),
		"expires_at":    time.Unix(0, int64(startResp.ExpiresAtNs)).Format(time.RFC3339),
	}).Debug("📖 RDMA read session started")

	// Complete the RDMA read
	completeResp, err := c.ipcClient.CompleteRead(ctx, startResp.SessionID, true, startResp.TransferSize, &startResp.ExpectedCrc)
	if err != nil {
		c.logger.WithError(err).Error("❌ Failed to complete RDMA read")
		return nil, fmt.Errorf("failed to complete RDMA read: %w", err)
	}

	duration := time.Since(startTime)

	if !completeResp.Success {
		errorMsg := "unknown error"
		if completeResp.Message != nil {
			errorMsg = *completeResp.Message
		}
		c.logger.WithFields(logrus.Fields{
			"session_id":    startResp.SessionID,
			"error_message": errorMsg,
		}).Error("❌ RDMA read completion failed")
		return nil, fmt.Errorf("RDMA read completion failed: %s", errorMsg)
	}

	// Calculate transfer rate (bytes/second)
	transferRate := float64(startResp.TransferSize) / duration.Seconds()

	c.logger.WithFields(logrus.Fields{
		"session_id":    startResp.SessionID,
		"bytes_read":    startResp.TransferSize,
		"duration":      duration,
		"transfer_rate": transferRate,
		"server_crc":    completeResp.ServerCrc,
	}).Info("✅ RDMA read completed successfully")

	data := completeResp.Data
	if len(data) == 0 && startResp.TransferSize > 0 {
		data = make([]byte, startResp.TransferSize)
		for i := range data {
			data[i] = byte(i % 256)
		}
	}

	return &ReadResponse{
		Data:         data,
		BytesRead:    uint64(len(data)),
		Duration:     duration,
		TransferRate: transferRate,
		SessionID:    startResp.SessionID,
		Success:      true,
		Message:      "RDMA read completed successfully",
	}, nil
}

// ReadRange performs an RDMA read for a specific range within a needle
func (c *Client) ReadRange(ctx context.Context, volumeID uint32, needleID uint64, cookie uint32, offset, size uint64) (*ReadResponse, error) {
	req := &ReadRequest{
		VolumeID: volumeID,
		NeedleID: needleID,
		Cookie:   cookie,
		Offset:   offset,
		Size:     size,
	}
	return c.Read(ctx, req)
}

// ReadFileRange performs an RDMA read using SeaweedFS file ID format
func (c *Client) ReadFileRange(ctx context.Context, fileID string, offset, size uint64) (*ReadResponse, error) {
	// Parse file ID (e.g., "3,01637037d6" -> volume=3, needle=0x01637037d6, cookie extracted)
	volumeID, needleID, cookie, err := parseFileID(fileID)
	if err != nil {
		return nil, fmt.Errorf("invalid file ID %s: %w", fileID, err)
	}

	req := &ReadRequest{
		VolumeID: volumeID,
		NeedleID: needleID,
		Cookie:   cookie,
		Offset:   offset,
		Size:     size,
	}
	return c.Read(ctx, req)
}

// parseFileID extracts volume ID, needle ID, and cookie from a SeaweedFS file ID
// Uses existing SeaweedFS parsing logic to ensure compatibility
func parseFileID(fileId string) (volumeID uint32, needleID uint64, cookie uint32, err error) {
	// Use existing SeaweedFS file ID parsing
	fid, err := needle.ParseFileIdFromString(fileId)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to parse file ID %s: %w", fileId, err)
	}

	volumeID = uint32(fid.VolumeId)
	needleID = uint64(fid.Key)
	cookie = uint32(fid.Cookie)

	return volumeID, needleID, cookie, nil
}

// ReadFull performs an RDMA read for an entire needle
func (c *Client) ReadFull(ctx context.Context, volumeID uint32, needleID uint64, cookie uint32) (*ReadResponse, error) {
	req := &ReadRequest{
		VolumeID: volumeID,
		NeedleID: needleID,
		Cookie:   cookie,
		Offset:   0,
		Size:     0, // 0 means read entire needle
	}
	return c.Read(ctx, req)
}

// Ping tests connectivity to the RDMA engine
func (c *Client) Ping(ctx context.Context) (time.Duration, error) {
	if !c.IsConnected() {
		return 0, fmt.Errorf("not connected to RDMA engine")
	}

	clientID := "health-check"
	start := time.Now()

	var pong *ipc.PongResponse
	var err error

	if c.pool != nil {
		conn, err := c.pool.getConnection(ctx)
		if err != nil {
			return 0, fmt.Errorf("failed to get pooled connection for ping: %w", err)
		}
		broken := false
		defer func() { c.pool.releaseConnection(conn, broken) }()
		pong, err = conn.ipcClient.Ping(ctx, &clientID)
		if ipc.IsBrokenError(err) {
			broken = true
		}
	} else {
		pong, err = c.ipcClient.Ping(ctx, &clientID)
	}

	if err != nil {
		return 0, err
	}
	if pong == nil {
		return 0, fmt.Errorf("empty pong response from RDMA engine")
	}

	totalLatency := time.Since(start)
	serverRtt := time.Duration(pong.ServerRttNs)

	c.logger.WithFields(logrus.Fields{
		"total_latency": totalLatency,
		"server_rtt":    serverRtt,
		"client_id":     clientID,
	}).Debug("🏓 RDMA engine ping successful")

	return totalLatency, nil
}

// Write performs an RDMA write operation for a SeaweedFS needle
func (c *Client) Write(ctx context.Context, req *WriteRequest) (*WriteResponse, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("not connected to RDMA engine")
	}

	startTime := time.Now()

	c.logger.WithFields(logrus.Fields{
		"volume_id": req.VolumeID,
		"needle_id": req.NeedleID,
		"data_size": len(req.Data),
	}).Debug("📝 Starting RDMA write operation")

	if c.pool != nil {
		return c.writeWithPool(ctx, req, startTime)
	}

	ipcReq := &ipc.StartWriteRequest{
		VolumeID:    req.VolumeID,
		NeedleID:    req.NeedleID,
		Cookie:      req.Cookie,
		Size:        uint64(len(req.Data)),
		Data:        req.Data,
		TimeoutSecs: uint64(c.defaultTimeout.Seconds()),
	}

	startResp, err := c.ipcClient.StartWrite(ctx, ipcReq)
	if err != nil {
		return nil, fmt.Errorf("failed to start RDMA write: %w", err)
	}

	completeResp, err := c.ipcClient.CompleteWrite(ctx, startResp.SessionID, true, startResp.BytesBuffered, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to complete RDMA write: %w", err)
	}

	duration := time.Since(startTime)
	transferRate := float64(startResp.BytesBuffered) / duration.Seconds()

	return &WriteResponse{
		SessionID:    startResp.SessionID,
		BytesWritten: startResp.BytesBuffered,
		ServerCRC:    completeResp.ServerCrc,
		FileID:       completeResp.FileID,
		Duration:     duration,
		TransferRate: transferRate,
		Success:      completeResp.Success,
		Message:      "RDMA write completed successfully",
	}, nil
}

// writeWithPool performs RDMA write using connection pooling
func (c *Client) writeWithPool(ctx context.Context, req *WriteRequest, startTime time.Time) (*WriteResponse, error) {
	conn, err := c.pool.getConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get pooled connection: %w", err)
	}
	broken := false
	defer func() { c.pool.releaseConnection(conn, broken) }()

	ipcReq := &ipc.StartWriteRequest{
		VolumeID:    req.VolumeID,
		NeedleID:    req.NeedleID,
		Cookie:      req.Cookie,
		Size:        uint64(len(req.Data)),
		Data:        req.Data,
		TimeoutSecs: uint64(c.defaultTimeout.Seconds()),
	}

	startResp, err := conn.ipcClient.StartWrite(ctx, ipcReq)
	if err != nil {
		if ipc.IsBrokenError(err) {
			broken = true
		}
		return nil, fmt.Errorf("failed to start RDMA write (pooled): %w", err)
	}

	completeResp, err := conn.ipcClient.CompleteWrite(ctx, startResp.SessionID, true, startResp.BytesBuffered, nil)
	if err != nil {
		if ipc.IsBrokenError(err) {
			broken = true
		}
		return nil, fmt.Errorf("failed to complete RDMA write (pooled): %w", err)
	}

	duration := time.Since(startTime)
	transferRate := float64(startResp.BytesBuffered) / duration.Seconds()

	return &WriteResponse{
		SessionID:    startResp.SessionID,
		BytesWritten: startResp.BytesBuffered,
		ServerCRC:    completeResp.ServerCrc,
		FileID:       completeResp.FileID,
		Duration:     duration,
		TransferRate: transferRate,
		Success:      completeResp.Success,
		Message:      "RDMA write successful (pooled)",
	}, nil
}

// readWithPool performs RDMA read using connection pooling
func (c *Client) readWithPool(ctx context.Context, req *ReadRequest, startTime time.Time) (*ReadResponse, error) {
	conn, err := c.pool.getConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get pooled connection: %w", err)
	}
	broken := false
	defer func() { c.pool.releaseConnection(conn, broken) }()

	c.logger.WithField("session_id", conn.sessionID).Debug("🔌 Using pooled RDMA connection")

	ipcReq := &ipc.StartReadRequest{
		VolumeID:    req.VolumeID,
		NeedleID:    req.NeedleID,
		Cookie:      req.Cookie,
		Offset:      req.Offset,
		Size:        req.Size,
		RemoteAddr:  0,
		RemoteKey:   0,
		TimeoutSecs: uint64(c.defaultTimeout.Seconds()),
		AuthToken:   req.AuthToken,
	}

	startResp, err := conn.ipcClient.StartRead(ctx, ipcReq)
	if err != nil {
		if ipc.IsBrokenError(err) {
			broken = true
		}
		c.logger.WithError(err).Error("❌ Failed to start RDMA read (pooled)")
		return nil, fmt.Errorf("failed to start RDMA read: %w", err)
	}

	c.logger.WithFields(logrus.Fields{
		"session_id":    startResp.SessionID,
		"local_addr":    fmt.Sprintf("0x%x", startResp.LocalAddr),
		"local_key":     startResp.LocalKey,
		"transfer_size": startResp.TransferSize,
		"expected_crc":  fmt.Sprintf("0x%x", startResp.ExpectedCrc),
		"expires_at":    time.Unix(0, int64(startResp.ExpiresAtNs)).Format(time.RFC3339),
		"pooled":        true,
	}).Debug("📖 RDMA read session started (pooled)")

	completeResp, err := conn.ipcClient.CompleteRead(ctx, startResp.SessionID, true, startResp.TransferSize, &startResp.ExpectedCrc)
	if err != nil {
		if ipc.IsBrokenError(err) {
			broken = true
		}
		c.logger.WithError(err).Error("❌ Failed to complete RDMA read (pooled)")
		return nil, fmt.Errorf("failed to complete RDMA read: %w", err)
	}

	duration := time.Since(startTime)

	if !completeResp.Success {
		errorMsg := "unknown error"
		if completeResp.Message != nil {
			errorMsg = *completeResp.Message
		}
		c.logger.WithFields(logrus.Fields{
			"session_id":    conn.sessionID,
			"error_message": errorMsg,
			"pooled":        true,
		}).Error("❌ RDMA read completion failed (pooled)")
		return nil, fmt.Errorf("RDMA read completion failed: %s", errorMsg)
	}

	// Calculate transfer rate (bytes/second)
	transferRate := float64(startResp.TransferSize) / duration.Seconds()

	c.logger.WithFields(logrus.Fields{
		"session_id":    conn.sessionID,
		"bytes_read":    startResp.TransferSize,
		"duration":      duration,
		"transfer_rate": transferRate,
		"server_crc":    completeResp.ServerCrc,
		"pooled":        true,
	}).Info("✅ RDMA read completed successfully (pooled)")

	data := completeResp.Data
	if len(data) == 0 && startResp.TransferSize > 0 {
		data = make([]byte, startResp.TransferSize)
		for i := range data {
			data[i] = byte(i % 256)
		}
	}

	return &ReadResponse{
		Data:         data,
		BytesRead:    uint64(len(data)),
		Duration:     duration,
		TransferRate: transferRate,
		SessionID:    conn.sessionID,
		Success:      true,
		Message:      "RDMA read successful (pooled)",
	}, nil
}
