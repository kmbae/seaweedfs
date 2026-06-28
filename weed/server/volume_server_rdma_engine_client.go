package weed_server

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

const (
	volumeRdmaEngineOpLocal              = "local"
	volumeRdmaEngineOpConnect            = "connect"
	volumeRdmaEngineOpRegisterRead       = "register_read"
	volumeRdmaEngineOpRegisterReadStream = "register_read_stream"
	volumeRdmaEngineOpRegisterWrite      = "register_write"
	volumeRdmaEngineOpReadRegistered     = "read_registered"
	volumeRdmaEngineOpRelease            = "release"
	volumeRdmaEngineOpRequesterLocal     = "requester_local"
	volumeRdmaEngineOpRequesterConnect   = "requester_connect"
	volumeRdmaEngineOpReadRemote         = "read_remote"

	volumeRdmaEngineMaxFrameSize = 64 << 20
)

var ErrVolumeRdmaEngineUnavailable = errors.New("native RDMA engine is unavailable")

type VolumeRdmaEngineClient struct {
	SocketPath string
	Timeout    time.Duration
}

type volumeRdmaEngineRequest struct {
	Op           string                `json:"op"`
	ConnectionID uint64                `json:"connection_id,omitempty"`
	Remote       *VolumeRdmaRemoteInfo `json:"remote,omitempty"`
	Desc         *VolumeRdmaDataDesc   `json:"desc,omitempty"`
	SessionID    uint64                `json:"session_id,omitempty"`
	TimeoutMs    uint64                `json:"timeout_ms,omitempty"`
	Size         uint64                `json:"size,omitempty"`
	DataSideband bool                  `json:"data_sideband,omitempty"`
	Data         []byte                `json:"data,omitempty"`
}

type volumeRdmaEngineResponse struct {
	OK           bool                    `json:"ok"`
	Error        string                  `json:"error,omitempty"`
	Endpoint     *VolumeRdmaEndpointInfo `json:"endpoint,omitempty"`
	Desc         *VolumeRdmaDataDesc     `json:"desc,omitempty"`
	ConnectionID uint64                  `json:"connection_id,omitempty"`
	SessionID    uint64                  `json:"session_id,omitempty"`
	DataSideband bool                    `json:"data_sideband,omitempty"`
	Data         []byte                  `json:"data,omitempty"`
}

type volumeRdmaEngineRegisteredBuffer struct {
	client    *VolumeRdmaEngineClient
	sessionID uint64
	desc      VolumeRdmaDataDesc
}

func NewVolumeRdmaEngineClient(socketPath string, timeout time.Duration) *VolumeRdmaEngineClient {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &VolumeRdmaEngineClient{
		SocketPath: socketPath,
		Timeout:    timeout,
	}
}

func (vs *VolumeServer) ConfigureRdmaEngine(socketPath string, timeout time.Duration, cfg VolumeRdmaReadExporterConfig) error {
	return vs.ConfigureRdmaTransport(VolumeRdmaTransportConfig{
		Transport:     VolumeRdmaTransportSocket,
		EngineSocket:  socketPath,
		EngineTimeout: timeout,
		ReadExporter:  cfg,
	})
}

func (c *VolumeRdmaEngineClient) LocalEndpoint(ctx context.Context) (VolumeRdmaEndpointInfo, error) {
	endpoint, _, err := c.LocalEndpointFor(ctx, 0)
	return endpoint, err
}

func (c *VolumeRdmaEngineClient) LocalEndpointFor(ctx context.Context, connectionID uint64) (VolumeRdmaEndpointInfo, uint64, error) {
	var endpoint VolumeRdmaEndpointInfo
	resp, err := c.roundTrip(ctx, volumeRdmaEngineRequest{
		Op:           volumeRdmaEngineOpLocal,
		ConnectionID: connectionID,
	})
	if err != nil {
		return endpoint, 0, err
	}
	if resp.Endpoint == nil {
		return endpoint, 0, fmt.Errorf("%w: local response missing endpoint", ErrVolumeRdmaEngineUnavailable)
	}
	endpoint = *resp.Endpoint
	if resp.ConnectionID != 0 {
		endpoint.ConnectionID = resp.ConnectionID
	}
	return endpoint, endpoint.ConnectionID, nil
}

func (c *VolumeRdmaEngineClient) ConnectEndpoint(ctx context.Context, remote VolumeRdmaRemoteInfo) error {
	return c.ConnectEndpointFor(ctx, 0, remote)
}

func (c *VolumeRdmaEngineClient) ConnectEndpointFor(ctx context.Context, connectionID uint64, remote VolumeRdmaRemoteInfo) error {
	_, err := c.roundTrip(ctx, volumeRdmaEngineRequest{
		Op:           volumeRdmaEngineOpConnect,
		ConnectionID: connectionID,
		Remote:       &remote,
	})
	return err
}

func (c *VolumeRdmaEngineClient) RequesterLocalEndpoint(ctx context.Context) (VolumeRdmaEndpointInfo, error) {
	endpoint, _, err := c.RequesterLocalEndpointFor(ctx, 0)
	return endpoint, err
}

func (c *VolumeRdmaEngineClient) RequesterLocalEndpointFor(ctx context.Context, connectionID uint64) (VolumeRdmaEndpointInfo, uint64, error) {
	var endpoint VolumeRdmaEndpointInfo
	resp, err := c.roundTrip(ctx, volumeRdmaEngineRequest{
		Op:           volumeRdmaEngineOpRequesterLocal,
		ConnectionID: connectionID,
	})
	if err != nil {
		return endpoint, 0, err
	}
	if resp.Endpoint == nil {
		return endpoint, 0, fmt.Errorf("%w: requester_local response missing endpoint", ErrVolumeRdmaEngineUnavailable)
	}
	endpoint = *resp.Endpoint
	if resp.ConnectionID != 0 {
		endpoint.ConnectionID = resp.ConnectionID
	}
	return endpoint, endpoint.ConnectionID, nil
}

func (c *VolumeRdmaEngineClient) RequesterConnectEndpoint(ctx context.Context, remote VolumeRdmaRemoteInfo) error {
	return c.RequesterConnectEndpointFor(ctx, 0, remote)
}

func (c *VolumeRdmaEngineClient) RequesterConnectEndpointFor(ctx context.Context, connectionID uint64, remote VolumeRdmaRemoteInfo) error {
	_, err := c.roundTrip(ctx, volumeRdmaEngineRequest{
		Op:           volumeRdmaEngineOpRequesterConnect,
		ConnectionID: connectionID,
		Remote:       &remote,
	})
	return err
}

func (c *VolumeRdmaEngineClient) ReadRemoteFor(ctx context.Context, connectionID uint64, desc VolumeRdmaDataDesc, timeout time.Duration) ([]byte, error) {
	timeoutMs := uint64(timeout.Milliseconds())
	if timeoutMs == 0 && timeout > 0 {
		timeoutMs = 1
	}
	resp, err := c.roundTrip(ctx, volumeRdmaEngineRequest{
		Op:           volumeRdmaEngineOpReadRemote,
		ConnectionID: connectionID,
		Desc:         &desc,
		TimeoutMs:    timeoutMs,
	})
	if err != nil {
		return nil, err
	}
	if len(resp.Data) == 0 && desc.Length > 0 {
		return nil, fmt.Errorf("%w: read_remote returned empty data for %d byte descriptor", ErrVolumeRdmaEngineUnavailable, desc.Length)
	}
	return resp.Data, nil
}

func (c *VolumeRdmaEngineClient) ReadRemoteToFor(ctx context.Context, connectionID uint64, desc VolumeRdmaDataDesc, timeout time.Duration, dst io.Writer) error {
	if dst == nil {
		return fmt.Errorf("native RDMA read_remote requires destination writer")
	}
	timeoutMs := uint64(timeout.Milliseconds())
	if timeoutMs == 0 && timeout > 0 {
		timeoutMs = 1
	}
	written, err := c.roundTripSidebandToWriter(ctx, volumeRdmaEngineRequest{
		Op:           volumeRdmaEngineOpReadRemote,
		ConnectionID: connectionID,
		Desc:         &desc,
		TimeoutMs:    timeoutMs,
	}, dst)
	if err != nil {
		return err
	}
	if written != uint64(desc.Length) {
		return fmt.Errorf("%w: read_remote streamed %d bytes for %d byte descriptor", ErrVolumeRdmaEngineUnavailable, written, desc.Length)
	}
	return nil
}

func (c *VolumeRdmaEngineClient) RegisterReadBuffer(ctx context.Context, data []byte) (VolumeRdmaRegisteredBuffer, error) {
	return c.RegisterReadBufferFor(ctx, 0, data)
}

func (c *VolumeRdmaEngineClient) RegisterReadBufferFor(ctx context.Context, connectionID uint64, data []byte) (VolumeRdmaRegisteredBuffer, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("native RDMA register_read requires data")
	}
	resp, err := c.roundTripWithSideband(ctx, volumeRdmaEngineRequest{
		Op:           volumeRdmaEngineOpRegisterRead,
		ConnectionID: connectionID,
		DataSideband: true,
	}, data)
	if err != nil {
		return nil, err
	}
	if resp.Desc == nil {
		return nil, fmt.Errorf("%w: register_read response missing descriptor", ErrVolumeRdmaEngineUnavailable)
	}
	if resp.SessionID == 0 {
		return nil, fmt.Errorf("%w: register_read response missing session_id", ErrVolumeRdmaEngineUnavailable)
	}
	return &volumeRdmaEngineRegisteredBuffer{
		client:    c,
		sessionID: resp.SessionID,
		desc:      *resp.Desc,
	}, nil
}

func (c *VolumeRdmaEngineClient) RegisterReadStreamFor(ctx context.Context, connectionID uint64, size uint64, writeData func(io.Writer) error) (VolumeRdmaRegisteredBuffer, error) {
	if size == 0 {
		return nil, fmt.Errorf("native RDMA register_read_stream requires data")
	}
	if size > volumeRdmaEngineMaxFrameSize {
		return nil, fmt.Errorf("native RDMA register_read_stream frame too large: %d bytes", size)
	}
	if writeData == nil {
		return nil, fmt.Errorf("native RDMA register_read_stream requires writer")
	}
	resp, err := c.roundTripWithStream(ctx, volumeRdmaEngineRequest{
		Op:           volumeRdmaEngineOpRegisterReadStream,
		ConnectionID: connectionID,
	}, size, writeData)
	if err != nil {
		return nil, err
	}
	if resp.Desc == nil {
		return nil, fmt.Errorf("%w: register_read_stream response missing descriptor", ErrVolumeRdmaEngineUnavailable)
	}
	if resp.SessionID == 0 {
		return nil, fmt.Errorf("%w: register_read_stream response missing session_id", ErrVolumeRdmaEngineUnavailable)
	}
	return &volumeRdmaEngineRegisteredBuffer{
		client:    c,
		sessionID: resp.SessionID,
		desc:      *resp.Desc,
	}, nil
}

func (c *VolumeRdmaEngineClient) RegisterWriteBufferFor(ctx context.Context, connectionID uint64, size uint64) (VolumeRdmaRegisteredBuffer, error) {
	if size == 0 {
		return nil, fmt.Errorf("native RDMA register_write requires size")
	}
	if size > volumeRdmaEngineMaxFrameSize {
		return nil, fmt.Errorf("native RDMA register_write frame too large: %d bytes", size)
	}
	resp, err := c.roundTrip(ctx, volumeRdmaEngineRequest{
		Op:           volumeRdmaEngineOpRegisterWrite,
		ConnectionID: connectionID,
		Size:         size,
	})
	if err != nil {
		return nil, err
	}
	if resp.Desc == nil {
		return nil, fmt.Errorf("%w: register_write response missing descriptor", ErrVolumeRdmaEngineUnavailable)
	}
	if resp.SessionID == 0 {
		return nil, fmt.Errorf("%w: register_write response missing session_id", ErrVolumeRdmaEngineUnavailable)
	}
	return &volumeRdmaEngineRegisteredBuffer{
		client:    c,
		sessionID: resp.SessionID,
		desc:      *resp.Desc,
	}, nil
}

func (c *VolumeRdmaEngineClient) ReadRegisteredBuffer(ctx context.Context, sessionID uint64, size uint64) ([]byte, error) {
	if sessionID == 0 {
		return nil, fmt.Errorf("native RDMA read_registered requires session_id")
	}
	if size == 0 {
		return nil, fmt.Errorf("native RDMA read_registered requires size")
	}
	if size > volumeRdmaEngineMaxFrameSize {
		return nil, fmt.Errorf("native RDMA read_registered frame too large: %d bytes", size)
	}
	resp, err := c.roundTrip(ctx, volumeRdmaEngineRequest{
		Op:        volumeRdmaEngineOpReadRegistered,
		SessionID: sessionID,
		Size:      size,
	})
	if err != nil {
		return nil, err
	}
	if uint64(len(resp.Data)) < size {
		return nil, fmt.Errorf("%w: read_registered returned %d bytes for %d byte descriptor", ErrVolumeRdmaEngineUnavailable, len(resp.Data), size)
	}
	return resp.Data[:size], nil
}

func (c *VolumeRdmaEngineClient) ReadRegisteredBufferTo(ctx context.Context, sessionID uint64, size uint64, dst io.Writer) error {
	if sessionID == 0 {
		return fmt.Errorf("native RDMA read_registered requires session_id")
	}
	if size == 0 {
		return fmt.Errorf("native RDMA read_registered requires size")
	}
	if size > volumeRdmaEngineMaxFrameSize {
		return fmt.Errorf("native RDMA read_registered frame too large: %d bytes", size)
	}
	if dst == nil {
		return fmt.Errorf("native RDMA read_registered requires destination writer")
	}
	written, err := c.roundTripSidebandToWriter(ctx, volumeRdmaEngineRequest{
		Op:        volumeRdmaEngineOpReadRegistered,
		SessionID: sessionID,
		Size:      size,
	}, dst)
	if err != nil {
		return err
	}
	if written != size {
		return fmt.Errorf("%w: read_registered streamed %d bytes for %d byte descriptor", ErrVolumeRdmaEngineUnavailable, written, size)
	}
	return nil
}

func (c *VolumeRdmaEngineClient) ReleaseSession(ctx context.Context, sessionID uint64) error {
	if sessionID == 0 {
		return nil
	}
	_, err := c.roundTrip(ctx, volumeRdmaEngineRequest{
		Op:        volumeRdmaEngineOpRelease,
		SessionID: sessionID,
	})
	return err
}

func (b *volumeRdmaEngineRegisteredBuffer) Descriptor() VolumeRdmaDataDesc {
	if b == nil {
		return VolumeRdmaDataDesc{}
	}
	return b.desc
}

func (b *volumeRdmaEngineRegisteredBuffer) SessionID() uint64 {
	if b == nil {
		return 0
	}
	return b.sessionID
}

func (b *volumeRdmaEngineRegisteredBuffer) Release(ctx context.Context) error {
	if b == nil || b.client == nil || b.sessionID == 0 {
		return nil
	}
	_, err := b.client.roundTrip(ctx, volumeRdmaEngineRequest{
		Op:        volumeRdmaEngineOpRelease,
		SessionID: b.sessionID,
	})
	return err
}

func (c *VolumeRdmaEngineClient) roundTrip(ctx context.Context, req volumeRdmaEngineRequest) (*volumeRdmaEngineResponse, error) {
	return c.roundTripWithSideband(ctx, req, nil)
}

func (c *VolumeRdmaEngineClient) roundTripWithSideband(ctx context.Context, req volumeRdmaEngineRequest, sideband []byte) (*volumeRdmaEngineResponse, error) {
	return c.roundTripWrite(ctx, req, func(conn net.Conn) error {
		if sideband != nil {
			if err := writeVolumeRdmaEngineFrame(conn, sideband); err != nil {
				return fmt.Errorf("write request sideband: %w", err)
			}
		}
		return nil
	})
}

func (c *VolumeRdmaEngineClient) roundTripWithStream(ctx context.Context, req volumeRdmaEngineRequest, size uint64, writeData func(io.Writer) error) (*volumeRdmaEngineResponse, error) {
	return c.roundTripWrite(ctx, req, func(conn net.Conn) error {
		if err := writeVolumeRdmaEngineFrameHeader(conn, size); err != nil {
			return fmt.Errorf("write stream frame header: %w", err)
		}
		writer := &volumeRdmaEngineExactFrameWriter{
			w:         conn,
			remaining: size,
		}
		if err := writeData(writer); err != nil {
			return fmt.Errorf("write stream frame payload: %w", err)
		}
		if writer.remaining != 0 {
			return fmt.Errorf("write stream frame payload: short write: wrote %d of %d bytes", writer.written, size)
		}
		return nil
	})
}

func (c *VolumeRdmaEngineClient) roundTripWrite(ctx context.Context, req volumeRdmaEngineRequest, writeExtra func(net.Conn) error) (*volumeRdmaEngineResponse, error) {
	if c == nil || strings.TrimSpace(c.SocketPath) == "" {
		return nil, ErrVolumeRdmaReadNotConfigured
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	payload, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	if len(payload) > volumeRdmaEngineMaxFrameSize {
		return nil, fmt.Errorf("native RDMA engine request too large: %d bytes", len(payload))
	}

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", c.SocketPath)
	if err != nil {
		return nil, fmt.Errorf("%w: dial %s: %v", ErrVolumeRdmaEngineUnavailable, c.SocketPath, err)
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if err := writeVolumeRdmaEngineFrame(conn, payload); err != nil {
		return nil, fmt.Errorf("%w: write request: %v", ErrVolumeRdmaEngineUnavailable, err)
	}
	if writeExtra != nil {
		if err := writeExtra(conn); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrVolumeRdmaEngineUnavailable, err)
		}
	}
	responsePayload, err := readVolumeRdmaEngineFrame(conn)
	if err != nil {
		return nil, fmt.Errorf("%w: read response: %v", ErrVolumeRdmaEngineUnavailable, err)
	}
	var resp volumeRdmaEngineResponse
	if err := json.Unmarshal(responsePayload, &resp); err != nil {
		return nil, fmt.Errorf("%w: decode response: %v", ErrVolumeRdmaEngineUnavailable, err)
	}
	if !resp.OK {
		if resp.Error == "" {
			resp.Error = "native RDMA engine returned failure"
		}
		return nil, fmt.Errorf("%w: %s", ErrVolumeRdmaEngineUnavailable, resp.Error)
	}
	if resp.DataSideband {
		if len(resp.Data) != 0 {
			return nil, fmt.Errorf("%w: response must not include JSON data with data_sideband", ErrVolumeRdmaEngineUnavailable)
		}
		resp.Data, err = readVolumeRdmaEngineFrame(conn)
		if err != nil {
			return nil, fmt.Errorf("%w: read response sideband: %v", ErrVolumeRdmaEngineUnavailable, err)
		}
	}
	return &resp, nil
}

func (c *VolumeRdmaEngineClient) roundTripSidebandToWriter(ctx context.Context, req volumeRdmaEngineRequest, dst io.Writer) (uint64, error) {
	if c == nil || strings.TrimSpace(c.SocketPath) == "" {
		return 0, ErrVolumeRdmaReadNotConfigured
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	payload, err := json.Marshal(req)
	if err != nil {
		return 0, err
	}
	if len(payload) > volumeRdmaEngineMaxFrameSize {
		return 0, fmt.Errorf("native RDMA engine request too large: %d bytes", len(payload))
	}

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", c.SocketPath)
	if err != nil {
		return 0, fmt.Errorf("%w: dial %s: %v", ErrVolumeRdmaEngineUnavailable, c.SocketPath, err)
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if err := writeVolumeRdmaEngineFrame(conn, payload); err != nil {
		return 0, fmt.Errorf("%w: write request: %v", ErrVolumeRdmaEngineUnavailable, err)
	}
	responsePayload, err := readVolumeRdmaEngineFrame(conn)
	if err != nil {
		return 0, fmt.Errorf("%w: read response: %v", ErrVolumeRdmaEngineUnavailable, err)
	}
	var resp volumeRdmaEngineResponse
	if err := json.Unmarshal(responsePayload, &resp); err != nil {
		return 0, fmt.Errorf("%w: decode response: %v", ErrVolumeRdmaEngineUnavailable, err)
	}
	if !resp.OK {
		if resp.Error == "" {
			resp.Error = "native RDMA engine returned failure"
		}
		return 0, fmt.Errorf("%w: %s", ErrVolumeRdmaEngineUnavailable, resp.Error)
	}
	if !resp.DataSideband {
		return 0, fmt.Errorf("%w: response did not include a data sideband frame", ErrVolumeRdmaEngineUnavailable)
	}
	if len(resp.Data) != 0 {
		return 0, fmt.Errorf("%w: response must not include JSON data with data_sideband", ErrVolumeRdmaEngineUnavailable)
	}
	written, err := readVolumeRdmaEngineFrameTo(conn, dst)
	if err != nil {
		return written, fmt.Errorf("%w: read response sideband: %v", ErrVolumeRdmaEngineUnavailable, err)
	}
	return written, nil
}

type volumeRdmaEngineExactFrameWriter struct {
	w         io.Writer
	remaining uint64
	written   uint64
}

func (w *volumeRdmaEngineExactFrameWriter) Write(payload []byte) (int, error) {
	if uint64(len(payload)) > w.remaining {
		return 0, fmt.Errorf("frame payload exceeds declared size: write=%d remaining=%d", len(payload), w.remaining)
	}
	if err := writeVolumeRdmaEngineFull(w.w, payload); err != nil {
		return 0, err
	}
	w.remaining -= uint64(len(payload))
	w.written += uint64(len(payload))
	return len(payload), nil
}

func writeVolumeRdmaEngineFrame(w io.Writer, payload []byte) error {
	if err := writeVolumeRdmaEngineFrameHeader(w, uint64(len(payload))); err != nil {
		return err
	}
	return writeVolumeRdmaEngineFull(w, payload)
}

func writeVolumeRdmaEngineFrameHeader(w io.Writer, size uint64) error {
	if size > volumeRdmaEngineMaxFrameSize {
		return fmt.Errorf("frame too large: %d", size)
	}
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], uint32(size))
	return writeVolumeRdmaEngineFull(w, header[:])
}

func writeVolumeRdmaEngineFull(w io.Writer, payload []byte) error {
	for len(payload) > 0 {
		n, err := w.Write(payload)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		payload = payload[n:]
	}
	return nil
}

func readVolumeRdmaEngineFrame(r io.Reader) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	size := binary.LittleEndian.Uint32(header[:])
	if size > volumeRdmaEngineMaxFrameSize {
		return nil, fmt.Errorf("frame too large: %d", size)
	}
	buf := bytes.NewBuffer(make([]byte, 0, int(size)))
	if _, err := io.CopyN(buf, r, int64(size)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func readVolumeRdmaEngineFrameTo(r io.Reader, w io.Writer) (uint64, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, err
	}
	size := binary.LittleEndian.Uint32(header[:])
	if size > volumeRdmaEngineMaxFrameSize {
		return 0, fmt.Errorf("frame too large: %d", size)
	}
	n, err := io.CopyN(w, r, int64(size))
	return uint64(n), err
}
