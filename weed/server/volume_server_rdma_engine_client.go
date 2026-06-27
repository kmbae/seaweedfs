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
	volumeRdmaEngineOpLocal        = "local"
	volumeRdmaEngineOpConnect      = "connect"
	volumeRdmaEngineOpRegisterRead = "register_read"
	volumeRdmaEngineOpRelease      = "release"

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
	SessionID    uint64                `json:"session_id,omitempty"`
	Data         []byte                `json:"data,omitempty"`
}

type volumeRdmaEngineResponse struct {
	OK           bool                    `json:"ok"`
	Error        string                  `json:"error,omitempty"`
	Endpoint     *VolumeRdmaEndpointInfo `json:"endpoint,omitempty"`
	Desc         *VolumeRdmaDataDesc     `json:"desc,omitempty"`
	ConnectionID uint64                  `json:"connection_id,omitempty"`
	SessionID    uint64                  `json:"session_id,omitempty"`
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
	if vs == nil {
		return fmt.Errorf("volume server is nil")
	}
	socketPath = strings.TrimSpace(socketPath)
	if socketPath == "" {
		return fmt.Errorf("native RDMA engine socket path is required")
	}
	client := NewVolumeRdmaEngineClient(socketPath, timeout)
	vs.SetRdmaEndpoint(client)
	vs.SetRdmaReadExporter(NewVolumeStoreRdmaReadExporter(vs.store, client, cfg))
	return nil
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

func (c *VolumeRdmaEngineClient) RegisterReadBuffer(ctx context.Context, data []byte) (VolumeRdmaRegisteredBuffer, error) {
	return c.RegisterReadBufferFor(ctx, 0, data)
}

func (c *VolumeRdmaEngineClient) RegisterReadBufferFor(ctx context.Context, connectionID uint64, data []byte) (VolumeRdmaRegisteredBuffer, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("native RDMA register_read requires data")
	}
	resp, err := c.roundTrip(ctx, volumeRdmaEngineRequest{
		Op:           volumeRdmaEngineOpRegisterRead,
		ConnectionID: connectionID,
		Data:         data,
	})
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

func (b *volumeRdmaEngineRegisteredBuffer) Descriptor() VolumeRdmaDataDesc {
	if b == nil {
		return VolumeRdmaDataDesc{}
	}
	return b.desc
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
	return &resp, nil
}

func writeVolumeRdmaEngineFrame(w io.Writer, payload []byte) error {
	if len(payload) > volumeRdmaEngineMaxFrameSize {
		return fmt.Errorf("frame too large: %d", len(payload))
	}
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], uint32(len(payload)))
	if err := writeVolumeRdmaEngineFull(w, header[:]); err != nil {
		return err
	}
	return writeVolumeRdmaEngineFull(w, payload)
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
