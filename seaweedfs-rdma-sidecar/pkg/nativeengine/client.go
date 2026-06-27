package nativeengine

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"seaweedfs-rdma-sidecar/pkg/swvfsdaemon"
	"seaweedfs-rdma-sidecar/pkg/swvfsproto"
)

const maxFrameSize = 64 * 1024 * 1024

type Client struct {
	SocketPath string
	Timeout    time.Duration
}

type request struct {
	Op           string          `json:"op"`
	ConnectionID uint64          `json:"connection_id,omitempty"`
	Remote       *remoteInfo     `json:"remote,omitempty"`
	Desc         *rdmaDataDesc   `json:"desc,omitempty"`
	SessionID    uint64          `json:"session_id,omitempty"`
	TimeoutMs    uint64          `json:"timeout_ms,omitempty"`
	Data         json.RawMessage `json:"data,omitempty"`
}

type response struct {
	OK           bool                           `json:"ok"`
	Error        string                         `json:"error,omitempty"`
	Endpoint     *swvfsdaemon.RDMALocalEndpoint `json:"endpoint,omitempty"`
	Desc         *rdmaDataDesc                  `json:"desc,omitempty"`
	ConnectionID uint64                         `json:"connection_id,omitempty"`
	SessionID    uint64                         `json:"session_id,omitempty"`
	Data         []byte                         `json:"data,omitempty"`
}

type remoteInfo struct {
	ABIVersion uint32    `json:"abi_version"`
	Flags      uint32    `json:"flags"`
	QPN        uint32    `json:"qpn"`
	LID        uint32    `json:"lid"`
	PSN        uint32    `json:"psn"`
	Port       uint32    `json:"port"`
	GIDIndex   uint32    `json:"gid_index"`
	SL         uint32    `json:"sl"`
	GID        [16]byte  `json:"gid"`
	Reserved   [8]uint64 `json:"reserved"`
}

type rdmaDataDesc struct {
	RemoteAddr uint64    `json:"RemoteAddr"`
	RKey       uint32    `json:"RKey"`
	Length     uint32    `json:"Length"`
	Reserved   [4]uint64 `json:"Reserved"`
}

func New(socketPath string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &Client{SocketPath: socketPath, Timeout: timeout}
}

func (c *Client) RequesterLocal(ctx context.Context) (swvfsdaemon.RDMALocalEndpoint, error) {
	endpoint, _, err := c.RequesterLocalFor(ctx, 0)
	return endpoint, err
}

func (c *Client) RequesterLocalFor(ctx context.Context, connectionID uint64) (swvfsdaemon.RDMALocalEndpoint, uint64, error) {
	var endpoint swvfsdaemon.RDMALocalEndpoint
	resp, err := c.roundTrip(ctx, request{Op: "requester_local", ConnectionID: connectionID})
	if err != nil {
		return endpoint, 0, err
	}
	if resp.Endpoint == nil {
		return endpoint, 0, fmt.Errorf("native engine requester_local response missing endpoint")
	}
	endpoint = *resp.Endpoint
	if resp.ConnectionID != 0 {
		endpoint.ConnectionID = resp.ConnectionID
	}
	return endpoint, endpoint.ConnectionID, nil
}

func (c *Client) RequesterConnect(ctx context.Context, remote swvfsproto.RDMARemoteInfo) error {
	return c.RequesterConnectFor(ctx, 0, remote)
}

func (c *Client) RequesterConnectFor(ctx context.Context, connectionID uint64, remote swvfsproto.RDMARemoteInfo) error {
	_, err := c.roundTrip(ctx, request{
		Op:           "requester_connect",
		ConnectionID: connectionID,
		Remote:       toRemoteInfo(remote),
	})
	return err
}

func (c *Client) ReadRemote(ctx context.Context, desc swvfsproto.RDMADataDesc, timeout time.Duration) ([]byte, error) {
	return c.ReadRemoteFor(ctx, 0, desc, timeout)
}

func (c *Client) ReadRemoteFor(ctx context.Context, connectionID uint64, desc swvfsproto.RDMADataDesc, timeout time.Duration) ([]byte, error) {
	timeoutMs := uint64(timeout.Milliseconds())
	if timeoutMs == 0 && timeout > 0 {
		timeoutMs = 1
	}
	resp, err := c.roundTrip(ctx, request{
		Op:           "read_remote",
		ConnectionID: connectionID,
		Desc:         toDataDesc(desc),
		TimeoutMs:    timeoutMs,
	})
	if err != nil {
		return nil, err
	}
	if len(resp.Data) == 0 && desc.Length > 0 {
		return nil, fmt.Errorf("native engine read_remote returned empty data for %d byte descriptor", desc.Length)
	}
	return resp.Data, nil
}

func (c *Client) roundTrip(ctx context.Context, req request) (*response, error) {
	if c == nil || strings.TrimSpace(c.SocketPath) == "" {
		return nil, fmt.Errorf("native engine socket is not configured")
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
	if len(payload) > maxFrameSize {
		return nil, fmt.Errorf("native engine request too large: %d bytes", len(payload))
	}

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", c.SocketPath)
	if err != nil {
		return nil, fmt.Errorf("dial native engine %s: %w", c.SocketPath, err)
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if err := writeFrame(conn, payload); err != nil {
		return nil, err
	}
	frame, err := readFrame(conn)
	if err != nil {
		return nil, err
	}
	var resp response
	if err := json.Unmarshal(frame, &resp); err != nil {
		return nil, fmt.Errorf("decode native engine response: %w", err)
	}
	if !resp.OK {
		if resp.Error == "" {
			resp.Error = "native engine request failed"
		}
		return nil, fmt.Errorf("%s", resp.Error)
	}
	return &resp, nil
}

func writeFrame(w io.Writer, payload []byte) error {
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], uint32(len(payload)))
	if _, err := w.Write(header[:]); err != nil {
		return fmt.Errorf("write native engine frame header: %w", err)
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("write native engine frame payload: %w", err)
	}
	return nil
}

func readFrame(r io.Reader) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, fmt.Errorf("read native engine frame header: %w", err)
	}
	size := binary.LittleEndian.Uint32(header[:])
	if size > maxFrameSize {
		return nil, fmt.Errorf("native engine response too large: %d bytes", size)
	}
	payload := make([]byte, int(size))
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("read native engine frame payload: %w", err)
	}
	return payload, nil
}

func toRemoteInfo(in swvfsproto.RDMARemoteInfo) *remoteInfo {
	return &remoteInfo{
		ABIVersion: in.ABIVersion,
		Flags:      in.Flags,
		QPN:        in.QPN,
		LID:        in.LID,
		PSN:        in.PSN,
		Port:       in.Port,
		GIDIndex:   in.GIDIndex,
		SL:         in.SL,
		GID:        in.GID,
		Reserved:   in.Reserved,
	}
}

func toDataDesc(in swvfsproto.RDMADataDesc) *rdmaDataDesc {
	return &rdmaDataDesc{
		RemoteAddr: in.RemoteAddr,
		RKey:       in.RKey,
		Length:     in.Length,
		Reserved:   in.Reserved,
	}
}
