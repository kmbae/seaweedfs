// Package remote provides client-side remote needle reads over the RDMA network port.
package remote

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

const DefaultRemotePort = 18515

// NeedleReadRequest matches the Rust RemoteNeedleReadRequest.
type NeedleReadRequest struct {
	VolumeID         uint32 `msgpack:"volume_id"`
	NeedleID         uint64 `msgpack:"needle_id"`
	Cookie           uint32 `msgpack:"cookie"`
	Offset           uint64 `msgpack:"offset"`
	Size             uint64 `msgpack:"size"`
	WorkerAddressB64 string `msgpack:"worker_address_b64,omitempty"`
	RemoteAddr       uint64 `msgpack:"remote_addr,omitempty"`
	RemoteKeyB64     string `msgpack:"remote_key_b64,omitempty"`
}

// NeedleReadResponse matches the Rust RemoteNeedleReadResponse.
type NeedleReadResponse struct {
	Success   bool   `msgpack:"success"`
	Data      []byte `msgpack:"data"`
	Size      uint64 `msgpack:"size,omitempty"`
	Transport string `msgpack:"transport,omitempty"`
	RealRDMA  bool   `msgpack:"real_rdma,omitempty"`
	Source    string `msgpack:"source,omitempty"`
	Message   string `msgpack:"message,omitempty"`
}

// ReadResult includes the bytes and the transport used by the remote engine.
type ReadResult struct {
	Data      []byte
	Size      uint64
	Transport string
	RealRDMA  bool
	Source    string
}

// WorkerAddressInfo is returned by the sidecar /rdma/worker-address endpoint.
type WorkerAddressInfo struct {
	WorkerAddressB64 string `json:"worker_address_b64"`
	ListenPort       uint16 `json:"listen_port"`
	RealRdma         bool   `json:"real_rdma"`
}

// ParseVolumeHost extracts the hostname from a volume server URL.
func ParseVolumeHost(volumeServer string) (string, error) {
	if volumeServer == "" {
		return "", fmt.Errorf("empty volume server URL")
	}
	if !strings.Contains(volumeServer, "://") {
		volumeServer = "http://" + volumeServer
	}
	u, err := url.Parse(volumeServer)
	if err != nil {
		return "", err
	}
	host := u.Hostname()
	if host == "" {
		return "", fmt.Errorf("no host in volume server URL")
	}
	return host, nil
}

// IsLocalHost returns true when the volume server is on the same pod/node.
func IsLocalHost(volumeServer string) bool {
	host, err := ParseVolumeHost(volumeServer)
	if err != nil {
		return false
	}
	switch strings.ToLower(host) {
	case "127.0.0.1", "localhost", "::1", "0.0.0.0":
		return true
	default:
		return false
	}
}

// ReadNeedleResult performs a remote needle read and returns transport metadata.
func ReadNeedleResult(ctx context.Context, volumeServer string, port uint16, req *NeedleReadRequest) (*ReadResult, error) {
	host, err := ParseVolumeHost(volumeServer)
	if err != nil {
		return nil, err
	}
	if port == 0 {
		port = DefaultRemotePort
	}

	dialer := &net.Dialer{Timeout: 30 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(int(port))))
	if err != nil {
		return nil, fmt.Errorf("remote rdma connect %s:%d: %w", host, port, err)
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}

	if err := writeMsgpack(conn, req); err != nil {
		return nil, fmt.Errorf("remote rdma write request: %w", err)
	}

	var resp NeedleReadResponse
	if err := readMsgpack(conn, &resp); err != nil {
		return nil, fmt.Errorf("remote rdma read response: %w", err)
	}
	if !resp.Success {
		if resp.Message != "" {
			return nil, fmt.Errorf("remote rdma read failed: %s", resp.Message)
		}
		return nil, fmt.Errorf("remote rdma read failed")
	}
	return &ReadResult{
		Data:      resp.Data,
		Size:      resp.Size,
		Transport: normalizeTransport(resp.Transport),
		RealRDMA:  resp.RealRDMA,
		Source:    strings.TrimSpace(resp.Source),
	}, nil
}

// ReadNeedle performs a remote needle read over TCP (binds to IB/RoCE CNI IP on volume pods).
func ReadNeedle(ctx context.Context, volumeServer string, port uint16, req *NeedleReadRequest) ([]byte, error) {
	result, err := ReadNeedleResult(ctx, volumeServer, port, req)
	if err != nil {
		return nil, err
	}
	return result.Data, nil
}

// NeedleWriteRequest matches the Rust RemoteNeedleWriteRequest.
type NeedleWriteRequest struct {
	VolumeID         uint32 `msgpack:"volume_id"`
	NeedleID         uint64 `msgpack:"needle_id"`
	Cookie           uint32 `msgpack:"cookie"`
	Data             []byte `msgpack:"data"`
	Size             uint64 `msgpack:"size,omitempty"`
	WorkerAddressB64 string `msgpack:"worker_address_b64,omitempty"`
	RemoteAddr       uint64 `msgpack:"remote_addr,omitempty"`
	RemoteKeyB64     string `msgpack:"remote_key_b64,omitempty"`
}

// NeedleWriteResponse matches the Rust RemoteNeedleWriteResponse.
type NeedleWriteResponse struct {
	Success   bool   `msgpack:"success"`
	FileID    string `msgpack:"file_id"`
	Transport string `msgpack:"transport,omitempty"`
	RealRDMA  bool   `msgpack:"real_rdma,omitempty"`
	Source    string `msgpack:"source,omitempty"`
	Message   string `msgpack:"message,omitempty"`
}

// WriteResult includes the committed file id and the transport used by the remote engine.
type WriteResult struct {
	FileID    string
	Transport string
	RealRDMA  bool
	Source    string
}

// WriteNeedleResult performs a remote needle write and returns transport metadata.
func WriteNeedleResult(ctx context.Context, volumeServer string, port uint16, req *NeedleWriteRequest) (*WriteResult, error) {
	host, err := ParseVolumeHost(volumeServer)
	if err != nil {
		return nil, err
	}
	if port == 0 {
		port = DefaultRemotePort
	}

	dialer := &net.Dialer{Timeout: 30 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(int(port))))
	if err != nil {
		return nil, fmt.Errorf("remote rdma write connect %s:%d: %w", host, port, err)
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}

	if err := writeMsgpack(conn, req); err != nil {
		return nil, fmt.Errorf("remote rdma write request: %w", err)
	}

	var resp NeedleWriteResponse
	if err := readMsgpack(conn, &resp); err != nil {
		return nil, fmt.Errorf("remote rdma write response: %w", err)
	}
	if !resp.Success {
		if resp.Message != "" {
			return nil, fmt.Errorf("remote rdma write failed: %s", resp.Message)
		}
		return nil, fmt.Errorf("remote rdma write failed")
	}
	return &WriteResult{
		FileID:    resp.FileID,
		Transport: normalizeTransport(resp.Transport),
		RealRDMA:  resp.RealRDMA,
		Source:    strings.TrimSpace(resp.Source),
	}, nil
}

// WriteNeedle performs a remote needle write over TCP.
func WriteNeedle(ctx context.Context, volumeServer string, port uint16, req *NeedleWriteRequest) (string, error) {
	result, err := WriteNeedleResult(ctx, volumeServer, port, req)
	if err != nil {
		return "", err
	}
	return result.FileID, nil
}

// FetchWorkerAddress queries a remote sidecar for its UCX worker address.
func FetchWorkerAddress(ctx context.Context, sidecarBase string) (*WorkerAddressInfo, error) {
	base := strings.TrimRight(sidecarBase, "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/rdma/worker-address", nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("worker address HTTP %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	var info WorkerAddressInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, err
	}
	return &info, nil
}

func writeMsgpack(w net.Conn, v interface{}) error {
	data, err := msgpack.Marshal(v)
	if err != nil {
		return err
	}
	lenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBytes, uint32(len(data)))
	if _, err := w.Write(lenBytes); err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func readMsgpack(r net.Conn, v interface{}) error {
	lenBytes := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBytes); err != nil {
		return err
	}
	length := binary.LittleEndian.Uint32(lenBytes)
	if length > 64*1024*1024 {
		return fmt.Errorf("response too large: %d", length)
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}
	return msgpack.Unmarshal(data, v)
}

func normalizeTransport(transport string) string {
	transport = strings.ToLower(strings.TrimSpace(transport))
	if transport == "" {
		return "tcp"
	}
	return transport
}
