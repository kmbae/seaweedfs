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
	VolumeID uint32 `msgpack:"volume_id"`
	NeedleID uint64 `msgpack:"needle_id"`
	Cookie   uint32 `msgpack:"cookie"`
	Offset   uint64 `msgpack:"offset"`
	Size     uint64 `msgpack:"size"`
}

// NeedleReadResponse matches the Rust RemoteNeedleReadResponse.
type NeedleReadResponse struct {
	Success bool   `msgpack:"success"`
	Data    []byte `msgpack:"data"`
	Message string `msgpack:"message,omitempty"`
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

// ReadNeedle performs a remote needle read over TCP (binds to IB/RoCE CNI IP on volume pods).
func ReadNeedle(ctx context.Context, volumeServer string, port uint16, req *NeedleReadRequest) ([]byte, error) {
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
	return resp.Data, nil
}

// NeedleWriteRequest matches the Rust RemoteNeedleWriteRequest.
type NeedleWriteRequest struct {
	VolumeID uint32 `msgpack:"volume_id"`
	NeedleID uint64 `msgpack:"needle_id"`
	Cookie   uint32 `msgpack:"cookie"`
	Data     []byte `msgpack:"data"`
}

// NeedleWriteResponse matches the Rust RemoteNeedleWriteResponse.
type NeedleWriteResponse struct {
	Success bool   `msgpack:"success"`
	FileID  string `msgpack:"file_id"`
	Message string `msgpack:"message,omitempty"`
}

// WriteNeedle performs a remote needle write over TCP.
func WriteNeedle(ctx context.Context, volumeServer string, port uint16, req *NeedleWriteRequest) (string, error) {
	host, err := ParseVolumeHost(volumeServer)
	if err != nil {
		return "", err
	}
	if port == 0 {
		port = DefaultRemotePort
	}

	dialer := &net.Dialer{Timeout: 30 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(int(port))))
	if err != nil {
		return "", fmt.Errorf("remote rdma write connect %s:%d: %w", host, port, err)
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}

	if err := writeMsgpack(conn, req); err != nil {
		return "", fmt.Errorf("remote rdma write request: %w", err)
	}

	var resp NeedleWriteResponse
	if err := readMsgpack(conn, &resp); err != nil {
		return "", fmt.Errorf("remote rdma write response: %w", err)
	}
	if !resp.Success {
		if resp.Message != "" {
			return "", fmt.Errorf("remote rdma write failed: %s", resp.Message)
		}
		return "", fmt.Errorf("remote rdma write failed")
	}
	return resp.FileID, nil
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
