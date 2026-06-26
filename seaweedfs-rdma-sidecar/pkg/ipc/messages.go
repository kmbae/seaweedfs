// Package ipc provides communication between Go sidecar and Rust RDMA engine
package ipc

import "time"

// IpcMessage represents the tagged union of all IPC messages
// This matches the Rust enum: #[serde(tag = "type", content = "data")]
type IpcMessage struct {
	Type string      `msgpack:"type"`
	Data interface{} `msgpack:"data"`
}

// Request message types
const (
	MsgStartRead        = "StartRead"
	MsgCompleteRead     = "CompleteRead"
	MsgStartWrite       = "StartWrite"
	MsgCompleteWrite    = "CompleteWrite"
	MsgGetCapabilities  = "GetCapabilities"
	MsgGetWorkerAddress = "GetWorkerAddress"
	MsgPing             = "Ping"
)

// Response message types
const (
	MsgStartReadResponse        = "StartReadResponse"
	MsgCompleteReadResponse     = "CompleteReadResponse"
	MsgStartWriteResponse       = "StartWriteResponse"
	MsgCompleteWriteResponse    = "CompleteWriteResponse"
	MsgGetCapabilitiesResponse  = "GetCapabilitiesResponse"
	MsgGetWorkerAddressResponse = "GetWorkerAddressResponse"
	MsgPong                     = "Pong"
	MsgError                    = "Error"
)

// StartReadRequest corresponds to Rust StartReadRequest
type StartReadRequest struct {
	VolumeID    uint32  `msgpack:"volume_id"`
	NeedleID    uint64  `msgpack:"needle_id"`
	Cookie      uint32  `msgpack:"cookie"`
	Offset      uint64  `msgpack:"offset"`
	Size        uint64  `msgpack:"size"`
	RemoteAddr  uint64  `msgpack:"remote_addr"`
	RemoteKey   uint32  `msgpack:"remote_key"`
	TimeoutSecs uint64  `msgpack:"timeout_secs"`
	AuthToken   *string `msgpack:"auth_token,omitempty"`
}

// StartReadResponse corresponds to Rust StartReadResponse
type StartReadResponse struct {
	SessionID    string `msgpack:"session_id"`
	LocalAddr    uint64 `msgpack:"local_addr"`
	LocalKey     uint32 `msgpack:"local_key"`
	RemoteKeyB64 string `msgpack:"remote_key_b64"`
	TransferSize uint64 `msgpack:"transfer_size"`
	ExpectedCrc  uint32 `msgpack:"expected_crc"`
	ExpiresAtNs  uint64 `msgpack:"expires_at_ns"`
}

// CompleteReadRequest corresponds to Rust CompleteReadRequest
type CompleteReadRequest struct {
	SessionID        string  `msgpack:"session_id"`
	Success          bool    `msgpack:"success"`
	BytesTransferred uint64  `msgpack:"bytes_transferred"`
	ClientCrc        *uint32 `msgpack:"client_crc,omitempty"`
	ErrorMessage     *string `msgpack:"error_message,omitempty"`
}

// CompleteReadResponse corresponds to Rust CompleteReadResponse
type CompleteReadResponse struct {
	Success      bool    `msgpack:"success"`
	ServerCrc    *uint32 `msgpack:"server_crc,omitempty"`
	Data         []byte  `msgpack:"data"`
	DataSideband bool    `msgpack:"data_sideband,omitempty"`
	DataSize     uint64  `msgpack:"data_size,omitempty"`
	Message      *string `msgpack:"message,omitempty"`
}

// StartWriteRequest corresponds to Rust StartWriteRequest
type StartWriteRequest struct {
	VolumeID     uint32  `msgpack:"volume_id"`
	NeedleID     uint64  `msgpack:"needle_id"`
	Cookie       uint32  `msgpack:"cookie"`
	Size         uint64  `msgpack:"size"`
	Data         []byte  `msgpack:"data"`
	DataSideband bool    `msgpack:"data_sideband,omitempty"`
	TimeoutSecs  uint64  `msgpack:"timeout_secs"`
	AuthToken    *string `msgpack:"auth_token,omitempty"`
}

// StartWriteResponse corresponds to Rust StartWriteResponse
type StartWriteResponse struct {
	SessionID     string `msgpack:"session_id"`
	BytesBuffered uint64 `msgpack:"bytes_buffered"`
	LocalAddr     uint64 `msgpack:"local_addr"`
	LocalKey      uint32 `msgpack:"local_key"`
	RemoteKeyB64  string `msgpack:"remote_key_b64"`
	Success       bool   `msgpack:"success"`
}

// CompleteWriteRequest corresponds to Rust CompleteWriteRequest
type CompleteWriteRequest struct {
	SessionID    string  `msgpack:"session_id"`
	Success      bool    `msgpack:"success"`
	BytesWritten uint64  `msgpack:"bytes_written"`
	ClientCrc    *uint32 `msgpack:"client_crc,omitempty"`
	ErrorMessage *string `msgpack:"error_message,omitempty"`
}

// CompleteWriteResponse corresponds to Rust CompleteWriteResponse
type CompleteWriteResponse struct {
	Success   bool    `msgpack:"success"`
	ServerCrc *uint32 `msgpack:"server_crc,omitempty"`
	FileID    string  `msgpack:"file_id"`
	Message   *string `msgpack:"message,omitempty"`
}

// GetCapabilitiesRequest corresponds to Rust GetCapabilitiesRequest
type GetCapabilitiesRequest struct {
	ClientID *string `msgpack:"client_id,omitempty"`
}

// GetCapabilitiesResponse corresponds to Rust GetCapabilitiesResponse
type GetCapabilitiesResponse struct {
	DeviceName      string   `msgpack:"device_name"`
	VendorId        uint32   `msgpack:"vendor_id"`
	MaxTransferSize uint64   `msgpack:"max_transfer_size"`
	MaxSessions     usize    `msgpack:"max_sessions"`
	ActiveSessions  usize    `msgpack:"active_sessions"`
	PortGid         string   `msgpack:"port_gid"`
	PortLid         uint16   `msgpack:"port_lid"`
	SupportedAuth   []string `msgpack:"supported_auth"`
	Version         string   `msgpack:"version"`
	RealRdma        bool     `msgpack:"real_rdma"`
}

// usize corresponds to Rust's usize type (platform dependent, but typically uint64 on 64-bit systems)
type usize uint64

// PingRequest corresponds to Rust PingRequest
type PingRequest struct {
	TimestampNs uint64  `msgpack:"timestamp_ns"`
	ClientID    *string `msgpack:"client_id,omitempty"`
}

// PongResponse corresponds to Rust PongResponse
type PongResponse struct {
	ClientTimestampNs uint64 `msgpack:"client_timestamp_ns"`
	ServerTimestampNs uint64 `msgpack:"server_timestamp_ns"`
	ServerRttNs       uint64 `msgpack:"server_rtt_ns"`
}

// GetWorkerAddressRequest corresponds to Rust GetWorkerAddressRequest
type GetWorkerAddressRequest struct{}

// GetWorkerAddressResponse corresponds to Rust GetWorkerAddressResponse
type GetWorkerAddressResponse struct {
	WorkerAddressB64 string `msgpack:"worker_address_b64"`
	ListenPort       uint16 `msgpack:"listen_port"`
	RealRdma         bool   `msgpack:"real_rdma"`
}

// ErrorResponse corresponds to Rust ErrorResponse
type ErrorResponse struct {
	Code    string  `msgpack:"code"`
	Message string  `msgpack:"message"`
	Details *string `msgpack:"details,omitempty"`
}

// Helper functions for creating messages
func NewStartReadMessage(req *StartReadRequest) *IpcMessage {
	return &IpcMessage{
		Type: MsgStartRead,
		Data: req,
	}
}

func NewCompleteReadMessage(sessionID string, success bool, bytesTransferred uint64, clientCrc *uint32, errorMessage *string) *IpcMessage {
	return &IpcMessage{
		Type: MsgCompleteRead,
		Data: &CompleteReadRequest{
			SessionID:        sessionID,
			Success:          success,
			BytesTransferred: bytesTransferred,
			ClientCrc:        clientCrc,
			ErrorMessage:     errorMessage,
		},
	}
}

func NewStartWriteMessage(req *StartWriteRequest) *IpcMessage {
	return &IpcMessage{
		Type: MsgStartWrite,
		Data: req,
	}
}

func NewCompleteWriteMessage(sessionID string, success bool, bytesWritten uint64, clientCrc *uint32, errorMessage *string) *IpcMessage {
	return &IpcMessage{
		Type: MsgCompleteWrite,
		Data: &CompleteWriteRequest{
			SessionID:    sessionID,
			Success:      success,
			BytesWritten: bytesWritten,
			ClientCrc:    clientCrc,
			ErrorMessage: errorMessage,
		},
	}
}

func NewGetCapabilitiesMessage(clientID *string) *IpcMessage {
	return &IpcMessage{
		Type: MsgGetCapabilities,
		Data: &GetCapabilitiesRequest{
			ClientID: clientID,
		},
	}
}

func NewPingMessage(clientID *string) *IpcMessage {
	return &IpcMessage{
		Type: MsgPing,
		Data: &PingRequest{
			TimestampNs: uint64(time.Now().UnixNano()),
			ClientID:    clientID,
		},
	}
}

func NewGetWorkerAddressMessage() *IpcMessage {
	return &IpcMessage{
		Type: MsgGetWorkerAddress,
		Data: &GetWorkerAddressRequest{},
	}
}

func NewErrorMessage(code, message string, details *string) *IpcMessage {
	return &IpcMessage{
		Type: MsgError,
		Data: &ErrorResponse{
			Code:    code,
			Message: message,
			Details: details,
		},
	}
}
