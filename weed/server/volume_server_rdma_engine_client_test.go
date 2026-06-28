package weed_server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func TestVolumeRdmaEngineClientEndpointAndRegistrar(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "volume-rdma-engine.sock")
	requests := make(chan volumeRdmaEngineRequest, 16)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	defer listener.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				payload, err := readVolumeRdmaEngineFrame(conn)
				if err != nil {
					t.Errorf("read frame: %v", err)
					return
				}
				var req volumeRdmaEngineRequest
				if err := json.Unmarshal(payload, &req); err != nil {
					t.Errorf("decode request: %v", err)
					return
				}
				resp := volumeRdmaEngineResponse{OK: true}
				var responseSideband []byte
				switch req.Op {
				case volumeRdmaEngineOpLocal:
					resp.Endpoint = &VolumeRdmaEndpointInfo{
						ABIVersion:    VolumeRdmaABIVersion,
						KernelEnabled: true,
						EndpointReady: true,
						Device:        "mlx5_0",
						Port:          1,
						QPNum:         123,
						PSN:           0x123456,
						LID:           0x42,
						LinkLayer:     VolumeRdmaLinkInfiniBand,
					}
				case volumeRdmaEngineOpRequesterLocal:
					resp.Endpoint = &VolumeRdmaEndpointInfo{
						ABIVersion:    VolumeRdmaABIVersion,
						KernelEnabled: true,
						EndpointReady: true,
						Device:        "mlx5_0",
						Port:          1,
						QPNum:         456,
						PSN:           0x654321,
						LID:           0x45,
						LinkLayer:     VolumeRdmaLinkInfiniBand,
					}
					resp.ConnectionID = 55
				case volumeRdmaEngineOpConnect:
				case volumeRdmaEngineOpRequesterConnect:
					if req.ConnectionID != 55 {
						t.Errorf("requester_connect connection_id = %d, want 55", req.ConnectionID)
					}
				case volumeRdmaEngineOpReadRemote:
					if req.ConnectionID != 55 || req.Desc == nil || req.Desc.RemoteAddr != 0xfeed || req.TimeoutMs != 12 {
						t.Errorf("unexpected read_remote request: %+v", req)
					}
					resp.DataSideband = true
					responseSideband = []byte("remote-data")
				case volumeRdmaEngineOpRegisterRead:
					if !req.DataSideband {
						t.Errorf("register_read did not request sideband data")
					}
					if len(req.Data) != 0 {
						t.Errorf("register_read JSON data = %q, want empty", req.Data)
					}
					sideband, err := readVolumeRdmaEngineFrame(conn)
					if err != nil {
						t.Errorf("read sideband: %v", err)
						return
					}
					req.Data = sideband
					if string(req.Data) != "needle-data" {
						t.Errorf("register data = %q", req.Data)
					}
					resp.Desc = &VolumeRdmaDataDesc{
						RemoteAddr: 0xbeef,
						RKey:       77,
						Length:     uint32(len(req.Data)),
					}
					resp.SessionID = 99
				case volumeRdmaEngineOpRegisterReadStream:
					if req.DataSideband {
						t.Errorf("register_read_stream unexpectedly requested sideband data")
					}
					if len(req.Data) != 0 {
						t.Errorf("register_read_stream JSON data = %q, want empty", req.Data)
					}
					streamed, err := readVolumeRdmaEngineFrame(conn)
					if err != nil {
						t.Errorf("read stream frame: %v", err)
						return
					}
					req.Data = streamed
					if string(req.Data) != "stream-data" {
						t.Errorf("stream data = %q", req.Data)
					}
					resp.Desc = &VolumeRdmaDataDesc{
						RemoteAddr: 0xcafe,
						RKey:       88,
						Length:     uint32(len(req.Data)),
					}
					resp.SessionID = 100
				case volumeRdmaEngineOpReadRegistered:
					if req.SessionID != 101 || req.Size != uint64(len("registered-data")) {
						t.Errorf("unexpected read_registered request: %+v", req)
					}
					resp.DataSideband = true
					responseSideband = []byte("registered-data")
				case volumeRdmaEngineOpRelease:
				default:
					resp.OK = false
					resp.Error = "unknown op"
				}
				requests <- req
				encoded, err := json.Marshal(resp)
				if err != nil {
					t.Errorf("encode response: %v", err)
					return
				}
				if err := writeVolumeRdmaEngineFrame(conn, encoded); err != nil {
					t.Errorf("write response: %v", err)
				}
				if responseSideband != nil {
					if err := writeVolumeRdmaEngineFrame(conn, responseSideband); err != nil {
						t.Errorf("write response sideband: %v", err)
					}
				}
			}(conn)
		}
	}()

	client := NewVolumeRdmaEngineClient(socketPath, time.Second)
	local, err := client.LocalEndpoint(context.Background())
	if err != nil {
		t.Fatalf("LocalEndpoint: %v", err)
	}
	if local.QPNum != 123 || !local.ReadyForConnect() {
		t.Fatalf("unexpected local endpoint: %+v", local)
	}
	if err := client.ConnectEndpoint(context.Background(), VolumeRdmaRemoteInfo{
		ABIVersion: VolumeRdmaABIVersion,
		QPN:        321,
		LID:        0x24,
		PSN:        0x654321,
		Port:       1,
	}); err != nil {
		t.Fatalf("ConnectEndpoint: %v", err)
	}
	requesterLocal, requesterConnectionID, err := client.RequesterLocalEndpointFor(context.Background(), 0)
	if err != nil {
		t.Fatalf("RequesterLocalEndpointFor: %v", err)
	}
	if requesterConnectionID != 55 || requesterLocal.ConnectionID != 55 || requesterLocal.QPNum != 456 {
		t.Fatalf("unexpected requester endpoint: id=%d endpoint=%+v", requesterConnectionID, requesterLocal)
	}
	if err := client.RequesterConnectEndpointFor(context.Background(), requesterConnectionID, VolumeRdmaRemoteInfo{
		ABIVersion: VolumeRdmaABIVersion,
		QPN:        654,
		LID:        0x54,
		PSN:        0x456789,
		Port:       1,
	}); err != nil {
		t.Fatalf("RequesterConnectEndpointFor: %v", err)
	}
	remoteData, err := client.ReadRemoteFor(context.Background(), requesterConnectionID, VolumeRdmaDataDesc{
		RemoteAddr: 0xfeed,
		RKey:       66,
		Length:     uint32(len("remote-data")),
	}, 12*time.Millisecond)
	if err != nil {
		t.Fatalf("ReadRemoteFor: %v", err)
	}
	if string(remoteData) != "remote-data" {
		t.Fatalf("remote data = %q", remoteData)
	}
	buffer, err := client.RegisterReadBuffer(context.Background(), []byte("needle-data"))
	if err != nil {
		t.Fatalf("RegisterReadBuffer: %v", err)
	}
	desc := buffer.Descriptor()
	if desc.RemoteAddr != 0xbeef || desc.RKey != 77 || desc.Length != uint32(len("needle-data")) {
		t.Fatalf("unexpected descriptor: %+v", desc)
	}
	streamBuffer, err := client.RegisterReadStreamFor(context.Background(), 7, uint64(len("stream-data")), func(w io.Writer) error {
		_, err := w.Write([]byte("stream-data"))
		return err
	})
	if err != nil {
		t.Fatalf("RegisterReadStreamFor: %v", err)
	}
	streamDesc := streamBuffer.Descriptor()
	if streamDesc.RemoteAddr != 0xcafe || streamDesc.RKey != 88 || streamDesc.Length != uint32(len("stream-data")) {
		t.Fatalf("unexpected stream descriptor: %+v", streamDesc)
	}
	var registered bytes.Buffer
	if err := client.ReadRegisteredBufferTo(context.Background(), 101, uint64(len("registered-data")), &registered); err != nil {
		t.Fatalf("ReadRegisteredBufferTo: %v", err)
	}
	if registered.String() != "registered-data" {
		t.Fatalf("registered data = %q", registered.String())
	}
	if err := buffer.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}

	ops := []string{
		volumeRdmaEngineOpLocal,
		volumeRdmaEngineOpConnect,
		volumeRdmaEngineOpRequesterLocal,
		volumeRdmaEngineOpRequesterConnect,
		volumeRdmaEngineOpReadRemote,
		volumeRdmaEngineOpRegisterRead,
		volumeRdmaEngineOpRegisterReadStream,
		volumeRdmaEngineOpReadRegistered,
		volumeRdmaEngineOpRelease,
	}
	for _, want := range ops {
		select {
		case got := <-requests:
			if got.Op != want {
				t.Fatalf("op = %q, want %q", got.Op, want)
			}
			if got.Op == volumeRdmaEngineOpRelease && got.SessionID != 99 {
				t.Fatalf("release session = %d, want 99", got.SessionID)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for op %q", want)
		}
	}
}

func TestVolumeRdmaEngineClientUnavailable(t *testing.T) {
	client := NewVolumeRdmaEngineClient(filepath.Join(t.TempDir(), "missing.sock"), 10*time.Millisecond)
	_, err := client.LocalEndpoint(context.Background())
	if err == nil {
		t.Fatalf("expected error")
	}
}
