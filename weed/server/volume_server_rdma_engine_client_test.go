package weed_server

import (
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
	requests := make(chan volumeRdmaEngineRequest, 8)
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
				case volumeRdmaEngineOpConnect:
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
	if err := buffer.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}

	ops := []string{
		volumeRdmaEngineOpLocal,
		volumeRdmaEngineOpConnect,
		volumeRdmaEngineOpRegisterRead,
		volumeRdmaEngineOpRegisterReadStream,
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
