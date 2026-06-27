package nativeengine

import (
	"encoding/base64"
	"encoding/json"
	"net"
	"path/filepath"
	"testing"
	"time"

	"seaweedfs-rdma-sidecar/pkg/swvfsdaemon"
	"seaweedfs-rdma-sidecar/pkg/swvfsproto"
)

func TestClientRequesterLocalConnectAndReadRemote(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "native.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	requests := make(chan request, 4)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				frame, err := readFrame(conn)
				if err != nil {
					t.Errorf("read request: %v", err)
					return
				}
				var req request
				if err := json.Unmarshal(frame, &req); err != nil {
					t.Errorf("decode request: %v", err)
					return
				}
				requests <- req

				resp := response{OK: true}
				var sideband []byte
				switch req.Op {
				case "requester_local":
					if req.ConnectionID != 0 {
						t.Errorf("unexpected requester_local connection_id: %d", req.ConnectionID)
					}
					resp.Endpoint = &testEndpoint
					resp.ConnectionID = 42
				case "requester_connect":
					if req.ConnectionID != 42 || req.Remote == nil || req.Remote.QPN != 7 {
						t.Errorf("unexpected remote: %+v", req.Remote)
					}
				case "read_remote":
					if req.ConnectionID != 42 || req.Desc == nil || req.Desc.RemoteAddr != 0xbeef || req.TimeoutMs != 25 {
						t.Errorf("unexpected read request: %+v", req)
					}
					resp.DataSideband = true
					sideband = []byte("needle")
				default:
					resp.OK = false
					resp.Error = "unknown op"
				}
				payload, err := json.Marshal(resp)
				if err != nil {
					t.Errorf("encode response: %v", err)
					return
				}
				if err := writeFrame(conn, payload); err != nil {
					t.Errorf("write response: %v", err)
					return
				}
				if sideband != nil {
					if err := writeFrame(conn, sideband); err != nil {
						t.Errorf("write sideband: %v", err)
					}
				}
			}(conn)
		}
	}()

	client := New(socketPath, time.Second)
	local, connectionID, err := client.RequesterLocalFor(t.Context(), 0)
	if err != nil {
		t.Fatalf("RequesterLocal: %v", err)
	}
	if local.QPNum != testEndpoint.QPNum {
		t.Fatalf("local = %+v", local)
	}
	if connectionID != 42 || local.ConnectionID != 42 {
		t.Fatalf("connectionID = %d local=%+v", connectionID, local)
	}
	if err := client.RequesterConnectFor(t.Context(), connectionID, swvfsproto.RDMARemoteInfo{
		ABIVersion: swvfsproto.RDMAABIVersion,
		QPN:        7,
		LID:        8,
		PSN:        9,
		Port:       1,
	}); err != nil {
		t.Fatalf("RequesterConnect: %v", err)
	}
	data, err := client.ReadRemoteFor(t.Context(), connectionID, swvfsproto.RDMADataDesc{
		RemoteAddr: 0xbeef,
		RKey:       0,
		Length:     6,
	}, 25*time.Millisecond)
	if err != nil {
		t.Fatalf("ReadRemote: %v", err)
	}
	if string(data) != "needle" {
		t.Fatalf("data = %q", data)
	}

	for _, want := range []string{"requester_local", "requester_connect", "read_remote"} {
		select {
		case got := <-requests:
			if got.Op != want {
				t.Fatalf("op = %q, want %q", got.Op, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for %s", want)
		}
	}
}

func TestResponseDataAcceptsBase64(t *testing.T) {
	payload := []byte(`{"ok":true,"data":"` + base64.StdEncoding.EncodeToString([]byte("abc")) + `"}`)
	var resp response
	if err := json.Unmarshal(payload, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if string(resp.Data) != "abc" {
		t.Fatalf("data = %q", resp.Data)
	}
}

var testEndpoint = swvfsdaemon.RDMALocalEndpoint{
	ABIVersion:    swvfsproto.RDMAABIVersion,
	Device:        "mlx5_0",
	Port:          1,
	QPNum:         7,
	PSN:           9,
	LID:           8,
	LinkLayer:     swvfsproto.RDMALinkInfiniBand,
	KernelEnabled: true,
	EndpointReady: true,
}
