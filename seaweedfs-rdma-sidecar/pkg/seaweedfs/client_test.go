package seaweedfs

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"seaweedfs-rdma-sidecar/pkg/rdma"
	"seaweedfs-rdma-sidecar/pkg/swvfsdaemon"
	"seaweedfs-rdma-sidecar/pkg/swvfsproto"
)

func TestFetchNeedleDataRequiresDataSource(t *testing.T) {
	client := &SeaweedFSRDMAClient{
		logger: logrus.New(),
	}

	_, _, _, err := client.fetchNeedleData(t.Context(), &NeedleReadRequest{
		VolumeID:     1,
		NeedleID:     1,
		Cookie:       1,
		Offset:       0,
		Size:         16,
		VolumeServer: "",
	})
	if err == nil {
		t.Fatal("expected error when no data source is configured")
	}
}

func TestRemoteSourceDefaultsTCP(t *testing.T) {
	if got := remoteReadSource(""); got != "remote-tcp" {
		t.Fatalf("unexpected read source: %s", got)
	}
	if got := remoteWriteSource("UCX"); got != "remote-ucx-write" {
		t.Fatalf("unexpected write source: %s", got)
	}
}

func TestIsRealRdmaDefaultsFalse(t *testing.T) {
	c := rdma.NewClient(&rdma.Config{})
	if c.IsRealRdma() {
		t.Fatal("expected mock engine to report real_rdma=false")
	}
}

func TestHTTPFallbackSlicesFullNeedleBody(t *testing.T) {
	body := []byte("0123456789abcdef")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("offset") != "4" || r.URL.Query().Get("size") != "6" {
			t.Fatalf("unexpected query: %s", r.URL.RawQuery)
		}
		_, _ = w.Write(body)
	}))
	defer server.Close()

	client := &SeaweedFSRDMAClient{
		logger:          logrus.New(),
		volumeServerURL: server.URL,
	}
	got, err := client.httpFallback(t.Context(), &NeedleReadRequest{
		VolumeID: 1,
		NeedleID: 2,
		Cookie:   3,
		Offset:   4,
		Size:     6,
	})
	if err != nil {
		t.Fatalf("httpFallback: %v", err)
	}
	if want := []byte("456789"); !bytes.Equal(got, want) {
		t.Fatalf("fallback data = %q, want %q", got, want)
	}
}

func TestHTTPFallbackTruncatesPartialContent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 4-12/*")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("456789abc"))
	}))
	defer server.Close()

	client := &SeaweedFSRDMAClient{
		logger:          logrus.New(),
		volumeServerURL: server.URL,
	}
	got, err := client.httpFallback(t.Context(), &NeedleReadRequest{
		VolumeID: 1,
		NeedleID: 2,
		Cookie:   3,
		Offset:   4,
		Size:     6,
	})
	if err != nil {
		t.Fatalf("httpFallback: %v", err)
	}
	if want := []byte("456789"); !bytes.Equal(got, want) {
		t.Fatalf("fallback data = %q, want %q", got, want)
	}
}

func TestReadNeedleUsesNativeVolumeRDMA(t *testing.T) {
	native := &fakeNativeVolumeEngine{
		local: swvfsdaemon.RDMALocalEndpoint{
			ABIVersion:    swvfsproto.RDMAABIVersion,
			Device:        "mlx5_0",
			Port:          1,
			QPNum:         11,
			PSN:           0x111111,
			LID:           0x11,
			LinkLayer:     swvfsproto.RDMALinkInfiniBand,
			KernelEnabled: true,
			EndpointReady: true,
		},
		data: []byte("needle"),
	}

	var (
		connects        int
		releasedSession uint64
	)
	volumeEndpoint := swvfsdaemon.RDMALocalEndpoint{
		ABIVersion:    swvfsproto.RDMAABIVersion,
		Device:        "mlx5_1",
		Port:          1,
		QPNum:         22,
		PSN:           0x222222,
		LID:           0x22,
		LinkLayer:     swvfsproto.RDMALinkInfiniBand,
		KernelEnabled: true,
		EndpointReady: true,
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case swvfsdaemon.VolumeRDMALocalPath:
			writeTestJSON(t, w, volumeEndpoint)
		case swvfsdaemon.VolumeRDMAConnectPath:
			connects++
			var local swvfsdaemon.RDMALocalEndpoint
			if err := json.NewDecoder(r.Body).Decode(&local); err != nil {
				t.Errorf("decode connect: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if local.QPNum != native.local.QPNum {
				t.Errorf("posted local endpoint = %+v", local)
			}
			writeTestJSON(t, w, map[string]bool{"connected": true})
		case swvfsdaemon.VolumeRDMAReadDescPath:
			var req swvfsdaemon.VolumeRDMAReadDescRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode read desc: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if req.VolumeID != 3 || req.NeedleID != 4 || req.Cookie != 5 || req.Size != 6 {
				t.Errorf("unexpected read desc request: %+v", req)
			}
			writeTestJSON(t, w, swvfsdaemon.VolumeRDMAReadDescResponse{
				Desc: swvfsproto.RDMADataDesc{
					RemoteAddr: 0xbeef,
					RKey:       0,
					Length:     6,
				},
				SessionID: 99,
			})
		case swvfsdaemon.VolumeRDMAReleaseDescPath:
			var req swvfsdaemon.VolumeRDMAReleaseDescRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode release: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			releasedSession = req.SessionID
			writeTestJSON(t, w, map[string]bool{"released": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := &SeaweedFSRDMAClient{
		nativeEngine:     native,
		logger:           logrus.New(),
		nativeVolumeRDMA: true,
		operationTimeout: time.Second,
		nativePeers:      make(map[string]struct{}),
	}
	resp, err := client.ReadNeedle(t.Context(), &NeedleReadRequest{
		VolumeID:     3,
		NeedleID:     4,
		Cookie:       5,
		Size:         6,
		VolumeServer: server.URL,
	})
	if err != nil {
		t.Fatalf("ReadNeedle: %v", err)
	}
	if !resp.IsRDMA || !resp.RealRDMA || resp.Source != "native-volume-rdma" {
		t.Fatalf("unexpected response metadata: %+v", resp)
	}
	if string(resp.Data) != "needle" {
		t.Fatalf("data = %q", resp.Data)
	}
	if connects != 1 {
		t.Fatalf("connects = %d, want 1", connects)
	}
	if !native.connected || native.remote.QPN != volumeEndpoint.QPNum {
		t.Fatalf("native engine was not connected to volume endpoint: %+v", native.remote)
	}
	if native.readDesc.RemoteAddr != 0xbeef || native.readDesc.RKey != 0 {
		t.Fatalf("native read desc = %+v", native.readDesc)
	}
	if releasedSession != 99 {
		t.Fatalf("released session = %d", releasedSession)
	}
}

type fakeNativeVolumeEngine struct {
	local     swvfsdaemon.RDMALocalEndpoint
	remote    swvfsproto.RDMARemoteInfo
	readDesc  swvfsproto.RDMADataDesc
	data      []byte
	connected bool
}

func (e *fakeNativeVolumeEngine) RequesterLocal(context.Context) (swvfsdaemon.RDMALocalEndpoint, error) {
	return e.local, nil
}

func (e *fakeNativeVolumeEngine) RequesterConnect(_ context.Context, remote swvfsproto.RDMARemoteInfo) error {
	e.remote = remote
	e.connected = true
	return nil
}

func (e *fakeNativeVolumeEngine) ReadRemote(_ context.Context, desc swvfsproto.RDMADataDesc, _ time.Duration) ([]byte, error) {
	e.readDesc = desc
	return append([]byte(nil), e.data...), nil
}

func writeTestJSON(t *testing.T, w http.ResponseWriter, value interface{}) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatalf("encode json: %v", err)
	}
}
