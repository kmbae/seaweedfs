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
		ConnectionID:  44,
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
			if r.URL.Query().Get("connection_id") != "44" {
				t.Errorf("connect connection_id = %q", r.URL.Query().Get("connection_id"))
			}
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
			if req.ConnectionID != 44 || req.VolumeID != 3 || req.NeedleID != 4 || req.Cookie != 5 || req.Size != 6 {
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
		nativePeers:      make(map[string]nativeVolumePeer),
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
	if native.connectConnectionID != 33 || native.readConnectionID != 33 {
		t.Fatalf("native requester connection IDs connect=%d read=%d", native.connectConnectionID, native.readConnectionID)
	}
	if native.readDesc.RemoteAddr != 0xbeef || native.readDesc.RKey != 0 {
		t.Fatalf("native read desc = %+v", native.readDesc)
	}
	if releasedSession != 99 {
		t.Fatalf("released session = %d", releasedSession)
	}
}

func TestWriteNeedleUsesNativeVolumeRDMA(t *testing.T) {
	native := &fakeNativeVolumeEngine{
		local: swvfsdaemon.RDMALocalEndpoint{
			ABIVersion:    swvfsproto.RDMAABIVersion,
			Device:        "mlx5_0",
			Port:          1,
			QPNum:         31,
			PSN:           0x313131,
			LID:           0x31,
			LinkLayer:     swvfsproto.RDMALinkInfiniBand,
			KernelEnabled: true,
			EndpointReady: true,
		},
		registerDesc: swvfsproto.RDMADataDesc{
			RemoteAddr: 0xcafe,
			RKey:       17,
			Length:     7,
		},
	}

	var (
		requesterConnects int
		writeReq          swvfsdaemon.VolumeRDMAWriteRequest
	)
	volumeRequester := swvfsdaemon.RDMALocalEndpoint{
		ConnectionID:  88,
		ABIVersion:    swvfsproto.RDMAABIVersion,
		Device:        "mlx5_1",
		Port:          1,
		QPNum:         41,
		PSN:           0x414141,
		LID:           0x41,
		LinkLayer:     swvfsproto.RDMALinkInfiniBand,
		KernelEnabled: true,
		EndpointReady: true,
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case swvfsdaemon.VolumeRDMARequesterLocalPath:
			writeTestJSON(t, w, volumeRequester)
		case swvfsdaemon.VolumeRDMARequesterConnectPath:
			requesterConnects++
			if r.URL.Query().Get("connection_id") != "88" {
				t.Errorf("requester connect connection_id = %q", r.URL.Query().Get("connection_id"))
			}
			var local swvfsdaemon.RDMALocalEndpoint
			if err := json.NewDecoder(r.Body).Decode(&local); err != nil {
				t.Errorf("decode requester connect: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if local.QPNum != native.local.QPNum {
				t.Errorf("posted worker endpoint = %+v", local)
			}
			writeTestJSON(t, w, map[string]bool{"connected": true})
		case swvfsdaemon.VolumeRDMAWritePath:
			if err := json.NewDecoder(r.Body).Decode(&writeReq); err != nil {
				t.Errorf("decode write: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			writeTestJSON(t, w, swvfsdaemon.VolumeRDMAWriteResponse{
				FileID: writeReq.FileID,
				Size:   writeReq.Size,
				Source: "native-volume-rdma-write",
			})
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
		nativePeers:      make(map[string]nativeVolumePeer),
	}
	resp, err := client.WriteNeedle(t.Context(), &NeedleWriteRequest{
		VolumeID:     7,
		NeedleID:     8,
		Cookie:       9,
		Data:         []byte("payload"),
		VolumeServer: server.URL,
	})
	if err != nil {
		t.Fatalf("WriteNeedle: %v", err)
	}
	if !resp.IsRDMA || !resp.RealRDMA || resp.Source != "native-volume-rdma-write" || resp.DataSource != "volume-native-write" {
		t.Fatalf("unexpected response metadata: %+v", resp)
	}
	if requesterConnects != 1 {
		t.Fatalf("requesterConnects = %d, want 1", requesterConnects)
	}
	if native.providerConnectConnectionID != 55 || native.providerRemote.QPN != volumeRequester.QPNum {
		t.Fatalf("native provider was not connected to volume requester: conn=%d remote=%+v", native.providerConnectConnectionID, native.providerRemote)
	}
	if native.registerConnectionID != 55 || string(native.registeredData) != "payload" {
		t.Fatalf("registered conn=%d data=%q", native.registerConnectionID, native.registeredData)
	}
	if native.releasedSession != 66 {
		t.Fatalf("released session = %d, want 66", native.releasedSession)
	}
	if writeReq.ConnectionID != 88 || writeReq.VolumeID != 7 || writeReq.NeedleID != 8 || writeReq.Cookie != 9 || writeReq.Size != 7 {
		t.Fatalf("unexpected native write request: %+v", writeReq)
	}
	if writeReq.Desc.RemoteAddr != native.registerDesc.RemoteAddr || writeReq.Desc.RKey != native.registerDesc.RKey {
		t.Fatalf("write desc = %+v, want %+v", writeReq.Desc, native.registerDesc)
	}
}

type fakeNativeVolumeEngine struct {
	local                       swvfsdaemon.RDMALocalEndpoint
	remote                      swvfsproto.RDMARemoteInfo
	providerRemote              swvfsproto.RDMARemoteInfo
	readDesc                    swvfsproto.RDMADataDesc
	registerDesc                swvfsproto.RDMADataDesc
	data                        []byte
	registeredData              []byte
	requesterConnection         uint64
	providerConnection          uint64
	connected                   bool
	providerConnected           bool
	readConnectionID            uint64
	connectConnectionID         uint64
	providerConnectConnectionID uint64
	registerConnectionID        uint64
	releasedSession             uint64
}

func (e *fakeNativeVolumeEngine) RequesterLocal(context.Context) (swvfsdaemon.RDMALocalEndpoint, error) {
	return e.local, nil
}

func (e *fakeNativeVolumeEngine) RequesterLocalFor(context.Context, uint64) (swvfsdaemon.RDMALocalEndpoint, uint64, error) {
	if e.requesterConnection == 0 {
		e.requesterConnection = 33
	}
	local := e.local
	local.ConnectionID = e.requesterConnection
	return local, e.requesterConnection, nil
}

func (e *fakeNativeVolumeEngine) RequesterConnect(_ context.Context, remote swvfsproto.RDMARemoteInfo) error {
	e.remote = remote
	e.connected = true
	return nil
}

func (e *fakeNativeVolumeEngine) RequesterConnectFor(_ context.Context, connectionID uint64, remote swvfsproto.RDMARemoteInfo) error {
	e.connectConnectionID = connectionID
	return e.RequesterConnect(context.Background(), remote)
}

func (e *fakeNativeVolumeEngine) ReadRemote(_ context.Context, desc swvfsproto.RDMADataDesc, _ time.Duration) ([]byte, error) {
	e.readDesc = desc
	return append([]byte(nil), e.data...), nil
}

func (e *fakeNativeVolumeEngine) ReadRemoteFor(_ context.Context, connectionID uint64, desc swvfsproto.RDMADataDesc, timeout time.Duration) ([]byte, error) {
	e.readConnectionID = connectionID
	return e.ReadRemote(context.Background(), desc, timeout)
}

func (e *fakeNativeVolumeEngine) ProviderLocal(context.Context) (swvfsdaemon.RDMALocalEndpoint, error) {
	return e.local, nil
}

func (e *fakeNativeVolumeEngine) ProviderLocalFor(context.Context, uint64) (swvfsdaemon.RDMALocalEndpoint, uint64, error) {
	if e.providerConnection == 0 {
		e.providerConnection = 55
	}
	local := e.local
	local.ConnectionID = e.providerConnection
	return local, e.providerConnection, nil
}

func (e *fakeNativeVolumeEngine) ProviderConnect(_ context.Context, remote swvfsproto.RDMARemoteInfo) error {
	e.providerRemote = remote
	e.providerConnected = true
	return nil
}

func (e *fakeNativeVolumeEngine) ProviderConnectFor(_ context.Context, connectionID uint64, remote swvfsproto.RDMARemoteInfo) error {
	e.providerConnectConnectionID = connectionID
	return e.ProviderConnect(context.Background(), remote)
}

func (e *fakeNativeVolumeEngine) RegisterReadBufferFor(_ context.Context, connectionID uint64, data []byte) (swvfsproto.RDMADataDesc, uint64, error) {
	e.registerConnectionID = connectionID
	e.registeredData = append([]byte(nil), data...)
	desc := e.registerDesc
	if desc.RemoteAddr == 0 {
		desc = swvfsproto.RDMADataDesc{RemoteAddr: 0xabcd, RKey: 1, Length: uint32(len(data))}
	}
	return desc, 66, nil
}

func (e *fakeNativeVolumeEngine) ReleaseRead(_ context.Context, sessionID uint64) error {
	e.releasedSession = sessionID
	return nil
}

func writeTestJSON(t *testing.T, w http.ResponseWriter, value interface{}) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatalf("encode json: %v", err)
	}
}
