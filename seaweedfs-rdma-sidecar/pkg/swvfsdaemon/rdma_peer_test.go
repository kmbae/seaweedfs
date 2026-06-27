package swvfsdaemon

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"seaweedfs-rdma-sidecar/pkg/swvfsproto"
)

type fakeRDMAControl struct {
	local     swvfsproto.RDMALocalInfo
	remote    swvfsproto.RDMARemoteInfo
	connected bool
}

type fakeReadStager struct {
	path   string
	offset uint64
	size   uint64
	desc   swvfsproto.RDMADataDesc
	attr   *swvfsproto.Attr
}

func (f *fakeRDMAControl) GetLocal() (swvfsproto.RDMALocalInfo, error) {
	return f.local, nil
}

func (f *fakeRDMAControl) Connect(remote swvfsproto.RDMARemoteInfo) error {
	f.remote = remote
	f.connected = true
	return nil
}

func (f *fakeReadStager) StageReadRDMA(ctx context.Context, path string, offset, size uint64) (*swvfsproto.RDMADataDesc, *swvfsproto.Attr, error) {
	f.path = path
	f.offset = offset
	f.size = size
	return &f.desc, f.attr, nil
}

func TestRDMALocalEndpointFromInfo(t *testing.T) {
	info := readyInfo(7, 11, 13)
	copy(info.Device[:], "mlx5_0")
	endpoint := RDMALocalEndpointFromInfo(info)

	if !endpoint.ReadyForConnect() {
		t.Fatalf("endpoint should be ready: %+v", endpoint)
	}
	if endpoint.Device != "mlx5_0" || endpoint.QPNum != 11 || endpoint.LID != 7 || endpoint.PSN != 13 {
		t.Fatalf("unexpected endpoint: %+v", endpoint)
	}
}

func TestRDMALocalEndpointRemoteInfo(t *testing.T) {
	endpoint := RDMALocalEndpointFromInfo(readyInfo(7, 11, 13))
	remote, err := endpoint.RemoteInfo(3)
	if err != nil {
		t.Fatalf("RemoteInfo: %v", err)
	}
	if remote.ABIVersion != swvfsproto.RDMAABIVersion || remote.QPN != 11 || remote.LID != 7 || remote.PSN != 13 || remote.SL != 3 {
		t.Fatalf("unexpected remote info: %+v", remote)
	}
	if remote.Flags&swvfsproto.RDMARemoteFGIDValid == 0 {
		t.Fatalf("expected GID valid flag, got flags=0x%x", remote.Flags)
	}
}

func TestRDMAPeerControlServerLocalAndConnect(t *testing.T) {
	local := readyInfo(7, 11, 13)
	control := &fakeRDMAControl{local: local}
	server := httptest.NewServer((&RDMAPeerControlServer{Control: control}).Handler())
	defer server.Close()

	endpoint, err := FetchRDMAPeerEndpoint(context.Background(), server.Client(), server.URL)
	if err != nil {
		t.Fatalf("FetchRDMAPeerEndpoint: %v", err)
	}
	if endpoint.QPNum != local.QPNum {
		t.Fatalf("endpoint qpn = %d, want %d", endpoint.QPNum, local.QPNum)
	}

	if err := PostRDMAPeerConnect(context.Background(), server.Client(), server.URL, endpoint, 0); err != nil {
		t.Fatalf("PostRDMAPeerConnect: %v", err)
	}
	if !control.connected || control.remote.QPN != local.QPNum {
		t.Fatalf("connect was not delegated: connected=%v remote=%+v", control.connected, control.remote)
	}
}

func TestRDMAPeerControlServerReadDesc(t *testing.T) {
	stager := &fakeReadStager{
		desc: swvfsproto.RDMADataDesc{RemoteAddr: 0x1234, RKey: 99, Length: 4096},
		attr: &swvfsproto.Attr{Ino: 4, Size: 4096, Mode: 0100644, Nlink: 1},
	}
	server := httptest.NewServer((&RDMAPeerControlServer{
		Control:    &fakeRDMAControl{local: readyInfo(7, 11, 13)},
		ReadStager: stager,
	}).Handler())
	defer server.Close()

	desc, attr, err := PostRDMAPeerReadDesc(context.Background(), server.Client(), server.URL, "/file", 8, 4096)
	if err != nil {
		t.Fatalf("PostRDMAPeerReadDesc: %v", err)
	}
	if desc.RemoteAddr != 0x1234 || desc.RKey != 99 || desc.Length != 4096 {
		t.Fatalf("descriptor mismatch: %+v", desc)
	}
	if attr == nil || attr.Ino != 4 || stager.path != "/file" || stager.offset != 8 || stager.size != 4096 {
		t.Fatalf("read-desc delegation mismatch: attr=%+v path=%q off=%d size=%d", attr, stager.path, stager.offset, stager.size)
	}
}

func TestRemoteRDMAReadDescriptorClient(t *testing.T) {
	local := readyInfo(1, 10, 100)
	local.Flags |= swvfsproto.RDMAFQPConnected
	remote := RDMALocalEndpointFromInfo(readyInfo(2, 20, 200))
	stager := &fakeReadStager{
		desc: swvfsproto.RDMADataDesc{RemoteAddr: 0xbeef, RKey: 12, Length: 512},
		attr: &swvfsproto.Attr{Ino: 5, Size: 512, Mode: 0100644, Nlink: 1},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case RDMAPeerLocalPath:
			writeJSON(w, remote)
		case RDMAPeerReadDescPath:
			(&RDMAPeerControlServer{ReadStager: stager}).handleReadDesc(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := &RemoteRDMAReadDescriptorClient{
		Control: &fakeRDMAControl{local: local},
		Peers:   []string{server.URL},
		Client:  server.Client(),
		Timeout: time.Second,
	}
	desc, attr, err := client.ReadFileRDMA(context.Background(), "/file", 0, 512)
	if err != nil {
		t.Fatalf("ReadFileRDMA: %v", err)
	}
	if desc.RemoteAddr != 0xbeef || desc.RKey != 12 || desc.Length != 512 {
		t.Fatalf("descriptor mismatch: %+v", desc)
	}
	if attr == nil || attr.Ino != 5 || stager.path != "/file" || stager.size != 512 {
		t.Fatalf("remote descriptor delegation mismatch: attr=%+v path=%q size=%d", attr, stager.path, stager.size)
	}
}

func TestSelectRDMAPairedPeer(t *testing.T) {
	local1 := RDMALocalEndpointFromInfo(readyInfo(1, 10, 100))
	local2 := RDMALocalEndpointFromInfo(readyInfo(2, 20, 200))
	local3 := RDMALocalEndpointFromInfo(readyInfo(3, 30, 300))

	peer, ok := SelectRDMAPairedPeer(local1, []RDMALocalEndpoint{local2, local3})
	if !ok || !peer.SamePeer(local2) {
		t.Fatalf("local1 peer = %+v ok=%v, want local2", peer, ok)
	}
	peer, ok = SelectRDMAPairedPeer(local2, []RDMALocalEndpoint{local1, local3})
	if !ok || !peer.SamePeer(local1) {
		t.Fatalf("local2 peer = %+v ok=%v, want local1", peer, ok)
	}
	if peer, ok = SelectRDMAPairedPeer(local3, []RDMALocalEndpoint{local1, local2}); ok {
		t.Fatalf("local3 should be unpaired with odd endpoint count, got %+v", peer)
	}
}

func TestNormalizeRDMAPeerURL(t *testing.T) {
	got, err := normalizeRDMAPeerURL("10.0.0.1:18084", RDMAPeerLocalPath)
	if err != nil {
		t.Fatalf("normalizeRDMAPeerURL: %v", err)
	}
	if got != "http://10.0.0.1:18084/rdma/local" {
		t.Fatalf("normalized URL = %q", got)
	}

	got, err = normalizeRDMAPeerURL("http://10.0.0.1:18084/custom", RDMAPeerConnectPath)
	if err != nil {
		t.Fatalf("normalize custom URL: %v", err)
	}
	if got != "http://10.0.0.1:18084/custom" {
		t.Fatalf("custom URL changed to %q", got)
	}
}

func readyInfo(lid, qpn, psn uint32) swvfsproto.RDMALocalInfo {
	var info swvfsproto.RDMALocalInfo
	info.ABIVersion = swvfsproto.RDMAABIVersion
	info.Flags = swvfsproto.RDMAFKernelEnabled | swvfsproto.RDMAFEndpointReady | swvfsproto.RDMAFGIDValid
	info.Port = 1
	info.QPNum = qpn
	info.PSN = psn
	info.LID = lid
	info.LinkLayer = swvfsproto.RDMALinkInfiniBand
	for i := range info.GID {
		info.GID[i] = byte(lid + uint32(i))
	}
	return info
}

func TestFetchRDMAPeerEndpointStatusError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	if _, err := FetchRDMAPeerEndpoint(context.Background(), server.Client(), server.URL); err == nil {
		t.Fatal("expected fetch error")
	}
}
