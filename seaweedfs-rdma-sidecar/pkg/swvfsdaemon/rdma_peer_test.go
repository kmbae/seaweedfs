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
	path           string
	offset         uint64
	size           uint64
	desc           swvfsproto.RDMADataDesc
	attr           *swvfsproto.Attr
	sessionID      uint64
	releaseSession uint64
}

type fakeWriteStager struct {
	path          string
	offset        uint64
	size          uint64
	commitPath    string
	commitOffset  uint64
	commitSize    uint64
	commitSession uint64
	abortSession  uint64
	desc          swvfsproto.RDMADataDesc
	attr          *swvfsproto.Attr
}

func (f *fakeRDMAControl) GetLocal() (swvfsproto.RDMALocalInfo, error) {
	return f.local, nil
}

func (f *fakeRDMAControl) Connect(remote swvfsproto.RDMARemoteInfo) error {
	f.remote = remote
	f.connected = true
	return nil
}

func (f *fakeReadStager) StageReadRDMA(ctx context.Context, path string, offset, size uint64) (*RDMAReadDescriptorLease, error) {
	f.path = path
	f.offset = offset
	f.size = size
	return &RDMAReadDescriptorLease{
		Desc:      f.desc,
		Attr:      f.attr,
		SessionID: f.sessionID,
	}, nil
}

func (f *fakeReadStager) ReleaseReadRDMA(ctx context.Context, sessionID uint64) error {
	f.releaseSession = sessionID
	return nil
}

func (f *fakeWriteStager) PrepareWriteRDMA(ctx context.Context, path string, offset, size uint64) (*swvfsproto.RDMADataDesc, *swvfsproto.Attr, error) {
	f.path = path
	f.offset = offset
	f.size = size
	return &f.desc, f.attr, nil
}

func (f *fakeWriteStager) CommitWriteRDMA(ctx context.Context, path string, offset, size uint64) (*swvfsproto.Attr, error) {
	return f.CommitWriteRDMASession(ctx, 0, path, offset, size)
}

func (f *fakeWriteStager) CommitWriteRDMASession(ctx context.Context, sessionID uint64, path string, offset, size uint64) (*swvfsproto.Attr, error) {
	f.commitSession = sessionID
	f.commitPath = path
	f.commitOffset = offset
	f.commitSize = size
	return f.attr, nil
}

func (f *fakeWriteStager) AbortWriteRDMASession(ctx context.Context, sessionID uint64) error {
	f.abortSession = sessionID
	return nil
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
		desc:      swvfsproto.RDMADataDesc{RemoteAddr: 0x1234, RKey: 99, Length: 4096},
		attr:      &swvfsproto.Attr{Ino: 4, Size: 4096, Mode: 0100644, Nlink: 1},
		sessionID: 17,
	}
	server := httptest.NewServer((&RDMAPeerControlServer{
		Control:    &fakeRDMAControl{local: readyInfo(7, 11, 13)},
		ReadStager: stager,
	}).Handler())
	defer server.Close()

	desc, attr, sessionID, err := PostRDMAPeerReadDesc(context.Background(), server.Client(), server.URL, "/file", 8, 4096)
	if err != nil {
		t.Fatalf("PostRDMAPeerReadDesc: %v", err)
	}
	if desc.RemoteAddr != 0x1234 || desc.RKey != 99 || desc.Length != 4096 {
		t.Fatalf("descriptor mismatch: %+v", desc)
	}
	if attr == nil || attr.Ino != 4 || stager.path != "/file" || stager.offset != 8 || stager.size != 4096 {
		t.Fatalf("read-desc delegation mismatch: attr=%+v path=%q off=%d size=%d", attr, stager.path, stager.offset, stager.size)
	}
	if sessionID != 17 {
		t.Fatalf("session id = %d, want 17", sessionID)
	}
	if err := PostRDMAPeerReleaseDesc(context.Background(), server.Client(), server.URL, sessionID); err != nil {
		t.Fatalf("PostRDMAPeerReleaseDesc: %v", err)
	}
	if stager.releaseSession != 17 {
		t.Fatalf("release session = %d, want 17", stager.releaseSession)
	}
}

func TestRDMAPeerControlServerWritePrepareCommit(t *testing.T) {
	stager := &fakeWriteStager{
		desc: swvfsproto.RDMADataDesc{RemoteAddr: 0x2000, RKey: 7, Length: 4096, Reserved: [4]uint64{55}},
		attr: &swvfsproto.Attr{Ino: 6, Size: 4096, Mode: 0100644, Nlink: 1},
	}
	server := httptest.NewServer((&RDMAPeerControlServer{
		Control:     &fakeRDMAControl{local: readyInfo(7, 11, 13)},
		WriteStager: stager,
	}).Handler())
	defer server.Close()

	desc, attr, sessionID, err := PostRDMAPeerWritePrepare(context.Background(), server.Client(), server.URL, "/file", 16, 4096)
	if err != nil {
		t.Fatalf("PostRDMAPeerWritePrepare: %v", err)
	}
	if desc.RemoteAddr != 0x2000 || desc.RKey != 7 || desc.Length != 4096 || sessionID != 55 {
		t.Fatalf("write prepare mismatch: desc=%+v session=%d", desc, sessionID)
	}
	if attr == nil || attr.Ino != 6 || stager.path != "/file" || stager.offset != 16 || stager.size != 4096 {
		t.Fatalf("write prepare delegation mismatch: attr=%+v path=%q off=%d size=%d", attr, stager.path, stager.offset, stager.size)
	}
	commitAttr, err := PostRDMAPeerWriteCommit(context.Background(), server.Client(), server.URL, sessionID, "/file", 16, 4096)
	if err != nil {
		t.Fatalf("PostRDMAPeerWriteCommit: %v", err)
	}
	if commitAttr == nil || stager.commitSession != 55 || stager.commitPath != "/file" || stager.commitOffset != 16 || stager.commitSize != 4096 {
		t.Fatalf("write commit delegation mismatch: attr=%+v session=%d path=%q off=%d size=%d", commitAttr, stager.commitSession, stager.commitPath, stager.commitOffset, stager.commitSize)
	}
	if err := PostRDMAPeerWriteAbort(context.Background(), server.Client(), server.URL, sessionID); err != nil {
		t.Fatalf("PostRDMAPeerWriteAbort: %v", err)
	}
	if stager.abortSession != 55 {
		t.Fatalf("abort session = %d, want 55", stager.abortSession)
	}
}

func TestRemoteRDMAReadDescriptorClient(t *testing.T) {
	local := readyInfo(1, 10, 100)
	local.Flags |= swvfsproto.RDMAFQPConnected
	remote := RDMALocalEndpointFromInfo(readyInfo(2, 20, 200))
	stager := &fakeReadStager{
		desc:      swvfsproto.RDMADataDesc{RemoteAddr: 0xbeef, RKey: 12, Length: 512},
		attr:      &swvfsproto.Attr{Ino: 5, Size: 512, Mode: 0100644, Nlink: 1},
		sessionID: 23,
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case RDMAPeerLocalPath:
			writeJSON(w, remote)
		case RDMAPeerReadDescPath:
			(&RDMAPeerControlServer{ReadStager: stager}).handleReadDesc(w, r)
		case RDMAPeerReleaseDescPath:
			(&RDMAPeerControlServer{ReadStager: stager}).handleReleaseDesc(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := &RemoteRDMAReadDescriptorClient{
		Control:      &fakeRDMAControl{local: local},
		Peers:        []string{server.URL},
		Client:       server.Client(),
		Timeout:      time.Second,
		ReleaseDelay: time.Millisecond,
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
	deadline := time.After(time.Second)
	for stager.releaseSession != 23 {
		select {
		case <-deadline:
			t.Fatalf("release session = %d, want 23", stager.releaseSession)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestRemoteRDMAWriteDescriptorClient(t *testing.T) {
	local := readyInfo(1, 10, 100)
	local.Flags |= swvfsproto.RDMAFQPConnected
	remote := RDMALocalEndpointFromInfo(readyInfo(2, 20, 200))
	stager := &fakeWriteStager{
		desc: swvfsproto.RDMADataDesc{RemoteAddr: 0xbead, RKey: 88, Length: 512, Reserved: [4]uint64{44}},
		attr: &swvfsproto.Attr{Ino: 8, Size: 512, Mode: 0100644, Nlink: 1},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case RDMAPeerLocalPath:
			writeJSON(w, remote)
		case RDMAPeerWritePrepare:
			(&RDMAPeerControlServer{WriteStager: stager}).handleWritePrepare(w, r)
		case RDMAPeerWriteCommit:
			(&RDMAPeerControlServer{WriteStager: stager}).handleWriteCommit(w, r)
		case RDMAPeerWriteAbort:
			(&RDMAPeerControlServer{WriteStager: stager}).handleWriteAbort(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := &RemoteRDMAWriteDescriptorClient{
		Control:    &fakeRDMAControl{local: local},
		Peers:      []string{server.URL},
		Client:     server.Client(),
		Timeout:    time.Second,
		AbortDelay: time.Hour,
	}
	desc, attr, err := client.PrepareWriteRDMA(context.Background(), "/file", 0, 512)
	if err != nil {
		t.Fatalf("PrepareWriteRDMA: %v", err)
	}
	if desc.RemoteAddr != 0xbead || desc.RKey != 88 || desc.Length != 512 || desc.Reserved[0] == 0 {
		t.Fatalf("descriptor mismatch: %+v", desc)
	}
	if attr == nil || attr.Ino != 8 || stager.path != "/file" || stager.size != 512 {
		t.Fatalf("remote write prepare delegation mismatch: attr=%+v path=%q size=%d", attr, stager.path, stager.size)
	}
	commitAttr, err := client.CommitWriteRDMA(context.Background(), "/file", 0, 512)
	if err != nil {
		t.Fatalf("CommitWriteRDMA: %v", err)
	}
	if commitAttr == nil || stager.commitSession != 44 || stager.commitPath != "/file" || stager.commitSize != 512 {
		t.Fatalf("remote write commit delegation mismatch: attr=%+v session=%d path=%q size=%d", commitAttr, stager.commitSession, stager.commitPath, stager.commitSize)
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

func TestShouldInitiateRDMAPeerConnectUsesStableEndpoint(t *testing.T) {
	local := RDMALocalEndpointFromInfo(readyInfo(1, 10, 100))
	peer := RDMALocalEndpointFromInfo(readyInfo(2, 20, 200))

	if !ShouldInitiateRDMAPeerConnect(local, peer) {
		t.Fatalf("lower stable endpoint should initiate")
	}
	if ShouldInitiateRDMAPeerConnect(peer, local) {
		t.Fatalf("higher stable endpoint should wait as responder")
	}

	peer.QPNum = 21
	peer.PSN = 201
	if !ShouldInitiateRDMAPeerConnect(local, peer) {
		t.Fatalf("initiator decision should survive peer QP churn")
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
