package weed_server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type fakeVolumeRdmaEndpoint struct {
	local       VolumeRdmaEndpointInfo
	remote      VolumeRdmaRemoteInfo
	connected   bool
	connectErr  error
	localErr    error
	localCalled bool

	requesterLocal     VolumeRdmaEndpointInfo
	requesterRemote    VolumeRdmaRemoteInfo
	requesterConnected bool
	requesterLocalID   uint64
	requesterConnectID uint64
	requesterReadData  []byte
	registeredReads    int
	releasedSessions   []uint64
}

func (e *fakeVolumeRdmaEndpoint) LocalEndpoint(ctx context.Context) (VolumeRdmaEndpointInfo, error) {
	e.localCalled = true
	if e.localErr != nil {
		return VolumeRdmaEndpointInfo{}, e.localErr
	}
	return e.local, nil
}

func (e *fakeVolumeRdmaEndpoint) ConnectEndpoint(ctx context.Context, remote VolumeRdmaRemoteInfo) error {
	e.remote = remote
	e.connected = true
	return e.connectErr
}

func (e *fakeVolumeRdmaEndpoint) RequesterLocalEndpoint(ctx context.Context) (VolumeRdmaEndpointInfo, error) {
	local, _, err := e.RequesterLocalEndpointFor(ctx, 0)
	return local, err
}

func (e *fakeVolumeRdmaEndpoint) RequesterLocalEndpointFor(ctx context.Context, connectionID uint64) (VolumeRdmaEndpointInfo, uint64, error) {
	if e.requesterLocalID == 0 {
		e.requesterLocalID = 77
	}
	local := e.requesterLocal
	local.ConnectionID = e.requesterLocalID
	return local, e.requesterLocalID, nil
}

func (e *fakeVolumeRdmaEndpoint) RequesterConnectEndpoint(ctx context.Context, remote VolumeRdmaRemoteInfo) error {
	return e.RequesterConnectEndpointFor(ctx, 0, remote)
}

func (e *fakeVolumeRdmaEndpoint) RequesterConnectEndpointFor(ctx context.Context, connectionID uint64, remote VolumeRdmaRemoteInfo) error {
	e.requesterConnectID = connectionID
	e.requesterRemote = remote
	e.requesterConnected = true
	return nil
}

func (e *fakeVolumeRdmaEndpoint) ReadRemoteFor(ctx context.Context, connectionID uint64, desc VolumeRdmaDataDesc, timeout time.Duration) ([]byte, error) {
	return append([]byte(nil), e.requesterReadData...), nil
}

func (e *fakeVolumeRdmaEndpoint) ReadRemoteToFor(ctx context.Context, connectionID uint64, desc VolumeRdmaDataDesc, timeout time.Duration, dst io.Writer) error {
	_, err := dst.Write(e.requesterReadData)
	return err
}

func (e *fakeVolumeRdmaEndpoint) RegisterWriteBufferFor(ctx context.Context, connectionID uint64, size uint64) (VolumeRdmaRegisteredBuffer, error) {
	return nil, errors.New("not implemented")
}

func (e *fakeVolumeRdmaEndpoint) ReadRegisteredBuffer(ctx context.Context, sessionID uint64, size uint64) ([]byte, error) {
	e.registeredReads++
	return nil, errors.New("not implemented")
}

func (e *fakeVolumeRdmaEndpoint) ReleaseSession(ctx context.Context, sessionID uint64) error {
	e.releasedSessions = append(e.releasedSessions, sessionID)
	return nil
}

func readyVolumeRdmaEndpoint(qpn uint32) VolumeRdmaEndpointInfo {
	return VolumeRdmaEndpointInfo{
		ABIVersion:    VolumeRdmaABIVersion,
		KernelEnabled: true,
		EndpointReady: true,
		Device:        "mlx5_0",
		Port:          1,
		QPNum:         qpn,
		PSN:           0x123456,
		LID:           0x42,
		GIDIndex:      0,
		LinkLayer:     VolumeRdmaLinkInfiniBand,
	}
}

func TestVolumeRdmaStatusHandlerReportsConfiguration(t *testing.T) {
	vs := &VolumeServer{
		rdmaEndpoint:     &fakeVolumeRdmaEndpoint{},
		rdmaReadExporter: &fakeVolumeRdmaReadExporter{},
		rdmaTransport:    VolumeRdmaTransportSocket,
	}
	vs.rdmaStats.readDescRequests.Add(2)
	vs.rdmaStats.readDescSuccesses.Add(1)
	vs.rdmaStats.readDescBytes.Add(8192)
	req := httptest.NewRequest(http.MethodGet, VolumeRdmaNativeStatusPath, nil)
	rec := httptest.NewRecorder()

	vs.volumeRdmaStatusHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var resp volumeRdmaNativeStatusResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if !resp.EndpointConfigured || !resp.ReadExporterConfigured {
		t.Fatalf("unexpected status response: %+v", resp)
	}
	if resp.Transport != VolumeRdmaTransportSocket {
		t.Fatalf("transport = %q, want %q", resp.Transport, VolumeRdmaTransportSocket)
	}
	if resp.LocalPath != VolumeRdmaNativeLocalPath ||
		resp.ConnectPath != VolumeRdmaNativeConnectPath ||
		resp.ReadDescBatchPath != VolumeRdmaNativeReadDescBatchPath ||
		resp.ReleaseDescBatchPath != VolumeRdmaNativeReleaseDescBatchPath ||
		resp.WriteCommitBatchPath != VolumeRdmaNativeWriteCommitBatchPath {
		t.Fatalf("unexpected endpoint paths: %+v", resp)
	}
	if resp.Counters["read_desc_requests"] != 2 || resp.Counters["read_desc_successes"] != 1 || resp.Counters["read_desc_bytes"] != 8192 {
		t.Fatalf("unexpected counters: %+v", resp.Counters)
	}
}

func TestVolumeRdmaLocalHandlerReturnsEndpoint(t *testing.T) {
	endpoint := &fakeVolumeRdmaEndpoint{local: readyVolumeRdmaEndpoint(99)}
	vs := &VolumeServer{rdmaEndpoint: endpoint}
	req := httptest.NewRequest(http.MethodGet, VolumeRdmaNativeLocalPath, nil)
	rec := httptest.NewRecorder()

	vs.volumeRdmaLocalHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var resp VolumeRdmaEndpointInfo
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode local: %v", err)
	}
	if !endpoint.localCalled || resp.QPNum != 99 || !resp.ReadyForConnect() {
		t.Fatalf("unexpected local endpoint: called=%v resp=%+v", endpoint.localCalled, resp)
	}
}

func TestVolumeRdmaLocalHandlerNotConfigured(t *testing.T) {
	vs := &VolumeServer{}
	req := httptest.NewRequest(http.MethodGet, VolumeRdmaNativeLocalPath, nil)
	rec := httptest.NewRecorder()

	vs.volumeRdmaLocalHandler(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestVolumeRdmaConnectHandlerConvertsPeerEndpoint(t *testing.T) {
	endpoint := &fakeVolumeRdmaEndpoint{}
	vs := &VolumeServer{rdmaEndpoint: endpoint}
	body, err := json.Marshal(readyVolumeRdmaEndpoint(1234))
	if err != nil {
		t.Fatalf("marshal endpoint: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, VolumeRdmaNativeConnectPath+"?sl=3", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	vs.volumeRdmaConnectHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if !endpoint.connected {
		t.Fatalf("endpoint was not connected")
	}
	if endpoint.remote.QPN != 1234 || endpoint.remote.LID != 0x42 || endpoint.remote.SL != 3 {
		t.Fatalf("unexpected remote info: %+v", endpoint.remote)
	}
}

func TestVolumeRdmaConnectHandlerRejectsUnreadyPeer(t *testing.T) {
	endpoint := &fakeVolumeRdmaEndpoint{}
	vs := &VolumeServer{rdmaEndpoint: endpoint}
	body, err := json.Marshal(VolumeRdmaEndpointInfo{ABIVersion: VolumeRdmaABIVersion})
	if err != nil {
		t.Fatalf("marshal endpoint: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, VolumeRdmaNativeConnectPath, bytes.NewReader(body))
	rec := httptest.NewRecorder()

	vs.volumeRdmaConnectHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if endpoint.connected {
		t.Fatalf("endpoint should not be connected")
	}
}

func TestVolumeRdmaRequesterLocalHandlerReturnsEndpoint(t *testing.T) {
	endpoint := &fakeVolumeRdmaEndpoint{requesterLocal: readyVolumeRdmaEndpoint(199)}
	vs := &VolumeServer{rdmaEndpoint: endpoint}
	req := httptest.NewRequest(http.MethodGet, VolumeRdmaNativeRequesterLocalPath, nil)
	rec := httptest.NewRecorder()

	vs.volumeRdmaRequesterLocalHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var resp VolumeRdmaEndpointInfo
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode requester local: %v", err)
	}
	if resp.ConnectionID != 77 || resp.QPNum != 199 || !resp.ReadyForConnect() {
		t.Fatalf("unexpected requester endpoint: %+v", resp)
	}
}

func TestVolumeRdmaRequesterConnectHandlerConvertsPeerEndpoint(t *testing.T) {
	endpoint := &fakeVolumeRdmaEndpoint{}
	vs := &VolumeServer{rdmaEndpoint: endpoint}
	body, err := json.Marshal(readyVolumeRdmaEndpoint(2233))
	if err != nil {
		t.Fatalf("marshal endpoint: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, VolumeRdmaNativeRequesterConnectPath+"?connection_id=77&sl=4", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	vs.volumeRdmaRequesterConnectHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if !endpoint.requesterConnected || endpoint.requesterConnectID != 77 {
		t.Fatalf("requester was not connected: endpoint=%+v", endpoint)
	}
	if endpoint.requesterRemote.QPN != 2233 || endpoint.requesterRemote.SL != 4 {
		t.Fatalf("unexpected requester remote info: %+v", endpoint.requesterRemote)
	}
}

func TestVolumeRdmaWriteCommitBatchHandlerReportsEntryValidation(t *testing.T) {
	endpoint := &fakeVolumeRdmaEndpoint{}
	vs := &VolumeServer{rdmaEndpoint: endpoint}
	body, err := json.Marshal(VolumeRdmaWriteCommitBatchRequest{
		Entries: []VolumeRdmaWriteCommitRequest{
			{SessionID: 0, FileID: "3,abc", VolumeID: 3, NeedleID: 1, Cookie: 2, Size: 4096},
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, VolumeRdmaNativeWriteCommitBatchPath, bytes.NewReader(body))
	rec := httptest.NewRecorder()

	vs.volumeRdmaWriteCommitBatchHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var resp volumeRdmaWriteCommitBatchResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Results) != 1 || resp.Results[0].Status == 0 || resp.Results[0].Error == "" {
		t.Fatalf("unexpected batch response: %+v", resp)
	}
	if endpoint.registeredReads != 0 {
		t.Fatalf("registered reads = %d, want 0", endpoint.registeredReads)
	}
}
