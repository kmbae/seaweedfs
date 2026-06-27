package weed_server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

type fakeVolumeRdmaEndpoint struct {
	local       VolumeRdmaEndpointInfo
	remote      VolumeRdmaRemoteInfo
	connected   bool
	connectErr  error
	localErr    error
	localCalled bool
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
	}
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
	if resp.LocalPath != VolumeRdmaNativeLocalPath || resp.ConnectPath != VolumeRdmaNativeConnectPath {
		t.Fatalf("unexpected endpoint paths: %+v", resp)
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
