package weed_server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

type fakeVolumeRdmaReadExporter struct {
	req             VolumeRdmaReadRequest
	releasedSession uint64
	lease           *VolumeRdmaReadLease
}

func (e *fakeVolumeRdmaReadExporter) PrepareRead(ctx context.Context, req VolumeRdmaReadRequest) (*VolumeRdmaReadLease, error) {
	e.req = req
	return e.lease, nil
}

func (e *fakeVolumeRdmaReadExporter) ReleaseRead(ctx context.Context, sessionID uint64) error {
	e.releasedSession = sessionID
	return nil
}

func TestVolumeRdmaReadDescHandlerReturnsDescriptor(t *testing.T) {
	exporter := &fakeVolumeRdmaReadExporter{
		lease: &VolumeRdmaReadLease{
			Desc: VolumeRdmaDataDesc{
				RemoteAddr: 0xbeef,
				RKey:       77,
				Length:     4096,
			},
			SessionID: 99,
		},
	}
	vs := &VolumeServer{rdmaReadExporter: exporter}
	body := bytes.NewBufferString(`{"file_id":"3,01637037d6","volume_id":3,"needle_id":123,"cookie":456,"offset":7,"size":4096}`)
	req := httptest.NewRequest(http.MethodPost, "/rdma/native/read-desc", body)
	rec := httptest.NewRecorder()

	vs.volumeRdmaReadDescHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if exporter.req.VolumeID != 3 || exporter.req.NeedleID != 123 || exporter.req.Offset != 7 || exporter.req.Size != 4096 {
		t.Fatalf("unexpected request: %+v", exporter.req)
	}
	var resp volumeRdmaReadDescResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Desc.RemoteAddr != 0xbeef || resp.Desc.RKey != 77 || resp.Desc.Length != 4096 || resp.SessionID != 99 {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestVolumeRdmaReadDescHandlerNotConfigured(t *testing.T) {
	vs := &VolumeServer{}
	req := httptest.NewRequest(http.MethodPost, "/rdma/native/read-desc", bytes.NewBufferString(`{"volume_id":3,"needle_id":123,"size":4096}`))
	rec := httptest.NewRecorder()

	vs.volumeRdmaReadDescHandler(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestVolumeRdmaReleaseDescHandler(t *testing.T) {
	exporter := &fakeVolumeRdmaReadExporter{}
	vs := &VolumeServer{rdmaReadExporter: exporter}
	req := httptest.NewRequest(http.MethodPost, "/rdma/native/release-desc", bytes.NewBufferString(`{"session_id":99}`))
	rec := httptest.NewRecorder()

	vs.volumeRdmaReleaseDescHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if exporter.releasedSession != 99 {
		t.Fatalf("released session = %d", exporter.releasedSession)
	}
}
