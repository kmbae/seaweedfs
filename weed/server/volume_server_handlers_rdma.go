package weed_server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

type VolumeRdmaReadExporter interface {
	PrepareRead(context.Context, VolumeRdmaReadRequest) (*VolumeRdmaReadLease, error)
	ReleaseRead(context.Context, uint64) error
}

type VolumeRdmaReadRequest struct {
	FileID   string `json:"file_id"`
	VolumeID uint32 `json:"volume_id"`
	NeedleID uint64 `json:"needle_id"`
	Cookie   uint32 `json:"cookie"`
	Offset   uint64 `json:"offset"`
	Size     uint64 `json:"size"`
}

type VolumeRdmaDataDesc struct {
	RemoteAddr uint64
	RKey       uint32
	Length     uint32
	Reserved   [4]uint64
}

type VolumeRdmaReadLease struct {
	Desc      VolumeRdmaDataDesc
	SessionID uint64
}

type volumeRdmaReadDescResponse struct {
	Desc      VolumeRdmaDataDesc `json:"desc"`
	SessionID uint64             `json:"session_id,omitempty"`
}

type volumeRdmaReleaseDescRequest struct {
	SessionID uint64 `json:"session_id"`
}

func (vs *VolumeServer) volumeRdmaReadDescHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if vs == nil || vs.rdmaReadExporter == nil {
		http.Error(w, "native RDMA read exporter is not configured", http.StatusNotImplemented)
		return
	}

	var req VolumeRdmaReadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	if req.VolumeID == 0 || req.NeedleID == 0 || req.Size == 0 {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("volume_id, needle_id, and size are required"))
		return
	}

	lease, err := vs.rdmaReadExporter.PrepareRead(r.Context(), req)
	if err != nil {
		writeJsonError(w, r, volumeRdmaReadHTTPStatus(err), err)
		return
	}
	if lease == nil || lease.Desc.RemoteAddr == 0 || lease.Desc.Length == 0 {
		writeJsonError(w, r, http.StatusNotImplemented, fmt.Errorf("native RDMA read exporter returned no exportable descriptor"))
		return
	}
	writeJsonQuiet(w, r, http.StatusOK, volumeRdmaReadDescResponse{
		Desc:      lease.Desc,
		SessionID: lease.SessionID,
	})
}

func (vs *VolumeServer) volumeRdmaReleaseDescHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if vs == nil || vs.rdmaReadExporter == nil {
		http.Error(w, "native RDMA read exporter is not configured", http.StatusNotImplemented)
		return
	}

	var req volumeRdmaReleaseDescRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	if req.SessionID == 0 {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("session_id is required"))
		return
	}
	if err := vs.rdmaReadExporter.ReleaseRead(r.Context(), req.SessionID); err != nil {
		writeJsonError(w, r, volumeRdmaReadHTTPStatus(err), err)
		return
	}
	writeJsonQuiet(w, r, http.StatusOK, map[string]bool{"released": true})
}

func volumeRdmaReadHTTPStatus(err error) int {
	switch {
	case err == nil:
		return http.StatusOK
	case errors.Is(err, ErrVolumeRdmaReadNotConfigured), errors.Is(err, ErrVolumeRdmaReadNotExportable):
		return http.StatusNotImplemented
	case errors.Is(err, ErrVolumeRdmaReadTooLarge):
		return http.StatusRequestEntityTooLarge
	default:
		return http.StatusServiceUnavailable
	}
}
