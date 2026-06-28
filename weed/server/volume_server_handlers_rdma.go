package weed_server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

type VolumeRdmaReadExporter interface {
	PrepareRead(context.Context, VolumeRdmaReadRequest) (*VolumeRdmaReadLease, error)
	ReleaseRead(context.Context, uint64) error
}

type VolumeRdmaReadRequest struct {
	ConnectionID uint64 `json:"connection_id,omitempty"`
	FileID       string `json:"file_id"`
	VolumeID     uint32 `json:"volume_id"`
	NeedleID     uint64 `json:"needle_id"`
	Cookie       uint32 `json:"cookie"`
	Offset       uint64 `json:"offset"`
	Size         uint64 `json:"size"`
}

type VolumeRdmaDataDesc struct {
	RemoteAddr uint64
	RKey       uint32
	Length     uint32
	Reserved   [4]uint64
}

type VolumeRdmaReadLease struct {
	Desc         VolumeRdmaDataDesc
	ConnectionID uint64
	SessionID    uint64
}

type VolumeRdmaWriteRequest struct {
	ConnectionID uint64             `json:"connection_id,omitempty"`
	FileID       string             `json:"file_id"`
	VolumeID     uint32             `json:"volume_id"`
	NeedleID     uint64             `json:"needle_id"`
	Cookie       uint32             `json:"cookie"`
	Size         uint64             `json:"size"`
	Desc         VolumeRdmaDataDesc `json:"desc"`
	TimeoutMs    uint64             `json:"timeout_ms,omitempty"`
}

type volumeRdmaReadDescResponse struct {
	Desc         VolumeRdmaDataDesc `json:"desc"`
	ConnectionID uint64             `json:"connection_id,omitempty"`
	SessionID    uint64             `json:"session_id,omitempty"`
}

type volumeRdmaWriteResponse struct {
	FileID string `json:"file_id"`
	Size   uint64 `json:"size"`
	Source string `json:"source"`
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
		Desc:         lease.Desc,
		ConnectionID: lease.ConnectionID,
		SessionID:    lease.SessionID,
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

func (vs *VolumeServer) volumeRdmaWriteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	requester, ok := vs.rdmaRequesterEndpoint()
	if !ok {
		http.Error(w, "native RDMA requester endpoint is not configured", http.StatusNotImplemented)
		return
	}

	var req VolumeRdmaWriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	if err := validateVolumeRdmaWriteRequest(req); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}

	timeout := 5 * time.Second
	if req.TimeoutMs != 0 {
		timeout = time.Duration(req.TimeoutMs) * time.Millisecond
	}
	data, err := requester.ReadRemoteFor(r.Context(), req.ConnectionID, req.Desc, timeout)
	if err != nil {
		writeJsonError(w, r, http.StatusServiceUnavailable, err)
		return
	}
	if uint64(len(data)) < req.Size {
		writeJsonError(w, r, http.StatusServiceUnavailable, fmt.Errorf("native RDMA write read %d bytes for %d byte payload", len(data), req.Size))
		return
	}

	fileID, err := vs.writeNeedleDataFromNativeRdma(r.Context(), req, data[:req.Size])
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}
	writeJsonQuiet(w, r, http.StatusOK, volumeRdmaWriteResponse{
		FileID: fileID,
		Size:   req.Size,
		Source: "native-volume-rdma-write",
	})
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

func validateVolumeRdmaWriteRequest(req VolumeRdmaWriteRequest) error {
	if req.VolumeID == 0 || req.NeedleID == 0 || req.Size == 0 {
		return fmt.Errorf("volume_id, needle_id, and size are required")
	}
	if req.ConnectionID == 0 {
		return fmt.Errorf("connection_id is required")
	}
	if req.Size > volumeRdmaEngineMaxFrameSize {
		return fmt.Errorf("native RDMA write too large: %d > %d", req.Size, volumeRdmaEngineMaxFrameSize)
	}
	if req.Desc.RemoteAddr == 0 || req.Desc.Length == 0 {
		return fmt.Errorf("native RDMA write descriptor is required")
	}
	if uint64(req.Desc.Length) < req.Size {
		return fmt.Errorf("native RDMA write descriptor length %d is smaller than payload size %d", req.Desc.Length, req.Size)
	}
	return nil
}

func (vs *VolumeServer) writeNeedleDataFromNativeRdma(ctx context.Context, req VolumeRdmaWriteRequest, data []byte) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if vs == nil || vs.store == nil {
		return "", fmt.Errorf("volume store is not configured")
	}
	if err := vs.CheckMaintenanceMode(); err != nil {
		return "", err
	}
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeID))
	if v == nil {
		return "", fmt.Errorf("not found volume id %d", req.VolumeID)
	}

	n := &needle.Needle{
		Id:           types.NeedleId(req.NeedleID),
		Cookie:       types.Cookie(req.Cookie),
		Data:         data,
		LastModified: uint64(time.Now().Unix()),
	}
	n.SetHasLastModifiedDate()
	n.Checksum = needle.NewCRC(n.Data)

	blob, size, err := needle.EncodeNeedleBlob(n, v.Version())
	if err != nil {
		return "", fmt.Errorf("encode needle blob: %w", err)
	}
	if err := v.WriteNeedleBlob(types.NeedleId(req.NeedleID), blob, types.Size(size)); err != nil {
		return "", fmt.Errorf("write blob needle %d size %d: %w", req.NeedleID, size, err)
	}
	if req.FileID != "" {
		return req.FileID, nil
	}
	return needle.NewFileId(needle.VolumeId(req.VolumeID), req.NeedleID, req.Cookie).String(), nil
}
