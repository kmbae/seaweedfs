package weed_server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/stats"
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

type VolumeRdmaWriteDescRequest struct {
	ConnectionID uint64 `json:"connection_id,omitempty"`
	FileID       string `json:"file_id"`
	VolumeID     uint32 `json:"volume_id"`
	NeedleID     uint64 `json:"needle_id"`
	Cookie       uint32 `json:"cookie"`
	Size         uint64 `json:"size"`
}

type VolumeRdmaWriteCommitRequest struct {
	SessionID uint64 `json:"session_id"`
	FileID    string `json:"file_id"`
	VolumeID  uint32 `json:"volume_id"`
	NeedleID  uint64 `json:"needle_id"`
	Cookie    uint32 `json:"cookie"`
	Size      uint64 `json:"size"`
}

type VolumeRdmaWriteCommitBatchRequest struct {
	Entries []VolumeRdmaWriteCommitRequest `json:"entries"`
}

type VolumeRdmaWriteAbortRequest struct {
	SessionID uint64 `json:"session_id"`
}

type volumeRdmaReadDescResponse struct {
	Desc         VolumeRdmaDataDesc `json:"desc"`
	ConnectionID uint64             `json:"connection_id,omitempty"`
	SessionID    uint64             `json:"session_id,omitempty"`
}

type volumeRdmaWriteDescResponse struct {
	Desc         VolumeRdmaDataDesc `json:"desc"`
	ConnectionID uint64             `json:"connection_id,omitempty"`
	SessionID    uint64             `json:"session_id,omitempty"`
}

type volumeRdmaWriteResponse struct {
	FileID string `json:"file_id"`
	Size   uint64 `json:"size"`
	Source string `json:"source"`
}

type volumeRdmaWriteCommitResult struct {
	SessionID uint64 `json:"session_id,omitempty"`
	FileID    string `json:"file_id,omitempty"`
	Size      uint64 `json:"size,omitempty"`
	Source    string `json:"source,omitempty"`
	Status    int32  `json:"status"`
	Error     string `json:"error,omitempty"`
}

type volumeRdmaWriteCommitBatchResponse struct {
	Results []volumeRdmaWriteCommitResult `json:"results"`
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
	start := time.Now()
	success := false
	var bytesExported uint64
	vs.rdmaStats.readDescRequests.Add(1)
	defer func() {
		recordLatency(&vs.rdmaStats.readDescLatencyNs, start)
		if success {
			vs.rdmaStats.readDescSuccesses.Add(1)
			vs.rdmaStats.readDescBytes.Add(int64(bytesExported))
		} else {
			vs.rdmaStats.readDescFailures.Add(1)
		}
	}()

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
	success = true
	bytesExported = req.Size
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
	start := time.Now()
	success := false
	vs.rdmaStats.releaseDescRequests.Add(1)
	defer func() {
		recordLatency(&vs.rdmaStats.releaseDescLatencyNs, start)
		if success {
			vs.rdmaStats.releaseDescSuccesses.Add(1)
		} else {
			vs.rdmaStats.releaseDescFailures.Add(1)
		}
	}()

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
	success = true
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
	start := time.Now()
	success := false
	var bytesWritten uint64
	vs.rdmaStats.writeRequests.Add(1)
	defer func() {
		recordLatency(&vs.rdmaStats.writeLatencyNs, start)
		if success {
			vs.rdmaStats.writeSuccesses.Add(1)
			vs.rdmaStats.writeBytes.Add(int64(bytesWritten))
		} else {
			vs.rdmaStats.writeFailures.Add(1)
		}
	}()

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
	readDesc := req.Desc
	readDesc.Length = uint32(req.Size)
	if streamer, ok := requester.(volumeRdmaRemoteReadStreamer); ok {
		var readErr error
		fileID, err := vs.writeNeedleDataFromNativeRdmaStream(r.Context(), req, func(w io.Writer) error {
			readErr = streamer.ReadRemoteToFor(r.Context(), req.ConnectionID, readDesc, timeout, w)
			return readErr
		})
		if err != nil {
			if readErr != nil {
				writeJsonError(w, r, http.StatusServiceUnavailable, readErr)
			} else {
				writeJsonError(w, r, http.StatusInternalServerError, err)
			}
			return
		}
		writeJsonQuiet(w, r, http.StatusOK, volumeRdmaWriteResponse{
			FileID: fileID,
			Size:   req.Size,
			Source: "native-volume-rdma-write-stream",
		})
		success = true
		bytesWritten = req.Size
		stats.VolumeServerRdmaTransferBytes.WithLabelValues("write_stream").Add(float64(req.Size))
		stats.VolumeServerRdmaTransferChunks.WithLabelValues("write_stream").Inc()
		return
	}
	stats.VolumeServerRdmaFallbacks.WithLabelValues("write_remote_copy").Inc()
	data, err := requester.ReadRemoteFor(r.Context(), req.ConnectionID, readDesc, timeout)
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
	success = true
	bytesWritten = req.Size
}

func (vs *VolumeServer) volumeRdmaWriteDescHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	registrar, ok := vs.rdmaWriteTargetEndpoint()
	if !ok {
		http.Error(w, "native RDMA write target endpoint is not configured", http.StatusNotImplemented)
		return
	}
	start := time.Now()
	success := false
	var bytesRegistered uint64
	vs.rdmaStats.writeDescRequests.Add(1)
	defer func() {
		recordLatency(&vs.rdmaStats.writeDescLatencyNs, start)
		if success {
			vs.rdmaStats.writeDescSuccesses.Add(1)
			vs.rdmaStats.writeDescBytes.Add(int64(bytesRegistered))
		} else {
			vs.rdmaStats.writeDescFailures.Add(1)
		}
	}()

	var req VolumeRdmaWriteDescRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	if err := validateVolumeRdmaWriteDescRequest(req); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	buffer, err := registrar.RegisterWriteBufferFor(r.Context(), req.ConnectionID, req.Size)
	if err != nil {
		writeJsonError(w, r, http.StatusServiceUnavailable, err)
		return
	}
	desc := buffer.Descriptor()
	sessionID := uint64(0)
	if withSession, ok := buffer.(interface{ SessionID() uint64 }); ok {
		sessionID = withSession.SessionID()
	}
	if sessionID == 0 || desc.RemoteAddr == 0 || desc.RKey == 0 || desc.Length == 0 {
		_ = buffer.Release(r.Context())
		writeJsonError(w, r, http.StatusServiceUnavailable, fmt.Errorf("native RDMA write descriptor is not exportable"))
		return
	}
	if uint64(desc.Length) < req.Size {
		_ = buffer.Release(r.Context())
		writeJsonError(w, r, http.StatusServiceUnavailable, fmt.Errorf("native RDMA write descriptor length %d is smaller than payload size %d", desc.Length, req.Size))
		return
	}
	desc.Length = uint32(req.Size)
	desc.Reserved[0] = sessionID
	writeJsonQuiet(w, r, http.StatusOK, volumeRdmaWriteDescResponse{
		Desc:         desc,
		ConnectionID: req.ConnectionID,
		SessionID:    sessionID,
	})
	success = true
	bytesRegistered = req.Size
}

func (vs *VolumeServer) volumeRdmaWriteCommitHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	reader, ok := vs.rdmaWriteTargetEndpoint()
	if !ok {
		http.Error(w, "native RDMA write target endpoint is not configured", http.StatusNotImplemented)
		return
	}
	start := time.Now()
	success := false
	var bytesCommitted uint64
	vs.rdmaStats.writeCommitRequests.Add(1)
	defer func() {
		recordLatency(&vs.rdmaStats.writeCommitLatencyNs, start)
		if success {
			vs.rdmaStats.writeCommitSuccesses.Add(1)
			vs.rdmaStats.writeCommitBytes.Add(int64(bytesCommitted))
		} else {
			vs.rdmaStats.writeCommitFailures.Add(1)
		}
	}()

	var req VolumeRdmaWriteCommitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	if err := validateVolumeRdmaWriteCommitRequest(req); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	fileID, err := vs.commitVolumeRdmaWriteRequest(r.Context(), reader, req)
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}
	writeJsonQuiet(w, r, http.StatusOK, volumeRdmaWriteResponse{
		FileID: fileID,
		Size:   req.Size,
		Source: "native-volume-rdma-write-desc",
	})
	success = true
	bytesCommitted = req.Size
}

func (vs *VolumeServer) volumeRdmaWriteCommitBatchHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	reader, ok := vs.rdmaWriteTargetEndpoint()
	if !ok {
		http.Error(w, "native RDMA write target endpoint is not configured", http.StatusNotImplemented)
		return
	}
	start := time.Now()
	var entries int64
	var successEntries int64
	var failedEntries int64
	var bytesCommitted uint64
	vs.rdmaStats.writeCommitBatchRequests.Add(1)
	defer func() {
		recordLatency(&vs.rdmaStats.writeCommitBatchLatencyNs, start)
		if entries > 0 {
			vs.rdmaStats.writeCommitBatchEntries.Add(entries)
		}
		if successEntries > 0 {
			vs.rdmaStats.writeCommitBatchEntrySuccesses.Add(successEntries)
		}
		if failedEntries > 0 {
			vs.rdmaStats.writeCommitBatchEntryFailures.Add(failedEntries)
		}
		if bytesCommitted > 0 {
			vs.rdmaStats.writeCommitBatchBytes.Add(int64(bytesCommitted))
		}
	}()

	var req VolumeRdmaWriteCommitBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	if len(req.Entries) == 0 {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("entries are required"))
		return
	}
	entries = int64(len(req.Entries))
	results := make([]volumeRdmaWriteCommitResult, len(req.Entries))
	for i, entry := range req.Entries {
		results[i] = volumeRdmaWriteCommitResult{
			SessionID: entry.SessionID,
			FileID:    entry.FileID,
			Size:      entry.Size,
		}
		if err := validateVolumeRdmaWriteCommitRequest(entry); err != nil {
			results[i].Status = -int32(syscall.EINVAL)
			results[i].Error = err.Error()
			failedEntries++
			continue
		}
		fileID, err := vs.commitVolumeRdmaWriteRequest(r.Context(), reader, entry)
		if err != nil {
			results[i].Status = volumeRdmaWriteCommitStatus(err)
			results[i].Error = err.Error()
			failedEntries++
			continue
		}
		results[i].FileID = fileID
		results[i].Source = "native-volume-rdma-write-desc"
		successEntries++
		bytesCommitted += entry.Size
	}
	writeJsonQuiet(w, r, http.StatusOK, volumeRdmaWriteCommitBatchResponse{Results: results})
}

func (vs *VolumeServer) volumeRdmaWriteAbortHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	target, ok := vs.rdmaWriteTargetEndpoint()
	if !ok {
		http.Error(w, "native RDMA write target endpoint is not configured", http.StatusNotImplemented)
		return
	}
	start := time.Now()
	success := false
	vs.rdmaStats.writeAbortRequests.Add(1)
	defer func() {
		recordLatency(&vs.rdmaStats.writeAbortLatencyNs, start)
		if success {
			vs.rdmaStats.writeAbortSuccesses.Add(1)
		} else {
			vs.rdmaStats.writeAbortFailures.Add(1)
		}
	}()
	var req VolumeRdmaWriteAbortRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	if req.SessionID == 0 {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("session_id is required"))
		return
	}
	if err := target.ReleaseSession(r.Context(), req.SessionID); err != nil {
		writeJsonError(w, r, http.StatusServiceUnavailable, err)
		return
	}
	success = true
	writeJsonQuiet(w, r, http.StatusOK, map[string]bool{"aborted": true})
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

func validateVolumeRdmaWriteDescRequest(req VolumeRdmaWriteDescRequest) error {
	if req.VolumeID == 0 || req.NeedleID == 0 || req.Size == 0 {
		return fmt.Errorf("volume_id, needle_id, and size are required")
	}
	if req.ConnectionID == 0 {
		return fmt.Errorf("connection_id is required")
	}
	if req.Size > volumeRdmaEngineMaxFrameSize {
		return fmt.Errorf("native RDMA write too large: %d > %d", req.Size, volumeRdmaEngineMaxFrameSize)
	}
	return nil
}

func validateVolumeRdmaWriteCommitRequest(req VolumeRdmaWriteCommitRequest) error {
	if req.SessionID == 0 {
		return fmt.Errorf("session_id is required")
	}
	if req.VolumeID == 0 || req.NeedleID == 0 || req.Size == 0 {
		return fmt.Errorf("volume_id, needle_id, and size are required")
	}
	if req.Size > volumeRdmaEngineMaxFrameSize {
		return fmt.Errorf("native RDMA write too large: %d > %d", req.Size, volumeRdmaEngineMaxFrameSize)
	}
	return nil
}

func volumeRdmaWriteCommitStatus(err error) int32 {
	if err == nil {
		return 0
	}
	return -int32(syscall.EIO)
}

type volumeRdmaWriteTargetEndpoint interface {
	RegisterWriteBufferFor(context.Context, uint64, uint64) (VolumeRdmaRegisteredBuffer, error)
	ReadRegisteredBuffer(context.Context, uint64, uint64) ([]byte, error)
	ReleaseSession(context.Context, uint64) error
}

type volumeRdmaWriteStreamTargetEndpoint interface {
	ReadRegisteredBufferTo(context.Context, uint64, uint64, io.Writer) error
}

type volumeRdmaRemoteReadStreamer interface {
	ReadRemoteToFor(context.Context, uint64, VolumeRdmaDataDesc, time.Duration, io.Writer) error
}

func (vs *VolumeServer) rdmaWriteTargetEndpoint() (volumeRdmaWriteTargetEndpoint, bool) {
	if vs == nil || vs.rdmaEndpoint == nil {
		return nil, false
	}
	target, ok := vs.rdmaEndpoint.(volumeRdmaWriteTargetEndpoint)
	return target, ok
}

func (vs *VolumeServer) commitVolumeRdmaWriteRequest(ctx context.Context, reader volumeRdmaWriteTargetEndpoint, req VolumeRdmaWriteCommitRequest) (string, error) {
	if streamer, ok := reader.(volumeRdmaWriteStreamTargetEndpoint); ok {
		defer func() {
			_ = reader.ReleaseSession(context.Background(), req.SessionID)
		}()
		return vs.writeNeedleDataFromNativeRdmaStream(ctx, VolumeRdmaWriteRequest{
			FileID:   req.FileID,
			VolumeID: req.VolumeID,
			NeedleID: req.NeedleID,
			Cookie:   req.Cookie,
			Size:     req.Size,
		}, func(w io.Writer) error {
			return streamer.ReadRegisteredBufferTo(ctx, req.SessionID, req.Size, w)
		})
	}

	data, err := reader.ReadRegisteredBuffer(ctx, req.SessionID, req.Size)
	if err != nil {
		_ = reader.ReleaseSession(ctx, req.SessionID)
		return "", err
	}
	defer func() {
		_ = reader.ReleaseSession(context.Background(), req.SessionID)
	}()
	return vs.writeNeedleDataFromNativeRdma(ctx, VolumeRdmaWriteRequest{
		FileID:   req.FileID,
		VolumeID: req.VolumeID,
		NeedleID: req.NeedleID,
		Cookie:   req.Cookie,
		Size:     req.Size,
	}, data)
}

func (vs *VolumeServer) writeNeedleDataFromNativeRdmaStream(ctx context.Context, req VolumeRdmaWriteRequest, writeData func(io.Writer) error) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if vs == nil || vs.store == nil {
		return "", fmt.Errorf("volume store is not configured")
	}
	if writeData == nil {
		return "", fmt.Errorf("native RDMA write stream is not configured")
	}
	if err := vs.CheckMaintenanceMode(); err != nil {
		return "", err
	}
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeID))
	if v == nil {
		return "", fmt.Errorf("not found volume id %d", req.VolumeID)
	}

	if err := v.WriteNeedleDataStream(types.NeedleId(req.NeedleID), types.Cookie(req.Cookie), req.Size, func(w io.Writer) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return writeData(w)
	}); err != nil {
		return "", fmt.Errorf("write streamed needle %d size %d: %w", req.NeedleID, req.Size, err)
	}
	if req.FileID != "" {
		return req.FileID, nil
	}
	return needle.NewFileId(needle.VolumeId(req.VolumeID), req.NeedleID, req.Cookie).String(), nil
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
