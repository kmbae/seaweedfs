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
	"github.com/seaweedfs/seaweedfs/weed/storage"
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

const (
	volumeRdmaDescLeaseIDIndex      = 0
	volumeRdmaDescConnectionIDIndex = 1
	volumeRdmaDescFileOffsetIndex   = 2
)

func (d VolumeRdmaDataDesc) LeaseID() uint64 {
	return d.Reserved[volumeRdmaDescLeaseIDIndex]
}

func (d *VolumeRdmaDataDesc) SetLeaseID(leaseID uint64) {
	if d != nil {
		d.Reserved[volumeRdmaDescLeaseIDIndex] = leaseID
	}
}

func (d VolumeRdmaDataDesc) ConnectionID() uint64 {
	return d.Reserved[volumeRdmaDescConnectionIDIndex]
}

func (d *VolumeRdmaDataDesc) SetConnectionID(connectionID uint64) {
	if d != nil {
		d.Reserved[volumeRdmaDescConnectionIDIndex] = connectionID
	}
}

func (d VolumeRdmaDataDesc) FileOffset() uint64 {
	return d.Reserved[volumeRdmaDescFileOffsetIndex]
}

func (d *VolumeRdmaDataDesc) SetFileOffset(offset uint64) {
	if d != nil {
		d.Reserved[volumeRdmaDescFileOffsetIndex] = offset
	}
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

type VolumeRdmaReadDescBatchRequest struct {
	Entries []VolumeRdmaReadRequest `json:"entries"`
}

type volumeRdmaReadDescBatchResult struct {
	Index        int                `json:"index"`
	FileID       string             `json:"file_id,omitempty"`
	Size         uint64             `json:"size,omitempty"`
	Desc         VolumeRdmaDataDesc `json:"desc,omitempty"`
	ConnectionID uint64             `json:"connection_id,omitempty"`
	SessionID    uint64             `json:"session_id,omitempty"`
	Status       int32              `json:"status"`
	Error        string             `json:"error,omitempty"`
}

type volumeRdmaReadDescBatchResponse struct {
	Results []volumeRdmaReadDescBatchResult `json:"results"`
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

type volumeRdmaReleaseDescBatchRequest struct {
	SessionIDs []uint64 `json:"session_ids"`
}

type volumeRdmaReleaseDescBatchResult struct {
	SessionID uint64 `json:"session_id"`
	Released  bool   `json:"released"`
	Status    int32  `json:"status"`
	Error     string `json:"error,omitempty"`
}

type volumeRdmaReleaseDescBatchResponse struct {
	Results []volumeRdmaReleaseDescBatchResult `json:"results"`
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

func (vs *VolumeServer) volumeRdmaReadDescBatchHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if vs == nil || vs.rdmaReadExporter == nil {
		http.Error(w, "native RDMA read exporter is not configured", http.StatusNotImplemented)
		return
	}

	var req VolumeRdmaReadDescBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	if len(req.Entries) == 0 {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("entries are required"))
		return
	}

	resp := volumeRdmaReadDescBatchResponse{
		Results: make([]volumeRdmaReadDescBatchResult, 0, len(req.Entries)),
	}
	for i, entry := range req.Entries {
		start := time.Now()
		success := false
		bytesExported := uint64(0)
		vs.rdmaStats.readDescRequests.Add(1)

		result := volumeRdmaReadDescBatchResult{
			Index:  i,
			FileID: entry.FileID,
			Size:   entry.Size,
			Status: http.StatusOK,
		}
		if entry.VolumeID == 0 || entry.NeedleID == 0 || entry.Size == 0 {
			result.Status = http.StatusBadRequest
			result.Error = "volume_id, needle_id, and size are required"
		} else {
			lease, err := vs.rdmaReadExporter.PrepareRead(r.Context(), entry)
			if err != nil {
				result.Status = int32(volumeRdmaReadHTTPStatus(err))
				result.Error = err.Error()
			} else if lease == nil || lease.Desc.RemoteAddr == 0 || lease.Desc.Length == 0 {
				result.Status = http.StatusNotImplemented
				result.Error = "native RDMA read exporter returned no exportable descriptor"
			} else {
				success = true
				bytesExported = entry.Size
				result.Desc = lease.Desc
				result.ConnectionID = lease.ConnectionID
				result.SessionID = lease.SessionID
			}
		}

		recordLatency(&vs.rdmaStats.readDescLatencyNs, start)
		if success {
			vs.rdmaStats.readDescSuccesses.Add(1)
			vs.rdmaStats.readDescBytes.Add(int64(bytesExported))
		} else {
			vs.rdmaStats.readDescFailures.Add(1)
		}
		resp.Results = append(resp.Results, result)
	}

	writeJsonQuiet(w, r, http.StatusOK, resp)
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

func (vs *VolumeServer) volumeRdmaReleaseDescBatchHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if vs == nil || vs.rdmaReadExporter == nil {
		http.Error(w, "native RDMA read exporter is not configured", http.StatusNotImplemented)
		return
	}

	var req volumeRdmaReleaseDescBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	if len(req.SessionIDs) == 0 {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("session_ids are required"))
		return
	}

	resp := volumeRdmaReleaseDescBatchResponse{
		Results: make([]volumeRdmaReleaseDescBatchResult, 0, len(req.SessionIDs)),
	}
	for _, sessionID := range req.SessionIDs {
		start := time.Now()
		success := false
		vs.rdmaStats.releaseDescRequests.Add(1)

		result := volumeRdmaReleaseDescBatchResult{
			SessionID: sessionID,
			Status:    http.StatusOK,
		}
		if sessionID == 0 {
			result.Status = http.StatusBadRequest
			result.Error = "session_id is required"
		} else if err := vs.rdmaReadExporter.ReleaseRead(r.Context(), sessionID); err != nil {
			result.Status = int32(volumeRdmaReadHTTPStatus(err))
			result.Error = err.Error()
		} else {
			success = true
			result.Released = true
		}

		recordLatency(&vs.rdmaStats.releaseDescLatencyNs, start)
		if success {
			vs.rdmaStats.releaseDescSuccesses.Add(1)
		} else {
			vs.rdmaStats.releaseDescFailures.Add(1)
		}
		resp.Results = append(resp.Results, result)
	}

	writeJsonQuiet(w, r, http.StatusOK, resp)
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
	desc.SetLeaseID(sessionID)
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

	decodeStart := time.Now()
	var req VolumeRdmaWriteCommitBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	recordLatency(&vs.rdmaStats.writeCommitBatchDecodeLatencyNs, decodeStart)
	if len(req.Entries) == 0 {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("entries are required"))
		return
	}
	entries = int64(len(req.Entries))
	results := make([]volumeRdmaWriteCommitResult, len(req.Entries))
	validEntries := make([]volumeRdmaWriteCommitBatchEntry, 0, len(req.Entries))
	validateStart := time.Now()
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
		validEntries = append(validEntries, volumeRdmaWriteCommitBatchEntry{Index: i, Request: entry})
	}
	recordLatency(&vs.rdmaStats.writeCommitBatchValidateLatencyNs, validateStart)

	if len(validEntries) > 0 {
		storageStart := time.Now()
		vs.rdmaStats.writeCommitBatchStorageRequests.Add(1)
		if _, ok := reader.(volumeRdmaWriteStreamTargetEndpoint); ok {
			vs.commitVolumeRdmaWriteRequestBatch(r.Context(), reader, validEntries, results)
		} else {
			vs.rdmaStats.writeCommitBatchStorageFallbacks.Add(int64(len(validEntries)))
			for _, valid := range validEntries {
				fileID, err := vs.commitVolumeRdmaWriteRequest(r.Context(), reader, valid.Request)
				if err != nil {
					results[valid.Index].Status = volumeRdmaWriteCommitStatus(err)
					results[valid.Index].Error = err.Error()
					continue
				}
				results[valid.Index].FileID = fileID
				results[valid.Index].Source = "native-volume-rdma-write-desc"
			}
		}
		recordLatency(&vs.rdmaStats.writeCommitBatchStorageLatencyNs, storageStart)
	}

	successEntries = 0
	failedEntries = 0
	bytesCommitted = 0
	for _, result := range results {
		if result.Status == 0 {
			successEntries++
			bytesCommitted += result.Size
		} else {
			failedEntries++
		}
	}
	writeJsonQuiet(w, r, http.StatusOK, volumeRdmaWriteCommitBatchResponse{Results: results})
}

type volumeRdmaWriteCommitBatchEntry struct {
	Index   int
	Request VolumeRdmaWriteCommitRequest
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

func (vs *VolumeServer) commitVolumeRdmaWriteRequestBatch(ctx context.Context, reader volumeRdmaWriteTargetEndpoint, entries []volumeRdmaWriteCommitBatchEntry, results []volumeRdmaWriteCommitResult) {
	if err := ctx.Err(); err != nil {
		setVolumeRdmaWriteBatchError(results, entries, err)
		return
	}
	if vs == nil || vs.store == nil {
		setVolumeRdmaWriteBatchError(results, entries, fmt.Errorf("volume store is not configured"))
		return
	}
	streamer, ok := reader.(volumeRdmaWriteStreamTargetEndpoint)
	if !ok {
		setVolumeRdmaWriteBatchError(results, entries, fmt.Errorf("native RDMA write stream is not configured"))
		return
	}
	if err := vs.CheckMaintenanceMode(); err != nil {
		setVolumeRdmaWriteBatchError(results, entries, err)
		return
	}

	groups := make(map[uint32][]volumeRdmaWriteCommitBatchEntry)
	for _, entry := range entries {
		v := vs.store.GetVolume(needle.VolumeId(entry.Request.VolumeID))
		if v == nil {
			results[entry.Index].Status = volumeRdmaWriteCommitStatus(fmt.Errorf("not found volume id %d", entry.Request.VolumeID))
			results[entry.Index].Error = fmt.Sprintf("not found volume id %d", entry.Request.VolumeID)
			_ = reader.ReleaseSession(context.Background(), entry.Request.SessionID)
			continue
		}
		groups[entry.Request.VolumeID] = append(groups[entry.Request.VolumeID], entry)
	}

	for volumeID, group := range groups {
		v := vs.store.GetVolume(needle.VolumeId(volumeID))
		if v == nil {
			setVolumeRdmaWriteBatchError(results, group, fmt.Errorf("not found volume id %d", volumeID))
			continue
		}
		storageEntries := make([]storage.NeedleDataStreamBatchEntry, len(group))
		for i, item := range group {
			req := item.Request
			storageEntries[i] = storage.NeedleDataStreamBatchEntry{
				NeedleID: types.NeedleId(req.NeedleID),
				Cookie:   types.Cookie(req.Cookie),
				DataSize: req.Size,
				WriteData: func(w io.Writer) error {
					if err := ctx.Err(); err != nil {
						return err
					}
					return streamer.ReadRegisteredBufferTo(ctx, req.SessionID, req.Size, w)
				},
			}
		}
		storageResults := v.WriteNeedleDataStreamBatch(storageEntries)
		for i, storageResult := range storageResults {
			item := group[i]
			req := item.Request
			_ = reader.ReleaseSession(context.Background(), req.SessionID)
			if storageResult.Err != nil {
				results[item.Index].Status = volumeRdmaWriteCommitStatus(storageResult.Err)
				results[item.Index].Error = storageResult.Err.Error()
				continue
			}
			if req.FileID != "" {
				results[item.Index].FileID = req.FileID
			} else {
				results[item.Index].FileID = needle.NewFileId(needle.VolumeId(req.VolumeID), req.NeedleID, req.Cookie).String()
			}
			results[item.Index].Source = "native-volume-rdma-write-desc"
		}
	}
}

func setVolumeRdmaWriteBatchError(results []volumeRdmaWriteCommitResult, entries []volumeRdmaWriteCommitBatchEntry, err error) {
	for _, entry := range entries {
		results[entry.Index].Status = volumeRdmaWriteCommitStatus(err)
		results[entry.Index].Error = err.Error()
	}
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
