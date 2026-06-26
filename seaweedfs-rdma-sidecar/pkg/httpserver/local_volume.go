package httpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"seaweedfs-rdma-sidecar/pkg/seaweedfs"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/sirupsen/logrus"
)

// LocalVolumeHandler serves SeaweedFS file-id shaped requests from a colocated
// volume sidecar. Reads bypass the volume server HTTP path and load from the
// shared volume directory; writes use the volume server gRPC blob API.
type LocalVolumeHandler struct {
	Client          *seaweedfs.SeaweedFSRDMAClient
	Logger          *logrus.Logger
	Timeout         time.Duration
	VolumeServerURL string
}

func (h *LocalVolumeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fileID := strings.TrimPrefix(r.URL.Path, "/local-volume/")
	if fileID == "" || strings.Contains(fileID, "/") {
		http.Error(w, "file id is required", http.StatusBadRequest)
		return
	}
	fid, err := needle.ParseFileIdFromString(fileID)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid file id: %v", err), http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.handleRead(w, r, fid)
	case http.MethodPost:
		h.handleWrite(w, r, fid)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *LocalVolumeHandler) handleRead(w http.ResponseWriter, r *http.Request, fid *needle.FileId) {
	offset, size, ranged, err := localReadRange(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if size == 0 {
		http.Error(w, "read size is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), operationTimeout(h.Timeout))
	defer cancel()
	data, err := h.Client.ReadLocalNeedle(ctx, &seaweedfs.NeedleReadRequest{
		VolumeID: uint32(fid.VolumeId),
		NeedleID: uint64(fid.Key),
		Cookie:   uint32(fid.Cookie),
		Offset:   offset,
		Size:     size,
	})
	if err != nil {
		h.Logger.WithError(err).Warn("local volume read failed")
		http.Error(w, fmt.Sprintf("local read failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set("X-Source", "local-volume")
	if ranged {
		rangeEnd := offset
		if len(data) > 0 {
			rangeEnd = offset + uint64(len(data)) - 1
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/*", offset, rangeEnd))
		w.WriteHeader(http.StatusPartialContent)
	}
	if _, err := w.Write(data); err != nil {
		h.Logger.WithError(err).Warn("failed to write local volume response")
	}
}

func (h *LocalVolumeHandler) handleWrite(w http.ResponseWriter, r *http.Request, fid *needle.FileId) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("read body: %v", err), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	if len(data) == 0 {
		http.Error(w, "empty request body", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), operationTimeout(h.Timeout))
	defer cancel()
	fileID, err := h.Client.WriteNeedleBlobGRPC(ctx, &seaweedfs.NeedleWriteRequest{
		VolumeID:     uint32(fid.VolumeId),
		NeedleID:     uint64(fid.Key),
		Cookie:       uint32(fid.Cookie),
		Data:         data,
		VolumeServer: h.VolumeServerURL,
	})
	if err != nil {
		h.Logger.WithError(err).Warn("local volume gRPC write failed")
		http.Error(w, fmt.Sprintf("local write failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Source", "local-volume-grpc")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"file_id": fileID,
		"size":    len(data),
	})
}

func localReadRange(r *http.Request) (offset, size uint64, ranged bool, err error) {
	query := r.URL.Query()
	if offsetText := query.Get("offset"); offsetText != "" {
		offset, err = strconv.ParseUint(offsetText, 10, 64)
		if err != nil {
			return 0, 0, false, fmt.Errorf("invalid offset")
		}
	}
	if sizeText := query.Get("size"); sizeText != "" {
		size, err = strconv.ParseUint(sizeText, 10, 64)
		if err != nil {
			return 0, 0, false, fmt.Errorf("invalid size")
		}
	}

	rangeHeader := strings.TrimSpace(r.Header.Get("Range"))
	if rangeHeader == "" {
		return offset, size, false, nil
	}
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return 0, 0, false, fmt.Errorf("unsupported range")
	}
	parts := strings.SplitN(strings.TrimPrefix(rangeHeader, "bytes="), "-", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return 0, 0, false, fmt.Errorf("unsupported range")
	}
	start, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, false, fmt.Errorf("invalid range start")
	}
	end, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil || end < start {
		return 0, 0, false, fmt.Errorf("invalid range end")
	}
	return start, end - start + 1, true, nil
}
