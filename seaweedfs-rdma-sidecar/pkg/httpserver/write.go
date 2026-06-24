package httpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"seaweedfs-rdma-sidecar/pkg/seaweedfs"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/sirupsen/logrus"
)

// WriteHandler serves mount-compatible POST /write requests.
type WriteHandler struct {
	Client *seaweedfs.SeaweedFSRDMAClient
	Logger *logrus.Logger
}

func (h *WriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query()
	fileID := query.Get("file_id")
	volumeServer := query.Get("volume_server")

	if fileID == "" {
		http.Error(w, "file_id parameter is required", http.StatusBadRequest)
		return
	}
	if volumeServer == "" {
		http.Error(w, "volume_server parameter is required", http.StatusBadRequest)
		return
	}

	fid, err := needle.ParseFileIdFromString(fileID)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid file_id: %v", err), http.StatusBadRequest)
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read body: %v", err), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if len(data) == 0 {
		http.Error(w, "empty request body", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	start := time.Now()
	resp, err := h.Client.WriteNeedle(ctx, &seaweedfs.NeedleWriteRequest{
		VolumeID:     uint32(fid.VolumeId),
		NeedleID:     uint64(fid.Key),
		Cookie:       uint32(fid.Cookie),
		Data:         data,
		VolumeServer: volumeServer,
	})
	if err != nil {
		h.Logger.WithError(err).Error("needle write failed")
		http.Error(w, fmt.Sprintf("Write failed: %v", err), http.StatusInternalServerError)
		return
	}

	duration := time.Since(start)
	h.Logger.WithFields(logrus.Fields{
		"file_id":      fileID,
		"source":       resp.Source,
		"is_rdma":      resp.IsRDMA,
		"session_rdma": resp.SessionRDMA,
		"real_rdma":    resp.RealRDMA,
		"data_source":  resp.DataSource,
		"duration":     duration,
		"size":         resp.Size,
	}).Info("needle write completed")

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":      resp.Success,
		"is_rdma":      resp.IsRDMA,
		"session_rdma": resp.SessionRDMA,
		"real_rdma":    resp.RealRDMA,
		"source":       resp.Source,
		"data_source":  resp.DataSource,
		"file_id":      resp.FileID,
		"size":         resp.Size,
	})
}
