// Package httpserver provides HTTP handlers shared by sidecar binaries.
package httpserver

import (
	"context"
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

// ReadHandler serves mount-compatible GET /read requests with binary payloads.
type ReadHandler struct {
	Client *seaweedfs.SeaweedFSRDMAClient
	Logger *logrus.Logger
}

func (h *ReadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	req, err := parseNeedleReadRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	start := time.Now()
	resp, err := h.Client.ReadNeedle(ctx, req)
	if err != nil {
		h.Logger.WithError(err).Error("needle read failed")
		http.Error(w, fmt.Sprintf("Read failed: %v", err), http.StatusInternalServerError)
		return
	}

	duration := time.Since(start)
	h.Logger.WithFields(logrus.Fields{
		"volume_id": req.VolumeID,
		"needle_id": req.NeedleID,
		"source":    resp.Source,
		"is_rdma":   resp.IsRDMA,
		"duration":  duration,
		"data_size": len(resp.Data),
	}).Info("needle read completed")

	if resp.UseTempFile && resp.TempFilePath != "" {
		w.Header().Set("X-Use-Temp-File", "true")
		w.Header().Set("X-Temp-File", resp.TempFilePath)
		w.Header().Set("X-Source", resp.Source)
		w.Header().Set("X-RDMA-Used", fmt.Sprintf("%t", resp.IsRDMA))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		return
	}

	w.Header().Set("X-Source", resp.Source)
	w.Header().Set("X-RDMA-Used", fmt.Sprintf("%t", resp.IsRDMA))
	if resp.SessionID != "" {
		w.Header().Set("X-RDMA-Session-ID", resp.SessionID)
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	if _, err := w.Write(resp.Data); err != nil {
		h.Logger.WithError(err).Warn("failed to write read response")
	}
}

func parseNeedleReadRequest(r *http.Request) (*seaweedfs.NeedleReadRequest, error) {
	query := r.URL.Query()
	volumeServer := query.Get("volume_server")
	fileID := query.Get("file_id")

	var volumeID, needleID, cookie uint64
	var err error

	if fileID != "" {
		fid, parseErr := needle.ParseFileIdFromString(fileID)
		if parseErr != nil {
			return nil, fmt.Errorf("invalid file_id: %w", parseErr)
		}
		volumeID = uint64(fid.VolumeId)
		needleID = uint64(fid.Key)
		cookie = uint64(fid.Cookie)
	} else {
		volumeID, err = strconv.ParseUint(query.Get("volume"), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid volume parameter")
		}
		needleID, err = strconv.ParseUint(query.Get("needle"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid needle parameter")
		}
		cookieStr := query.Get("cookie")
		if strings.HasPrefix(strings.ToLower(cookieStr), "0x") {
			cookie, err = strconv.ParseUint(cookieStr[2:], 16, 32)
		} else if cookieStr != "" {
			cookie, err = strconv.ParseUint(cookieStr, 10, 32)
		}
		if err != nil {
			return nil, fmt.Errorf("invalid cookie parameter")
		}
	}

	var offset, size uint64
	if offsetStr := query.Get("offset"); offsetStr != "" {
		offset, err = strconv.ParseUint(offsetStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid offset parameter")
		}
	}
	if sizeStr := query.Get("size"); sizeStr != "" {
		size, err = strconv.ParseUint(sizeStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid size parameter")
		}
	}

	if volumeServer == "" {
		return nil, fmt.Errorf("volume_server parameter is required")
	}
	if volumeID == 0 || needleID == 0 {
		return nil, fmt.Errorf("volume and needle are required")
	}
	if size == 0 {
		size = 4096
	}

	return &seaweedfs.NeedleReadRequest{
		VolumeID:     uint32(volumeID),
		NeedleID:     needleID,
		Cookie:       uint32(cookie),
		Offset:       offset,
		Size:         size,
		VolumeServer: volumeServer,
	}, nil
}

// DrainBody closes optional request bodies for linter completeness.
func DrainBody(r *http.Request) {
	if r.Body != nil {
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
	}
}
