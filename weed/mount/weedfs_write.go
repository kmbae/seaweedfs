package mount

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (wfs *WFS) saveDataAsChunk(fullPath util.FullPath) filer.SaveDataAsChunkFunctionType {

	// Backstop: FUSE entry points sanitize names before they reach
	// inodeToPath, but async flush paths (e.g. writebackCache, handles whose
	// RememberPath was set from an older code path) may still carry bytes
	// that predate sanitization. Proto3 string fields require valid UTF-8,
	// so scrub the full path once here before every AssignVolume call.
	assignPath := fullPath.Sanitized()

	return func(reader io.Reader, filename string, offset int64, tsNs int64, _ uint64) (chunk *filer_pb.FileChunk, err error) {

		// Try RDMA write path first if enabled and not read-only
		if wfs.rdmaClient != nil && wfs.option.RdmaEnabled && !wfs.option.RdmaReadOnly {
			chunk, err = wfs.rdmaUploadChunk(reader, assignPath, filename, offset, tsNs)
			if err == nil {
				return chunk, nil
			}
			if !wfs.option.RdmaFallback {
				return nil, fmt.Errorf("RDMA write failed (no fallback): %w", err)
			}
			glog.V(2).Infof("RDMA write failed for %s, falling back to HTTP: %v", filename, err)
		}

		// Normal HTTP upload path
		uploader, err := operation.NewUploader()
		if err != nil {
			return
		}

		uploadOption := &operation.UploadOption{
			Filename:          filename,
			Cipher:            wfs.option.Cipher,
			IsInputCompressed: false,
			MimeType:          "",
			PairMap:           nil,
		}
		genFileUrlFn := func(host, fileId string) string {
			fileUrl := fmt.Sprintf("http://%s/%s", host, fileId)
			if wfs.option.VolumeServerAccess == "filerProxy" {
				fileUrl = fmt.Sprintf("http://%s/?proxyChunkId=%s", wfs.getCurrentFiler(), fileId)
			}
			return fileUrl
		}

		fileId, uploadResult, err, data := uploader.UploadWithRetry(
			wfs,
			&filer_pb.AssignVolumeRequest{
				Count:       1,
				Replication: wfs.option.Replication,
				Collection:  wfs.option.Collection,
				TtlSec:      wfs.option.TtlSec,
				DiskType:    string(wfs.option.DiskType),
				DataCenter:  wfs.option.DataCenter,
				Path:        assignPath,
			},
			uploadOption, genFileUrlFn, reader,
		)

		if err != nil {
			glog.V(0).Infof("upload data %v: %v", filename, err)
			return nil, fmt.Errorf("upload data: %w", err)
		}
		if uploadResult.Error != "" {
			glog.V(0).Infof("upload failure %v: %v", filename, err)
			return nil, fmt.Errorf("upload result: %v", uploadResult.Error)
		}

		shouldCache := wfs.chunkCache != nil && (offset == 0 || wfs.peerAnnouncer != nil)
		if shouldCache {
			wfs.chunkCache.SetChunk(fileId, data)
		}
		if wfs.peerAnnouncer != nil && shouldCache {
			wfs.peerAnnouncer.EnqueueAnnounce(fileId)
		}

		chunk = uploadResult.ToPbFileChunk(fileId, offset, tsNs)
		return chunk, nil
	}
}

// rdmaUploadChunk assigns a volume via filer gRPC and uploads data through the RDMA sidecar.
func (wfs *WFS) rdmaUploadChunk(reader io.Reader, assignPath string, filename string, offset int64, tsNs int64) (*filer_pb.FileChunk, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read input for RDMA: %w", err)
	}

	var fileId string
	var volumeServerUrl string

	if grpcErr := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, assignErr := client.AssignVolume(context.Background(), &filer_pb.AssignVolumeRequest{
			Count:            1,
			Replication:      wfs.option.Replication,
			Collection:       wfs.option.Collection,
			TtlSec:           wfs.option.TtlSec,
			DiskType:         string(wfs.option.DiskType),
			DataCenter:       wfs.option.DataCenter,
			Path:             assignPath,
			ExpectedDataSize: uint64(len(data)),
		})
		if assignErr != nil {
			return assignErr
		}
		if resp.Error != "" {
			return fmt.Errorf("assign volume: %s", resp.Error)
		}
		fileId = resp.FileId
		volumeServerUrl = fmt.Sprintf("http://%s", wfs.AdjustedUrl(resp.Location))
		return nil
	}); grpcErr != nil {
		return nil, fmt.Errorf("RDMA assign volume: %w", grpcErr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), wfs.rdmaClient.timeout)
	defer cancel()

	writeResp, err := wfs.rdmaClient.WriteNeedle(ctx, fileId, data, volumeServerUrl)
	if err != nil {
		return nil, fmt.Errorf("RDMA write needle: %w", err)
	}

	if !writeResp.Success {
		return nil, fmt.Errorf("RDMA write not successful for %s", fileId)
	}

	glog.V(3).Infof("RDMA upload %s to %s: %d bytes, rdma=%v", fileId, volumeServerUrl, len(data), writeResp.IsRDMA)

	shouldCache := wfs.chunkCache != nil && (offset == 0 || wfs.peerAnnouncer != nil)
	if shouldCache {
		wfs.chunkCache.SetChunk(fileId, data)
	}
	if wfs.peerAnnouncer != nil && shouldCache {
		wfs.peerAnnouncer.EnqueueAnnounce(fileId)
	}

	chunk := &filer_pb.FileChunk{
		FileId:       fileId,
		Offset:       offset,
		Size:         uint64(len(data)),
		ModifiedTsNs: tsNs,
		Fid:          &filer_pb.FileId{},
	}

	return chunk, nil
}

// rdmaUploadChunkFromBuffer is a convenience wrapper for callers that already have data in a buffer.
func (wfs *WFS) rdmaUploadChunkFromBuffer(data []byte, assignPath string, filename string, offset int64, tsNs int64) (*filer_pb.FileChunk, error) {
	return wfs.rdmaUploadChunk(bytes.NewReader(data), assignPath, filename, offset, tsNs)
}

