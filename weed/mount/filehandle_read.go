package mount

import (
	"context"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func (fh *FileHandle) lockForRead(startOffset int64, size int) {
	fh.dirtyPages.LockForRead(startOffset, startOffset+int64(size))
}
func (fh *FileHandle) unlockForRead(startOffset int64, size int) {
	fh.dirtyPages.UnlockForRead(startOffset, startOffset+int64(size))
}

func (fh *FileHandle) readFromDirtyPages(buff []byte, startOffset int64, tsNs int64) (maxStop int64) {
	maxStop = fh.dirtyPages.ReadDirtyDataAt(buff, startOffset, tsNs)
	return
}

func (fh *FileHandle) readFromChunks(buff []byte, offset int64) (int64, int64, error) {
	return fh.readFromChunksWithContext(context.Background(), buff, offset)
}

func (fh *FileHandle) readFromChunksWithContext(ctx context.Context, buff []byte, offset int64) (int64, int64, error) {
	fh.entryLock.RLock()
	defer fh.entryLock.RUnlock()

	fileFullPath := fh.FullPath()
	rdmaFallbackPending := false

	entry := fh.GetEntry()

	if entry.IsInRemoteOnly() {
		glog.V(4).Infof("download remote entry %s", fileFullPath)
		err := fh.downloadRemoteEntry(entry)
		if err != nil {
			glog.V(1).Infof("download remote entry %s: %v", fileFullPath, err)
			return 0, 0, err
		}
	}

	fileSize := int64(entry.Attributes.FileSize)
	if fileSize == 0 {
		fileSize = int64(filer.FileSize(entry.GetEntry()))
	}

	if fileSize == 0 {
		glog.V(1).Infof("empty fh %v", fileFullPath)
		return 0, 0, io.EOF
	} else if offset == fileSize {
		return 0, 0, io.EOF
	} else if offset >= fileSize {
		glog.V(1).Infof("invalid read, fileSize %d, offset %d for %s", fileSize, offset, fileFullPath)
		return 0, 0, io.EOF
	}

	if offset < int64(len(entry.Content)) {
		totalRead := copy(buff, entry.Content[offset:])
		glog.V(4).Infof("file handle read cached %s [%d,%d] %d", fileFullPath, offset, offset+int64(totalRead), totalRead)
		return int64(totalRead), 0, nil
	}

	// Try RDMA acceleration first if available
	if fh.wfs.rdmaClient != nil && fh.wfs.option.RdmaEnabled {
		totalRead, ts, err := fh.tryRDMARead(ctx, fileSize, buff, offset, entry)
		if err == nil {
			glog.V(4).Infof("RDMA read successful for %s [%d,%d] %d", fileFullPath, offset, offset+int64(totalRead), totalRead)
			return int64(totalRead), ts, nil
		}
		if !fh.wfs.option.RdmaFallback {
			return 0, 0, fmt.Errorf("RDMA read failed (no fallback): %w", err)
		}
		rdmaFallbackPending = true
		glog.V(4).Infof("RDMA read failed for %s, falling back to HTTP: %v", fileFullPath, err)
	}

	// Peer chunk sharing: try a peer mount's cache before the volume tier.
	// Any failure falls through transparently. See design-weed-mount-
	// peer-chunk-sharing.md §4.3.
	if fh.wfs.option.PeerEnabled && fh.wfs.peerGrpcServer != nil {
		totalRead, ts, err := fh.tryPeerRead(ctx, fileSize, buff, offset, entry)
		if err == nil {
			glog.V(4).Infof("peer read successful for %s [%d,%d] %d", fileFullPath, offset, offset+int64(totalRead), totalRead)
			return int64(totalRead), ts, nil
		}
		// Skip the "failed" log for benign skip reasons (local cache
		// hit, no peer owner yet, etc.) — the cache/volume fallback is
		// the expected outcome, not a failure.
		if err != errPeerReadSkipped {
			glog.V(4).Infof("peer read failed for %s, falling back to volume: %v", fileFullPath, err)
		}
	}

	// Fall back to normal chunk reading
	totalRead, ts, err := fh.entryChunkGroup.ReadDataAt(ctx, fileSize, buff, offset)
	if rdmaFallbackPending && fh.wfs.rdmaClient != nil {
		fh.wfs.rdmaClient.RecordFallbackRead(int64(totalRead), err)
	}

	if err != nil && err != io.EOF {
		glog.Errorf("file handle read %s: %v", fileFullPath, err)
	}

	// glog.V(4).Infof("file handle read %s [%d,%d] %d : %v", fileFullPath, offset, offset+int64(totalRead), totalRead, err)

	return int64(totalRead), ts, err
}

// tryRDMARead attempts to read file data using RDMA acceleration
func (fh *FileHandle) tryRDMARead(ctx context.Context, fileSize int64, buff []byte, offset int64, entry *LockedEntry) (int64, int64, error) {
	chunks := entry.GetEntry().Chunks
	if len(chunks) == 0 {
		return 0, 0, fmt.Errorf("no chunks available for RDMA read")
	}

	plan, err := planRDMAChunkRead(chunks, fileSize, len(buff), offset)
	if err != nil {
		return 0, 0, err
	}

	glog.V(4).Infof("RDMA read attempt: chunk=%s (fileId=%s), chunkOffset=%d, readSize=%d",
		plan.fileID, plan.fileID, plan.chunkOffset, plan.readSize)

	writer := &fixedBufferWriter{buf: buff[:plan.readSize]}
	totalRead, isRDMA, err := fh.wfs.rdmaClient.ReadNeedleTo(ctx, plan.fileID, uint64(plan.chunkOffset), uint64(plan.readSize), writer)
	if err != nil {
		return 0, 0, fmt.Errorf("RDMA read failed: %w", err)
	}

	if !isRDMA {
		return 0, 0, fmt.Errorf("RDMA not available for chunk")
	}

	return totalRead, plan.chunk.ModifiedTsNs, nil
}

type rdmaChunkReadPlan struct {
	chunk       *filer_pb.FileChunk
	fileID      string
	chunkOffset int64
	readSize    int64
}

func planRDMAChunkRead(chunks []*filer_pb.FileChunk, fileSize int64, bufferSize int, offset int64) (*rdmaChunkReadPlan, error) {
	if bufferSize <= 0 {
		return nil, fmt.Errorf("empty read buffer")
	}
	if offset < 0 {
		return nil, fmt.Errorf("negative read offset %d", offset)
	}
	if offset >= fileSize {
		return nil, io.EOF
	}

	targetChunk, chunkOffset := findChunkContaining(chunks, offset)
	if targetChunk == nil {
		return nil, fmt.Errorf("no chunk found for offset %d", offset)
	}

	fileID := targetChunk.GetFileIdString()
	if fileID == "" {
		return nil, fmt.Errorf("chunk at offset %d has no file id", targetChunk.Offset)
	}

	readStop := offset + int64(bufferSize)
	if readStop > fileSize {
		readStop = fileSize
	}

	remainingInChunk := targetChunk.Offset + int64(targetChunk.Size) - offset
	readSize := min(readStop-offset, remainingInChunk)
	if readSize <= 0 {
		return nil, io.EOF
	}

	return &rdmaChunkReadPlan{
		chunk:       targetChunk,
		fileID:      fileID,
		chunkOffset: chunkOffset,
		readSize:    readSize,
	}, nil
}

type fixedBufferWriter struct {
	buf []byte
	n   int
}

func (w *fixedBufferWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	remaining := len(w.buf) - w.n
	if remaining <= 0 {
		return 0, io.ErrShortWrite
	}
	if len(p) > remaining {
		copied := copy(w.buf[w.n:], p[:remaining])
		w.n += copied
		return copied, io.ErrShortWrite
	}
	copied := copy(w.buf[w.n:], p)
	w.n += copied
	return copied, nil
}

func (fh *FileHandle) downloadRemoteEntry(entry *LockedEntry) error {

	fileFullPath := fh.FullPath()
	dir, _ := fileFullPath.DirAndName()

	err := fh.wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.CacheRemoteObjectToLocalClusterRequest{
			Directory: string(dir),
			Name:      entry.Name,
		}

		glog.V(4).Infof("download entry: %v", request)
		resp, err := client.CacheRemoteObjectToLocalCluster(context.Background(), request)
		if err != nil {
			return fmt.Errorf("CacheRemoteObjectToLocalCluster file %s: %v", fileFullPath, err)
		}

		fh.SetEntry(resp.Entry)

		event := resp.GetMetadataEvent()
		if event == nil {
			event = metadataUpdateEvent(request.Directory, resp.Entry)
		}
		if applyErr := fh.wfs.applyLocalMetadataEvent(context.Background(), event); applyErr != nil {
			glog.Warningf("CacheRemoteObject %s: best-effort metadata apply failed: %v", fileFullPath, applyErr)
		}

		return nil
	})

	return err
}
