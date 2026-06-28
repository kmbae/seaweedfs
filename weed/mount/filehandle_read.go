package mount

import (
	"context"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

const (
	rdmaReadAheadChunkCount    = 4
	rdmaReadAheadMaxChunks     = 16
	rdmaReadAheadMaxBytes      = 64 << 20
	rdmaReadAheadMaxChunkBytes = 16 << 20
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

	if totalRead, ok := fh.readRDMAReadAhead(plan, buff); ok {
		glog.V(4).Infof("RDMA read-ahead cache hit: fileId=%s, chunkOffset=%d, readSize=%d",
			plan.fileID, plan.chunkOffset, plan.readSize)
		return totalRead, plan.chunk.ModifiedTsNs, nil
	}

	if err := fh.prefetchRDMAReadAhead(ctx, chunks, plan); err == nil {
		if totalRead, ok := fh.readRDMAReadAhead(plan, buff); ok {
			glog.V(4).Infof("RDMA read-ahead served after prefetch: fileId=%s, chunkOffset=%d, readSize=%d",
				plan.fileID, plan.chunkOffset, plan.readSize)
			return totalRead, plan.chunk.ModifiedTsNs, nil
		}
	} else {
		glog.V(4).Infof("RDMA read-ahead prefetch skipped for fileId=%s: %v", plan.fileID, err)
	}

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

func (fh *FileHandle) prefetchRDMAReadAhead(ctx context.Context, chunks []*filer_pb.FileChunk, current *rdmaChunkReadPlan) error {
	plans := selectRDMAReadAheadPlans(chunks, current, fh.rdmaReadAheadHas)
	if len(plans) == 0 {
		return fmt.Errorf("no eligible RDMA read-ahead chunks")
	}

	reads := make([]RDMANeedleReadRequest, 0, len(plans))
	buffers := make([][]byte, 0, len(plans))
	var expected int64
	for _, plan := range plans {
		if plan.readSize <= 0 || plan.readSize > int64(maxInt) {
			continue
		}
		data := make([]byte, int(plan.readSize))
		reads = append(reads, RDMANeedleReadRequest{
			FileID: plan.fileID,
			Offset: 0,
			Size:   uint64(plan.readSize),
			Dst:    &fixedBufferWriter{buf: data},
		})
		buffers = append(buffers, data)
		expected += plan.readSize
	}
	if len(reads) == 0 {
		return fmt.Errorf("no RDMA read-ahead reads were built")
	}

	totalRead, isRDMA, err := fh.wfs.rdmaClient.ReadNeedlesTo(ctx, reads)
	if err != nil {
		return err
	}
	if !isRDMA {
		return fmt.Errorf("RDMA read-ahead did not use RDMA")
	}
	if totalRead != expected {
		return fmt.Errorf("RDMA read-ahead copied %d bytes, expected %d", totalRead, expected)
	}

	for i, read := range reads {
		fh.storeRDMAReadAhead(read.FileID, buffers[i])
	}
	glog.V(4).Infof("RDMA read-ahead prefetched %d chunks (%d bytes)", len(reads), totalRead)
	return nil
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
	if targetChunk.IsCompressed {
		return nil, fmt.Errorf("RDMA read does not support compressed chunk %s", fileID)
	}
	if len(targetChunk.CipherKey) > 0 {
		return nil, fmt.Errorf("RDMA read does not support encrypted chunk %s", fileID)
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

func selectRDMAReadAheadPlans(chunks []*filer_pb.FileChunk, current *rdmaChunkReadPlan, cached func(string) bool) []rdmaChunkReadPlan {
	if current == nil || current.chunk == nil {
		return nil
	}
	plans := make([]rdmaChunkReadPlan, 0, rdmaReadAheadChunkCount)
	for _, chunk := range chunks {
		if chunk == nil || chunk.Offset < current.chunk.Offset {
			continue
		}
		plan, err := planRDMAWholeChunkRead(chunk)
		if err != nil {
			continue
		}
		if cached != nil && cached(plan.fileID) {
			continue
		}
		plans = append(plans, *plan)
		if len(plans) >= rdmaReadAheadChunkCount {
			break
		}
	}
	return plans
}

func planRDMAWholeChunkRead(chunk *filer_pb.FileChunk) (*rdmaChunkReadPlan, error) {
	if chunk == nil {
		return nil, fmt.Errorf("nil chunk")
	}
	fileID := chunk.GetFileIdString()
	if fileID == "" {
		return nil, fmt.Errorf("chunk at offset %d has no file id", chunk.Offset)
	}
	if chunk.IsCompressed {
		return nil, fmt.Errorf("RDMA read does not support compressed chunk %s", fileID)
	}
	if len(chunk.CipherKey) > 0 {
		return nil, fmt.Errorf("RDMA read does not support encrypted chunk %s", fileID)
	}
	if chunk.Size == 0 {
		return nil, fmt.Errorf("empty chunk %s", fileID)
	}
	if chunk.Size > maxUint32 || chunk.Size > uint64(maxInt) || chunk.Size > rdmaReadAheadMaxChunkBytes {
		return nil, fmt.Errorf("chunk %s is too large for RDMA read-ahead: %d", fileID, chunk.Size)
	}
	return &rdmaChunkReadPlan{
		chunk:       chunk,
		fileID:      fileID,
		chunkOffset: 0,
		readSize:    int64(chunk.Size),
	}, nil
}

func (fh *FileHandle) readRDMAReadAhead(plan *rdmaChunkReadPlan, buff []byte) (int64, bool) {
	if plan == nil || plan.chunkOffset < 0 || plan.readSize <= 0 {
		return 0, false
	}
	data, ok := fh.getRDMAReadAhead(plan.fileID)
	if !ok {
		return 0, false
	}
	stop := plan.chunkOffset + plan.readSize
	if stop > int64(len(data)) {
		fh.dropRDMAReadAhead(plan.fileID)
		return 0, false
	}
	copied := copy(buff[:plan.readSize], data[plan.chunkOffset:stop])
	return int64(copied), copied == int(plan.readSize)
}

func (fh *FileHandle) getRDMAReadAhead(fileID string) ([]byte, bool) {
	fh.rdmaReadAheadLock.Lock()
	defer fh.rdmaReadAheadLock.Unlock()
	data, ok := fh.rdmaReadAhead[fileID]
	return data, ok
}

func (fh *FileHandle) rdmaReadAheadHas(fileID string) bool {
	fh.rdmaReadAheadLock.Lock()
	defer fh.rdmaReadAheadLock.Unlock()
	_, ok := fh.rdmaReadAhead[fileID]
	return ok
}

func (fh *FileHandle) storeRDMAReadAhead(fileID string, data []byte) {
	if fileID == "" || len(data) == 0 || len(data) > rdmaReadAheadMaxChunkBytes {
		return
	}
	fh.rdmaReadAheadLock.Lock()
	defer fh.rdmaReadAheadLock.Unlock()
	if fh.rdmaReadAhead == nil {
		fh.rdmaReadAhead = make(map[string][]byte)
	}
	if old, ok := fh.rdmaReadAhead[fileID]; ok {
		fh.rdmaReadAheadBytes -= int64(len(old))
		fh.removeRDMAReadAheadOrderLocked(fileID)
	}
	fh.rdmaReadAhead[fileID] = data
	fh.rdmaReadAheadOrder = append(fh.rdmaReadAheadOrder, fileID)
	fh.rdmaReadAheadBytes += int64(len(data))

	for (fh.rdmaReadAheadBytes > rdmaReadAheadMaxBytes || len(fh.rdmaReadAhead) > rdmaReadAheadMaxChunks) && len(fh.rdmaReadAheadOrder) > 0 {
		victim := fh.rdmaReadAheadOrder[0]
		fh.rdmaReadAheadOrder = fh.rdmaReadAheadOrder[1:]
		if old, ok := fh.rdmaReadAhead[victim]; ok {
			delete(fh.rdmaReadAhead, victim)
			fh.rdmaReadAheadBytes -= int64(len(old))
		}
	}
}

func (fh *FileHandle) dropRDMAReadAhead(fileID string) {
	fh.rdmaReadAheadLock.Lock()
	defer fh.rdmaReadAheadLock.Unlock()
	if fh.rdmaReadAhead == nil {
		return
	}
	if old, ok := fh.rdmaReadAhead[fileID]; ok {
		delete(fh.rdmaReadAhead, fileID)
		fh.rdmaReadAheadBytes -= int64(len(old))
		fh.removeRDMAReadAheadOrderLocked(fileID)
	}
}

func (fh *FileHandle) clearRDMAReadAhead() {
	fh.rdmaReadAheadLock.Lock()
	defer fh.rdmaReadAheadLock.Unlock()
	fh.rdmaReadAhead = nil
	fh.rdmaReadAheadOrder = nil
	fh.rdmaReadAheadBytes = 0
}

func (fh *FileHandle) removeRDMAReadAheadOrderLocked(fileID string) {
	for i, cachedFileID := range fh.rdmaReadAheadOrder {
		if cachedFileID == fileID {
			copy(fh.rdmaReadAheadOrder[i:], fh.rdmaReadAheadOrder[i+1:])
			fh.rdmaReadAheadOrder = fh.rdmaReadAheadOrder[:len(fh.rdmaReadAheadOrder)-1]
			return
		}
	}
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
