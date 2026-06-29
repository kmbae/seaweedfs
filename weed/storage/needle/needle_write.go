package needle

import (
	"bytes"
	"fmt"
	"io"
	"math"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/buffer_pool"
)

func (n *Needle) Append(w backend.BackendStorageFile, version Version) (offset uint64, size Size, actualSize int64, err error) {
	end, _, e := w.GetStat()
	if e != nil {
		err = fmt.Errorf("Cannot Read Current Volume Position: %w", e)
		return
	}
	offset = uint64(end)
	if offset >= MaxPossibleVolumeSize && len(n.Data) != 0 {
		err = fmt.Errorf("Volume Size %d Exceeded %d", offset, MaxPossibleVolumeSize)
		return
	}
	bytesBuffer := buffer_pool.SyncPoolGetBuffer()
	defer func() {
		if err != nil {
			if te := w.Truncate(end); te != nil {
				// handle error or log
			}
		}
		buffer_pool.SyncPoolPutBuffer(bytesBuffer)
	}()

	size, actualSize, err = writeNeedleByVersion(version, n, offset, bytesBuffer)
	if err != nil {
		return
	}

	_, err = w.WriteAt(bytesBuffer.Bytes(), int64(offset))
	if err != nil {
		err = fmt.Errorf("failed to write %d bytes to %s at offset %d: %w", actualSize, w.Name(), offset, err)
	}

	return offset, size, actualSize, err
}

// EncodeNeedleBlob serializes a needle into the on-disk blob format expected by
// WriteNeedleBlob. The volume server will stamp the final append timestamp.
func EncodeNeedleBlob(n *Needle, version Version) (data []byte, size Size, err error) {
	var bytesBuffer bytes.Buffer
	_, _, err = writeNeedleByVersion(version, n, 0, &bytesBuffer)
	if err != nil {
		return nil, 0, err
	}
	return append([]byte(nil), bytesBuffer.Bytes()...), n.Size, nil
}

func WriteNeedleBlob(w backend.BackendStorageFile, dataSlice []byte, size Size, appendAtNs uint64, version Version) (offset uint64, err error) {

	if end, _, e := w.GetStat(); e == nil {
		defer func(w backend.BackendStorageFile, off int64) {
			if err != nil {
				if te := w.Truncate(end); te != nil {
					glog.V(0).Infof("Failed to truncate %s back to %d with error: %v", w.Name(), end, te)
				}
			}
		}(w, end)
		offset = uint64(end)
	} else {
		err = fmt.Errorf("Cannot Read Current Volume Position: %v", e)
		return
	}

	if version == Version3 {
		// compute byte offset as int to compare and slice correctly
		tsOffset := int(NeedleHeaderSize) + int(size) + NeedleChecksumSize
		// Ensure dataSlice has enough capacity for the timestamp
		if tsOffset < 0 {
			err = fmt.Errorf("invalid needle size %d results in negative timestamp offset %d", size, tsOffset)
			return
		}
		if tsOffset+TimestampSize > len(dataSlice) {
			err = fmt.Errorf("needle blob buffer too small: need %d bytes, have %d", tsOffset+TimestampSize, len(dataSlice))
			return
		}
		util.Uint64toBytes(dataSlice[tsOffset:tsOffset+TimestampSize], appendAtNs)
	}

	if err == nil {
		_, err = w.WriteAt(dataSlice, int64(offset))
	}

	return

}

func WriteNeedleDataStream(w backend.BackendStorageFile, needleId NeedleId, cookie Cookie, dataSize uint64, lastModified uint64, appendAtNs uint64, version Version, writeData func(io.Writer) error) (offset uint64, size Size, err error) {
	end, _, e := w.GetStat()
	if e != nil {
		return 0, 0, fmt.Errorf("Cannot Read Current Volume Position: %w", e)
	}
	defer func() {
		if err != nil {
			if te := w.Truncate(end); te != nil {
				glog.V(0).Infof("Failed to truncate %s back to %d with error: %v", w.Name(), end, te)
			}
		}
	}()
	offset = uint64(end)
	size, err = WriteNeedleDataStreamAt(w, offset, needleId, cookie, dataSize, lastModified, appendAtNs, version, writeData)
	return offset, size, err
}

func NeedleDataStreamSize(dataSize uint64, version Version) (Size, int64, error) {
	if dataSize == 0 {
		return 0, 0, fmt.Errorf("empty needle data stream")
	}
	if dataSize > math.MaxUint32 {
		return 0, 0, fmt.Errorf("needle data stream too large: %d", dataSize)
	}
	if dataSize > uint64(math.MaxInt32) {
		return 0, 0, fmt.Errorf("needle data stream exceeds index size limit: %d", dataSize)
	}
	if !IsSupportedVersion(version) {
		return 0, 0, fmt.Errorf("unsupported version: %d", version)
	}
	var needleSize Size
	if version == Version1 {
		needleSize = Size(dataSize)
	} else {
		needleSize = Size(DataSizeSize + dataSize + 1 + LastModifiedBytesLength)
	}
	return needleSize, GetActualSize(needleSize, version), nil
}

func WriteNeedleDataStreamAt(w backend.BackendStorageFile, offset uint64, needleId NeedleId, cookie Cookie, dataSize uint64, lastModified uint64, appendAtNs uint64, version Version, writeData func(io.Writer) error) (size Size, err error) {
	if writeData == nil {
		return 0, fmt.Errorf("nil needle data stream writer")
	}
	needleSize, _, err := NeedleDataStreamSize(dataSize, version)
	if err != nil {
		return 0, err
	}

	header := make([]byte, NeedleHeaderSize+TimestampSize)
	CookieToBytes(header[0:CookieSize], cookie)
	NeedleIdToBytes(header[CookieSize:CookieSize+NeedleIdSize], needleId)
	SizeToBytes(header[CookieSize+NeedleIdSize:CookieSize+NeedleIdSize+SizeSize], needleSize)

	pos := int64(offset)
	if _, err = w.WriteAt(header[:NeedleHeaderSize], pos); err != nil {
		return 0, fmt.Errorf("write needle stream header: %w", err)
	}
	pos += NeedleHeaderSize

	if version != Version1 {
		util.Uint32toBytes(header[:DataSizeSize], uint32(dataSize))
		if _, err = w.WriteAt(header[:DataSizeSize], pos); err != nil {
			return 0, fmt.Errorf("write needle stream data size: %w", err)
		}
		pos += DataSizeSize
	}

	payload := &needleStreamPayloadWriter{
		file:      w,
		offset:    pos,
		remaining: dataSize,
	}
	if err = writeData(payload); err != nil {
		return 0, fmt.Errorf("write needle stream payload: %w", err)
	}
	if payload.remaining != 0 {
		return 0, fmt.Errorf("short needle stream payload: wrote %d of %d bytes", payload.written, dataSize)
	}
	pos += int64(dataSize)

	if version != Version1 {
		util.Uint8toBytes(header[:1], FlagHasLastModifiedDate)
		if _, err = w.WriteAt(header[:1], pos); err != nil {
			return 0, fmt.Errorf("write needle stream flags: %w", err)
		}
		pos++
		util.Uint64toBytes(header[:8], lastModified)
		if _, err = w.WriteAt(header[8-LastModifiedBytesLength:8], pos); err != nil {
			return 0, fmt.Errorf("write needle stream last modified: %w", err)
		}
		pos += LastModifiedBytesLength
	}

	padding := PaddingLength(needleSize, version)
	util.Uint32toBytes(header[:NeedleChecksumSize], uint32(payload.crc))
	tailLength := NeedleChecksumSize + int(padding)
	if version == Version3 {
		util.Uint64toBytes(header[NeedleChecksumSize:NeedleChecksumSize+TimestampSize], appendAtNs)
		tailLength += TimestampSize
	}
	if _, err = w.WriteAt(header[:tailLength], pos); err != nil {
		return 0, fmt.Errorf("write needle stream tail: %w", err)
	}

	return needleSize, nil
}

type needleStreamPayloadWriter struct {
	file      backend.BackendStorageFile
	offset    int64
	remaining uint64
	written   uint64
	crc       CRC
}

func (w *needleStreamPayloadWriter) Write(payload []byte) (int, error) {
	if uint64(len(payload)) > w.remaining {
		return 0, fmt.Errorf("needle stream payload exceeds declared size: write=%d remaining=%d", len(payload), w.remaining)
	}
	n, err := w.file.WriteAt(payload, w.offset)
	if n > 0 {
		w.crc = w.crc.Update(payload[:n])
		w.offset += int64(n)
		w.written += uint64(n)
		w.remaining -= uint64(n)
	}
	if err != nil {
		return n, err
	}
	if n != len(payload) {
		return n, io.ErrShortWrite
	}
	return n, nil
}
