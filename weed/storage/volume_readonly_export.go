package storage

import (
	"bytes"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// OpenReadonlyVolume opens an existing on-disk volume for read-only needle access.
// Used by the RDMA sidecar when it shares the volume server's data directory.
func OpenReadonlyVolume(dataDir, idxDir, collection string, id needle.VolumeId) (*Volume, error) {
	if idxDir == "" {
		idxDir = dataDir
	}
	return loadReadonlyVolumeWithoutWorker(dataDir, idxDir, collection, id, NeedleMapInMemory, 0)
}

// ReadNeedleRange reads needle payload bytes at the given offset.
func (v *Volume) ReadNeedleRange(needleId NeedleId, cookie Cookie, offset, size int64) ([]byte, error) {
	if offset < 0 || size < 0 {
		return nil, fmt.Errorf("invalid read range offset %d size %d", offset, size)
	}

	n := &needle.Needle{Id: needleId, Cookie: cookie}
	readOption := &ReadOption{ReadBufferSize: 1024 * 1024}
	if size == 0 {
		if _, err := v.readNeedle(n, readOption, nil); err != nil {
			return nil, err
		}
		if n.Cookie != cookie {
			return nil, fmt.Errorf("cookie mismatch for needle %d: got %08x, want %08x", needleId, uint32(n.Cookie), uint32(cookie))
		}
		if offset >= int64(len(n.Data)) {
			return nil, nil
		}
		return n.Data[offset:], nil
	}

	var buf bytes.Buffer
	if err := v.readNeedleDataInto(n, readOption, &buf, offset, size); err != nil {
		return nil, err
	}
	if n.Cookie != cookie {
		return nil, fmt.Errorf("cookie mismatch for needle %d: got %08x, want %08x", needleId, uint32(n.Cookie), uint32(cookie))
	}
	return buf.Bytes(), nil
}
