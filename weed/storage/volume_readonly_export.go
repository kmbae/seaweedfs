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
	if size <= 0 {
		return nil, fmt.Errorf("read size must be positive")
	}

	n := &needle.Needle{Id: needleId, Cookie: cookie}
	readOption := &ReadOption{ReadBufferSize: 1024 * 1024}
	var buf bytes.Buffer
	if err := v.readNeedleDataInto(n, readOption, &buf, offset, size); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
