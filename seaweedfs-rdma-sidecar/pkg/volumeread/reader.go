// Package volumeread reads needles directly from local volume files.
package volumeread

import (
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// Reader loads volume files from a shared data directory (colocated sidecar use case).
type Reader struct {
	dataDir    string
	idxDir     string
	collection string

	mu      sync.Mutex
	volumes map[needle.VolumeId]*storage.Volume
}

// NewReader creates a reader for volume data under dataDir.
func NewReader(dataDir, idxDir, collection string) *Reader {
	if idxDir == "" {
		idxDir = dataDir
	}
	return &Reader{
		dataDir:    dataDir,
		idxDir:     idxDir,
		collection: collection,
		volumes:    make(map[needle.VolumeId]*storage.Volume),
	}
}

// ReadNeedle reads needle bytes from the local volume directory.
func (r *Reader) ReadNeedle(volumeID uint32, needleID uint64, cookie uint32, offset, size uint64) ([]byte, error) {
	if size == 0 {
		size = 4096
	}

	vol, err := r.getVolume(needle.VolumeId(volumeID))
	if err != nil {
		return nil, err
	}

	return vol.ReadNeedleRange(
		types.NeedleId(needleID),
		types.Cookie(cookie),
		int64(offset),
		int64(size),
	)
}

// VolumeVersion returns the on-disk format version for a local volume.
func (r *Reader) VolumeVersion(volumeID uint32) (needle.Version, error) {
	vol, err := r.getVolume(needle.VolumeId(volumeID))
	if err != nil {
		return 0, err
	}
	return vol.Version(), nil
}

func (r *Reader) getVolume(id needle.VolumeId) (*storage.Volume, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if vol, ok := r.volumes[id]; ok {
		return vol, nil
	}

	vol, err := storage.OpenReadonlyVolume(r.dataDir, r.idxDir, r.collection, id)
	if err != nil {
		return nil, fmt.Errorf("open volume %d: %w", id, err)
	}
	r.volumes[id] = vol
	return vol, nil
}

// Close releases opened volume handles.
func (r *Reader) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, vol := range r.volumes {
		vol.Close()
	}
	r.volumes = make(map[needle.VolumeId]*storage.Volume)
}
