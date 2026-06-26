package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

func TestOpenReadonlyVolumeForcesReadOnlyDataAndIndex(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}

	n := newRandomNeedle(1)
	if _, _, _, err := v.writeNeedle2(n, true, false); err != nil {
		t.Fatalf("write needle: %v", err)
	}
	payload := append([]byte(nil), n.Data...)
	v.Close()

	readonly, err := OpenReadonlyVolume(dir, dir, "", 1)
	if err != nil {
		t.Fatalf("open readonly volume: %v", err)
	}
	defer readonly.Close()

	if !readonly.noWriteOrDelete {
		t.Fatal("readonly volume was not marked no-write")
	}
	if _, err := os.Stat(filepath.Join(dir, "1.sdx")); !os.IsNotExist(err) {
		t.Fatalf("readonly volume should not create sorted index file: %v", err)
	}

	data, err := readonly.ReadNeedleRange(n.Id, n.Cookie, 0, int64(len(payload)))
	if err != nil {
		t.Fatalf("read needle range: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("read payload mismatch: got %d bytes, want %d", len(data), len(payload))
	}
}
