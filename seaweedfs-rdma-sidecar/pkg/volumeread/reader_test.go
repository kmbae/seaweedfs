package volumeread

import (
	"bytes"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestReaderReloadsVolumeOnNeedleNotFound(t *testing.T) {
	dir := t.TempDir()
	vol, err := storage.NewVolume(dir, dir, "", 1, storage.NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("create volume: %v", err)
	}
	defer vol.Close()

	reader := NewReader(dir, dir, "")
	defer reader.Close()

	firstNeedle, firstPayload := writeNeedleBlob(t, vol, 1, []byte("first payload"))
	data, err := reader.ReadNeedle(1, uint64(firstNeedle.Id), uint32(firstNeedle.Cookie), 0, uint64(len(firstPayload)))
	if err != nil {
		t.Fatalf("read first needle: %v", err)
	}
	if !bytes.Equal(data, firstPayload) {
		t.Fatalf("first payload mismatch: got %q want %q", data, firstPayload)
	}

	secondNeedle, secondPayload := writeNeedleBlob(t, vol, 2, []byte("second payload after reader cache"))
	data, err = reader.ReadNeedle(1, uint64(secondNeedle.Id), uint32(secondNeedle.Cookie), 0, uint64(len(secondPayload)))
	if err != nil {
		t.Fatalf("read second needle after reload: %v", err)
	}
	if !bytes.Equal(data, secondPayload) {
		t.Fatalf("second payload mismatch: got %q want %q", data, secondPayload)
	}
}

func writeNeedleBlob(t *testing.T, vol *storage.Volume, id uint64, payload []byte) (*needle.Needle, []byte) {
	t.Helper()

	n := &needle.Needle{
		Id:           types.NeedleId(id),
		Cookie:       types.Cookie(id + 1000),
		Data:         append([]byte(nil), payload...),
		LastModified: uint64(time.Now().Unix()),
	}
	n.SetHasLastModifiedDate()
	n.Checksum = needle.NewCRC(n.Data)

	blob, size, err := needle.EncodeNeedleBlob(n, vol.Version())
	if err != nil {
		t.Fatalf("encode needle blob: %v", err)
	}
	if err := vol.WriteNeedleBlob(n.Id, blob, size); err != nil {
		t.Fatalf("write needle blob: %v", err)
	}
	return n, append([]byte(nil), payload...)
}
