package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestSearchVolumesWithDeletedNeedles(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()

	count := 20

	for i := 1; i < count; i++ {
		n := newRandomNeedle(uint64(i))
		_, _, _, err := v.writeNeedle2(n, true, false)
		if err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
	}

	for i := 1; i < 15; i++ {
		n := newEmptyNeedle(uint64(i))
		err := v.nm.Put(n.Id, types.Offset{}, types.TombstoneFileSize)
		if err != nil {
			t.Fatalf("delete needle %d: %v", i, err)
		}
	}

	ts1 := time.Now().UnixNano()

	for i := 15; i < count; i++ {
		n := newEmptyNeedle(uint64(i))
		_, err := v.doDeleteRequest(n)
		if err != nil {
			t.Fatalf("delete needle %d: %v", i, err)
		}
	}

	offset, isLast, err := v.BinarySearchByAppendAtNs(uint64(ts1))
	if err != nil {
		t.Fatalf("lookup by ts: %v", err)
	}
	fmt.Printf("offset: %v, isLast: %v\n", offset.ToActualOffset(), isLast)

}

func TestWriteNeedleDataStreamBatchRoundTrip(t *testing.T) {
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()

	payloads := [][]byte{
		[]byte("first rdma batch payload"),
		[]byte("second rdma batch payload"),
	}
	results := v.WriteNeedleDataStreamBatch([]NeedleDataStreamBatchEntry{
		{
			NeedleID: 101,
			Cookie:   1001,
			DataSize: uint64(len(payloads[0])),
			WriteData: func(w io.Writer) error {
				_, err := w.Write(payloads[0])
				return err
			},
		},
		{
			NeedleID: 102,
			Cookie:   1002,
			DataSize: uint64(len(payloads[1])),
			WriteData: func(w io.Writer) error {
				_, err := w.Write(payloads[1])
				return err
			},
		},
	})
	for i, result := range results {
		if result.Err != nil {
			t.Fatalf("result[%d] err = %v", i, result.Err)
		}
	}

	for i, payload := range payloads {
		n := &needle.Needle{
			Id:     types.NeedleId(101 + i),
			Cookie: types.Cookie(1001 + i),
		}
		if _, err := v.readNeedle(n, &ReadOption{}, nil); err != nil {
			t.Fatalf("read batch needle %d: %v", i, err)
		}
		if !bytes.Equal(n.Data, payload) {
			t.Fatalf("needle %d data = %q, want %q", i, n.Data, payload)
		}
	}
}

func TestWriteNeedleDataStreamBatchRollback(t *testing.T) {
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()

	startSize, _, err := v.DataBackend.GetStat()
	if err != nil {
		t.Fatalf("stat volume: %v", err)
	}
	payload := []byte("rollback payload")
	results := v.WriteNeedleDataStreamBatch([]NeedleDataStreamBatchEntry{
		{
			NeedleID: 201,
			Cookie:   2001,
			DataSize: uint64(len(payload)),
			WriteData: func(w io.Writer) error {
				_, err := w.Write(payload)
				return err
			},
		},
		{
			NeedleID: 202,
			Cookie:   2002,
			DataSize: uint64(len(payload)),
			WriteData: func(w io.Writer) error {
				return fmt.Errorf("injected batch write failure")
			},
		},
	})
	if len(results) != 2 || results[0].Err == nil || results[1].Err == nil {
		t.Fatalf("expected both results to fail after rollback: %+v", results)
	}
	endSize, _, err := v.DataBackend.GetStat()
	if err != nil {
		t.Fatalf("stat volume after rollback: %v", err)
	}
	if endSize != startSize {
		t.Fatalf("volume size after rollback = %d, want %d", endSize, startSize)
	}
}

func isFileExist(path string) (bool, error) {
	if _, err := os.Stat(path); err == nil {
		return true, nil
	} else if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else {
		return false, err
	}
}

func assertFileExist(t *testing.T, expected bool, path string) {
	exist, err := isFileExist(path)
	if err != nil {
		t.Fatalf("isFileExist: %v", err)
	}
	assert.Equal(t, expected, exist)
}

func TestDestroyEmptyVolumeWithOnlyEmpty(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	path := v.DataBackend.Name()

	// should can Destroy empty volume with onlyEmpty
	assertFileExist(t, true, path)
	err = v.Destroy(true, false)
	if err != nil {
		t.Fatalf("destroy volume: %v", err)
	}
	assertFileExist(t, false, path)
}

func TestDestroyEmptyVolumeWithoutOnlyEmpty(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	path := v.DataBackend.Name()

	// should can Destroy empty volume without onlyEmpty
	assertFileExist(t, true, path)
	err = v.Destroy(false, false)
	if err != nil {
		t.Fatalf("destroy volume: %v", err)
	}
	assertFileExist(t, false, path)
}

func TestDestroyNonemptyVolumeWithOnlyEmpty(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()
	path := v.DataBackend.Name()

	// should return "volume not empty" error and do not delete file when Destroy non-empty volume
	_, _, _, err = v.writeNeedle2(newRandomNeedle(1), true, false)
	if err != nil {
		t.Fatalf("write needle: %v", err)
	}
	assert.Equal(t, uint64(1), v.FileCount())

	assertFileExist(t, true, path)
	err = v.Destroy(true, false)
	assert.EqualError(t, err, "volume not empty")
	assertFileExist(t, true, path)

	// should keep working after "volume not empty"
	_, _, _, err = v.writeNeedle2(newRandomNeedle(2), true, false)
	if err != nil {
		t.Fatalf("write needle: %v", err)
	}

	assert.Equal(t, uint64(2), v.FileCount())
}

func TestDestroyNonemptyVolumeWithoutOnlyEmpty(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	path := v.DataBackend.Name()

	// should can Destroy non-empty volume without onlyEmpty
	_, _, _, err = v.writeNeedle2(newRandomNeedle(1), true, false)
	if err != nil {
		t.Fatalf("write needle: %v", err)
	}
	assert.Equal(t, uint64(1), v.FileCount())

	assertFileExist(t, true, path)
	err = v.Destroy(false, false)
	if err != nil {
		t.Fatalf("destroy volume: %v", err)
	}
	assertFileExist(t, false, path)
}
