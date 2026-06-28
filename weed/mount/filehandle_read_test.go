package mount

import (
	"errors"
	"io"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestPlanRDMAChunkReadUsesLogicalChunkOffsets(t *testing.T) {
	chunks := []*filer_pb.FileChunk{
		{Offset: 0, Size: 4096, FileId: "1,aaa", ModifiedTsNs: 11},
		{Offset: 1 << 20, Size: 128 << 10, FileId: "2,bbb", ModifiedTsNs: 22},
	}

	plan, err := planRDMAChunkRead(chunks, int64(2<<20), 64<<10, (1<<20)+8192)
	if err != nil {
		t.Fatalf("planRDMAChunkRead: %v", err)
	}

	if plan.chunk != chunks[1] {
		t.Fatalf("selected chunk = %v, want second chunk", plan.chunk)
	}
	if plan.fileID != "2,bbb" {
		t.Fatalf("fileID = %q, want %q", plan.fileID, "2,bbb")
	}
	if plan.chunkOffset != 8192 {
		t.Fatalf("chunkOffset = %d, want 8192", plan.chunkOffset)
	}
	if plan.readSize != 64<<10 {
		t.Fatalf("readSize = %d, want %d", plan.readSize, 64<<10)
	}
}

func TestPlanRDMAChunkReadClampsToChunkBoundary(t *testing.T) {
	chunks := []*filer_pb.FileChunk{
		{Offset: 1 << 20, Size: 128 << 10, FileId: "2,bbb"},
	}

	plan, err := planRDMAChunkRead(chunks, int64(2<<20), 64<<10, (1<<20)+(120<<10))
	if err != nil {
		t.Fatalf("planRDMAChunkRead: %v", err)
	}

	if plan.chunkOffset != 120<<10 {
		t.Fatalf("chunkOffset = %d, want %d", plan.chunkOffset, 120<<10)
	}
	if plan.readSize != 8<<10 {
		t.Fatalf("readSize = %d, want %d", plan.readSize, 8<<10)
	}
}

func TestPlanRDMAChunkReadClampsToFileSize(t *testing.T) {
	chunks := []*filer_pb.FileChunk{
		{Offset: 1 << 20, Size: 128 << 10, FileId: "2,bbb"},
	}

	plan, err := planRDMAChunkRead(chunks, int64((1<<20)+(96<<10)), 64<<10, (1<<20)+(80<<10))
	if err != nil {
		t.Fatalf("planRDMAChunkRead: %v", err)
	}

	if plan.chunkOffset != 80<<10 {
		t.Fatalf("chunkOffset = %d, want %d", plan.chunkOffset, 80<<10)
	}
	if plan.readSize != 16<<10 {
		t.Fatalf("readSize = %d, want %d", plan.readSize, 16<<10)
	}
}

func TestPlanRDMAChunkReadRejectsHoles(t *testing.T) {
	chunks := []*filer_pb.FileChunk{
		{Offset: 1 << 20, Size: 128 << 10, FileId: "2,bbb"},
	}

	_, err := planRDMAChunkRead(chunks, int64(2<<20), 4096, 4096)
	if err == nil {
		t.Fatal("expected missing chunk error")
	}
}

func TestPlanRDMAChunkReadRejectsCompressedChunks(t *testing.T) {
	chunks := []*filer_pb.FileChunk{
		{Offset: 0, Size: 4096, FileId: "1,compressed", IsCompressed: true},
	}

	_, err := planRDMAChunkRead(chunks, 4096, 4096, 0)
	if err == nil {
		t.Fatal("expected compressed chunk error")
	}
}

func TestPlanRDMAChunkReadRejectsEncryptedChunks(t *testing.T) {
	chunks := []*filer_pb.FileChunk{
		{Offset: 0, Size: 4096, FileId: "1,encrypted", CipherKey: []byte("key")},
	}

	_, err := planRDMAChunkRead(chunks, 4096, 4096, 0)
	if err == nil {
		t.Fatal("expected encrypted chunk error")
	}
}

func TestPlanRDMAChunkReadEOF(t *testing.T) {
	chunks := []*filer_pb.FileChunk{
		{Offset: 0, Size: 4096, FileId: "1,aaa"},
	}

	_, err := planRDMAChunkRead(chunks, 4096, 4096, 4096)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF", err)
	}
}

func TestSelectRDMAReadAheadPlansSkipsCachedAndUnsafeChunks(t *testing.T) {
	chunks := []*filer_pb.FileChunk{
		{Offset: 0, Size: 4096, FileId: "1,aaa"},
		{Offset: 4096, Size: 4096, FileId: "2,compressed", IsCompressed: true},
		{Offset: 8192, Size: 4096, FileId: "3,cached"},
		{Offset: 12288, Size: 4096, FileId: "4,encrypted", CipherKey: []byte("key")},
		{Offset: 16384, Size: 4096, FileId: "5,bbb"},
	}
	current := &rdmaChunkReadPlan{chunk: chunks[0], fileID: "1,aaa"}

	plans := selectRDMAReadAheadPlans(chunks, current, func(fileID string) bool {
		return fileID == "3,cached"
	})

	if len(plans) != 2 {
		t.Fatalf("plans = %d, want 2: %+v", len(plans), plans)
	}
	if plans[0].fileID != "1,aaa" || plans[0].readSize != 4096 {
		t.Fatalf("unexpected first plan: %+v", plans[0])
	}
	if plans[1].fileID != "5,bbb" || plans[1].chunkOffset != 0 {
		t.Fatalf("unexpected second plan: %+v", plans[1])
	}
}

func TestRDMAReadAheadCacheCopiesRange(t *testing.T) {
	fh := &FileHandle{}
	fh.storeRDMAReadAhead("1,aaa", []byte("0123456789"))

	buf := make([]byte, 4)
	n, ok := fh.readRDMAReadAhead(&rdmaChunkReadPlan{
		fileID:      "1,aaa",
		chunkOffset: 3,
		readSize:    4,
	}, buf)

	if !ok || n != 4 || string(buf) != "3456" {
		t.Fatalf("unexpected cache read: ok=%v n=%d buf=%q", ok, n, buf)
	}
	fh.clearRDMAReadAhead()
	if _, ok := fh.readRDMAReadAhead(&rdmaChunkReadPlan{fileID: "1,aaa", readSize: 1}, make([]byte, 1)); ok {
		t.Fatal("expected cache miss after clear")
	}
}
