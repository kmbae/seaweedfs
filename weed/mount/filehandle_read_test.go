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
