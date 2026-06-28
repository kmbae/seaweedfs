package weed_server

import "testing"

func TestPlanVolumeRdmaChunks(t *testing.T) {
	chunks := planVolumeRdmaChunks(10, 4)
	if len(chunks) != 3 {
		t.Fatalf("len(chunks) = %d, want 3", len(chunks))
	}
	want := []volumeRdmaChunkRange{
		{Index: 0, Offset: 0, Length: 4},
		{Index: 1, Offset: 4, Length: 4},
		{Index: 2, Offset: 8, Length: 2},
	}
	for i := range want {
		if chunks[i] != want[i] {
			t.Fatalf("chunk[%d] = %+v, want %+v", i, chunks[i], want[i])
		}
	}
}

func TestPlanVolumeRdmaChunksUsesDefaultChunkSize(t *testing.T) {
	chunks := planVolumeRdmaChunks(uint32(volumeRdmaPipelineChunkSize+17), 0)
	if len(chunks) != 2 {
		t.Fatalf("len(chunks) = %d, want 2", len(chunks))
	}
	if chunks[0].Length != volumeRdmaPipelineChunkSize {
		t.Fatalf("first chunk length = %d, want %d", chunks[0].Length, volumeRdmaPipelineChunkSize)
	}
	if chunks[1].Offset != volumeRdmaPipelineChunkSize || chunks[1].Length != 17 {
		t.Fatalf("unexpected tail chunk: %+v", chunks[1])
	}
}
