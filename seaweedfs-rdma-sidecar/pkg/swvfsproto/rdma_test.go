package swvfsproto

import (
	"testing"
	"unsafe"
)

func TestRDMAStructSizesMatchKernelABI(t *testing.T) {
	if got := unsafe.Sizeof(RDMALocalInfo{}); got != 280 {
		t.Fatalf("RDMALocalInfo size = %d, want 280", got)
	}
	if got := unsafe.Sizeof(RDMARemoteInfo{}); got != 112 {
		t.Fatalf("RDMARemoteInfo size = %d, want 112", got)
	}
	if got := unsafe.Sizeof(RDMADataDesc{}); got != RDMADataDescSize {
		t.Fatalf("RDMADataDesc size = %d, want %d", got, RDMADataDescSize)
	}
	if got := unsafe.Sizeof(RDMAWriteCommitEntry{}); got != RDMAWriteCommitEntrySize {
		t.Fatalf("RDMAWriteCommitEntry size = %d, want %d", got, RDMAWriteCommitEntrySize)
	}
	if got := unsafe.Sizeof(RDMAWriteCommitResult{}); got != RDMAWriteCommitResultSize {
		t.Fatalf("RDMAWriteCommitResult size = %d, want %d", got, RDMAWriteCommitResultSize)
	}
	if got := unsafe.Sizeof(RDMATestMR{}); got != RDMATestMRSize {
		t.Fatalf("RDMATestMR size = %d, want %d", got, RDMATestMRSize)
	}
}

func TestRDMALocalInfoHelpers(t *testing.T) {
	var info RDMALocalInfo
	info.Flags = RDMAFKernelEnabled | RDMAFEndpointReady | RDMAFQPConnected | RDMAFGIDValid
	copy(info.Device[:], "mlx5_0")
	for i := range info.GID {
		info.GID[i] = byte(i)
	}

	if !info.KernelEnabled() || !info.EndpointReady() || !info.Connected() || !info.GIDValid() {
		t.Fatalf("flag helpers returned false for flags 0x%x", info.Flags)
	}
	if got := info.DeviceName(); got != "mlx5_0" {
		t.Fatalf("DeviceName = %q", got)
	}
	if got := info.GIDHex(); got != "000102030405060708090a0b0c0d0e0f" {
		t.Fatalf("GIDHex = %q", got)
	}
}

func TestDecodeGIDHex(t *testing.T) {
	gid, ok := DecodeGIDHex("000102030405060708090a0b0c0d0e0f")
	if !ok {
		t.Fatal("DecodeGIDHex returned false")
	}
	if gid[15] != 0x0f {
		t.Fatalf("last gid byte = %#x", gid[15])
	}
	if _, ok := DecodeGIDHex("bad"); ok {
		t.Fatal("invalid gid decoded successfully")
	}
}

func TestRDMADataDescEncodeDecode(t *testing.T) {
	desc := RDMADataDesc{
		RemoteAddr: 0x0102030405060708,
		RKey:       0xaabbccdd,
		Length:     4096,
		Reserved:   [4]uint64{1, 2, 3, 4},
	}

	encoded := EncodeRDMADataDesc(desc)
	if len(encoded) != RDMADataDescSize {
		t.Fatalf("encoded length = %d, want %d", len(encoded), RDMADataDescSize)
	}
	got, err := DecodeRDMADataDesc(encoded)
	if err != nil {
		t.Fatalf("DecodeRDMADataDesc: %v", err)
	}
	if got.RemoteAddr != desc.RemoteAddr || got.RKey != desc.RKey || got.Length != desc.Length || got.Reserved != desc.Reserved {
		t.Fatalf("decoded desc mismatch: %+v", got)
	}
}

func TestRDMAWriteCommitEntryEncodeDecode(t *testing.T) {
	entries := []RDMAWriteCommitEntry{
		{Offset: 4, Size: 8},
		{Offset: 12, Size: 16},
	}
	encoded := EncodeRDMAWriteCommitEntries(entries)
	if len(encoded) != len(entries)*RDMAWriteCommitEntrySize {
		t.Fatalf("encoded length = %d", len(encoded))
	}
	got, err := DecodeRDMAWriteCommitEntries(encoded)
	if err != nil {
		t.Fatalf("DecodeRDMAWriteCommitEntries: %v", err)
	}
	if len(got) != len(entries) || got[0] != entries[0] || got[1] != entries[1] {
		t.Fatalf("decoded entries mismatch: %+v", got)
	}
}

func TestRDMAWriteCommitResultEncodeDecode(t *testing.T) {
	results := []RDMAWriteCommitResult{
		{Offset: 4, Size: 8, Status: 0},
		{Offset: 12, Size: 16, Status: -5},
	}
	encoded := EncodeRDMAWriteCommitResults(results)
	if len(encoded) != len(results)*RDMAWriteCommitResultSize {
		t.Fatalf("encoded length = %d", len(encoded))
	}
	got, err := DecodeRDMAWriteCommitResults(encoded)
	if err != nil {
		t.Fatalf("DecodeRDMAWriteCommitResults: %v", err)
	}
	if len(got) != len(results) || got[0] != results[0] || got[1] != results[1] {
		t.Fatalf("decoded results mismatch: %+v", got)
	}
}
