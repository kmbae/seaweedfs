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
	info.SetConnectionID(44)
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
	if got := info.ConnectionID(); got != 44 || info.Reserved[RDMALocalConnectionIDIndex] != 44 {
		t.Fatalf("ConnectionID = %d reserved=%d", got, info.Reserved[RDMALocalConnectionIDIndex])
	}
}

func TestRDMADescriptorFieldHelpers(t *testing.T) {
	var remote RDMARemoteInfo
	remote.SetConnectionID(55)
	if got := remote.ConnectionID(); got != 55 || remote.Reserved[RDMARemoteConnectionIDIndex] != 55 {
		t.Fatalf("remote connection id = %d reserved=%d", got, remote.Reserved[RDMARemoteConnectionIDIndex])
	}

	var desc RDMADataDesc
	desc.SetLeaseID(11)
	desc.SetConnectionID(22)
	desc.SetFileOffset(33)
	if desc.LeaseID() != 11 || desc.ConnectionID() != 22 || desc.FileOffset() != 33 {
		t.Fatalf("desc helpers returned lease=%d conn=%d off=%d", desc.LeaseID(), desc.ConnectionID(), desc.FileOffset())
	}
	if desc.Reserved != [4]uint64{11, 22, 33, 0} {
		t.Fatalf("desc reserved fields = %#v", desc.Reserved)
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

func TestRDMADataDescArrayEncodeDecode(t *testing.T) {
	descs := []RDMADataDesc{
		{RemoteAddr: 0x1000, RKey: 1, Length: 4096, Reserved: [4]uint64{11, 21, 0, 0}},
		{RemoteAddr: 0x2000, RKey: 2, Length: 8192, Reserved: [4]uint64{12, 22, 4096, 0}},
	}
	encoded := EncodeRDMADataDescs(descs)
	if len(encoded) != len(descs)*RDMADataDescSize {
		t.Fatalf("encoded length = %d", len(encoded))
	}
	got, err := DecodeRDMADataDescs(encoded)
	if err != nil {
		t.Fatalf("DecodeRDMADataDescs: %v", err)
	}
	if len(got) != len(descs) || got[0] != descs[0] || got[1] != descs[1] {
		t.Fatalf("decoded descs mismatch: %+v", got)
	}
	if _, err := DecodeRDMADataDescs(encoded[:len(encoded)-1]); err == nil {
		t.Fatal("short descriptor array decoded successfully")
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
