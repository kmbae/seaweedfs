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
