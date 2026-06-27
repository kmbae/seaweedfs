package swvfsdaemon

import (
	"testing"
	"unsafe"

	"seaweedfs-rdma-sidecar/pkg/swvfsproto"
)

func TestRDMAIoctlNumbersMatchKernelABI(t *testing.T) {
	if got, want := ioctlRDMAGetLocal, uintptr(0x81187701); got != want {
		t.Fatalf("ioctlRDMAGetLocal = %#x, want %#x", got, want)
	}
	if got, want := ioctlRDMAConnect, uintptr(0x40707702); got != want {
		t.Fatalf("ioctlRDMAConnect = %#x, want %#x", got, want)
	}
}

func TestRDMAIoctlStructSizes(t *testing.T) {
	if got := unsafe.Sizeof(swvfsproto.RDMALocalInfo{}); got != 280 {
		t.Fatalf("local info size = %d", got)
	}
	if got := unsafe.Sizeof(swvfsproto.RDMARemoteInfo{}); got != 112 {
		t.Fatalf("remote info size = %d", got)
	}
}
