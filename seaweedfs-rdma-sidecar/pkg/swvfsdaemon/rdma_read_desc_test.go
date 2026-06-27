package swvfsdaemon

import (
	"context"
	"testing"

	"seaweedfs-rdma-sidecar/pkg/swvfsproto"
)

type fakeTestMRControl struct {
	allocLength  uint32
	writeSession uint64
	freeSession  uint64
	written      []byte
	info         swvfsproto.RDMATestMR
}

func (f *fakeTestMRControl) TestMRAlloc(length uint32, pattern uint32) (swvfsproto.RDMATestMR, error) {
	f.allocLength = length
	f.info = swvfsproto.RDMATestMR{
		ABIVersion: swvfsproto.RDMAABIVersion,
		Flags:      swvfsproto.RDMATestFAllocated | swvfsproto.RDMATestFRegisteredMR,
		RemoteAddr: 0xfeed,
		RKey:       77,
		Length:     length,
		SessionID:  42,
	}
	return f.info, nil
}

func (f *fakeTestMRControl) TestMRInfo(sessionID uint64) (swvfsproto.RDMATestMR, error) {
	f.info.SessionID = sessionID
	return f.info, nil
}

func (f *fakeTestMRControl) TestMRWrite(sessionID uint64, data []byte) (swvfsproto.RDMATestMR, error) {
	f.writeSession = sessionID
	f.written = append([]byte(nil), data...)
	f.info.UserLength = uint32(len(data))
	return f.info, nil
}

func (f *fakeTestMRControl) TestMRFree(sessionID uint64) error {
	f.freeSession = sessionID
	return nil
}

func TestKernelMRReadStagerStagesDataIntoKernelMR(t *testing.T) {
	control := &fakeTestMRControl{}
	reader := &fakeFileBackend{}
	stager := &KernelMRReadStager{Control: control, Reader: reader}

	lease, err := stager.StageReadRDMA(context.Background(), "/file", 0, 4)
	if err != nil {
		t.Fatalf("StageReadRDMA: %v", err)
	}
	desc := lease.Desc
	if desc.RemoteAddr != 0xfeed || desc.RKey != 77 || desc.Length != 4 {
		t.Fatalf("descriptor mismatch: %+v", desc)
	}
	if lease.SessionID != 42 || desc.Reserved[0] != 42 || control.writeSession != 42 {
		t.Fatalf("session mismatch: lease=%d desc=%d write=%d", lease.SessionID, desc.Reserved[0], control.writeSession)
	}
	if string(control.written) != "data" || control.allocLength != 4 {
		t.Fatalf("staged data mismatch: data=%q alloc=%d", control.written, control.allocLength)
	}
	if lease.Attr == nil || lease.Attr.Ino != 1 || reader.readPreferRDMA {
		t.Fatalf("reader contract mismatch: attr=%+v prefer=%v", lease.Attr, reader.readPreferRDMA)
	}
	if err := stager.ReleaseReadRDMA(context.Background(), lease.SessionID); err != nil {
		t.Fatalf("ReleaseReadRDMA: %v", err)
	}
	if control.freeSession != 42 {
		t.Fatalf("free session = %d, want 42", control.freeSession)
	}
}
