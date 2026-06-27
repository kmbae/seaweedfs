package swvfsdaemon

import (
	"context"
	"testing"

	"seaweedfs-rdma-sidecar/pkg/swvfsproto"
)

type fakeTestMRControl struct {
	allocLength uint32
	written     []byte
	info        swvfsproto.RDMATestMR
}

func (f *fakeTestMRControl) TestMRAlloc(length uint32, pattern uint32) (swvfsproto.RDMATestMR, error) {
	f.allocLength = length
	f.info = swvfsproto.RDMATestMR{
		ABIVersion: swvfsproto.RDMAABIVersion,
		Flags:      swvfsproto.RDMATestFAllocated | swvfsproto.RDMATestFRegisteredMR,
		RemoteAddr: 0xfeed,
		RKey:       77,
		Length:     length,
	}
	return f.info, nil
}

func (f *fakeTestMRControl) TestMRInfo() (swvfsproto.RDMATestMR, error) {
	return f.info, nil
}

func (f *fakeTestMRControl) TestMRWrite(data []byte) (swvfsproto.RDMATestMR, error) {
	f.written = append([]byte(nil), data...)
	f.info.UserLength = uint32(len(data))
	return f.info, nil
}

func TestKernelMRReadStagerStagesDataIntoKernelMR(t *testing.T) {
	control := &fakeTestMRControl{}
	reader := &fakeFileBackend{}
	stager := &KernelMRReadStager{Control: control, Reader: reader}

	desc, attr, err := stager.StageReadRDMA(context.Background(), "/file", 0, 4)
	if err != nil {
		t.Fatalf("StageReadRDMA: %v", err)
	}
	if desc.RemoteAddr != 0xfeed || desc.RKey != 77 || desc.Length != 4 {
		t.Fatalf("descriptor mismatch: %+v", desc)
	}
	if string(control.written) != "data" || control.allocLength != 4 {
		t.Fatalf("staged data mismatch: data=%q alloc=%d", control.written, control.allocLength)
	}
	if attr == nil || attr.Ino != 1 || reader.readPreferRDMA {
		t.Fatalf("reader contract mismatch: attr=%+v prefer=%v", attr, reader.readPreferRDMA)
	}
}
