package swvfsdaemon

import (
	"context"
	"testing"

	"seaweedfs-rdma-sidecar/pkg/swvfsproto"
)

type fakeTestMRControl struct {
	allocLength  uint32
	allocCount   int
	writeSession uint64
	readSession  uint64
	freeSession  uint64
	written      []byte
	readData     []byte
	info         swvfsproto.RDMATestMR
}

func (f *fakeTestMRControl) TestMRAlloc(length uint32, pattern uint32) (swvfsproto.RDMATestMR, error) {
	f.allocLength = length
	f.allocCount++
	f.info = swvfsproto.RDMATestMR{
		ABIVersion: swvfsproto.RDMAABIVersion,
		Flags:      swvfsproto.RDMATestFAllocated | swvfsproto.RDMATestFRegisteredMR,
		RemoteAddr: 0xfeed,
		RKey:       77,
		Length:     length,
		SessionID:  uint64(41 + f.allocCount),
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

func (f *fakeTestMRControl) TestMRRead(sessionID uint64, length uint32) ([]byte, swvfsproto.RDMATestMR, error) {
	f.readSession = sessionID
	data := f.readData
	if data == nil {
		data = f.written
	}
	if uint32(len(data)) > length {
		data = data[:length]
	}
	f.info.UserLength = uint32(len(data))
	return append([]byte(nil), data...), f.info, nil
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

func TestKernelMRReadStagerReusesPoolSessions(t *testing.T) {
	control := &fakeTestMRControl{}
	reader := &fakeFileBackend{}
	pool := NewKernelMRPool(control, nil, KernelMRPoolConfig{MaxIdle: 2, MaxBytes: 2 << 20, MinBytes: 1024})
	stager := &KernelMRReadStager{Control: control, Reader: reader, Pool: pool}

	first, err := stager.StageReadRDMA(context.Background(), "/file", 0, 4)
	if err != nil {
		t.Fatalf("first StageReadRDMA: %v", err)
	}
	if err := stager.ReleaseReadRDMA(context.Background(), first.SessionID); err != nil {
		t.Fatalf("first ReleaseReadRDMA: %v", err)
	}
	second, err := stager.StageReadRDMA(context.Background(), "/file", 0, 4)
	if err != nil {
		t.Fatalf("second StageReadRDMA: %v", err)
	}
	if second.SessionID != first.SessionID {
		t.Fatalf("expected pooled session reuse, first=%d second=%d", first.SessionID, second.SessionID)
	}
	if control.allocCount != 1 {
		t.Fatalf("alloc count = %d, want 1", control.allocCount)
	}
}

func TestKernelMRWriteStagerCommitsKernelMRData(t *testing.T) {
	control := &fakeTestMRControl{readData: []byte("wxyz")}
	writer := &fakeFileBackend{}
	stager := &KernelMRWriteStager{Control: control, Writer: writer}

	desc, _, err := stager.PrepareWriteRDMA(context.Background(), "/file", 8, 4)
	if err != nil {
		t.Fatalf("PrepareWriteRDMA: %v", err)
	}
	if desc.RemoteAddr != 0xfeed || desc.RKey != 77 || desc.Length != 4 || desc.Reserved[0] != 42 {
		t.Fatalf("descriptor mismatch: %+v", desc)
	}
	attr, err := stager.CommitWriteRDMASession(context.Background(), desc.Reserved[0], "/file", 8, 4)
	if err != nil {
		t.Fatalf("CommitWriteRDMASession: %v", err)
	}
	if attr == nil || writer.writePath != "/file" || writer.writeOffset != 8 || string(writer.writeData) != "wxyz" || !writer.writePreferRDMA {
		t.Fatalf("write backend mismatch: attr=%+v path=%q off=%d data=%q prefer=%v", attr, writer.writePath, writer.writeOffset, writer.writeData, writer.writePreferRDMA)
	}
	if control.readSession != 42 || control.freeSession != 42 {
		t.Fatalf("session lifecycle mismatch: read=%d free=%d", control.readSession, control.freeSession)
	}
}

type fakeFlushFileBackend struct {
	fakeFileBackend
	flushedPath string
}

func (f *fakeFlushFileBackend) FlushFile(ctx context.Context, path string) (*swvfsproto.Attr, error) {
	f.flushedPath = path
	return &swvfsproto.Attr{Ino: 7, Size: uint64(len(f.writeData)), Mode: 0100644, Nlink: 1}, nil
}

func TestKernelMRWriteStagerFlushesRemoteBufferedWrite(t *testing.T) {
	control := &fakeTestMRControl{readData: []byte("flush-me")}
	writer := &fakeFlushFileBackend{}
	stager := &KernelMRWriteStager{Control: control, Writer: writer}

	desc, _, err := stager.PrepareWriteRDMA(context.Background(), "/file", 0, 8)
	if err != nil {
		t.Fatalf("PrepareWriteRDMA: %v", err)
	}
	attr, err := stager.CommitWriteRDMASession(context.Background(), desc.Reserved[0], "/file", 0, 8)
	if err != nil {
		t.Fatalf("CommitWriteRDMASession: %v", err)
	}
	if writer.flushedPath != "/file" || attr == nil || attr.Ino != 7 {
		t.Fatalf("flush mismatch: path=%q attr=%+v", writer.flushedPath, attr)
	}
}
