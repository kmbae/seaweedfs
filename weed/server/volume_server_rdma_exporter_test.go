package weed_server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

type fakeVolumeRdmaNeedleReader struct {
	volumeID needle.VolumeId
	needleID types.NeedleId
	cookie   types.Cookie
	offset   int64
	size     int64
	data     []byte
	err      error
}

func (r *fakeVolumeRdmaNeedleReader) ReadVolumeNeedleDataInto(volumeID needle.VolumeId, n *needle.Needle, readOption *storage.ReadOption, writer io.Writer, offset int64, size int64) error {
	r.volumeID = volumeID
	r.needleID = n.Id
	r.cookie = n.Cookie
	r.offset = offset
	r.size = size
	if r.err != nil {
		return r.err
	}
	if _, err := writer.Write(r.data); err != nil {
		return err
	}
	return nil
}

type fakeVolumeRdmaRegistrar struct {
	data     []byte
	desc     VolumeRdmaDataDesc
	err      error
	released bool
}

func (r *fakeVolumeRdmaRegistrar) RegisterReadBuffer(ctx context.Context, data []byte) (VolumeRdmaRegisteredBuffer, error) {
	r.data = append([]byte(nil), data...)
	if r.err != nil {
		return nil, r.err
	}
	return &fakeVolumeRdmaRegisteredBuffer{registrar: r, desc: r.desc}, nil
}

type fakeVolumeRdmaRegisteredBuffer struct {
	registrar *fakeVolumeRdmaRegistrar
	desc      VolumeRdmaDataDesc
}

func (b *fakeVolumeRdmaRegisteredBuffer) Descriptor() VolumeRdmaDataDesc {
	return b.desc
}

func (b *fakeVolumeRdmaRegisteredBuffer) Release(ctx context.Context) error {
	b.registrar.released = true
	return nil
}

func TestVolumeStoreRdmaReadExporterPrepareAndRelease(t *testing.T) {
	reader := &fakeVolumeRdmaNeedleReader{data: []byte("needle-range")}
	registrar := &fakeVolumeRdmaRegistrar{
		desc: VolumeRdmaDataDesc{
			RemoteAddr: 0xfeed,
			RKey:       0,
			Length:     4096,
		},
	}
	exporter := newVolumeStoreRdmaReadExporter(reader, registrar, VolumeRdmaReadExporterConfig{
		MaxSize:  4096,
		LeaseTTL: time.Minute,
	})

	lease, err := exporter.PrepareRead(context.Background(), VolumeRdmaReadRequest{
		FileID:   "3,01637037d6",
		VolumeID: 3,
		NeedleID: 123,
		Cookie:   456,
		Offset:   7,
		Size:     2048,
	})
	if err != nil {
		t.Fatalf("PrepareRead: %v", err)
	}
	if reader.volumeID != 3 || reader.needleID != types.Uint64ToNeedleId(123) || reader.cookie != types.Uint32ToCookie(456) {
		t.Fatalf("unexpected reader request: volume=%d needle=%d cookie=%d", reader.volumeID, reader.needleID, reader.cookie)
	}
	if reader.offset != 7 || reader.size != 2048 {
		t.Fatalf("unexpected range offset=%d size=%d", reader.offset, reader.size)
	}
	if !bytes.Equal(registrar.data, []byte("needle-range")) {
		t.Fatalf("registered data = %q", registrar.data)
	}
	if lease.SessionID == 0 || lease.Desc.Reserved[0] != lease.SessionID {
		t.Fatalf("lease session not encoded: %+v", lease)
	}
	if lease.Desc.RemoteAddr != 0xfeed || lease.Desc.RKey != 0 || lease.Desc.Length != uint32(len("needle-range")) {
		t.Fatalf("unexpected descriptor: %+v", lease.Desc)
	}

	if err := exporter.ReleaseRead(context.Background(), lease.SessionID); err != nil {
		t.Fatalf("ReleaseRead: %v", err)
	}
	if !registrar.released {
		t.Fatalf("registered buffer was not released")
	}
}

func TestVolumeStoreRdmaReadExporterRejectsOversizedRead(t *testing.T) {
	exporter := newVolumeStoreRdmaReadExporter(
		&fakeVolumeRdmaNeedleReader{data: []byte("x")},
		&fakeVolumeRdmaRegistrar{},
		VolumeRdmaReadExporterConfig{MaxSize: 4},
	)

	_, err := exporter.PrepareRead(context.Background(), VolumeRdmaReadRequest{
		VolumeID: 1,
		NeedleID: 2,
		Cookie:   3,
		Size:     5,
	})
	if !errors.Is(err, ErrVolumeRdmaReadTooLarge) {
		t.Fatalf("err = %v, want ErrVolumeRdmaReadTooLarge", err)
	}
}

func TestVolumeStoreRdmaReadExporterReleasesUnexportableBuffer(t *testing.T) {
	registrar := &fakeVolumeRdmaRegistrar{
		desc: VolumeRdmaDataDesc{RemoteAddr: 0, RKey: 77, Length: 4},
	}
	exporter := newVolumeStoreRdmaReadExporter(
		&fakeVolumeRdmaNeedleReader{data: []byte("data")},
		registrar,
		VolumeRdmaReadExporterConfig{MaxSize: 4},
	)

	_, err := exporter.PrepareRead(context.Background(), VolumeRdmaReadRequest{
		VolumeID: 1,
		NeedleID: 2,
		Cookie:   3,
		Size:     4,
	})
	if !errors.Is(err, ErrVolumeRdmaReadNotExportable) {
		t.Fatalf("err = %v, want ErrVolumeRdmaReadNotExportable", err)
	}
	if !registrar.released {
		t.Fatalf("unexportable buffer was not released")
	}
}
