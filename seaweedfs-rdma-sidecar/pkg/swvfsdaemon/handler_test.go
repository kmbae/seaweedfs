package swvfsdaemon

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"seaweedfs-rdma-sidecar/pkg/swvfsproto"
)

type fakeFileBackend struct {
	readPreferRDMA  bool
	writePreferRDMA bool
	writePath       string
	writeOffset     uint64
	writeData       []byte
	renamedOld      string
	renamedNew      string
	linkedOld       string
	linkedNew       string
	symlinkPath     string
	symlinkTarget   string
	mknodPath       string
	mknodMode       uint32
	mknodRdev       uint32
}

type fakeXAttrBackend struct {
	fakeFileBackend
	setPath   string
	setName   string
	setValue  []byte
	setFlags  uint32
	setRemove bool
}

type fakeRDMAFileBackend struct {
	fakeFileBackend
	readDesc         *swvfsproto.RDMADataDesc
	readDescPath     string
	readDescOffset   uint64
	readDescSize     uint64
	writeDesc        *swvfsproto.RDMADataDesc
	preparePath      string
	prepareOffset    uint64
	prepareSize      uint64
	commitPath       string
	commitOffset     uint64
	commitSize       uint64
	batchPath        string
	batchEntries     []swvfsproto.RDMAWriteCommitEntry
	releaseLeaseID   uint64
	releaseStatus    int32
	releaseBytes     uint64
	readDescFallback bool
	readDescErr      error
}

func (f *fakeFileBackend) ReadFile(ctx context.Context, path string, offset, size uint64, preferRDMA bool) ([]byte, *swvfsproto.Attr, error) {
	f.readPreferRDMA = preferRDMA
	return []byte("data"), &swvfsproto.Attr{Ino: 1, Size: 4, Mode: 0100644, Nlink: 1}, nil
}

func (f *fakeFileBackend) WriteFile(ctx context.Context, path string, offset uint64, data []byte, mode, uid, gid uint32, preferRDMA bool) (*swvfsproto.Attr, error) {
	f.writePreferRDMA = preferRDMA
	f.writePath = path
	f.writeOffset = offset
	f.writeData = append([]byte(nil), data...)
	return &swvfsproto.Attr{Ino: 1, Size: uint64(len(data)), Mode: 0100644, Nlink: 1}, nil
}

func (f *fakeRDMAFileBackend) ReadFileRDMA(ctx context.Context, path string, offset, size uint64) (*swvfsproto.RDMADataDesc, *swvfsproto.Attr, error) {
	f.readDescPath = path
	f.readDescOffset = offset
	f.readDescSize = size
	if f.readDescErr != nil {
		return nil, nil, f.readDescErr
	}
	if f.readDescFallback {
		return nil, nil, ErrnoError{Errno: ErrnoNoSys, Msg: "rdma read desc unavailable"}
	}
	return f.readDesc, &swvfsproto.Attr{Ino: 9, Size: size, Mode: 0100644, Nlink: 1}, nil
}

func (f *fakeRDMAFileBackend) PrepareWriteRDMA(ctx context.Context, path string, offset, size uint64) (*swvfsproto.RDMADataDesc, *swvfsproto.Attr, error) {
	f.preparePath = path
	f.prepareOffset = offset
	f.prepareSize = size
	return f.writeDesc, &swvfsproto.Attr{Ino: 10, Size: offset + size, Mode: 0100644, Nlink: 1}, nil
}

func (f *fakeRDMAFileBackend) CommitWriteRDMA(ctx context.Context, path string, offset, size uint64) (*swvfsproto.Attr, error) {
	f.commitPath = path
	f.commitOffset = offset
	f.commitSize = size
	return &swvfsproto.Attr{Ino: 10, Size: offset + size, Mode: 0100644, Nlink: 1}, nil
}

func (f *fakeRDMAFileBackend) CommitWriteRDMABatch(ctx context.Context, path string, entries []swvfsproto.RDMAWriteCommitEntry) ([]swvfsproto.RDMAWriteCommitResult, *swvfsproto.Attr, error) {
	f.batchPath = path
	f.batchEntries = append([]swvfsproto.RDMAWriteCommitEntry(nil), entries...)
	results := make([]swvfsproto.RDMAWriteCommitResult, len(entries))
	var attr *swvfsproto.Attr
	for i, entry := range entries {
		results[i] = swvfsproto.RDMAWriteCommitResult{Offset: entry.Offset, Size: entry.Size}
		attr = &swvfsproto.Attr{Ino: 10, Size: entry.Offset + entry.Size, Mode: 0100644, Nlink: 1}
	}
	return results, attr, nil
}

func (f *fakeRDMAFileBackend) ReleaseReadDescriptor(ctx context.Context, leaseID uint64, status int32, bytes uint64) error {
	f.releaseLeaseID = leaseID
	f.releaseStatus = status
	f.releaseBytes = bytes
	return nil
}

func (f *fakeFileBackend) StatFS(ctx context.Context, path string) (*swvfsproto.StatFS, error) {
	return &swvfsproto.StatFS{Blocks: 100, Bfree: 80, Bavail: 80, Files: 1000, Ffree: 900, Bsize: 4096, Namelen: 255}, nil
}

func (f *fakeFileBackend) RenameEntry(ctx context.Context, oldPath, newPath string) error {
	f.renamedOld = oldPath
	f.renamedNew = newPath
	return nil
}

func (f *fakeFileBackend) LinkEntry(ctx context.Context, oldPath, newPath string) (*swvfsproto.Attr, error) {
	f.linkedOld = oldPath
	f.linkedNew = newPath
	return &swvfsproto.Attr{Ino: 1, Size: 4, Mode: 0100644, Nlink: 2}, nil
}

func (f *fakeFileBackend) Symlink(ctx context.Context, linkPath, target string, uid, gid uint32) (*swvfsproto.Attr, error) {
	f.symlinkPath = linkPath
	f.symlinkTarget = target
	return &swvfsproto.Attr{Ino: 2, Size: uint64(len(target)), Mode: uint32(syscall.S_IFLNK | 0777), UID: uid, GID: gid, Nlink: 1}, nil
}

func (f *fakeFileBackend) ReadLink(ctx context.Context, linkPath string) ([]byte, error) {
	return []byte("target"), nil
}

func (f *fakeFileBackend) Mknod(ctx context.Context, path string, mode, uid, gid, rdev uint32) (*swvfsproto.Attr, error) {
	f.mknodPath = path
	f.mknodMode = mode
	f.mknodRdev = rdev
	return &swvfsproto.Attr{Ino: 3, Mode: mode, UID: uid, GID: gid, Rdev: rdev, Nlink: 1}, nil
}

func (f *fakeXAttrBackend) GetXAttr(ctx context.Context, path, name string) ([]byte, error) {
	if name == "user.exists" {
		return []byte("value"), nil
	}
	return nil, ErrnoError{Errno: ErrnoNoData, Msg: "xattr not found"}
}

func (f *fakeXAttrBackend) ListXAttr(ctx context.Context, path string) ([]byte, error) {
	return []byte("user.exists\x00"), nil
}

func (f *fakeXAttrBackend) SetXAttr(ctx context.Context, path, name string, value []byte, flags uint32, remove bool) error {
	f.setPath = path
	f.setName = name
	f.setValue = append([]byte(nil), value...)
	f.setFlags = flags
	f.setRemove = remove
	return nil
}

func TestHandlerPassesReadHint(t *testing.T) {
	backend := &fakeFileBackend{}
	h := &Handler{Backend: backend}
	req := &swvfsproto.Request{Header: swvfsproto.RequestHeader{Tag: 1, Op: swvfsproto.OpRead, Valid: swvfsproto.ReadFRDMAPreferred}, Path1: "/x"}
	reply, err := h.Handle(context.Background(), req)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if !backend.readPreferRDMA {
		t.Fatal("read hint was not passed to backend")
	}
	if string(reply.Data) != "data" {
		t.Fatalf("reply data = %q", reply.Data)
	}
}

func TestHandlerPassesWriteHint(t *testing.T) {
	backend := &fakeFileBackend{}
	h := &Handler{Backend: backend}
	req := &swvfsproto.Request{Header: swvfsproto.RequestHeader{Tag: 1, Op: swvfsproto.OpWrite, Valid: swvfsproto.WriteFRDMAPreferred}, Path1: "/x", Data: []byte("abc")}
	reply, err := h.Handle(context.Background(), req)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if !backend.writePreferRDMA {
		t.Fatal("write hint was not passed to backend")
	}
	if reply.Attr.Size != 3 {
		t.Fatalf("reply size = %d", reply.Attr.Size)
	}
}

func TestHandlerReturnsRDMAReadDescriptor(t *testing.T) {
	backend := &fakeRDMAFileBackend{
		readDesc: &swvfsproto.RDMADataDesc{RemoteAddr: 0x1000, RKey: 7, Length: 4096},
	}
	h := &Handler{Backend: backend}
	req := &swvfsproto.Request{
		Header: swvfsproto.RequestHeader{Tag: 3, Op: swvfsproto.OpRead, Offset: 128, Size: 4096, Valid: swvfsproto.ReadFRDMAPreferred},
		Path1:  "/rdma-file",
	}

	reply, err := h.Handle(context.Background(), req)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if reply.EOF&swvfsproto.ReplyFRDMAReadDesc == 0 {
		t.Fatalf("RDMA read desc flag not set: eof=0x%x", reply.EOF)
	}
	desc, err := swvfsproto.DecodeRDMADataDesc(reply.Data)
	if err != nil {
		t.Fatalf("DecodeRDMADataDesc: %v", err)
	}
	if desc.RemoteAddr != 0x1000 || desc.RKey != 7 || desc.Length != 4096 {
		t.Fatalf("descriptor mismatch: %+v", desc)
	}
	if backend.readDescPath != "/rdma-file" || reply.Attr.Ino != 9 {
		t.Fatalf("read desc backend/attr mismatch: path=%q attr=%+v", backend.readDescPath, reply.Attr)
	}
}

func TestHandlerRDMAReadPrepareReturnsDescriptorOnly(t *testing.T) {
	backend := &fakeRDMAFileBackend{
		readDesc: &swvfsproto.RDMADataDesc{RemoteAddr: 0x3000, RKey: 17, Length: 8192},
	}
	h := &Handler{Backend: backend}
	req := &swvfsproto.Request{
		Header: swvfsproto.RequestHeader{Tag: 7, Op: swvfsproto.OpRDMAReadPrepare, Offset: 256, Size: 8192},
		Path1:  "/direct-rdma-file",
	}

	reply, err := h.Handle(context.Background(), req)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if reply.EOF&swvfsproto.ReplyFRDMAReadDesc == 0 {
		t.Fatalf("RDMA read desc flag not set: eof=0x%x", reply.EOF)
	}
	desc, err := swvfsproto.DecodeRDMADataDesc(reply.Data)
	if err != nil {
		t.Fatalf("DecodeRDMADataDesc: %v", err)
	}
	if desc.RemoteAddr != 0x3000 || desc.RKey != 17 || desc.Length != 8192 {
		t.Fatalf("descriptor mismatch: %+v", desc)
	}
	if len(reply.Data) != swvfsproto.RDMADataDescSize {
		t.Fatalf("direct read prepare returned payload bytes: len=%d", len(reply.Data))
	}
	if backend.readDescPath != "/direct-rdma-file" || backend.readDescOffset != 256 || backend.readDescSize != 8192 {
		t.Fatalf("read desc backend mismatch: path=%q off=%d size=%d", backend.readDescPath, backend.readDescOffset, backend.readDescSize)
	}
}

func TestHandlerRDMAReadPrepareDoesNotFallbackToPayload(t *testing.T) {
	backend := &fakeRDMAFileBackend{readDescFallback: true}
	h := &Handler{Backend: backend}
	_, err := h.Handle(context.Background(), &swvfsproto.Request{
		Header: swvfsproto.RequestHeader{Tag: 8, Op: swvfsproto.OpRDMAReadPrepare, Size: 4096},
		Path1:  "/plain-file",
	})
	if err == nil {
		t.Fatal("expected direct RDMA read prepare to fail without payload fallback")
	}
	var errno ErrnoError
	if !errors.As(err, &errno) || errno.Errno != ErrnoNoSys {
		t.Fatalf("expected ENOSYS fallback signal, got %v", err)
	}
	if backend.readPreferRDMA {
		t.Fatal("direct RDMA read prepare unexpectedly fell back to payload read")
	}
}

func TestHandlerSkipsRDMAReadDescriptorBelowMinSize(t *testing.T) {
	backend := &fakeRDMAFileBackend{
		readDesc: &swvfsproto.RDMADataDesc{RemoteAddr: 0x1000, RKey: 7, Length: 4096},
	}
	h := &Handler{Backend: backend, ReadRDMAMinSize: 8192}
	req := &swvfsproto.Request{
		Header: swvfsproto.RequestHeader{Tag: 3, Op: swvfsproto.OpRead, Offset: 128, Size: 4096, Valid: swvfsproto.ReadFRDMAPreferred},
		Path1:  "/small-file",
	}

	reply, err := h.Handle(context.Background(), req)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if reply.EOF&swvfsproto.ReplyFRDMAReadDesc != 0 {
		t.Fatalf("unexpected RDMA desc flag: eof=0x%x", reply.EOF)
	}
	if backend.readDescPath != "" {
		t.Fatalf("read descriptor backend was called for a small read: %q", backend.readDescPath)
	}
	if string(reply.Data) != "data" || !backend.readPreferRDMA {
		t.Fatalf("fallback read mismatch: data=%q prefer=%v", reply.Data, backend.readPreferRDMA)
	}
}

func TestHandlerFallsBackWhenRDMAReadDescriptorUnsupported(t *testing.T) {
	backend := &fakeRDMAFileBackend{readDescFallback: true}
	h := &Handler{Backend: backend}
	req := &swvfsproto.Request{
		Header: swvfsproto.RequestHeader{Tag: 3, Op: swvfsproto.OpRead, Size: 4, Valid: swvfsproto.ReadFRDMAPreferred},
		Path1:  "/plain-file",
	}

	reply, err := h.Handle(context.Background(), req)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if reply.EOF&swvfsproto.ReplyFRDMAReadDesc != 0 {
		t.Fatalf("unexpected RDMA desc flag: eof=0x%x", reply.EOF)
	}
	if string(reply.Data) != "data" || !backend.readPreferRDMA {
		t.Fatalf("fallback read mismatch: data=%q prefer=%v", reply.Data, backend.readPreferRDMA)
	}
}

func TestHandlerFallsBackWhenRDMAReadDescriptorTooLarge(t *testing.T) {
	backend := &fakeRDMAFileBackend{
		readDescErr: ErrnoError{Errno: ErrnoTooLarge, Msg: "rdma read desc too large"},
	}
	h := &Handler{Backend: backend}
	req := &swvfsproto.Request{
		Header: swvfsproto.RequestHeader{Tag: 3, Op: swvfsproto.OpRead, Size: swvfsproto.RDMAIOMax + 1, Valid: swvfsproto.ReadFRDMAPreferred},
		Path1:  "/large-file",
	}

	reply, err := h.Handle(context.Background(), req)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if reply.EOF&swvfsproto.ReplyFRDMAReadDesc != 0 {
		t.Fatalf("unexpected RDMA desc flag: eof=0x%x", reply.EOF)
	}
	if string(reply.Data) != "data" || !backend.readPreferRDMA {
		t.Fatalf("fallback read mismatch: data=%q prefer=%v", reply.Data, backend.readPreferRDMA)
	}
}

func TestHandlerRDMAWritePrepareCommit(t *testing.T) {
	backend := &fakeRDMAFileBackend{
		writeDesc: &swvfsproto.RDMADataDesc{RemoteAddr: 0x2000, RKey: 11, Length: 8192},
	}
	h := &Handler{Backend: backend}

	prepare, err := h.Handle(context.Background(), &swvfsproto.Request{
		Header: swvfsproto.RequestHeader{Tag: 4, Op: swvfsproto.OpWriteRDMAPrepare, Offset: 512, Size: 8192},
		Path1:  "/write-file",
	})
	if err != nil {
		t.Fatalf("prepare Handle: %v", err)
	}
	if prepare.EOF&swvfsproto.ReplyFRDMAWriteDesc == 0 {
		t.Fatalf("RDMA write desc flag not set: eof=0x%x", prepare.EOF)
	}
	desc, err := swvfsproto.DecodeRDMADataDesc(prepare.Data)
	if err != nil {
		t.Fatalf("DecodeRDMADataDesc: %v", err)
	}
	if desc.RemoteAddr != 0x2000 || desc.RKey != 11 || desc.Length != 8192 {
		t.Fatalf("write descriptor mismatch: %+v", desc)
	}
	if backend.preparePath != "/write-file" || backend.prepareOffset != 512 || backend.prepareSize != 8192 {
		t.Fatalf("prepare backend mismatch: path=%q off=%d size=%d", backend.preparePath, backend.prepareOffset, backend.prepareSize)
	}

	commit, err := h.Handle(context.Background(), &swvfsproto.Request{
		Header: swvfsproto.RequestHeader{Tag: 5, Op: swvfsproto.OpWriteRDMACommit, Offset: 512, Size: 8192},
		Path1:  "/write-file",
	})
	if err != nil {
		t.Fatalf("commit Handle: %v", err)
	}
	if commit.Attr.Size != 8704 || backend.commitPath != "/write-file" || backend.commitOffset != 512 || backend.commitSize != 8192 {
		t.Fatalf("commit mismatch: attr=%+v path=%q off=%d size=%d", commit.Attr, backend.commitPath, backend.commitOffset, backend.commitSize)
	}
}

func TestHandlerRDMAWriteCommitBatch(t *testing.T) {
	backend := &fakeRDMAFileBackend{}
	h := &Handler{Backend: backend}
	entries := []swvfsproto.RDMAWriteCommitEntry{
		{Offset: 0, Size: 4096},
		{Offset: 4096, Size: 4096},
	}

	reply, err := h.Handle(context.Background(), &swvfsproto.Request{
		Header: swvfsproto.RequestHeader{Tag: 7, Op: swvfsproto.OpWriteRDMACommitBatch},
		Path1:  "/write-file",
		Data:   swvfsproto.EncodeRDMAWriteCommitEntries(entries),
	})
	if err != nil {
		t.Fatalf("batch Handle: %v", err)
	}
	results, err := swvfsproto.DecodeRDMAWriteCommitResults(reply.Data)
	if err != nil {
		t.Fatalf("DecodeRDMAWriteCommitResults: %v", err)
	}
	if reply.Tag != 7 || reply.Attr.Size != 8192 {
		t.Fatalf("reply mismatch: %+v", reply)
	}
	if backend.batchPath != "/write-file" || len(backend.batchEntries) != len(entries) || backend.batchEntries[1] != entries[1] {
		t.Fatalf("backend batch mismatch: path=%q entries=%+v", backend.batchPath, backend.batchEntries)
	}
	if len(results) != len(entries) || results[0].Status != 0 || results[1].Offset != entries[1].Offset {
		t.Fatalf("batch results mismatch: %+v", results)
	}
}

func TestHandlerSkipsRDMAWritePrepareBelowMinSize(t *testing.T) {
	backend := &fakeRDMAFileBackend{
		writeDesc: &swvfsproto.RDMADataDesc{RemoteAddr: 0x2000, RKey: 11, Length: 4096},
	}
	h := &Handler{Backend: backend, WriteRDMAMinSize: 8192}

	_, err := h.Handle(context.Background(), &swvfsproto.Request{
		Header: swvfsproto.RequestHeader{Tag: 4, Op: swvfsproto.OpWriteRDMAPrepare, Offset: 512, Size: 4096},
		Path1:  "/small-write",
	})
	if err == nil {
		t.Fatal("expected a fallback-capable error")
	}
	var errno ErrnoError
	if !errors.As(err, &errno) || errno.Errno != ErrnoNoSys {
		t.Fatalf("expected ENOSYS fallback error, got %v", err)
	}
	if backend.preparePath != "" {
		t.Fatalf("write descriptor backend was called for a small write: %q", backend.preparePath)
	}
}

func TestHandlerReleasesRDMAReadDescriptor(t *testing.T) {
	backend := &fakeRDMAFileBackend{}
	h := &Handler{Backend: backend}

	reply, err := h.Handle(context.Background(), &swvfsproto.Request{
		Header: swvfsproto.RequestHeader{
			Tag:    6,
			Op:     swvfsproto.OpRDMAReleaseRead,
			Offset: 99,
			Size:   4096,
			Valid:  0xfffffffb,
		},
	})
	if err != nil {
		t.Fatalf("release Handle: %v", err)
	}
	if reply.Tag != 6 || backend.releaseLeaseID != 99 || backend.releaseStatus != -5 || backend.releaseBytes != 4096 {
		t.Fatalf("release mismatch: reply=%+v lease=%d status=%d bytes=%d", reply, backend.releaseLeaseID, backend.releaseStatus, backend.releaseBytes)
	}
}

func TestHandlerForceRDMA(t *testing.T) {
	backend := &fakeFileBackend{}
	h := &Handler{Backend: backend, ForceReadRDMA: true, ForceWriteRDMA: true}

	if _, err := h.Handle(context.Background(), &swvfsproto.Request{Header: swvfsproto.RequestHeader{Tag: 1, Op: swvfsproto.OpRead}, Path1: "/x"}); err != nil {
		t.Fatalf("forced read Handle: %v", err)
	}
	if !backend.readPreferRDMA {
		t.Fatal("forced read did not prefer RDMA")
	}

	if _, err := h.Handle(context.Background(), &swvfsproto.Request{Header: swvfsproto.RequestHeader{Tag: 2, Op: swvfsproto.OpWrite}, Path1: "/x", Data: []byte("abc")}); err != nil {
		t.Fatalf("forced write Handle: %v", err)
	}
	if !backend.writePreferRDMA {
		t.Fatal("forced write did not prefer RDMA")
	}
}

func TestHandlerMetadataMutations(t *testing.T) {
	backend := &fakeFileBackend{}
	h := &Handler{Backend: backend}

	if _, err := h.Handle(context.Background(), &swvfsproto.Request{
		Header: swvfsproto.RequestHeader{Tag: 1, Op: swvfsproto.OpRename},
		Path1:  "/old",
		Path2:  "/new",
	}); err != nil {
		t.Fatalf("rename Handle: %v", err)
	}
	if backend.renamedOld != "/old" || backend.renamedNew != "/new" {
		t.Fatalf("rename paths = %q -> %q", backend.renamedOld, backend.renamedNew)
	}

	reply, err := h.Handle(context.Background(), &swvfsproto.Request{
		Header: swvfsproto.RequestHeader{Tag: 2, Op: swvfsproto.OpLink},
		Path1:  "/old",
		Path2:  "/hard",
	})
	if err != nil {
		t.Fatalf("link Handle: %v", err)
	}
	if backend.linkedOld != "/old" || backend.linkedNew != "/hard" || reply.Attr.Nlink != 2 {
		t.Fatalf("link mismatch: old=%q new=%q reply=%+v", backend.linkedOld, backend.linkedNew, reply.Attr)
	}

	reply, err = h.Handle(context.Background(), &swvfsproto.Request{
		Header: swvfsproto.RequestHeader{Tag: 3, Op: swvfsproto.OpSymlink, UID: 1000, GID: 1000},
		Path1:  "/link",
		Path2:  "target",
	})
	if err != nil {
		t.Fatalf("symlink Handle: %v", err)
	}
	if backend.symlinkPath != "/link" || backend.symlinkTarget != "target" || reply.Attr.Mode&uint32(syscall.S_IFMT) != uint32(syscall.S_IFLNK) {
		t.Fatalf("symlink mismatch: path=%q target=%q reply=%+v", backend.symlinkPath, backend.symlinkTarget, reply.Attr)
	}

	reply, err = h.Handle(context.Background(), &swvfsproto.Request{
		Header: swvfsproto.RequestHeader{Tag: 4, Op: swvfsproto.OpReadLink},
		Path1:  "/link",
	})
	if err != nil {
		t.Fatalf("readlink Handle: %v", err)
	}
	if string(reply.Data) != "target" {
		t.Fatalf("readlink data = %q", reply.Data)
	}

	reply, err = h.Handle(context.Background(), &swvfsproto.Request{
		Header: swvfsproto.RequestHeader{Tag: 5, Op: swvfsproto.OpMknod, Mode: uint32(syscall.S_IFIFO | 0600), UID: 1000, GID: 1000, Size: 123},
		Path1:  "/fifo",
	})
	if err != nil {
		t.Fatalf("mknod Handle: %v", err)
	}
	if backend.mknodPath != "/fifo" || backend.mknodMode != uint32(syscall.S_IFIFO|0600) || backend.mknodRdev != 123 || reply.Attr.Rdev != 123 {
		t.Fatalf("mknod mismatch: backend=%+v reply=%+v", backend, reply.Attr)
	}
}

func TestHandlerStatFS(t *testing.T) {
	h := &Handler{Backend: &fakeFileBackend{}}
	req := &swvfsproto.Request{Header: swvfsproto.RequestHeader{Tag: 1, Op: swvfsproto.OpStatFS}, Path1: "/"}
	reply, err := h.Handle(context.Background(), req)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	stat, err := swvfsproto.DecodeStatFS(reply.Data)
	if err != nil {
		t.Fatalf("DecodeStatFS: %v", err)
	}
	if stat.Blocks != 100 || stat.Bsize != 4096 {
		t.Fatalf("statfs = %+v", stat)
	}
}

func TestHandlerXAttrDefaults(t *testing.T) {
	h := &Handler{Backend: &fakeFileBackend{}}
	listReq := &swvfsproto.Request{Header: swvfsproto.RequestHeader{Tag: 1, Op: swvfsproto.OpListXAttr}, Path1: "/"}
	reply, err := h.Handle(context.Background(), listReq)
	if err != nil {
		t.Fatalf("LISTXATTR Handle: %v", err)
	}
	if len(reply.Data) != 0 {
		t.Fatalf("LISTXATTR data = %q", reply.Data)
	}

	getReq := &swvfsproto.Request{Header: swvfsproto.RequestHeader{Tag: 2, Op: swvfsproto.OpGetXAttr}, Path1: "/", Path2: "user.missing"}
	_, err = h.Handle(context.Background(), getReq)
	var errno ErrnoError
	if !errors.As(err, &errno) || errno.Errno != ErrnoNoData {
		t.Fatalf("expected ENODATA, got %v", err)
	}
}

func TestHandlerXAttrDelegatesToBackend(t *testing.T) {
	backend := &fakeXAttrBackend{}
	h := &Handler{Backend: backend}

	reply, err := h.Handle(context.Background(), &swvfsproto.Request{
		Header: swvfsproto.RequestHeader{Tag: 1, Op: swvfsproto.OpListXAttr},
		Path1:  "/file",
	})
	if err != nil {
		t.Fatalf("LISTXATTR Handle: %v", err)
	}
	if string(reply.Data) != "user.exists\x00" {
		t.Fatalf("LISTXATTR data = %q", reply.Data)
	}

	reply, err = h.Handle(context.Background(), &swvfsproto.Request{
		Header: swvfsproto.RequestHeader{Tag: 2, Op: swvfsproto.OpGetXAttr},
		Path1:  "/file",
		Path2:  "user.exists",
	})
	if err != nil {
		t.Fatalf("GETXATTR Handle: %v", err)
	}
	if string(reply.Data) != "value" {
		t.Fatalf("GETXATTR data = %q", reply.Data)
	}

	_, err = h.Handle(context.Background(), &swvfsproto.Request{
		Header: swvfsproto.RequestHeader{Tag: 3, Op: swvfsproto.OpSetXAttr, Mode: swvfsproto.XAttrCreate},
		Path1:  "/file",
		Path2:  "user.exists",
		Data:   []byte("new"),
	})
	if err != nil {
		t.Fatalf("SETXATTR Handle: %v", err)
	}
	if backend.setPath != "/file" || backend.setName != "user.exists" || string(backend.setValue) != "new" || backend.setFlags != swvfsproto.XAttrCreate || backend.setRemove {
		t.Fatalf("SETXATTR not delegated: %+v", backend)
	}

	_, err = h.Handle(context.Background(), &swvfsproto.Request{
		Header: swvfsproto.RequestHeader{Tag: 4, Op: swvfsproto.OpSetXAttr, Valid: swvfsproto.XAttrRemove},
		Path1:  "/file",
		Path2:  "user.exists",
	})
	if err != nil {
		t.Fatalf("REMOVEXATTR Handle: %v", err)
	}
	if !backend.setRemove {
		t.Fatal("REMOVEXATTR remove flag was not delegated")
	}
}
