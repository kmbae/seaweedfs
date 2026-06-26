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
	renamedOld      string
	renamedNew      string
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

func (f *fakeFileBackend) ReadFile(ctx context.Context, path string, offset, size uint64, preferRDMA bool) ([]byte, *swvfsproto.Attr, error) {
	f.readPreferRDMA = preferRDMA
	return []byte("data"), &swvfsproto.Attr{Ino: 1, Size: 4, Mode: 0100644, Nlink: 1}, nil
}

func (f *fakeFileBackend) WriteFile(ctx context.Context, path string, offset uint64, data []byte, mode, uid, gid uint32, preferRDMA bool) (*swvfsproto.Attr, error) {
	f.writePreferRDMA = preferRDMA
	return &swvfsproto.Attr{Ino: 1, Size: uint64(len(data)), Mode: 0100644, Nlink: 1}, nil
}

func (f *fakeFileBackend) StatFS(ctx context.Context, path string) (*swvfsproto.StatFS, error) {
	return &swvfsproto.StatFS{Blocks: 100, Bfree: 80, Bavail: 80, Files: 1000, Ffree: 900, Bsize: 4096, Namelen: 255}, nil
}

func (f *fakeFileBackend) RenameEntry(ctx context.Context, oldPath, newPath string) error {
	f.renamedOld = oldPath
	f.renamedNew = newPath
	return nil
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
		Header: swvfsproto.RequestHeader{Tag: 2, Op: swvfsproto.OpSymlink, UID: 1000, GID: 1000},
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
		Header: swvfsproto.RequestHeader{Tag: 3, Op: swvfsproto.OpReadLink},
		Path1:  "/link",
	})
	if err != nil {
		t.Fatalf("readlink Handle: %v", err)
	}
	if string(reply.Data) != "target" {
		t.Fatalf("readlink data = %q", reply.Data)
	}

	reply, err = h.Handle(context.Background(), &swvfsproto.Request{
		Header: swvfsproto.RequestHeader{Tag: 4, Op: swvfsproto.OpMknod, Mode: uint32(syscall.S_IFIFO | 0600), UID: 1000, GID: 1000, Size: 123},
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
