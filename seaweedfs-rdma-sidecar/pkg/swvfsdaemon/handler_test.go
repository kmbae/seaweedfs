package swvfsdaemon

import (
	"context"
	"errors"
	"testing"

	"seaweedfs-rdma-sidecar/pkg/swvfsproto"
)

type fakeFileBackend struct {
	readPreferRDMA  bool
	writePreferRDMA bool
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
