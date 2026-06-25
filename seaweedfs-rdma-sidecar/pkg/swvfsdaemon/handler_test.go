package swvfsdaemon

import (
	"context"
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
