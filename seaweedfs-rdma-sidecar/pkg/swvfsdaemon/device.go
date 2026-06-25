package swvfsdaemon

import (
	"context"
	"fmt"
	"io"

	"seaweedfs-rdma-sidecar/pkg/swvfsproto"
)

const DefaultMaxRequestSize = swvfsproto.RequestHeaderSize + 2*swvfsproto.PathMax + swvfsproto.MaxWrite

type RequestHandler interface {
	Handle(context.Context, *swvfsproto.Request) (*swvfsproto.Reply, error)
}

type LegacyDevice struct {
	RW             io.ReadWriter
	Handler        RequestHandler
	MaxRequestSize int
}

func (d *LegacyDevice) Serve(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := d.ServeOnce(ctx); err != nil {
			return err
		}
	}
}

func (d *LegacyDevice) ServeOnce(ctx context.Context) error {
	if d == nil || d.RW == nil || d.Handler == nil {
		return fmt.Errorf("incomplete swvfs legacy device")
	}
	max := d.MaxRequestSize
	if max <= 0 {
		max = DefaultMaxRequestSize
	}
	buf := make([]byte, max)
	n, err := d.RW.Read(buf)
	if err != nil {
		return err
	}
	if n == 0 {
		return io.ErrNoProgress
	}
	req, err := swvfsproto.DecodeRequest(buf[:n])
	if err != nil {
		return err
	}
	reply, handleErr := d.Handler.Handle(ctx, req)
	if handleErr != nil {
		reply = ReplyForError(req.Header.Tag, handleErr)
	}
	encoded, err := reply.Encode()
	if err != nil {
		return err
	}
	written, err := d.RW.Write(encoded)
	if err != nil {
		return err
	}
	if written != len(encoded) {
		return io.ErrShortWrite
	}
	return nil
}
