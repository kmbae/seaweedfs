package swvfsdaemon

import (
	"context"
	"fmt"
	"io"
	"time"

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
	Stats          *Stats
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
	totalStart := time.Now()
	max := d.MaxRequestSize
	if max <= 0 {
		max = DefaultMaxRequestSize
	}
	buf := make([]byte, max)
	readStart := time.Now()
	n, err := d.RW.Read(buf)
	d.Stats.Observe("device_read", time.Since(readStart))
	if err != nil {
		d.Stats.Inc("device_read_errors")
		return err
	}
	if n == 0 {
		d.Stats.Inc("device_zero_reads")
		return io.ErrNoProgress
	}
	d.Stats.Inc("device_requests")
	d.Stats.Add("device_request_bytes", uint64(n))
	decodeStart := time.Now()
	req, err := swvfsproto.DecodeRequest(buf[:n])
	d.Stats.Observe("device_decode", time.Since(decodeStart))
	if err != nil {
		d.Stats.Inc("device_decode_errors")
		return err
	}
	handleStart := time.Now()
	reply, handleErr := d.Handler.Handle(ctx, req)
	d.Stats.Observe("device_handle", time.Since(handleStart))
	if handleErr != nil {
		d.Stats.Inc("device_handler_errors")
		reply = ReplyForError(req.Header.Tag, handleErr)
	}
	encodeStart := time.Now()
	encoded, err := reply.Encode()
	d.Stats.Observe("device_encode", time.Since(encodeStart))
	if err != nil {
		d.Stats.Inc("device_encode_errors")
		return err
	}
	writeStart := time.Now()
	written, err := d.RW.Write(encoded)
	d.Stats.Observe("device_write", time.Since(writeStart))
	if err != nil {
		d.Stats.Inc("device_write_errors")
		return err
	}
	if written != len(encoded) {
		d.Stats.Inc("device_short_writes")
		return io.ErrShortWrite
	}
	d.Stats.Add("device_reply_bytes", uint64(written))
	d.Stats.Observe("device_roundtrip", time.Since(totalStart))
	return nil
}
