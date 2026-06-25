package swvfsdaemon

import (
	"context"
	"errors"
	"fmt"

	"seaweedfs-rdma-sidecar/pkg/swvfsproto"
)

const (
	ErrnoIO       int32 = -5
	ErrnoNoEnt    int32 = -2
	ErrnoNoSys    int32 = -38
	ErrnoInval    int32 = -22
	ErrnoTooLarge int32 = -7
)

type ErrnoError struct {
	Errno int32
	Msg   string
}

func (e ErrnoError) Error() string {
	if e.Msg != "" {
		return e.Msg
	}
	return fmt.Sprintf("errno %d", e.Errno)
}

type FileBackend interface {
	ReadFile(ctx context.Context, path string, offset, size uint64, preferRDMA bool) ([]byte, *swvfsproto.Attr, error)
	WriteFile(ctx context.Context, path string, offset uint64, data []byte, mode, uid, gid uint32, preferRDMA bool) (*swvfsproto.Attr, error)
}

type Handler struct {
	Backend FileBackend
}

func (h *Handler) Handle(ctx context.Context, req *swvfsproto.Request) (*swvfsproto.Reply, error) {
	if req == nil {
		return nil, ErrnoError{Errno: ErrnoInval, Msg: "nil request"}
	}
	if h == nil || h.Backend == nil {
		return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "no swvfs backend configured"}
	}
	switch req.Header.Op {
	case swvfsproto.OpRead:
		data, attr, err := h.Backend.ReadFile(ctx, req.Path1, req.Header.Offset, req.Header.Size, req.ReadRDMAPreferred())
		if err != nil {
			return nil, err
		}
		reply := &swvfsproto.Reply{Tag: req.Header.Tag, Data: data}
		if attr != nil {
			reply.Attr = *attr
		}
		return reply, nil
	case swvfsproto.OpWrite:
		attr, err := h.Backend.WriteFile(ctx, req.Path1, req.Header.Offset, req.Data, req.Header.Mode, req.Header.UID, req.Header.GID, req.WriteRDMAPreferred())
		if err != nil {
			return nil, err
		}
		reply := &swvfsproto.Reply{Tag: req.Header.Tag}
		if attr != nil {
			reply.Attr = *attr
		}
		return reply, nil
	default:
		return nil, ErrnoError{Errno: ErrnoNoSys, Msg: fmt.Sprintf("swvfs op %d not implemented by RDMA daemon", req.Header.Op)}
	}
}

func ReplyForError(tag uint64, err error) *swvfsproto.Reply {
	if err == nil {
		return &swvfsproto.Reply{Tag: tag}
	}
	var errno ErrnoError
	if errors.As(err, &errno) {
		return swvfsproto.ErrorReply(tag, errno.Errno)
	}
	return swvfsproto.ErrorReply(tag, ErrnoIO)
}
