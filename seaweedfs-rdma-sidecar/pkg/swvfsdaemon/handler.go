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
	ErrnoNoData   int32 = -61
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

type MetadataBackend interface {
	LookupFile(ctx context.Context, path string) (*swvfsproto.Attr, error)
	ReadDir(ctx context.Context, path string, offset uint64, limit uint32) ([]swvfsproto.Dirent, bool, error)
	CreateFile(ctx context.Context, path string, mode, uid, gid uint32) (*swvfsproto.Attr, error)
	Mkdir(ctx context.Context, path string, mode, uid, gid uint32) (*swvfsproto.Attr, error)
	DeleteFile(ctx context.Context, path string, recursive bool) error
	RenameEntry(ctx context.Context, oldPath, newPath string) error
	Symlink(ctx context.Context, linkPath, target string, uid, gid uint32) (*swvfsproto.Attr, error)
	ReadLink(ctx context.Context, linkPath string) ([]byte, error)
	Mknod(ctx context.Context, path string, mode, uid, gid, rdev uint32) (*swvfsproto.Attr, error)
	SetAttr(ctx context.Context, path string, header swvfsproto.RequestHeader) (*swvfsproto.Attr, error)
	StatFS(ctx context.Context, path string) (*swvfsproto.StatFS, error)
}

type Handler struct {
	Backend        FileBackend
	ForceReadRDMA  bool
	ForceWriteRDMA bool
}

func (h *Handler) Handle(ctx context.Context, req *swvfsproto.Request) (*swvfsproto.Reply, error) {
	if req == nil {
		return nil, ErrnoError{Errno: ErrnoInval, Msg: "nil request"}
	}
	if h == nil || h.Backend == nil {
		return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "no swvfs backend configured"}
	}
	switch req.Header.Op {
	case swvfsproto.OpLookup, swvfsproto.OpGetAttr:
		backend, ok := h.Backend.(interface {
			LookupFile(context.Context, string) (*swvfsproto.Attr, error)
		})
		if !ok {
			return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "lookup is not implemented"}
		}
		attr, err := backend.LookupFile(ctx, req.Path1)
		if err != nil {
			return nil, err
		}
		reply := &swvfsproto.Reply{Tag: req.Header.Tag}
		if attr != nil {
			reply.Attr = *attr
		}
		return reply, nil
	case swvfsproto.OpReadDir:
		backend, ok := h.Backend.(interface {
			ReadDir(context.Context, string, uint64, uint32) ([]swvfsproto.Dirent, bool, error)
		})
		if !ok {
			return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "readdir is not implemented"}
		}
		dirents, eof, err := backend.ReadDir(ctx, req.Path1, req.Header.Offset, swvfsproto.MaxDirents)
		if err != nil {
			return nil, err
		}
		reply := &swvfsproto.Reply{Tag: req.Header.Tag, Dirents: dirents}
		if eof {
			reply.EOF = 1
		}
		return reply, nil
	case swvfsproto.OpCreate:
		backend, ok := h.Backend.(interface {
			CreateFile(context.Context, string, uint32, uint32, uint32) (*swvfsproto.Attr, error)
		})
		if !ok {
			return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "create is not implemented"}
		}
		attr, err := backend.CreateFile(ctx, req.Path1, req.Header.Mode, req.Header.UID, req.Header.GID)
		if err != nil {
			return nil, err
		}
		reply := &swvfsproto.Reply{Tag: req.Header.Tag}
		if attr != nil {
			reply.Attr = *attr
		}
		return reply, nil
	case swvfsproto.OpMkdir:
		backend, ok := h.Backend.(interface {
			Mkdir(context.Context, string, uint32, uint32, uint32) (*swvfsproto.Attr, error)
		})
		if !ok {
			return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "mkdir is not implemented"}
		}
		attr, err := backend.Mkdir(ctx, req.Path1, req.Header.Mode, req.Header.UID, req.Header.GID)
		if err != nil {
			return nil, err
		}
		reply := &swvfsproto.Reply{Tag: req.Header.Tag}
		if attr != nil {
			reply.Attr = *attr
		}
		return reply, nil
	case swvfsproto.OpSetAttr:
		backend, ok := h.Backend.(interface {
			SetAttr(context.Context, string, swvfsproto.RequestHeader) (*swvfsproto.Attr, error)
		})
		if !ok {
			return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "setattr is not implemented"}
		}
		attr, err := backend.SetAttr(ctx, req.Path1, req.Header)
		if err != nil {
			return nil, err
		}
		reply := &swvfsproto.Reply{Tag: req.Header.Tag}
		if attr != nil {
			reply.Attr = *attr
		}
		return reply, nil
	case swvfsproto.OpRead:
		preferRDMA := h.ForceReadRDMA || req.ReadRDMAPreferred()
		data, attr, err := h.Backend.ReadFile(ctx, req.Path1, req.Header.Offset, req.Header.Size, preferRDMA)
		if err != nil {
			return nil, err
		}
		reply := &swvfsproto.Reply{Tag: req.Header.Tag, Data: data}
		if attr != nil {
			reply.Attr = *attr
		}
		return reply, nil
	case swvfsproto.OpWrite:
		preferRDMA := h.ForceWriteRDMA || req.WriteRDMAPreferred()
		attr, err := h.Backend.WriteFile(ctx, req.Path1, req.Header.Offset, req.Data, req.Header.Mode, req.Header.UID, req.Header.GID, preferRDMA)
		if err != nil {
			return nil, err
		}
		reply := &swvfsproto.Reply{Tag: req.Header.Tag}
		if attr != nil {
			reply.Attr = *attr
		}
		return reply, nil
	case swvfsproto.OpUnlink, swvfsproto.OpRmdir:
		backend, ok := h.Backend.(interface {
			DeleteFile(context.Context, string, bool) error
		})
		if !ok {
			return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "delete is not implemented"}
		}
		if err := backend.DeleteFile(ctx, req.Path1, req.Header.Op == swvfsproto.OpRmdir); err != nil {
			return nil, err
		}
		return &swvfsproto.Reply{Tag: req.Header.Tag}, nil
	case swvfsproto.OpRename:
		backend, ok := h.Backend.(interface {
			RenameEntry(context.Context, string, string) error
		})
		if !ok {
			return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "rename is not implemented"}
		}
		if err := backend.RenameEntry(ctx, req.Path1, req.Path2); err != nil {
			return nil, err
		}
		return &swvfsproto.Reply{Tag: req.Header.Tag}, nil
	case swvfsproto.OpSymlink:
		backend, ok := h.Backend.(interface {
			Symlink(context.Context, string, string, uint32, uint32) (*swvfsproto.Attr, error)
		})
		if !ok {
			return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "symlink is not implemented"}
		}
		attr, err := backend.Symlink(ctx, req.Path1, req.Path2, req.Header.UID, req.Header.GID)
		if err != nil {
			return nil, err
		}
		reply := &swvfsproto.Reply{Tag: req.Header.Tag}
		if attr != nil {
			reply.Attr = *attr
		}
		return reply, nil
	case swvfsproto.OpReadLink:
		backend, ok := h.Backend.(interface {
			ReadLink(context.Context, string) ([]byte, error)
		})
		if !ok {
			return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "readlink is not implemented"}
		}
		target, err := backend.ReadLink(ctx, req.Path1)
		if err != nil {
			return nil, err
		}
		return &swvfsproto.Reply{Tag: req.Header.Tag, Data: target}, nil
	case swvfsproto.OpMknod:
		backend, ok := h.Backend.(interface {
			Mknod(context.Context, string, uint32, uint32, uint32, uint32) (*swvfsproto.Attr, error)
		})
		if !ok {
			return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "mknod is not implemented"}
		}
		attr, err := backend.Mknod(ctx, req.Path1, req.Header.Mode, req.Header.UID, req.Header.GID, uint32(req.Header.Size))
		if err != nil {
			return nil, err
		}
		reply := &swvfsproto.Reply{Tag: req.Header.Tag}
		if attr != nil {
			reply.Attr = *attr
		}
		return reply, nil
	case swvfsproto.OpFlush, swvfsproto.OpRelease:
		return &swvfsproto.Reply{Tag: req.Header.Tag}, nil
	case swvfsproto.OpStatFS:
		backend, ok := h.Backend.(interface {
			StatFS(context.Context, string) (*swvfsproto.StatFS, error)
		})
		if !ok {
			return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "statfs is not implemented"}
		}
		stat, err := backend.StatFS(ctx, req.Path1)
		if err != nil {
			return nil, err
		}
		if stat == nil {
			return nil, ErrnoError{Errno: ErrnoIO, Msg: "empty statfs response"}
		}
		return &swvfsproto.Reply{Tag: req.Header.Tag, Data: swvfsproto.EncodeStatFS(*stat)}, nil
	case swvfsproto.OpListXAttr:
		return &swvfsproto.Reply{Tag: req.Header.Tag}, nil
	case swvfsproto.OpGetXAttr:
		return nil, ErrnoError{Errno: ErrnoNoData, Msg: "xattr not found"}
	case swvfsproto.OpSetXAttr:
		return &swvfsproto.Reply{Tag: req.Header.Tag}, nil
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
