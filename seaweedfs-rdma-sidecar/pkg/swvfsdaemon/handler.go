package swvfsdaemon

import (
	"context"
	"errors"
	"fmt"
	"time"

	"seaweedfs-rdma-sidecar/pkg/swvfsproto"
)

const (
	ErrnoIO       int32 = -5
	ErrnoPerm     int32 = -1
	ErrnoNoEnt    int32 = -2
	ErrnoNoSys    int32 = -38
	ErrnoInval    int32 = -22
	ErrnoNoData   int32 = -61
	ErrnoTooLarge int32 = -7
	ErrnoExist    int32 = -17
	ErrnoNotDir   int32 = -20
	ErrnoIsDir    int32 = -21
	ErrnoNotEmpty int32 = -39
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

type RDMAReadDescriptorBackend interface {
	ReadFileRDMA(ctx context.Context, path string, offset, size uint64) (*swvfsproto.RDMADataDesc, *swvfsproto.Attr, error)
}

type RDMAReadDescriptorReleaseBackend interface {
	ReleaseReadDescriptor(ctx context.Context, leaseID uint64, status int32, bytes uint64) error
}

type RDMAWriteDescriptorBackend interface {
	PrepareWriteRDMA(ctx context.Context, path string, offset, size uint64) (*swvfsproto.RDMADataDesc, *swvfsproto.Attr, error)
	CommitWriteRDMA(ctx context.Context, path string, offset, size uint64) (*swvfsproto.Attr, error)
}

type MetadataBackend interface {
	LookupFile(ctx context.Context, path string) (*swvfsproto.Attr, error)
	ReadDir(ctx context.Context, path string, offset uint64, limit uint32) ([]swvfsproto.Dirent, bool, error)
	CreateFile(ctx context.Context, path string, mode, uid, gid uint32) (*swvfsproto.Attr, error)
	Mkdir(ctx context.Context, path string, mode, uid, gid uint32) (*swvfsproto.Attr, error)
	DeleteFile(ctx context.Context, path string, recursive bool) error
	RenameEntry(ctx context.Context, oldPath, newPath string) error
	LinkEntry(ctx context.Context, oldPath, newPath string) (*swvfsproto.Attr, error)
	Symlink(ctx context.Context, linkPath, target string, uid, gid uint32) (*swvfsproto.Attr, error)
	ReadLink(ctx context.Context, linkPath string) ([]byte, error)
	Mknod(ctx context.Context, path string, mode, uid, gid, rdev uint32) (*swvfsproto.Attr, error)
	SetAttr(ctx context.Context, path string, header swvfsproto.RequestHeader) (*swvfsproto.Attr, error)
	GetXAttr(ctx context.Context, path, name string) ([]byte, error)
	SetXAttr(ctx context.Context, path, name string, value []byte, flags uint32, remove bool) error
	ListXAttr(ctx context.Context, path string) ([]byte, error)
	StatFS(ctx context.Context, path string) (*swvfsproto.StatFS, error)
}

type Handler struct {
	Backend          FileBackend
	ForceReadRDMA    bool
	ForceWriteRDMA   bool
	ReadRDMAMinSize  uint64
	WriteRDMAMinSize uint64
	Stats            *Stats
}

func (h *Handler) Handle(ctx context.Context, req *swvfsproto.Request) (*swvfsproto.Reply, error) {
	if req == nil {
		return nil, ErrnoError{Errno: ErrnoInval, Msg: "nil request"}
	}
	if h == nil || h.Backend == nil {
		return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "no swvfs backend configured"}
	}
	opName := fmt.Sprintf("op_%d", req.Header.Op)
	opStart := time.Now()
	h.Stats.Inc("handler_" + opName + "_requests")
	h.Stats.Add("handler_request_payload_bytes", uint64(len(req.Data)))
	defer func() {
		h.Stats.Observe("handler_"+opName, time.Since(opStart))
	}()
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
		h.Stats.Inc("handler_read_requests")
		h.Stats.Add("handler_read_requested_bytes", req.Header.Size)
		if preferRDMA {
			h.Stats.Inc("handler_read_prefer_rdma")
			if !h.shouldUseRDMAReadDescriptor(req.Header.Size) {
				h.Stats.Inc("handler_read_rdma_desc_policy_too_small")
			} else if rdmaBackend, ok := h.Backend.(RDMAReadDescriptorBackend); ok {
				readRDMAStart := time.Now()
				desc, attr, err := rdmaBackend.ReadFileRDMA(ctx, req.Path1, req.Header.Offset, req.Header.Size)
				h.Stats.Observe("handler_read_rdma_desc", time.Since(readRDMAStart))
				if err == nil && desc != nil {
					h.Stats.Inc("handler_read_rdma_desc_replies")
					h.Stats.Add("handler_read_rdma_desc_bytes", uint64(desc.Length))
					reply := &swvfsproto.Reply{
						Tag:  req.Header.Tag,
						EOF:  swvfsproto.ReplyFRDMAReadDesc,
						Data: swvfsproto.EncodeRDMADataDesc(*desc),
					}
					if attr != nil {
						reply.Attr = *attr
					}
					return reply, nil
				}
				if err != nil && !isRDMAReadFallback(err) {
					h.Stats.Inc("handler_read_rdma_desc_errors")
					return nil, err
				}
				if err != nil {
					h.Stats.Inc("handler_read_rdma_desc_fallbacks")
				}
			}
		}
		data, attr, err := h.Backend.ReadFile(ctx, req.Path1, req.Header.Offset, req.Header.Size, preferRDMA)
		if err != nil {
			h.Stats.Inc("handler_read_fallback_errors")
			return nil, err
		}
		h.Stats.Inc("handler_read_fallback_replies")
		h.Stats.Add("handler_read_fallback_bytes", uint64(len(data)))
		reply := &swvfsproto.Reply{Tag: req.Header.Tag, Data: data}
		if attr != nil {
			reply.Attr = *attr
		}
		return reply, nil
	case swvfsproto.OpWrite:
		preferRDMA := h.ForceWriteRDMA || req.WriteRDMAPreferred()
		h.Stats.Inc("handler_write_requests")
		h.Stats.Add("handler_write_payload_bytes", uint64(len(req.Data)))
		if preferRDMA {
			h.Stats.Inc("handler_write_prefer_rdma")
		}
		attr, err := h.Backend.WriteFile(ctx, req.Path1, req.Header.Offset, req.Data, req.Header.Mode, req.Header.UID, req.Header.GID, preferRDMA)
		if err != nil {
			h.Stats.Inc("handler_write_errors")
			return nil, err
		}
		reply := &swvfsproto.Reply{Tag: req.Header.Tag}
		if attr != nil {
			reply.Attr = *attr
		}
		return reply, nil
	case swvfsproto.OpRDMAReadPrepare:
		backend, ok := h.Backend.(RDMAReadDescriptorBackend)
		if !ok {
			return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "rdma read prepare is not implemented"}
		}
		h.Stats.Inc("handler_read_rdma_prepare_requests")
		h.Stats.Add("handler_read_rdma_prepare_bytes", req.Header.Size)
		if !h.shouldUseRDMAReadDescriptor(req.Header.Size) {
			h.Stats.Inc("handler_read_rdma_prepare_policy_too_small")
			return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "rdma read prepare below minimum size"}
		}
		prepareStart := time.Now()
		desc, attr, err := backend.ReadFileRDMA(ctx, req.Path1, req.Header.Offset, req.Header.Size)
		h.Stats.Observe("handler_read_rdma_prepare", time.Since(prepareStart))
		if err != nil {
			h.Stats.Inc("handler_read_rdma_prepare_errors")
			return nil, err
		}
		if desc == nil {
			return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "rdma read prepare returned no descriptor"}
		}
		h.Stats.Inc("handler_read_rdma_prepare_replies")
		h.Stats.Add("handler_read_rdma_prepare_desc_bytes", uint64(desc.Length))
		reply := &swvfsproto.Reply{
			Tag:  req.Header.Tag,
			EOF:  swvfsproto.ReplyFRDMAReadDesc,
			Data: swvfsproto.EncodeRDMADataDesc(*desc),
		}
		if attr != nil {
			reply.Attr = *attr
		}
		return reply, nil
	case swvfsproto.OpWriteRDMAPrepare:
		backend, ok := h.Backend.(RDMAWriteDescriptorBackend)
		if !ok {
			return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "rdma write prepare is not implemented"}
		}
		h.Stats.Inc("handler_write_rdma_prepare_requests")
		h.Stats.Add("handler_write_rdma_prepare_bytes", req.Header.Size)
		if !h.shouldUseRDMAWriteDescriptor(req.Header.Size) {
			h.Stats.Inc("handler_write_rdma_prepare_policy_too_small")
			return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "rdma write prepare below minimum size"}
		}
		prepareStart := time.Now()
		desc, attr, err := backend.PrepareWriteRDMA(ctx, req.Path1, req.Header.Offset, req.Header.Size)
		h.Stats.Observe("handler_write_rdma_prepare", time.Since(prepareStart))
		if err != nil {
			h.Stats.Inc("handler_write_rdma_prepare_errors")
			return nil, err
		}
		if desc == nil {
			return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "rdma write prepare returned no descriptor"}
		}
		reply := &swvfsproto.Reply{
			Tag:  req.Header.Tag,
			EOF:  swvfsproto.ReplyFRDMAWriteDesc,
			Data: swvfsproto.EncodeRDMADataDesc(*desc),
		}
		if attr != nil {
			reply.Attr = *attr
		}
		return reply, nil
	case swvfsproto.OpWriteRDMACommit:
		backend, ok := h.Backend.(RDMAWriteDescriptorBackend)
		if !ok {
			return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "rdma write commit is not implemented"}
		}
		h.Stats.Inc("handler_write_rdma_commit_requests")
		h.Stats.Add("handler_write_rdma_commit_bytes", req.Header.Size)
		commitStart := time.Now()
		attr, err := backend.CommitWriteRDMA(ctx, req.Path1, req.Header.Offset, req.Header.Size)
		h.Stats.Observe("handler_write_rdma_commit", time.Since(commitStart))
		if err != nil {
			h.Stats.Inc("handler_write_rdma_commit_errors")
			return nil, err
		}
		reply := &swvfsproto.Reply{Tag: req.Header.Tag}
		if attr != nil {
			reply.Attr = *attr
		}
		return reply, nil
	case swvfsproto.OpRDMAReleaseRead:
		backend, ok := h.Backend.(RDMAReadDescriptorReleaseBackend)
		if !ok {
			return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "rdma read descriptor release is not implemented"}
		}
		if req.Header.Offset == 0 {
			return nil, ErrnoError{Errno: ErrnoInval, Msg: "rdma read descriptor release missing lease id"}
		}
		h.Stats.Inc("handler_read_rdma_release_requests")
		h.Stats.Add("handler_read_rdma_release_bytes", req.Header.Size)
		releaseStart := time.Now()
		if err := backend.ReleaseReadDescriptor(ctx, req.Header.Offset, int32(req.Header.Valid), req.Header.Size); err != nil {
			h.Stats.Observe("handler_read_rdma_release", time.Since(releaseStart))
			h.Stats.Inc("handler_read_rdma_release_errors")
			return nil, err
		}
		h.Stats.Observe("handler_read_rdma_release", time.Since(releaseStart))
		h.Stats.Inc("handler_read_rdma_release_replies")
		return &swvfsproto.Reply{Tag: req.Header.Tag}, nil
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
	case swvfsproto.OpLink:
		backend, ok := h.Backend.(interface {
			LinkEntry(context.Context, string, string) (*swvfsproto.Attr, error)
		})
		if !ok {
			return nil, ErrnoError{Errno: ErrnoNoSys, Msg: "link is not implemented"}
		}
		attr, err := backend.LinkEntry(ctx, req.Path1, req.Path2)
		if err != nil {
			return nil, err
		}
		reply := &swvfsproto.Reply{Tag: req.Header.Tag}
		if attr != nil {
			reply.Attr = *attr
		}
		return reply, nil
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
		if backend, ok := h.Backend.(interface {
			FlushFile(context.Context, string) (*swvfsproto.Attr, error)
		}); ok {
			attr, err := backend.FlushFile(ctx, req.Path1)
			if err != nil {
				return nil, err
			}
			reply := &swvfsproto.Reply{Tag: req.Header.Tag}
			if attr != nil {
				reply.Attr = *attr
			}
			return reply, nil
		}
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
		backend, ok := h.Backend.(interface {
			ListXAttr(context.Context, string) ([]byte, error)
		})
		if !ok {
			return &swvfsproto.Reply{Tag: req.Header.Tag}, nil
		}
		data, err := backend.ListXAttr(ctx, req.Path1)
		if err != nil {
			return nil, err
		}
		return &swvfsproto.Reply{Tag: req.Header.Tag, Data: data}, nil
	case swvfsproto.OpGetXAttr:
		backend, ok := h.Backend.(interface {
			GetXAttr(context.Context, string, string) ([]byte, error)
		})
		if !ok {
			return nil, ErrnoError{Errno: ErrnoNoData, Msg: "xattr not found"}
		}
		data, err := backend.GetXAttr(ctx, req.Path1, req.Path2)
		if err != nil {
			return nil, err
		}
		return &swvfsproto.Reply{Tag: req.Header.Tag, Data: data}, nil
	case swvfsproto.OpSetXAttr:
		backend, ok := h.Backend.(interface {
			SetXAttr(context.Context, string, string, []byte, uint32, bool) error
		})
		if !ok {
			return &swvfsproto.Reply{Tag: req.Header.Tag}, nil
		}
		if err := backend.SetXAttr(ctx, req.Path1, req.Path2, req.Data, req.Header.Mode, req.Header.Valid&swvfsproto.XAttrRemove != 0); err != nil {
			return nil, err
		}
		return &swvfsproto.Reply{Tag: req.Header.Tag}, nil
	default:
		return nil, ErrnoError{Errno: ErrnoNoSys, Msg: fmt.Sprintf("swvfs op %d not implemented by RDMA daemon", req.Header.Op)}
	}
}

func (h *Handler) shouldUseRDMAReadDescriptor(size uint64) bool {
	return h == nil || h.ReadRDMAMinSize == 0 || size >= h.ReadRDMAMinSize
}

func (h *Handler) shouldUseRDMAWriteDescriptor(size uint64) bool {
	return h == nil || h.WriteRDMAMinSize == 0 || size >= h.WriteRDMAMinSize
}

func isNoSys(err error) bool {
	var errno ErrnoError
	return errors.As(err, &errno) && errno.Errno == ErrnoNoSys
}

func isRDMAReadFallback(err error) bool {
	var errno ErrnoError
	if !errors.As(err, &errno) {
		return false
	}
	switch errno.Errno {
	case ErrnoNoSys, ErrnoTooLarge:
		return true
	default:
		return false
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
