// Package swvfsfiler maps seaweedvfs path requests onto SeaweedFS filer/chunk IO.
package swvfsfiler

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/url"
	"path"
	"sort"
	"strings"
	"syscall"
	"time"

	"seaweedfs-rdma-sidecar/pkg/swvfsdaemon"
	"seaweedfs-rdma-sidecar/pkg/swvfsproto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"google.golang.org/protobuf/proto"
)

type MetadataStore interface {
	LookupEntry(ctx context.Context, fullPath string) (*filer_pb.Entry, error)
	SaveEntry(ctx context.Context, fullPath string, entry *filer_pb.Entry) error
	AssignVolume(ctx context.Context, fullPath string, size uint64) (fileID, volumeServer string, err error)
	LookupFileID(ctx context.Context, fileID string) ([]string, error)
}

type Backend struct {
	Store  MetadataStore
	Router *swvfsdaemon.Router
}

func (b *Backend) ReadFile(ctx context.Context, fullPath string, offset, size uint64, preferRDMA bool) ([]byte, *swvfsproto.Attr, error) {
	if b == nil || b.Store == nil || b.Router == nil {
		return nil, nil, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoSys, Msg: "swvfs filer backend is not configured"}
	}
	entry, err := b.Store.LookupEntry(ctx, cleanFullPath(fullPath))
	if err != nil {
		return nil, nil, mapLookupErr(err)
	}
	if entry.IsDirectory {
		return nil, nil, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoInval, Msg: "cannot read a directory"}
	}
	attr := AttrFromEntry(fullPath, entry)
	fileSize := entryFileSize(entry)
	if offset >= fileSize || size == 0 {
		return nil, attr, nil
	}
	readSize := minUint64(size, fileSize-offset)

	if len(entry.Content) > 0 {
		stop := minUint64(uint64(len(entry.Content)), offset+readSize)
		if offset >= stop {
			return nil, attr, nil
		}
		return append([]byte(nil), entry.Content[offset:stop]...), attr, nil
	}

	out := make([]byte, readSize)
	views, err := visibleChunkViews(entry.GetChunks(), int64(offset), int64(readSize))
	if err != nil {
		return nil, nil, err
	}
	for _, view := range views {
		if view.size == 0 {
			continue
		}
		if len(view.cipherKey) > 0 || view.isGzipped {
			return nil, nil, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoSys, Msg: "encrypted or compressed chunk is not supported by RDMA swvfs backend yet"}
		}
		volumeServer, err := b.volumeServerForFileID(ctx, view.fileID)
		if err != nil {
			return nil, nil, err
		}
		resp, err := b.Router.ReadNeedle(ctx, swvfsdaemon.NeedleReadRequest{
			FileID:       view.fileID,
			VolumeServer: volumeServer,
			RDMAServer:   volumeServer,
			Offset:       uint64(view.offsetInChunk),
			Size:         uint64(view.size),
			PreferRDMA:   preferRDMA,
		})
		if err != nil {
			return nil, nil, err
		}
		dstStart := uint64(view.start) - offset
		copy(out[dstStart:minUint64(uint64(len(out)), dstStart+uint64(view.size))], resp.Data)
	}
	return out, attr, nil
}

func (b *Backend) WriteFile(ctx context.Context, fullPath string, offset uint64, data []byte, mode, uid, gid uint32, preferRDMA bool) (*swvfsproto.Attr, error) {
	if b == nil || b.Store == nil || b.Router == nil {
		return nil, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoSys, Msg: "swvfs filer backend is not configured"}
	}
	fullPath = cleanFullPath(fullPath)
	if len(data) == 0 {
		entry, err := b.lookupOrCreateEntry(ctx, fullPath, mode, uid, gid)
		if err != nil {
			return nil, err
		}
		return AttrFromEntry(fullPath, entry), nil
	}

	fileID, volumeServer, err := b.Store.AssignVolume(ctx, fullPath, uint64(len(data)))
	if err != nil {
		return nil, err
	}
	if _, err := b.Router.WriteNeedle(ctx, swvfsdaemon.NeedleWriteRequest{
		FileID:       fileID,
		VolumeServer: volumeServer,
		RDMAServer:   volumeServer,
		Data:         data,
		PreferRDMA:   preferRDMA,
	}); err != nil {
		return nil, err
	}

	entry, err := b.lookupOrCreateEntry(ctx, fullPath, mode, uid, gid)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	tsNs := now.UnixNano()
	entry.Chunks = append(entry.GetChunks(), &filer_pb.FileChunk{
		FileId:       fileID,
		Offset:       int64(offset),
		Size:         uint64(len(data)),
		ModifiedTsNs: tsNs,
	})
	if entry.Attributes == nil {
		entry.Attributes = &filer_pb.FuseAttributes{}
	}
	entry.Attributes.FileSize = maxUint64(entry.Attributes.FileSize, offset+uint64(len(data)))
	entry.Attributes.Mtime = now.Unix()
	entry.Attributes.MtimeNs = int32(now.Nanosecond())
	entry.Attributes.Ctime = now.Unix()
	entry.Attributes.CtimeNs = int32(now.Nanosecond())
	if err := b.Store.SaveEntry(ctx, fullPath, entry); err != nil {
		return nil, err
	}
	return AttrFromEntry(fullPath, entry), nil
}

func (b *Backend) lookupOrCreateEntry(ctx context.Context, fullPath string, mode, uid, gid uint32) (*filer_pb.Entry, error) {
	entry, err := b.Store.LookupEntry(ctx, fullPath)
	if err == nil {
		return proto.Clone(entry).(*filer_pb.Entry), nil
	}
	if !errors.Is(err, filer_pb.ErrNotFound) {
		return nil, mapLookupErr(err)
	}
	now := time.Now()
	return &filer_pb.Entry{
		Name: path.Base(fullPath),
		Attributes: &filer_pb.FuseAttributes{
			FileMode: mode & 07777,
			Uid:      uid,
			Gid:      gid,
			Mtime:    now.Unix(),
			MtimeNs:  int32(now.Nanosecond()),
			Ctime:    now.Unix(),
			CtimeNs:  int32(now.Nanosecond()),
			Crtime:   now.Unix(),
			CrtimeNs: int32(now.Nanosecond()),
		},
	}, nil
}

func (b *Backend) lookupFileID(ctx context.Context, fileID string) ([]string, error) {
	return b.Store.LookupFileID(ctx, fileID)
}

func (b *Backend) volumeServerForFileID(ctx context.Context, fileID string) (string, error) {
	urls, err := b.Store.LookupFileID(ctx, fileID)
	if err != nil {
		return "", err
	}
	if len(urls) == 0 {
		return "", fmt.Errorf("no volume server for %s", fileID)
	}
	return volumeServerFromURL(urls[0])
}

func AttrFromEntry(fullPath string, entry *filer_pb.Entry) *swvfsproto.Attr {
	attr := &swvfsproto.Attr{
		Ino:   stableInode(fullPath),
		Nlink: 1,
		Mode:  uint32(syscall.S_IFREG | 0644),
	}
	if entry == nil {
		return attr
	}
	attr.Size = entryFileSize(entry)
	if entry.IsDirectory {
		attr.Mode = uint32(syscall.S_IFDIR | 0755)
		attr.Nlink = 2
	}
	if entry.Attributes != nil {
		a := entry.Attributes
		if a.Inode != 0 {
			attr.Ino = a.Inode
		}
		perm := a.FileMode
		if perm == 0 {
			perm = attr.Mode & 07777
		}
		switch {
		case entry.IsDirectory:
			attr.Mode = uint32(syscall.S_IFDIR) | (perm & 07777)
		case a.SymlinkTarget != "":
			attr.Mode = uint32(syscall.S_IFLNK) | (perm & 07777)
		default:
			attr.Mode = uint32(syscall.S_IFREG) | (perm & 07777)
		}
		attr.UID = a.Uid
		attr.GID = a.Gid
		attr.Rdev = a.Rdev
		attr.MtimeSec = a.Mtime
		attr.MtimeNsec = uint32(a.MtimeNs)
		attr.CtimeSec = a.Ctime
		attr.CtimeNsec = uint32(a.CtimeNs)
		attr.AtimeSec = a.Atime
		attr.AtimeNsec = uint32(a.AtimeNs)
		if a.FileSize > attr.Size {
			attr.Size = a.FileSize
		}
	}
	if attr.MtimeSec == 0 {
		now := time.Now()
		attr.MtimeSec = now.Unix()
		attr.CtimeSec = now.Unix()
		attr.AtimeSec = now.Unix()
	}
	return attr
}

type chunkView struct {
	fileID        string
	start         int64
	stop          int64
	offsetInChunk int64
	size          int64
	cipherKey     []byte
	isGzipped     bool
	modifiedTsNs  int64
	order         int
}

func visibleChunkViews(chunks []*filer_pb.FileChunk, offset, size int64) ([]chunkView, error) {
	if size <= 0 {
		return nil, nil
	}
	stop := offset + size
	ordered := make([]chunkView, 0, len(chunks))
	for i, chunk := range chunks {
		if chunk.GetIsChunkManifest() {
			return nil, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoSys, Msg: "chunk manifest is not supported by RDMA swvfs backend yet"}
		}
		fileID := chunk.GetFileIdString()
		if fileID == "" || chunk.Size == 0 {
			continue
		}
		chunkStart := chunk.Offset
		chunkStop := chunk.Offset + int64(chunk.Size)
		start := maxInt64(offset, chunkStart)
		end := minInt64(stop, chunkStop)
		if start >= end {
			continue
		}
		ordered = append(ordered, chunkView{
			fileID:        fileID,
			start:         start,
			stop:          end,
			offsetInChunk: start - chunkStart,
			size:          end - start,
			cipherKey:     chunk.CipherKey,
			isGzipped:     chunk.IsCompressed,
			modifiedTsNs:  chunk.ModifiedTsNs,
			order:         i,
		})
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].modifiedTsNs == ordered[j].modifiedTsNs {
			return ordered[i].order < ordered[j].order
		}
		return ordered[i].modifiedTsNs < ordered[j].modifiedTsNs
	})

	var visible []chunkView
	for _, next := range ordered {
		visible = overlayChunkView(visible, next)
	}
	sort.SliceStable(visible, func(i, j int) bool {
		return visible[i].start < visible[j].start
	})
	return visible, nil
}

func overlayChunkView(existing []chunkView, next chunkView) []chunkView {
	out := make([]chunkView, 0, len(existing)+1)
	for _, cur := range existing {
		if cur.stop <= next.start || next.stop <= cur.start {
			out = append(out, cur)
			continue
		}
		if cur.start < next.start {
			left := cur
			left.stop = next.start
			left.size = left.stop - left.start
			out = append(out, left)
		}
		if next.stop < cur.stop {
			right := cur
			right.offsetInChunk += next.stop - cur.start
			right.start = next.stop
			right.size = right.stop - right.start
			out = append(out, right)
		}
	}
	out = append(out, next)
	return out
}

func entryFileSize(entry *filer_pb.Entry) uint64 {
	if entry == nil {
		return 0
	}
	var size uint64
	if entry.Attributes != nil {
		size = entry.Attributes.FileSize
	}
	for _, chunk := range entry.GetChunks() {
		end := uint64(chunk.Offset + int64(chunk.Size))
		if end > size {
			size = end
		}
	}
	if entry.RemoteEntry != nil && entry.Attributes != nil &&
		entry.RemoteEntry.RemoteMtime > entry.Attributes.Mtime &&
		uint64(entry.RemoteEntry.RemoteSize) > size {
		size = uint64(entry.RemoteEntry.RemoteSize)
	}
	return size
}

func ParseFileID(fileID string) (volumeID uint32, needleID uint64, cookie uint32, err error) {
	fid, err := needle.ParseFileIdFromString(fileID)
	if err != nil {
		return 0, 0, 0, err
	}
	return uint32(fid.VolumeId), uint64(fid.Key), uint32(fid.Cookie), nil
}

func cleanFullPath(p string) string {
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return path.Clean(p)
}

func splitFullPath(fullPath string) (dir, name string) {
	fullPath = cleanFullPath(fullPath)
	if fullPath == "/" {
		return "/", ""
	}
	dir, name = path.Split(fullPath)
	dir = strings.TrimRight(dir, "/")
	if dir == "" {
		dir = "/"
	}
	return dir, name
}

func volumeServerFromURL(raw string) (string, error) {
	if raw == "" {
		return "", fmt.Errorf("empty volume url")
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if u.Host == "" {
		return "", fmt.Errorf("invalid volume url %q", raw)
	}
	u.Path = ""
	u.RawQuery = ""
	u.Fragment = ""
	return strings.TrimRight(u.String(), "/"), nil
}

func mapLookupErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, filer_pb.ErrNotFound) || errors.Is(err, io.EOF) {
		return swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoEnt, Msg: err.Error()}
	}
	return err
}

func stableInode(fullPath string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(cleanFullPath(fullPath)))
	v := h.Sum64()
	if v < 2 {
		v += 2
	}
	return v
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
