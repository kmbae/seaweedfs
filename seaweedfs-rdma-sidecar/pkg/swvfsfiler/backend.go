// Package swvfsfiler maps seaweedvfs path requests onto SeaweedFS filer/chunk IO.
package swvfsfiler

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
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
	ListEntries(ctx context.Context, dir string, start string, limit uint32) ([]*filer_pb.Entry, bool, error)
	SaveEntry(ctx context.Context, fullPath string, entry *filer_pb.Entry) error
	DeleteEntry(ctx context.Context, fullPath string, recursive bool) error
	RenameEntry(ctx context.Context, oldPath, newPath string) error
	AssignVolume(ctx context.Context, fullPath string, size uint64) (fileID, volumeServer string, err error)
	LookupFileID(ctx context.Context, fileID string) ([]string, error)
}

type Backend struct {
	Store                  MetadataStore
	Router                 *swvfsdaemon.Router
	ReadDescriptorBackend  swvfsdaemon.RDMAReadDescriptorBackend
	WriteDescriptorBackend swvfsdaemon.RDMAWriteDescriptorBackend

	mu      sync.Mutex
	pending map[string]*pendingWrite
}

const (
	direntTypeDir = 4
	direntTypeReg = 8
	direntTypeLnk = 10

	statBlockSize    = 4096
	defaultTotalSize = uint64(1) << 50
	defaultFileCount = uint64(1) << 32

	defaultBufferedWriteMax = 32 << 20
	defaultRegularMode      = uint32(syscall.S_IFREG | 0644)
)

type pendingWrite struct {
	path       string
	offset     uint64
	data       []byte
	mode       uint32
	uid        uint32
	gid        uint32
	preferRDMA bool
	updated    time.Time
}

func (p *pendingWrite) end() uint64 {
	if p == nil {
		return 0
	}
	return p.offset + uint64(len(p.data))
}

func (p *pendingWrite) clone() *pendingWrite {
	if p == nil {
		return nil
	}
	next := *p
	next.data = append([]byte(nil), p.data...)
	return &next
}

func (b *Backend) pendingSnapshot(fullPath string) *pendingWrite {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.pending == nil {
		return nil
	}
	return b.pending[cleanFullPath(fullPath)].clone()
}

func (b *Backend) takePending(fullPath string) *pendingWrite {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.pending == nil {
		return nil
	}
	fullPath = cleanFullPath(fullPath)
	p := b.pending[fullPath]
	if p != nil {
		delete(b.pending, fullPath)
	}
	return p
}

func attrFromPending(p *pendingWrite) *swvfsproto.Attr {
	if p == nil {
		return nil
	}
	now := p.updated
	if now.IsZero() {
		now = time.Now()
	}
	mode := p.mode
	if mode == 0 {
		mode = defaultRegularMode
	}
	if mode&uint32(syscall.S_IFMT) == 0 {
		mode = uint32(syscall.S_IFREG) | (mode & 07777)
	}
	return &swvfsproto.Attr{
		Ino:       stableInode(p.path),
		Size:      p.end(),
		Mode:      mode,
		Nlink:     1,
		UID:       p.uid,
		GID:       p.gid,
		MtimeSec:  now.Unix(),
		CtimeSec:  now.Unix(),
		AtimeSec:  now.Unix(),
		MtimeNsec: uint32(now.Nanosecond()),
		CtimeNsec: uint32(now.Nanosecond()),
		AtimeNsec: uint32(now.Nanosecond()),
	}
}

func overlayPendingAttr(attr *swvfsproto.Attr, pending *pendingWrite) *swvfsproto.Attr {
	if attr == nil || pending == nil {
		return attr
	}
	if end := pending.end(); end > attr.Size {
		attr.Size = end
	}
	if !pending.updated.IsZero() {
		attr.MtimeSec = pending.updated.Unix()
		attr.MtimeNsec = uint32(pending.updated.Nanosecond())
		attr.CtimeSec = pending.updated.Unix()
		attr.CtimeNsec = uint32(pending.updated.Nanosecond())
	}
	return attr
}

func overlayPendingData(dst []byte, readOffset uint64, pending *pendingWrite) {
	if len(dst) == 0 || pending == nil || len(pending.data) == 0 {
		return
	}
	readEnd := readOffset + uint64(len(dst))
	pendingStart := pending.offset
	pendingEnd := pending.end()
	if readEnd <= pendingStart || readOffset >= pendingEnd {
		return
	}
	start := maxUint64(readOffset, pendingStart)
	end := minUint64(readEnd, pendingEnd)
	copy(dst[int(start-readOffset):int(end-readOffset)], pending.data[int(start-pendingStart):int(end-pendingStart)])
}

func (b *Backend) LookupFile(ctx context.Context, fullPath string) (*swvfsproto.Attr, error) {
	fullPath = cleanFullPath(fullPath)
	if fullPath == "/" {
		return rootAttr(), nil
	}
	pending := b.pendingSnapshot(fullPath)
	entry, err := b.Store.LookupEntry(ctx, fullPath)
	if err != nil {
		if isLookupNotFound(err) {
			listedEntry, ok, listErr := b.lookupFileFromDirectory(ctx, fullPath)
			if listErr == nil && ok {
				attr, err := b.attrFromEntry(ctx, fullPath, listedEntry)
				if err != nil {
					return nil, err
				}
				return overlayPendingAttr(attr, pending), nil
			}
			if listErr != nil && !isLookupNotFound(listErr) {
				return nil, listErr
			}
			if pending != nil {
				return attrFromPending(pending), nil
			}
		}
		return nil, mapLookupErr(err)
	}
	attr, err := b.attrFromEntry(ctx, fullPath, entry)
	if err != nil {
		return nil, err
	}
	return overlayPendingAttr(attr, pending), nil
}

func (b *Backend) lookupFileFromDirectory(ctx context.Context, fullPath string) (*filer_pb.Entry, bool, error) {
	dir, name := splitFullPath(fullPath)
	if name == "" {
		return nil, false, nil
	}

	start := ""
	for i := 0; i < 16; i++ {
		entries, eof, err := b.Store.ListEntries(ctx, dir, start, swvfsproto.MaxDirents)
		if err != nil {
			return nil, false, err
		}
		if len(entries) == 0 {
			return nil, false, nil
		}
		for _, entry := range entries {
			if entry != nil && entry.Name == name {
				return entry, true, nil
			}
		}
		last := entries[len(entries)-1]
		if eof || last == nil || last.Name == "" || last.Name >= name {
			return nil, false, nil
		}
		start = last.Name
	}
	return nil, false, nil
}

func (b *Backend) ReadDir(ctx context.Context, fullPath string, offset uint64, limit uint32) ([]swvfsproto.Dirent, bool, error) {
	fullPath = cleanFullPath(fullPath)
	if limit == 0 || limit > swvfsproto.MaxDirents {
		limit = swvfsproto.MaxDirents
	}
	fetchLimit := uint32(offset) + limit
	if fetchLimit < limit {
		fetchLimit = limit
	}
	entries, eof, err := b.Store.ListEntries(ctx, fullPath, "", fetchLimit)
	if err != nil {
		return nil, false, err
	}
	if offset >= uint64(len(entries)) {
		return nil, eof, nil
	}
	entries = entries[int(offset):]
	if len(entries) > int(limit) {
		entries = entries[:limit]
		eof = false
	}
	dirents := make([]swvfsproto.Dirent, 0, len(entries))
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		entryPath := path.Join(fullPath, entry.Name)
		attr, err := b.attrFromEntry(ctx, entryPath, entry)
		if err != nil {
			return nil, false, err
		}
		dirents = append(dirents, swvfsproto.Dirent{
			Attr: *attr,
			Type: direntType(entry),
			Name: entry.Name,
		})
	}
	return dirents, eof && len(entries) < int(limit), nil
}

func (b *Backend) CreateFile(ctx context.Context, fullPath string, mode, uid, gid uint32) (*swvfsproto.Attr, error) {
	entry := newEntry(fullPath, false, mode, uid, gid)
	fullPath = cleanFullPath(fullPath)
	if err := b.Store.SaveEntry(ctx, fullPath, entry); err != nil {
		return nil, err
	}
	if err := b.touchParent(ctx, fullPath); err != nil {
		return nil, err
	}
	return AttrFromEntry(fullPath, entry), nil
}

func (b *Backend) Mkdir(ctx context.Context, fullPath string, mode, uid, gid uint32) (*swvfsproto.Attr, error) {
	entry := newEntry(fullPath, true, mode, uid, gid)
	fullPath = cleanFullPath(fullPath)
	if err := b.Store.SaveEntry(ctx, fullPath, entry); err != nil {
		return nil, err
	}
	if err := b.touchParent(ctx, fullPath); err != nil {
		return nil, err
	}
	return AttrFromEntry(fullPath, entry), nil
}

func (b *Backend) DeleteFile(ctx context.Context, fullPath string, isDir bool) error {
	fullPath = cleanFullPath(fullPath)
	if _, err := b.FlushFile(ctx, fullPath); err != nil {
		return err
	}
	entry, err := b.Store.LookupEntry(ctx, fullPath)
	if err != nil {
		return mapLookupErr(err)
	}
	if isDir {
		if !entry.IsDirectory {
			return swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNotDir, Msg: "not a directory"}
		}
		entries, _, err := b.Store.ListEntries(ctx, fullPath, "", 1)
		if err != nil {
			return err
		}
		if len(entries) > 0 {
			return swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNotEmpty, Msg: "directory not empty"}
		}
	} else if entry.IsDirectory {
		return swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoIsDir, Msg: "is a directory"}
	}
	if err := b.Store.DeleteEntry(ctx, fullPath, false); err != nil {
		return err
	}
	return b.touchParent(ctx, fullPath)
}

func (b *Backend) RenameEntry(ctx context.Context, oldPath, newPath string) error {
	oldPath = cleanFullPath(oldPath)
	newPath = cleanFullPath(newPath)
	if _, err := b.FlushFile(ctx, oldPath); err != nil {
		return err
	}
	if _, err := b.FlushFile(ctx, newPath); err != nil {
		return err
	}
	if oldPath == newPath {
		_, err := b.Store.LookupEntry(ctx, oldPath)
		return mapLookupErr(err)
	}
	oldEntry, err := b.Store.LookupEntry(ctx, oldPath)
	if err != nil {
		return mapLookupErr(err)
	}
	if target, err := b.Store.LookupEntry(ctx, newPath); err == nil {
		switch {
		case oldEntry.IsDirectory && !target.IsDirectory:
			return swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNotDir, Msg: "target is not a directory"}
		case !oldEntry.IsDirectory && target.IsDirectory:
			return swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoIsDir, Msg: "target is a directory"}
		case oldEntry.IsDirectory && target.IsDirectory:
			entries, _, err := b.Store.ListEntries(ctx, newPath, "", 1)
			if err != nil {
				return err
			}
			if len(entries) > 0 {
				return swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNotEmpty, Msg: "target directory not empty"}
			}
		}
	} else if !isLookupNotFound(err) {
		return mapLookupErr(err)
	}
	if err := b.Store.RenameEntry(ctx, oldPath, newPath); err != nil {
		return err
	}
	if err := b.touchParent(ctx, oldPath); err != nil {
		return err
	}
	oldDir, _ := splitFullPath(oldPath)
	newDir, _ := splitFullPath(newPath)
	if newDir != oldDir {
		return b.touchParent(ctx, newPath)
	}
	return nil
}

func (b *Backend) LinkEntry(ctx context.Context, oldPath, newPath string) (*swvfsproto.Attr, error) {
	oldPath = cleanFullPath(oldPath)
	newPath = cleanFullPath(newPath)
	if _, err := b.FlushFile(ctx, oldPath); err != nil {
		return nil, err
	}
	if _, err := b.FlushFile(ctx, newPath); err != nil {
		return nil, err
	}
	if newPath == "/" {
		return nil, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoPerm, Msg: "cannot link over root"}
	}
	if _, err := b.Store.LookupEntry(ctx, newPath); err == nil {
		return nil, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoExist, Msg: "link path exists"}
	} else if !isLookupNotFound(err) {
		return nil, mapLookupErr(err)
	}

	source, err := b.Store.LookupEntry(ctx, oldPath)
	if err != nil {
		return nil, mapLookupErr(err)
	}
	if source.IsDirectory {
		return nil, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoPerm, Msg: "cannot hardlink a directory"}
	}

	original, ok := proto.Clone(source).(*filer_pb.Entry)
	if !ok {
		return nil, fmt.Errorf("clone hardlink source entry")
	}
	updated, ok := proto.Clone(source).(*filer_pb.Entry)
	if !ok {
		return nil, fmt.Errorf("clone hardlink updated source entry")
	}
	if updated.Attributes == nil {
		updated.Attributes = &filer_pb.FuseAttributes{}
	}
	if len(updated.HardLinkId) == 0 {
		id, err := newHardLinkID()
		if err != nil {
			return nil, err
		}
		updated.HardLinkId = id
		updated.HardLinkCounter = 1
	}
	updated.HardLinkCounter++
	now := time.Now()
	updated.Attributes.Ctime = now.Unix()
	updated.Attributes.CtimeNs = int32(now.Nanosecond())

	if err := b.Store.SaveEntry(ctx, oldPath, updated); err != nil {
		return nil, err
	}
	linked, ok := proto.Clone(updated).(*filer_pb.Entry)
	if !ok {
		return nil, fmt.Errorf("clone hardlink target entry")
	}
	linked.Name = path.Base(newPath)
	if err := b.Store.SaveEntry(ctx, newPath, linked); err != nil {
		if rollbackErr := b.Store.SaveEntry(ctx, oldPath, original); rollbackErr != nil {
			return nil, fmt.Errorf("create hardlink: %w (rollback failed: %v)", err, rollbackErr)
		}
		return nil, err
	}
	if err := b.touchParent(ctx, newPath); err != nil {
		return nil, err
	}
	return AttrFromEntry(oldPath, updated), nil
}

func newHardLinkID() ([]byte, error) {
	id := make([]byte, 17)
	if _, err := rand.Read(id[:16]); err != nil {
		return nil, err
	}
	id[16] = 1
	return id, nil
}

func (b *Backend) Symlink(ctx context.Context, linkPath, target string, uid, gid uint32) (*swvfsproto.Attr, error) {
	linkPath = cleanFullPath(linkPath)
	entry := newSymlinkEntry(linkPath, target, uid, gid)
	if err := b.Store.SaveEntry(ctx, linkPath, entry); err != nil {
		return nil, err
	}
	if err := b.touchParent(ctx, linkPath); err != nil {
		return nil, err
	}
	return AttrFromEntry(linkPath, entry), nil
}

func (b *Backend) ReadLink(ctx context.Context, linkPath string) ([]byte, error) {
	linkPath = cleanFullPath(linkPath)
	entry, err := b.Store.LookupEntry(ctx, linkPath)
	if err != nil {
		return nil, mapLookupErr(err)
	}
	if entry.Attributes == nil || entry.Attributes.SymlinkTarget == "" {
		return nil, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoInval, Msg: "not a symbolic link"}
	}
	return []byte(entry.Attributes.SymlinkTarget), nil
}

func (b *Backend) Mknod(ctx context.Context, fullPath string, mode, uid, gid, rdev uint32) (*swvfsproto.Attr, error) {
	fullPath = cleanFullPath(fullPath)
	entry := newSpecialEntry(fullPath, mode, uid, gid, rdev)
	if err := b.Store.SaveEntry(ctx, fullPath, entry); err != nil {
		return nil, err
	}
	if err := b.touchParent(ctx, fullPath); err != nil {
		return nil, err
	}
	return AttrFromEntry(fullPath, entry), nil
}

func (b *Backend) SetAttr(ctx context.Context, fullPath string, header swvfsproto.RequestHeader) (*swvfsproto.Attr, error) {
	fullPath = cleanFullPath(fullPath)
	if _, err := b.FlushFile(ctx, fullPath); err != nil {
		return nil, err
	}
	entry, err := b.Store.LookupEntry(ctx, fullPath)
	if err != nil {
		return nil, mapLookupErr(err)
	}
	entry = proto.Clone(entry).(*filer_pb.Entry)
	if entry.Attributes == nil {
		entry.Attributes = &filer_pb.FuseAttributes{}
	}
	a := entry.Attributes
	if header.Valid&swvfsproto.SetMode != 0 {
		a.FileMode = normalizeFileMode(entry.IsDirectory, header.Mode)
	}
	if header.Valid&swvfsproto.SetUID != 0 {
		a.Uid = header.UID
	}
	if header.Valid&swvfsproto.SetGID != 0 {
		a.Gid = header.GID
	}
	if header.Valid&swvfsproto.SetSize != 0 {
		a.FileSize = header.Size
		entry.Chunks = trimChunksToSize(entry.GetChunks(), header.Size)
		if uint64(len(entry.Content)) > header.Size {
			entry.Content = entry.Content[:header.Size]
		}
	}
	if header.Valid&swvfsproto.SetMTime != 0 {
		a.Mtime = header.MtimeSec
		a.MtimeNs = int32(header.MtimeNsec)
	}
	if header.Valid&swvfsproto.SetATime != 0 {
		a.Atime = header.AtimeSec
		a.AtimeNs = int32(header.AtimeNsec)
	}
	now := time.Now()
	a.Ctime = now.Unix()
	a.CtimeNs = int32(now.Nanosecond())
	if err := b.Store.SaveEntry(ctx, fullPath, entry); err != nil {
		return nil, err
	}
	return AttrFromEntry(fullPath, entry), nil
}

func (b *Backend) StatFS(ctx context.Context, fullPath string) (*swvfsproto.StatFS, error) {
	total := defaultTotalSize
	used := uint64(0)
	files := defaultFileCount

	if b != nil && b.Store != nil {
		if provider, ok := b.Store.(interface {
			Statistics(context.Context) (totalSize, usedSize, fileCount uint64, err error)
		}); ok {
			totalSize, usedSize, fileCount, err := provider.Statistics(ctx)
			if err != nil {
				return nil, err
			}
			if totalSize > 0 {
				total = totalSize
			}
			if usedSize > 0 {
				used = usedSize
			}
			if fileCount > 0 {
				files = maxUint64(fileCount, defaultFileCount)
			}
		}
	}
	if used > total {
		total = used
	}
	free := total - used
	return &swvfsproto.StatFS{
		Blocks:  bytesToBlocks(total),
		Bfree:   bytesToBlocks(free),
		Bavail:  bytesToBlocks(free),
		Files:   files,
		Ffree:   files,
		Bsize:   statBlockSize,
		Namelen: swvfsproto.NameMax,
	}, nil
}

func (b *Backend) GetXAttr(ctx context.Context, fullPath, name string) ([]byte, error) {
	if name == "" {
		return nil, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoInval, Msg: "empty xattr name"}
	}
	entry, err := b.Store.LookupEntry(ctx, cleanFullPath(fullPath))
	if err != nil {
		return nil, mapLookupErr(err)
	}
	value, ok := entry.GetExtended()[name]
	if !ok {
		return nil, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoData, Msg: "xattr not found"}
	}
	return append([]byte(nil), value...), nil
}

func (b *Backend) ListXAttr(ctx context.Context, fullPath string) ([]byte, error) {
	entry, err := b.Store.LookupEntry(ctx, cleanFullPath(fullPath))
	if err != nil {
		return nil, mapLookupErr(err)
	}
	if len(entry.GetExtended()) == 0 {
		return nil, nil
	}
	names := make([]string, 0, len(entry.GetExtended()))
	for name := range entry.GetExtended() {
		names = append(names, name)
	}
	sort.Strings(names)
	var out []byte
	for _, name := range names {
		out = append(out, name...)
		out = append(out, 0)
	}
	return out, nil
}

func (b *Backend) SetXAttr(ctx context.Context, fullPath, name string, value []byte, flags uint32, remove bool) error {
	if name == "" {
		return swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoInval, Msg: "empty xattr name"}
	}
	fullPath = cleanFullPath(fullPath)
	if _, err := b.FlushFile(ctx, fullPath); err != nil {
		return err
	}
	entry, err := b.Store.LookupEntry(ctx, fullPath)
	if err != nil {
		return mapLookupErr(err)
	}
	entry = proto.Clone(entry).(*filer_pb.Entry)
	if entry.Extended == nil {
		entry.Extended = map[string][]byte{}
	}
	_, exists := entry.Extended[name]
	if remove {
		if !exists {
			return swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoData, Msg: "xattr not found"}
		}
		delete(entry.Extended, name)
	} else {
		if flags&swvfsproto.XAttrCreate != 0 && exists {
			return swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoExist, Msg: "xattr exists"}
		}
		if flags&swvfsproto.XAttrReplace != 0 && !exists {
			return swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoData, Msg: "xattr not found"}
		}
		entry.Extended[name] = append([]byte(nil), value...)
	}
	if entry.Attributes == nil {
		entry.Attributes = &filer_pb.FuseAttributes{}
	}
	now := time.Now()
	entry.Attributes.Ctime = now.Unix()
	entry.Attributes.CtimeNs = int32(now.Nanosecond())
	return b.Store.SaveEntry(ctx, fullPath, entry)
}

func (b *Backend) ReadFile(ctx context.Context, fullPath string, offset, size uint64, preferRDMA bool) ([]byte, *swvfsproto.Attr, error) {
	if b == nil || b.Store == nil || b.Router == nil {
		return nil, nil, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoSys, Msg: "swvfs filer backend is not configured"}
	}
	fullPath = cleanFullPath(fullPath)
	pending := b.pendingSnapshot(fullPath)
	entry, err := b.Store.LookupEntry(ctx, fullPath)
	if err != nil {
		if !isLookupNotFound(err) || pending == nil {
			return nil, nil, mapLookupErr(err)
		}
		attr := attrFromPending(pending)
		if offset >= attr.Size || size == 0 {
			return nil, attr, nil
		}
		readSize := minUint64(size, attr.Size-offset)
		out := make([]byte, readSize)
		overlayPendingData(out, offset, pending)
		return out, attr, nil
	}
	if entry.IsDirectory {
		return nil, nil, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoInval, Msg: "cannot read a directory"}
	}
	attr := overlayPendingAttr(AttrFromEntry(fullPath, entry), pending)
	fileSize := attr.Size
	if offset >= fileSize || size == 0 {
		return nil, attr, nil
	}
	readSize := minUint64(size, fileSize-offset)

	if len(entry.Content) > 0 {
		out := make([]byte, readSize)
		stop := minUint64(uint64(len(entry.Content)), offset+readSize)
		if offset < stop {
			copy(out, entry.Content[offset:stop])
		}
		overlayPendingData(out, offset, pending)
		return out, attr, nil
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
		dstEnd := minUint64(uint64(len(out)), dstStart+uint64(view.size))
		copy(out[int(dstStart):int(dstEnd)], resp.Data)
	}
	overlayPendingData(out, offset, pending)
	return out, attr, nil
}

func (b *Backend) ReadFileRDMA(ctx context.Context, fullPath string, offset, size uint64) (*swvfsproto.RDMADataDesc, *swvfsproto.Attr, error) {
	if b == nil || b.ReadDescriptorBackend == nil {
		return nil, nil, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoSys, Msg: "rdma read descriptor backend is not configured"}
	}
	return b.ReadDescriptorBackend.ReadFileRDMA(ctx, cleanFullPath(fullPath), offset, size)
}

func (b *Backend) ReleaseReadDescriptor(ctx context.Context, leaseID uint64, status int32, bytes uint64) error {
	if b == nil || b.ReadDescriptorBackend == nil {
		return swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoSys, Msg: "rdma read descriptor backend is not configured"}
	}
	releaser, ok := b.ReadDescriptorBackend.(swvfsdaemon.RDMAReadDescriptorReleaseBackend)
	if !ok {
		return swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoSys, Msg: "rdma read descriptor release backend is not configured"}
	}
	return releaser.ReleaseReadDescriptor(ctx, leaseID, status, bytes)
}

func (b *Backend) PrepareWriteRDMA(ctx context.Context, fullPath string, offset, size uint64) (*swvfsproto.RDMADataDesc, *swvfsproto.Attr, error) {
	if b == nil || b.WriteDescriptorBackend == nil {
		return nil, nil, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoSys, Msg: "rdma write descriptor backend is not configured"}
	}
	return b.WriteDescriptorBackend.PrepareWriteRDMA(ctx, cleanFullPath(fullPath), offset, size)
}

func (b *Backend) CommitWriteRDMA(ctx context.Context, fullPath string, offset, size uint64) (*swvfsproto.Attr, error) {
	if b == nil || b.WriteDescriptorBackend == nil {
		return nil, swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoSys, Msg: "rdma write descriptor backend is not configured"}
	}
	return b.WriteDescriptorBackend.CommitWriteRDMA(ctx, cleanFullPath(fullPath), offset, size)
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

	payload := append([]byte(nil), data...)
	for {
		var flush *pendingWrite
		b.mu.Lock()
		if b.pending == nil {
			b.pending = make(map[string]*pendingWrite)
		}
		pending := b.pending[fullPath]
		if pending != nil && offset != pending.end() {
			flush = pending
			delete(b.pending, fullPath)
			b.mu.Unlock()
			if _, err := b.persistPendingWrite(ctx, flush); err != nil {
				return nil, err
			}
			continue
		}
		if pending == nil {
			pending = &pendingWrite{
				path:       fullPath,
				offset:     offset,
				mode:       mode,
				uid:        uid,
				gid:        gid,
				preferRDMA: preferRDMA,
			}
			b.pending[fullPath] = pending
		}
		pending.data = append(pending.data, payload...)
		pending.preferRDMA = pending.preferRDMA || preferRDMA
		pending.updated = time.Now()
		if len(pending.data) >= defaultBufferedWriteMax {
			flush = pending
			delete(b.pending, fullPath)
			b.mu.Unlock()
			return b.persistPendingWrite(ctx, flush)
		}
		attr := attrFromPending(pending)
		b.mu.Unlock()
		return attr, nil
	}
}

func (b *Backend) FlushFile(ctx context.Context, fullPath string) (*swvfsproto.Attr, error) {
	pending := b.takePending(fullPath)
	if pending == nil {
		return nil, nil
	}
	return b.persistPendingWrite(ctx, pending)
}

func (b *Backend) persistPendingWrite(ctx context.Context, pending *pendingWrite) (*swvfsproto.Attr, error) {
	if pending == nil || len(pending.data) == 0 {
		return nil, nil
	}
	mode := pending.mode
	if mode == 0 {
		mode = defaultRegularMode
	}
	fileID, volumeServer, err := b.Store.AssignVolume(ctx, pending.path, uint64(len(pending.data)))
	if err != nil {
		return nil, err
	}
	if _, err := b.Router.WriteNeedle(ctx, swvfsdaemon.NeedleWriteRequest{
		FileID:       fileID,
		VolumeServer: volumeServer,
		RDMAServer:   volumeServer,
		Data:         pending.data,
		PreferRDMA:   pending.preferRDMA,
	}); err != nil {
		return nil, err
	}
	entry, err := b.lookupOrCreateEntry(ctx, pending.path, mode, pending.uid, pending.gid)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	tsNs := now.UnixNano()
	entry.Chunks = append(entry.GetChunks(), &filer_pb.FileChunk{
		FileId:       fileID,
		Offset:       int64(pending.offset),
		Size:         uint64(len(pending.data)),
		ModifiedTsNs: tsNs,
	})
	if entry.Attributes == nil {
		entry.Attributes = &filer_pb.FuseAttributes{}
	}
	entry.Attributes.FileSize = maxUint64(entry.Attributes.FileSize, pending.end())
	entry.Attributes.Mtime = now.Unix()
	entry.Attributes.MtimeNs = int32(now.Nanosecond())
	entry.Attributes.Ctime = now.Unix()
	entry.Attributes.CtimeNs = int32(now.Nanosecond())
	if err := b.Store.SaveEntry(ctx, pending.path, entry); err != nil {
		return nil, err
	}
	return AttrFromEntry(pending.path, entry), nil
}

func (b *Backend) lookupOrCreateEntry(ctx context.Context, fullPath string, mode, uid, gid uint32) (*filer_pb.Entry, error) {
	entry, err := b.Store.LookupEntry(ctx, fullPath)
	if err == nil {
		return proto.Clone(entry).(*filer_pb.Entry), nil
	}
	if !errors.Is(err, filer_pb.ErrNotFound) {
		return nil, mapLookupErr(err)
	}
	return newEntry(fullPath, false, mode, uid, gid), nil
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
		if perm == 0 && a.Inode == 0 {
			perm = attr.Mode & 07777
		}
		attr.Mode = linuxModeFromFuseAttributes(entry, a, perm)
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
	if entry.HardLinkCounter > 0 {
		attr.Nlink = uint32(entry.HardLinkCounter)
	}
	if len(entry.HardLinkId) > 0 {
		attr.Ino = stableInodeFromBytes(entry.HardLinkId)
	}
	if attr.MtimeSec == 0 {
		now := time.Now()
		attr.MtimeSec = now.Unix()
		attr.CtimeSec = now.Unix()
		attr.AtimeSec = now.Unix()
	}
	return attr
}

func (b *Backend) attrFromEntry(ctx context.Context, fullPath string, entry *filer_pb.Entry) (*swvfsproto.Attr, error) {
	attr := AttrFromEntry(fullPath, entry)
	if entry != nil && entry.IsDirectory {
		nlink, err := b.directoryLinkCount(ctx, fullPath)
		if err != nil {
			return nil, err
		}
		attr.Nlink = nlink
	}
	return attr, nil
}

func (b *Backend) directoryLinkCount(ctx context.Context, fullPath string) (uint32, error) {
	var childDirs uint32
	start := ""
	for {
		entries, eof, err := b.Store.ListEntries(ctx, fullPath, start, swvfsproto.MaxDirents)
		if err != nil {
			return 0, err
		}
		if len(entries) == 0 {
			return 2 + childDirs, nil
		}
		for _, entry := range entries {
			if entry != nil && entry.IsDirectory {
				childDirs++
			}
		}
		if eof {
			return 2 + childDirs, nil
		}
		last := entries[len(entries)-1]
		if last == nil || last.Name == "" {
			return 2 + childDirs, nil
		}
		start = last.Name
	}
}

func rootAttr() *swvfsproto.Attr {
	now := time.Now()
	return &swvfsproto.Attr{
		Ino:       1,
		Mode:      uint32(syscall.S_IFDIR | 0755),
		Nlink:     2,
		MtimeSec:  now.Unix(),
		CtimeSec:  now.Unix(),
		AtimeSec:  now.Unix(),
		MtimeNsec: uint32(now.Nanosecond()),
		CtimeNsec: uint32(now.Nanosecond()),
		AtimeNsec: uint32(now.Nanosecond()),
	}
}

func newEntry(fullPath string, isDir bool, mode, uid, gid uint32) *filer_pb.Entry {
	now := time.Now()
	return &filer_pb.Entry{
		Name:        path.Base(cleanFullPath(fullPath)),
		IsDirectory: isDir,
		Attributes: &filer_pb.FuseAttributes{
			Inode:    newEntryInode(fullPath, now),
			FileMode: linuxModeToFileMode(mode, isDir),
			Uid:      uid,
			Gid:      gid,
			Atime:    now.Unix(),
			AtimeNs:  int32(now.Nanosecond()),
			Mtime:    now.Unix(),
			MtimeNs:  int32(now.Nanosecond()),
			Ctime:    now.Unix(),
			CtimeNs:  int32(now.Nanosecond()),
			Crtime:   now.Unix(),
			CrtimeNs: int32(now.Nanosecond()),
		},
	}
}

func newSymlinkEntry(fullPath, target string, uid, gid uint32) *filer_pb.Entry {
	now := time.Now()
	return &filer_pb.Entry{
		Name: path.Base(cleanFullPath(fullPath)),
		Attributes: &filer_pb.FuseAttributes{
			Inode:         newEntryInode(fullPath, now),
			FileMode:      uint32(os.ModeSymlink | 0777),
			Uid:           uid,
			Gid:           gid,
			FileSize:      uint64(len(target)),
			Atime:         now.Unix(),
			AtimeNs:       int32(now.Nanosecond()),
			Mtime:         now.Unix(),
			MtimeNs:       int32(now.Nanosecond()),
			Ctime:         now.Unix(),
			CtimeNs:       int32(now.Nanosecond()),
			Crtime:        now.Unix(),
			CrtimeNs:      int32(now.Nanosecond()),
			SymlinkTarget: target,
		},
	}
}

func newSpecialEntry(fullPath string, mode, uid, gid, rdev uint32) *filer_pb.Entry {
	now := time.Now()
	return &filer_pb.Entry{
		Name: path.Base(cleanFullPath(fullPath)),
		Attributes: &filer_pb.FuseAttributes{
			Inode:    newEntryInode(fullPath, now),
			FileMode: linuxModeToFileMode(mode, false),
			Uid:      uid,
			Gid:      gid,
			Rdev:     rdev,
			Atime:    now.Unix(),
			AtimeNs:  int32(now.Nanosecond()),
			Mtime:    now.Unix(),
			MtimeNs:  int32(now.Nanosecond()),
			Ctime:    now.Unix(),
			CtimeNs:  int32(now.Nanosecond()),
			Crtime:   now.Unix(),
			CrtimeNs: int32(now.Nanosecond()),
		},
	}
}

func (b *Backend) touchParent(ctx context.Context, fullPath string) error {
	dir, _ := splitFullPath(fullPath)
	if dir == "" || dir == "/" {
		return nil
	}
	parent, err := b.Store.LookupEntry(ctx, dir)
	if err != nil {
		if isLookupNotFound(err) {
			return nil
		}
		return mapLookupErr(err)
	}
	parent = proto.Clone(parent).(*filer_pb.Entry)
	if parent.Attributes == nil {
		parent.Attributes = &filer_pb.FuseAttributes{FileMode: uint32(os.ModeDir | 0755)}
	}
	now := time.Now()
	parent.Attributes.Mtime = now.Unix()
	parent.Attributes.MtimeNs = int32(now.Nanosecond())
	parent.Attributes.Ctime = now.Unix()
	parent.Attributes.CtimeNs = int32(now.Nanosecond())
	return b.Store.SaveEntry(ctx, dir, parent)
}

func normalizeFileMode(isDir bool, mode uint32) uint32 {
	return linuxModeToFileMode(mode, isDir)
}

func linuxModeToFileMode(mode uint32, forceDir bool) uint32 {
	perm := os.FileMode(mode & 07777)
	switch mode & uint32(syscall.S_IFMT) {
	case uint32(syscall.S_IFDIR):
		return uint32(os.ModeDir | perm)
	case uint32(syscall.S_IFLNK):
		return uint32(os.ModeSymlink | perm)
	case uint32(syscall.S_IFIFO):
		return uint32(os.ModeNamedPipe | perm)
	case uint32(syscall.S_IFSOCK):
		return uint32(os.ModeSocket | perm)
	case uint32(syscall.S_IFCHR):
		return uint32(os.ModeDevice | os.ModeCharDevice | perm)
	case uint32(syscall.S_IFBLK):
		return uint32(os.ModeDevice | perm)
	}
	if forceDir {
		return uint32(os.ModeDir | perm)
	}
	return uint32(perm)
}

func linuxModeFromFuseAttributes(entry *filer_pb.Entry, attr *filer_pb.FuseAttributes, fallbackPerm uint32) uint32 {
	perm := fallbackPerm & 07777
	if attr == nil {
		return uint32(syscall.S_IFREG) | perm
	}
	fileMode := os.FileMode(attr.FileMode)
	switch {
	case entry != nil && entry.IsDirectory || fileMode&os.ModeDir != 0:
		return uint32(syscall.S_IFDIR) | perm
	case attr.SymlinkTarget != "" || fileMode&os.ModeSymlink != 0:
		return uint32(syscall.S_IFLNK) | perm
	case fileMode&os.ModeNamedPipe != 0:
		return uint32(syscall.S_IFIFO) | perm
	case fileMode&os.ModeSocket != 0:
		return uint32(syscall.S_IFSOCK) | perm
	case fileMode&os.ModeDevice != 0 && fileMode&os.ModeCharDevice != 0:
		return uint32(syscall.S_IFCHR) | perm
	case fileMode&os.ModeDevice != 0:
		return uint32(syscall.S_IFBLK) | perm
	default:
		return uint32(syscall.S_IFREG) | perm
	}
}

func direntType(entry *filer_pb.Entry) uint32 {
	if entry == nil {
		return direntTypeReg
	}
	if entry.IsDirectory {
		return direntTypeDir
	}
	if entry.Attributes != nil && entry.Attributes.SymlinkTarget != "" {
		return direntTypeLnk
	}
	return direntTypeReg
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
	if entry.Attributes != nil && entry.Attributes.FileSize > 0 {
		return entry.Attributes.FileSize
	}
	var size uint64
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

func trimChunksToSize(chunks []*filer_pb.FileChunk, size uint64) []*filer_pb.FileChunk {
	if size == 0 || len(chunks) == 0 {
		return nil
	}
	out := make([]*filer_pb.FileChunk, 0, len(chunks))
	for _, chunk := range chunks {
		if chunk == nil || chunk.Size == 0 {
			continue
		}
		if chunk.Offset < 0 || uint64(chunk.Offset) >= size {
			continue
		}
		next := proto.Clone(chunk).(*filer_pb.FileChunk)
		end := uint64(next.Offset) + next.Size
		if end > size {
			next.Size = size - uint64(next.Offset)
		}
		out = append(out, next)
	}
	return out
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
	if isLookupNotFound(err) {
		return swvfsdaemon.ErrnoError{Errno: swvfsdaemon.ErrnoNoEnt, Msg: err.Error()}
	}
	return err
}

func isLookupNotFound(err error) bool {
	return errors.Is(err, filer_pb.ErrNotFound) || errors.Is(err, io.EOF)
}

func stableInode(fullPath string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(cleanFullPath(fullPath)))
	return normalizeInode(h.Sum64())
}

func newEntryInode(fullPath string, now time.Time) uint64 {
	h := fnv.New64a()
	_, _ = fmt.Fprintf(h, "%s\x00%d\x00%d", cleanFullPath(fullPath), now.UnixNano(), os.Getpid())
	return normalizeInode(h.Sum64())
}

func stableInodeFromBytes(data []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(data)
	return normalizeInode(h.Sum64())
}

func normalizeInode(v uint64) uint64 {
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

func bytesToBlocks(size uint64) uint64 {
	return (size + statBlockSize - 1) / statBlockSize
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
