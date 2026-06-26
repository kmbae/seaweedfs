package swvfsfiler

import (
	"context"
	"errors"
	"os"
	"path"
	"sort"
	"syscall"
	"testing"

	"seaweedfs-rdma-sidecar/pkg/swvfsdaemon"
	"seaweedfs-rdma-sidecar/pkg/swvfsproto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

type fakeStore struct {
	entries      map[string]*filer_pb.Entry
	lookupMisses map[string]bool
	savedPath    string
	assignedPath string
	totalSize    uint64
	usedSize     uint64
	fileCount    uint64
}

func (s *fakeStore) LookupEntry(ctx context.Context, fullPath string) (*filer_pb.Entry, error) {
	if s.lookupMisses[fullPath] {
		return nil, filer_pb.ErrNotFound
	}
	entry := s.entries[fullPath]
	if entry == nil {
		return nil, filer_pb.ErrNotFound
	}
	return entry, nil
}

func (s *fakeStore) ListEntries(ctx context.Context, dir string, start string, limit uint32) ([]*filer_pb.Entry, bool, error) {
	var entries []*filer_pb.Entry
	dir = cleanFullPath(dir)
	for fullPath, entry := range s.entries {
		parent, name := splitFullPath(fullPath)
		if parent != dir || name == "" {
			continue
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name < entries[j].Name
	})
	if start != "" {
		keep := entries[:0]
		for _, entry := range entries {
			if entry.Name > start {
				keep = append(keep, entry)
			}
		}
		entries = keep
	}
	if len(entries) > int(limit) {
		return entries[:limit], false, nil
	}
	return entries, true, nil
}

func TestBackendLookupFallsBackToDirectoryListing(t *testing.T) {
	store := &fakeStore{
		entries: map[string]*filer_pb.Entry{
			"/dir/file": {
				Name: "file",
				Attributes: &filer_pb.FuseAttributes{
					FileMode: 0644,
					FileSize: 13,
				},
			},
		},
		lookupMisses: map[string]bool{"/dir/file": true},
	}
	backend := &Backend{Store: store}

	attr, err := backend.LookupFile(context.Background(), "/dir/file")
	if err != nil {
		t.Fatalf("LookupFile: %v", err)
	}
	if attr == nil || attr.Size != 13 || attr.Mode&0777 != 0644 {
		t.Fatalf("unexpected attr: %+v", attr)
	}
}

func (s *fakeStore) SaveEntry(ctx context.Context, fullPath string, entry *filer_pb.Entry) error {
	s.savedPath = fullPath
	s.entries[fullPath] = entry
	return nil
}

func (s *fakeStore) DeleteEntry(ctx context.Context, fullPath string, recursive bool) error {
	delete(s.entries, fullPath)
	return nil
}

func (s *fakeStore) RenameEntry(ctx context.Context, oldPath, newPath string) error {
	entry := s.entries[oldPath]
	if entry == nil {
		return filer_pb.ErrNotFound
	}
	delete(s.entries, oldPath)
	entry.Name = path.Base(newPath)
	s.entries[newPath] = entry
	return nil
}

func (s *fakeStore) AssignVolume(ctx context.Context, fullPath string, size uint64) (string, string, error) {
	s.assignedPath = fullPath
	return "3,01637037d6", "http://vol:8080", nil
}

func (s *fakeStore) LookupFileID(ctx context.Context, fileID string) ([]string, error) {
	return []string{"http://vol:8080/" + fileID}, nil
}

func (s *fakeStore) Statistics(ctx context.Context) (uint64, uint64, uint64, error) {
	return s.totalSize, s.usedSize, s.fileCount, nil
}

type capturePlane struct {
	readPrefer  bool
	writePrefer bool
	writes      int
}

func (p *capturePlane) ReadNeedle(ctx context.Context, req swvfsdaemon.NeedleReadRequest) (*swvfsdaemon.NeedleReadResult, error) {
	p.readPrefer = req.PreferRDMA
	return &swvfsdaemon.NeedleReadResult{Data: []byte("chunk-data"), Source: "rdma", UsedRDMA: req.PreferRDMA}, nil
}

func (p *capturePlane) WriteNeedle(ctx context.Context, req swvfsdaemon.NeedleWriteRequest) (*swvfsdaemon.NeedleWriteResult, error) {
	p.writePrefer = req.PreferRDMA
	p.writes++
	return &swvfsdaemon.NeedleWriteResult{FileID: req.FileID, Source: "rdma", UsedRDMA: req.PreferRDMA}, nil
}

func TestBackendReadUsesRDMAHint(t *testing.T) {
	store := &fakeStore{entries: map[string]*filer_pb.Entry{
		"/file": {
			Name: "file",
			Attributes: &filer_pb.FuseAttributes{
				FileMode: 0644,
				FileSize: 10,
			},
			Chunks: []*filer_pb.FileChunk{{
				FileId:       "3,01637037d6",
				Offset:       0,
				Size:         10,
				ModifiedTsNs: 1,
			}},
		},
	}}
	plane := &capturePlane{}
	backend := &Backend{
		Store: store,
		Router: &swvfsdaemon.Router{
			RDMA:            plane,
			Fallback:        plane,
			EnableReadRDMA:  true,
			FallbackOnError: true,
		},
	}
	data, attr, err := backend.ReadFile(context.Background(), "/file", 0, 10, true)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !plane.readPrefer {
		t.Fatal("RDMA read preference was not passed")
	}
	if string(data) != "chunk-data" {
		t.Fatalf("data = %q", data)
	}
	if attr.Size != 10 {
		t.Fatalf("attr size = %d", attr.Size)
	}
}

func TestBackendWriteAssignsAndSavesChunk(t *testing.T) {
	store := &fakeStore{entries: map[string]*filer_pb.Entry{}}
	plane := &capturePlane{}
	backend := &Backend{
		Store: store,
		Router: &swvfsdaemon.Router{
			RDMA:            plane,
			Fallback:        plane,
			EnableWriteRDMA: true,
			FallbackOnError: true,
		},
	}
	attr, err := backend.WriteFile(context.Background(), "/file", 4, []byte("abc"), 0644, 1000, 1000, true)
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if !plane.writePrefer || plane.writes != 1 {
		t.Fatalf("write routing mismatch: prefer=%v writes=%d", plane.writePrefer, plane.writes)
	}
	if store.savedPath != "/file" || store.assignedPath != "/file" {
		t.Fatalf("paths not saved/assigned: saved=%s assigned=%s", store.savedPath, store.assignedPath)
	}
	entry := store.entries["/file"]
	if entry == nil || len(entry.Chunks) != 1 {
		t.Fatalf("saved entry chunks = %+v", entry)
	}
	if entry.Chunks[0].Offset != 4 || entry.Chunks[0].Size != 3 {
		t.Fatalf("chunk = %+v", entry.Chunks[0])
	}
	if attr.Size != 7 {
		t.Fatalf("attr size = %d", attr.Size)
	}
}

func TestBackendMkdirStoresDirectoryMode(t *testing.T) {
	store := &fakeStore{entries: map[string]*filer_pb.Entry{}}
	backend := &Backend{Store: store}
	attr, err := backend.Mkdir(context.Background(), "/bench", uint32(syscall.S_IFDIR|0755), 1000, 1000)
	if err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	entry := store.entries["/bench"]
	if entry == nil {
		t.Fatal("directory entry was not saved")
	}
	if !entry.IsDirectory {
		t.Fatal("entry was not marked as directory")
	}
	if entry.Attributes == nil || os.FileMode(entry.Attributes.FileMode)&os.ModeDir == 0 {
		t.Fatalf("saved mode is not a directory: %#o", entry.Attributes.GetFileMode())
	}
	if attr.Mode&uint32(syscall.S_IFMT) != uint32(syscall.S_IFDIR) {
		t.Fatalf("attr mode is not a directory: %#o", attr.Mode)
	}
}

func TestBackendSetAttrUpdatesTimesAndSize(t *testing.T) {
	store := &fakeStore{entries: map[string]*filer_pb.Entry{
		"/file": {
			Name: "file",
			Attributes: &filer_pb.FuseAttributes{
				FileMode: 0644,
				FileSize: 20,
			},
			Chunks: []*filer_pb.FileChunk{{
				FileId: "3,01637037d6",
				Size:   20,
			}},
		},
	}}
	backend := &Backend{Store: store}
	attr, err := backend.SetAttr(context.Background(), "/file", swvfsproto.RequestHeader{
		Valid:     swvfsproto.SetSize | swvfsproto.SetMTime | swvfsproto.SetATime,
		Size:      0,
		MtimeSec:  123,
		MtimeNsec: 456,
		AtimeSec:  789,
		AtimeNsec: 987,
	})
	if err != nil {
		t.Fatalf("SetAttr: %v", err)
	}
	entry := store.entries["/file"]
	if entry.Attributes.GetFileSize() != 0 || len(entry.Chunks) != 0 {
		t.Fatalf("truncate not saved: size=%d chunks=%d", entry.Attributes.GetFileSize(), len(entry.Chunks))
	}
	if entry.Attributes.GetMtime() != 123 || entry.Attributes.GetMtimeNs() != 456 || entry.Attributes.GetAtime() != 789 || entry.Attributes.GetAtimeNs() != 987 {
		t.Fatalf("timestamps not saved: %+v", entry.Attributes)
	}
	if attr.Size != 0 || attr.MtimeSec != 123 || attr.AtimeSec != 789 {
		t.Fatalf("attr mismatch: %+v", attr)
	}
}

func TestBackendSetAttrTrimsChunksOnPartialTruncate(t *testing.T) {
	store := &fakeStore{entries: map[string]*filer_pb.Entry{
		"/file": {
			Name:       "file",
			Attributes: &filer_pb.FuseAttributes{FileMode: 0644, FileSize: 20},
			Chunks: []*filer_pb.FileChunk{
				{FileId: "3,aaa", Offset: 0, Size: 10},
				{FileId: "3,bbb", Offset: 10, Size: 10},
				{FileId: "3,ccc", Offset: 20, Size: 10},
			},
		},
	}}
	backend := &Backend{Store: store}
	attr, err := backend.SetAttr(context.Background(), "/file", swvfsproto.RequestHeader{
		Valid: swvfsproto.SetSize,
		Size:  12,
	})
	if err != nil {
		t.Fatalf("SetAttr: %v", err)
	}
	entry := store.entries["/file"]
	if attr.Size != 12 || entryFileSize(entry) != 12 || entry.Attributes.GetFileSize() != 12 {
		t.Fatalf("size mismatch: attr=%d entry=%d stored=%d", attr.Size, entryFileSize(entry), entry.Attributes.GetFileSize())
	}
	if len(entry.Chunks) != 2 || entry.Chunks[0].Size != 10 || entry.Chunks[1].Offset != 10 || entry.Chunks[1].Size != 2 {
		t.Fatalf("chunks not trimmed: %+v", entry.Chunks)
	}
}

func TestBackendDeleteFileEnforcesPOSIXDirectoryRules(t *testing.T) {
	store := &fakeStore{entries: map[string]*filer_pb.Entry{
		"/dir":       {Name: "dir", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{FileMode: uint32(os.ModeDir | 0755)}},
		"/dir/child": {Name: "child", Attributes: &filer_pb.FuseAttributes{FileMode: 0644}},
		"/file":      {Name: "file", Attributes: &filer_pb.FuseAttributes{FileMode: 0644}},
	}}
	backend := &Backend{Store: store}

	err := backend.DeleteFile(context.Background(), "/dir", true)
	var errno swvfsdaemon.ErrnoError
	if !errors.As(err, &errno) || errno.Errno != swvfsdaemon.ErrnoNotEmpty {
		t.Fatalf("expected ENOTEMPTY, got %v", err)
	}
	if store.entries["/dir"] == nil {
		t.Fatal("non-empty directory was deleted")
	}

	err = backend.DeleteFile(context.Background(), "/file", true)
	if !errors.As(err, &errno) || errno.Errno != swvfsdaemon.ErrnoNotDir {
		t.Fatalf("expected ENOTDIR, got %v", err)
	}

	err = backend.DeleteFile(context.Background(), "/dir", false)
	if !errors.As(err, &errno) || errno.Errno != swvfsdaemon.ErrnoIsDir {
		t.Fatalf("expected EISDIR, got %v", err)
	}

	delete(store.entries, "/dir/child")
	if err := backend.DeleteFile(context.Background(), "/dir", true); err != nil {
		t.Fatalf("empty rmdir: %v", err)
	}
	if store.entries["/dir"] != nil {
		t.Fatal("empty directory was not deleted")
	}
}

func TestBackendSymlinkAndReadLink(t *testing.T) {
	store := &fakeStore{entries: map[string]*filer_pb.Entry{}}
	backend := &Backend{Store: store}
	attr, err := backend.Symlink(context.Background(), "/link", "../target", 1000, 1000)
	if err != nil {
		t.Fatalf("Symlink: %v", err)
	}
	if attr.Mode&uint32(syscall.S_IFMT) != uint32(syscall.S_IFLNK) {
		t.Fatalf("attr mode is not a symlink: %#o", attr.Mode)
	}
	if attr.Size != uint64(len("../target")) {
		t.Fatalf("symlink attr size = %d", attr.Size)
	}
	target, err := backend.ReadLink(context.Background(), "/link")
	if err != nil {
		t.Fatalf("ReadLink: %v", err)
	}
	if string(target) != "../target" {
		t.Fatalf("target = %q", target)
	}
}

func TestBackendRenameEntry(t *testing.T) {
	store := &fakeStore{entries: map[string]*filer_pb.Entry{
		"/old": {Name: "old", Attributes: &filer_pb.FuseAttributes{FileMode: 0644}},
	}}
	backend := &Backend{Store: store}
	if err := backend.RenameEntry(context.Background(), "/old", "/dir/new"); err != nil {
		t.Fatalf("RenameEntry: %v", err)
	}
	if store.entries["/old"] != nil || store.entries["/dir/new"] == nil {
		t.Fatalf("rename not reflected: %+v", store.entries)
	}
	if store.entries["/dir/new"].Name != "new" {
		t.Fatalf("new entry name = %q", store.entries["/dir/new"].Name)
	}
}

func TestBackendLinkEntryStoresHardLinkMetadata(t *testing.T) {
	store := &fakeStore{entries: map[string]*filer_pb.Entry{
		"/file": {Name: "file", Attributes: &filer_pb.FuseAttributes{FileMode: 0644, FileSize: 9}},
	}}
	backend := &Backend{Store: store}

	attr, err := backend.LinkEntry(context.Background(), "/file", "/hard")
	if err != nil {
		t.Fatalf("LinkEntry: %v", err)
	}
	if attr.Nlink != 2 {
		t.Fatalf("link attr nlink = %d", attr.Nlink)
	}
	source := store.entries["/file"]
	linked := store.entries["/hard"]
	if source == nil || linked == nil {
		t.Fatalf("entries missing after link: %+v", store.entries)
	}
	if len(source.HardLinkId) == 0 || string(source.HardLinkId) != string(linked.HardLinkId) {
		t.Fatalf("hardlink ids differ: source=%x linked=%x", source.HardLinkId, linked.HardLinkId)
	}
	if source.HardLinkCounter != 2 || linked.HardLinkCounter != 2 {
		t.Fatalf("hardlink counters = source:%d linked:%d", source.HardLinkCounter, linked.HardLinkCounter)
	}
	if linked.Name != "hard" {
		t.Fatalf("linked name = %q", linked.Name)
	}

	_, err = backend.LinkEntry(context.Background(), "/file", "/hard")
	var errno swvfsdaemon.ErrnoError
	if !errors.As(err, &errno) || errno.Errno != swvfsdaemon.ErrnoExist {
		t.Fatalf("expected EEXIST for existing link path, got %v", err)
	}
}

func TestBackendLinkEntryRejectsDirectories(t *testing.T) {
	store := &fakeStore{entries: map[string]*filer_pb.Entry{
		"/dir": {Name: "dir", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{FileMode: uint32(os.ModeDir | 0755)}},
	}}
	backend := &Backend{Store: store}

	_, err := backend.LinkEntry(context.Background(), "/dir", "/dir-link")
	var errno swvfsdaemon.ErrnoError
	if !errors.As(err, &errno) || errno.Errno != swvfsdaemon.ErrnoPerm {
		t.Fatalf("expected EPERM for directory hardlink, got %v", err)
	}
}

func TestBackendMknodStoresSpecialMode(t *testing.T) {
	store := &fakeStore{entries: map[string]*filer_pb.Entry{}}
	backend := &Backend{Store: store}
	attr, err := backend.Mknod(context.Background(), "/fifo", uint32(syscall.S_IFIFO|0644), 1000, 1000, 0)
	if err != nil {
		t.Fatalf("Mknod: %v", err)
	}
	if attr.Mode&uint32(syscall.S_IFMT) != uint32(syscall.S_IFIFO) {
		t.Fatalf("attr mode is not FIFO: %#o", attr.Mode)
	}
	if store.entries["/fifo"].Attributes == nil || os.FileMode(store.entries["/fifo"].Attributes.FileMode)&os.ModeNamedPipe == 0 {
		t.Fatalf("stored mode is not named pipe: %#o", store.entries["/fifo"].Attributes.GetFileMode())
	}
}

func TestBackendXAttrLifecycle(t *testing.T) {
	store := &fakeStore{entries: map[string]*filer_pb.Entry{
		"/file": {Name: "file", Attributes: &filer_pb.FuseAttributes{FileMode: 0644}},
	}}
	backend := &Backend{Store: store}
	ctx := context.Background()

	if err := backend.SetXAttr(ctx, "/file", "user.alpha", []byte("one"), swvfsproto.XAttrCreate, false); err != nil {
		t.Fatalf("set create xattr: %v", err)
	}
	value, err := backend.GetXAttr(ctx, "/file", "user.alpha")
	if err != nil {
		t.Fatalf("get xattr: %v", err)
	}
	if string(value) != "one" {
		t.Fatalf("xattr value = %q", value)
	}
	list, err := backend.ListXAttr(ctx, "/file")
	if err != nil {
		t.Fatalf("list xattr: %v", err)
	}
	if string(list) != "user.alpha\x00" {
		t.Fatalf("xattr list = %q", list)
	}

	err = backend.SetXAttr(ctx, "/file", "user.alpha", []byte("again"), swvfsproto.XAttrCreate, false)
	var errno swvfsdaemon.ErrnoError
	if !errors.As(err, &errno) || errno.Errno != swvfsdaemon.ErrnoExist {
		t.Fatalf("expected EEXIST, got %v", err)
	}

	if err := backend.SetXAttr(ctx, "/file", "user.alpha", []byte("two"), swvfsproto.XAttrReplace, false); err != nil {
		t.Fatalf("replace xattr: %v", err)
	}
	value, err = backend.GetXAttr(ctx, "/file", "user.alpha")
	if err != nil || string(value) != "two" {
		t.Fatalf("replaced xattr = %q err=%v", value, err)
	}

	if err := backend.SetXAttr(ctx, "/file", "user.alpha", nil, 0, true); err != nil {
		t.Fatalf("remove xattr: %v", err)
	}
	_, err = backend.GetXAttr(ctx, "/file", "user.alpha")
	if !errors.As(err, &errno) || errno.Errno != swvfsdaemon.ErrnoNoData {
		t.Fatalf("expected ENODATA after remove, got %v", err)
	}
}

func TestBackendReadMapsNotFound(t *testing.T) {
	backend := &Backend{Store: &fakeStore{entries: map[string]*filer_pb.Entry{}}, Router: &swvfsdaemon.Router{}}
	_, _, err := backend.ReadFile(context.Background(), "/missing", 0, 1, false)
	var errno swvfsdaemon.ErrnoError
	if !errors.As(err, &errno) || errno.Errno != swvfsdaemon.ErrnoNoEnt {
		t.Fatalf("expected ENOENT, got %v", err)
	}
}

func TestBackendStatFSUsesStoreStatistics(t *testing.T) {
	backend := &Backend{Store: &fakeStore{
		entries:   map[string]*filer_pb.Entry{},
		totalSize: 4096 * 100,
		usedSize:  4096 * 20,
		fileCount: 12,
	}}
	stat, err := backend.StatFS(context.Background(), "/")
	if err != nil {
		t.Fatalf("StatFS: %v", err)
	}
	if stat.Blocks != 100 || stat.Bfree != 80 || stat.Bavail != 80 || stat.Bsize != 4096 || stat.Namelen != 255 {
		t.Fatalf("statfs = %+v", stat)
	}
}
