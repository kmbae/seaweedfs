package swvfsfiler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"sort"
	"syscall"
	"testing"
	"time"

	"seaweedfs-rdma-sidecar/pkg/swvfsdaemon"
	"seaweedfs-rdma-sidecar/pkg/swvfsproto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

type fakeStore struct {
	entries      map[string]*filer_pb.Entry
	lookupMisses map[string]bool
	savedPath    string
	assignedPath string
	assignFileID string
	assignServer string
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
	fileID := s.assignFileID
	if fileID == "" {
		fileID = "3,01637037d6"
	}
	server := s.assignServer
	if server == "" {
		server = "http://vol:8080"
	}
	return fileID, server, nil
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
	writeSizes  []int
}

func (p *capturePlane) ReadNeedle(ctx context.Context, req swvfsdaemon.NeedleReadRequest) (*swvfsdaemon.NeedleReadResult, error) {
	p.readPrefer = req.PreferRDMA
	return &swvfsdaemon.NeedleReadResult{Data: []byte("chunk-data"), Source: "rdma", UsedRDMA: req.PreferRDMA}, nil
}

func (p *capturePlane) WriteNeedle(ctx context.Context, req swvfsdaemon.NeedleWriteRequest) (*swvfsdaemon.NeedleWriteResult, error) {
	p.writePrefer = req.PreferRDMA
	p.writes++
	p.writeSizes = append(p.writeSizes, len(req.Data))
	return &swvfsdaemon.NeedleWriteResult{FileID: req.FileID, Source: "rdma", UsedRDMA: req.PreferRDMA}, nil
}

type captureNativeReadDescriptor struct {
	calls int
	req   swvfsdaemon.NeedleReadDescriptorRequest
	desc  swvfsproto.RDMADataDesc
	err   error
}

func (c *captureNativeReadDescriptor) ReadNeedleRDMA(ctx context.Context, req swvfsdaemon.NeedleReadDescriptorRequest) (*swvfsproto.RDMADataDesc, *swvfsproto.Attr, error) {
	c.calls++
	c.req = req
	if c.err != nil {
		return nil, nil, c.err
	}
	return &c.desc, nil, nil
}

func (c *captureNativeReadDescriptor) ReleaseReadDescriptor(ctx context.Context, leaseID uint64, status int32, bytes uint64) error {
	c.calls++
	return nil
}

type captureReadDescriptor struct {
	calls int
	path  string
	desc  swvfsproto.RDMADataDesc
}

func (c *captureReadDescriptor) ReadFileRDMA(ctx context.Context, path string, offset, size uint64) (*swvfsproto.RDMADataDesc, *swvfsproto.Attr, error) {
	c.calls++
	c.path = path
	return &c.desc, nil, nil
}

func (c *captureReadDescriptor) ReleaseReadDescriptor(ctx context.Context, leaseID uint64, status int32, bytes uint64) error {
	c.calls++
	return nil
}

type fakeNativeWriteControl struct {
	connected swvfsproto.RDMARemoteInfo
}

func (f *fakeNativeWriteControl) GetLocal() (swvfsproto.RDMALocalInfo, error) {
	return swvfsproto.RDMALocalInfo{
		ABIVersion: swvfsproto.RDMAABIVersion,
		Flags:      swvfsproto.RDMAFKernelEnabled | swvfsproto.RDMAFEndpointReady,
		Port:       1,
		QPNum:      10,
		PSN:        20,
		LID:        30,
		LinkLayer:  swvfsproto.RDMALinkInfiniBand,
	}, nil
}

func (f *fakeNativeWriteControl) Connect(remote swvfsproto.RDMARemoteInfo) error {
	f.connected = remote
	return nil
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

func TestBackendReadFileRDMAPrefersNativeSingleChunk(t *testing.T) {
	store := &fakeStore{entries: map[string]*filer_pb.Entry{
		"/file": {
			Name: "file",
			Attributes: &filer_pb.FuseAttributes{
				FileMode: 0644,
				FileSize: 1024,
			},
			Chunks: []*filer_pb.FileChunk{{
				FileId:       "3,01637037d6",
				Offset:       0,
				Size:         1024,
				ModifiedTsNs: 1,
			}},
		},
	}}
	native := &captureNativeReadDescriptor{desc: swvfsproto.RDMADataDesc{RemoteAddr: 0xbeef, RKey: 99, Length: 512}}
	staging := &captureReadDescriptor{desc: swvfsproto.RDMADataDesc{RemoteAddr: 0xdead, RKey: 7, Length: 512}}
	backend := &Backend{
		Store:                 store,
		NativeReadDescriptor:  native,
		ReadDescriptorBackend: staging,
	}

	desc, attr, err := backend.ReadFileRDMA(context.Background(), "/file", 128, 512)
	if err != nil {
		t.Fatalf("ReadFileRDMA: %v", err)
	}
	if native.calls != 1 || staging.calls != 0 {
		t.Fatalf("native/staging calls = %d/%d", native.calls, staging.calls)
	}
	if native.req.VolumeID != 3 || native.req.NeedleID == 0 || native.req.VolumeServer != "http://vol:8080" {
		t.Fatalf("unexpected native request: %+v", native.req)
	}
	if native.req.Offset != 128 || native.req.Size != 512 {
		t.Fatalf("unexpected native range: %+v", native.req)
	}
	if desc.RemoteAddr != 0xbeef || attr == nil || attr.Size != 1024 {
		t.Fatalf("desc=%+v attr=%+v", desc, attr)
	}
}

func TestBackendReadFileRDMAReturnsFirstNativeChunkForMultiChunk(t *testing.T) {
	store := &fakeStore{entries: map[string]*filer_pb.Entry{
		"/file": {
			Name: "file",
			Attributes: &filer_pb.FuseAttributes{
				FileMode: 0644,
				FileSize: 2048,
			},
			Chunks: []*filer_pb.FileChunk{
				{FileId: "3,01637037d6", Offset: 0, Size: 1024, ModifiedTsNs: 1},
				{FileId: "4,01637037d7", Offset: 1024, Size: 1024, ModifiedTsNs: 2},
			},
		},
	}}
	native := &captureNativeReadDescriptor{desc: swvfsproto.RDMADataDesc{RemoteAddr: 0xbeef, RKey: 99, Length: 1024}}
	staging := &captureReadDescriptor{desc: swvfsproto.RDMADataDesc{RemoteAddr: 0xdead, RKey: 7, Length: 2048}}
	backend := &Backend{
		Store:                 store,
		NativeReadDescriptor:  native,
		ReadDescriptorBackend: staging,
	}

	desc, _, err := backend.ReadFileRDMA(context.Background(), "/file", 0, 2048)
	if err != nil {
		t.Fatalf("ReadFileRDMA: %v", err)
	}
	if native.calls != 1 || staging.calls != 0 {
		t.Fatalf("native/staging calls = %d/%d", native.calls, staging.calls)
	}
	if native.req.Offset != 0 || native.req.Size != 1024 || native.req.FileID != "3,01637037d6" {
		t.Fatalf("unexpected native request: %+v", native.req)
	}
	if desc.RemoteAddr != 0xbeef || desc.Length != 1024 {
		t.Fatalf("desc = %+v", desc)
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
	if plane.writes != 0 {
		t.Fatalf("write flushed before FlushFile: writes=%d", plane.writes)
	}
	attr, err = backend.FlushFile(context.Background(), "/file")
	if err != nil {
		t.Fatalf("FlushFile: %v", err)
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

func TestBackendWriteRDMAPrefersNativeVolumeDescriptor(t *testing.T) {
	var sawConnect, sawPrepare, sawCommit bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case swvfsdaemon.VolumeRDMALocalPath:
			_ = json.NewEncoder(w).Encode(swvfsdaemon.RDMALocalEndpoint{
				ConnectionID:  44,
				ABIVersion:    swvfsproto.RDMAABIVersion,
				KernelEnabled: true,
				EndpointReady: true,
				Port:          1,
				QPNum:         123,
				PSN:           456,
				LID:           789,
				LinkLayer:     swvfsproto.RDMALinkInfiniBand,
			})
		case swvfsdaemon.VolumeRDMAConnectPath:
			sawConnect = true
			if got := r.URL.Query().Get("connection_id"); got != "44" {
				t.Errorf("connection_id = %q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]bool{"connected": true})
		case swvfsdaemon.VolumeRDMAWriteDescPath:
			sawPrepare = true
			var req swvfsdaemon.VolumeRDMAWriteDescRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode write desc: %v", err)
				return
			}
			if req.ConnectionID != 44 || req.FileID != "3,01637037d6" || req.VolumeID != 3 || req.Size != 4096 {
				t.Errorf("write desc request = %+v", req)
			}
			_ = json.NewEncoder(w).Encode(swvfsdaemon.VolumeRDMAWriteDescResponse{
				Desc: swvfsproto.RDMADataDesc{
					RemoteAddr: 0xbeef,
					RKey:       77,
					Length:     4096,
				},
				ConnectionID: 44,
				SessionID:    55,
			})
		case swvfsdaemon.VolumeRDMAWriteCommitPath:
			sawCommit = true
			var req swvfsdaemon.VolumeRDMAWriteCommitRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode write commit: %v", err)
				return
			}
			if req.SessionID != 55 || req.FileID != "3,01637037d6" || req.Size != 4096 {
				t.Errorf("write commit request = %+v", req)
			}
			_ = json.NewEncoder(w).Encode(swvfsdaemon.VolumeRDMAWriteResponse{
				FileID: req.FileID,
				Size:   req.Size,
				Source: "native-volume-rdma-write-desc",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store := &fakeStore{
		entries:      map[string]*filer_pb.Entry{},
		assignServer: server.URL,
		assignFileID: "3,01637037d6",
	}
	control := &fakeNativeWriteControl{}
	backend := &Backend{
		Store: store,
		NativeWriteDescriptor: &NativeVolumeWriteDescriptorClient{
			Control: control,
			Timeout: time.Second,
		},
	}

	desc, _, err := backend.PrepareWriteRDMA(context.Background(), "/native", 0, 4096)
	if err != nil {
		t.Fatalf("PrepareWriteRDMA: %v", err)
	}
	if desc.RemoteAddr != 0xbeef || desc.RKey != 77 || desc.Length != 4096 || desc.Reserved[0] != 55 || desc.Reserved[1] != 44 {
		t.Fatalf("desc = %+v", desc)
	}
	attr, err := backend.CommitWriteRDMA(context.Background(), "/native", 0, 4096)
	if err != nil {
		t.Fatalf("CommitWriteRDMA: %v", err)
	}
	if attr.Size != 4096 {
		t.Fatalf("attr size = %d", attr.Size)
	}
	if !sawConnect || !sawPrepare || !sawCommit {
		t.Fatalf("saw connect/prepare/commit = %v/%v/%v", sawConnect, sawPrepare, sawCommit)
	}
	if control.connected.QPN != 123 || control.connected.PSN != 456 || control.connected.LID != 789 {
		t.Fatalf("connected remote = %+v", control.connected)
	}
	entry := store.entries["/native"]
	if entry == nil || len(entry.Chunks) != 1 || entry.Chunks[0].FileId != "3,01637037d6" || entry.Chunks[0].Size != 4096 {
		t.Fatalf("saved entry = %+v", entry)
	}
}

func TestBackendSequentialWritesCoalesceUntilFlush(t *testing.T) {
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

	if _, err := backend.WriteFile(context.Background(), "/file", 0, []byte("abc"), 0644, 1000, 1000, true); err != nil {
		t.Fatalf("first WriteFile: %v", err)
	}
	if _, err := backend.WriteFile(context.Background(), "/file", 3, []byte("defg"), 0644, 1000, 1000, true); err != nil {
		t.Fatalf("second WriteFile: %v", err)
	}
	if plane.writes != 0 {
		t.Fatalf("writes before flush = %d", plane.writes)
	}
	data, attr, err := backend.ReadFile(context.Background(), "/file", 0, 7, true)
	if err != nil {
		t.Fatalf("ReadFile pending: %v", err)
	}
	if string(data) != "abcdefg" || attr.Size != 7 {
		t.Fatalf("pending read data=%q size=%d", data, attr.Size)
	}

	attr, err = backend.FlushFile(context.Background(), "/file")
	if err != nil {
		t.Fatalf("FlushFile: %v", err)
	}
	if plane.writes != 1 || len(plane.writeSizes) != 1 || plane.writeSizes[0] != 7 {
		t.Fatalf("coalesced writes mismatch: writes=%d sizes=%v", plane.writes, plane.writeSizes)
	}
	entry := store.entries["/file"]
	if entry == nil || len(entry.Chunks) != 1 || entry.Chunks[0].Size != 7 {
		t.Fatalf("saved chunks = %+v", entry)
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

func TestBackendCreateFileTouchesParentTimes(t *testing.T) {
	store := &fakeStore{entries: map[string]*filer_pb.Entry{
		"/dir": {
			Name:        "dir",
			IsDirectory: true,
			Attributes: &filer_pb.FuseAttributes{
				FileMode: uint32(os.ModeDir | 0755),
				Mtime:    1,
				Ctime:    1,
			},
		},
	}}
	backend := &Backend{Store: store}
	if _, err := backend.CreateFile(context.Background(), "/dir/file", uint32(syscall.S_IFREG|0644), 1000, 1000); err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	parent := store.entries["/dir"]
	if parent.Attributes.GetMtime() <= 1 || parent.Attributes.GetCtime() <= 1 {
		t.Fatalf("parent times were not touched: %+v", parent.Attributes)
	}
}

func TestAttrFromEntryPreservesZeroModeForNewEntry(t *testing.T) {
	entry := newEntry("/zero", false, uint32(syscall.S_IFREG), 1000, 1000)
	attr := AttrFromEntry("/zero", entry)
	if attr.Mode&uint32(syscall.S_IFMT) != uint32(syscall.S_IFREG) {
		t.Fatalf("attr mode is not regular: %#o", attr.Mode)
	}
	if attr.Mode&0777 != 0 {
		t.Fatalf("zero permissions were not preserved: %#o", attr.Mode)
	}

	legacy := &filer_pb.Entry{Name: "legacy", Attributes: &filer_pb.FuseAttributes{}}
	legacyAttr := AttrFromEntry("/legacy", legacy)
	if legacyAttr.Mode&0777 != 0644 {
		t.Fatalf("legacy empty file mode did not fall back to 0644: %#o", legacyAttr.Mode)
	}
}

func TestNewEntriesSetAccessTime(t *testing.T) {
	entries := []*filer_pb.Entry{
		newEntry("/file", false, uint32(syscall.S_IFREG|0644), 1000, 1000),
		newEntry("/dir", true, uint32(syscall.S_IFDIR|0755), 1000, 1000),
		newSymlinkEntry("/link", "target", 1000, 1000),
		newSpecialEntry("/fifo", uint32(syscall.S_IFIFO|0644), 1000, 1000, 0),
	}
	for _, entry := range entries {
		if entry.Attributes == nil || entry.Attributes.GetAtime() == 0 {
			t.Fatalf("%s access time was not initialized: %+v", entry.GetName(), entry.GetAttributes())
		}
	}
}

func TestBackendLookupDirectoryNlinkCountsImmediateChildDirectories(t *testing.T) {
	store := &fakeStore{entries: map[string]*filer_pb.Entry{
		"/dir":            {Name: "dir", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{FileMode: uint32(os.ModeDir | 0755)}},
		"/dir/file":       {Name: "file", Attributes: &filer_pb.FuseAttributes{FileMode: 0644}},
		"/dir/sub":        {Name: "sub", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{FileMode: uint32(os.ModeDir | 0755)}},
		"/dir/sub/nested": {Name: "nested", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{FileMode: uint32(os.ModeDir | 0755)}},
	}}
	backend := &Backend{Store: store}
	attr, err := backend.LookupFile(context.Background(), "/dir")
	if err != nil {
		t.Fatalf("LookupFile: %v", err)
	}
	if attr.Nlink != 3 {
		t.Fatalf("directory nlink = %d, want 3", attr.Nlink)
	}
}

func TestNewEntriesDoNotReusePathHashInode(t *testing.T) {
	file := newEntry("/same/path", false, uint32(syscall.S_IFREG|0644), 1000, 1000)
	time.Sleep(time.Nanosecond)
	fifo := newSpecialEntry("/same/path", uint32(syscall.S_IFIFO|0644), 1000, 1000, 0)

	fileAttr := AttrFromEntry("/same/path", file)
	fifoAttr := AttrFromEntry("/same/path", fifo)
	if fileAttr.Ino == 0 || fifoAttr.Ino == 0 {
		t.Fatalf("new entry inode is zero: file=%d fifo=%d", fileAttr.Ino, fifoAttr.Ino)
	}
	if fileAttr.Ino == fifoAttr.Ino {
		t.Fatalf("new entries reused inode for same path: %d", fileAttr.Ino)
	}
	if fileAttr.Ino == stableInode("/same/path") || fifoAttr.Ino == stableInode("/same/path") {
		t.Fatalf("new entry fell back to path hash inode: file=%d fifo=%d", fileAttr.Ino, fifoAttr.Ino)
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

func TestBackendRenameEntryRejectsNonEmptyTargetDirectory(t *testing.T) {
	store := &fakeStore{entries: map[string]*filer_pb.Entry{
		"/old":          {Name: "old", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{FileMode: uint32(os.ModeDir | 0755)}},
		"/target":       {Name: "target", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{FileMode: uint32(os.ModeDir | 0755)}},
		"/target/child": {Name: "child", Attributes: &filer_pb.FuseAttributes{FileMode: 0644}},
	}}
	backend := &Backend{Store: store}
	err := backend.RenameEntry(context.Background(), "/old", "/target")
	var errno swvfsdaemon.ErrnoError
	if !errors.As(err, &errno) || errno.Errno != swvfsdaemon.ErrnoNotEmpty {
		t.Fatalf("expected ENOTEMPTY, got %v", err)
	}
	if store.entries["/old"] == nil || store.entries["/target"] == nil || store.entries["/target/child"] == nil {
		t.Fatalf("rename changed entries despite failure: %+v", store.entries)
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
	sourceAttr := AttrFromEntry("/file", source)
	linkedAttr := AttrFromEntry("/hard", linked)
	if sourceAttr.Ino != linkedAttr.Ino {
		t.Fatalf("hardlinks have different inode numbers: source=%d linked=%d", sourceAttr.Ino, linkedAttr.Ino)
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
