package swvfsfiler

import (
	"context"
	"errors"
	"testing"

	"seaweedfs-rdma-sidecar/pkg/swvfsdaemon"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

type fakeStore struct {
	entries      map[string]*filer_pb.Entry
	savedPath    string
	assignedPath string
	totalSize    uint64
	usedSize     uint64
	fileCount    uint64
}

func (s *fakeStore) LookupEntry(ctx context.Context, fullPath string) (*filer_pb.Entry, error) {
	entry := s.entries[fullPath]
	if entry == nil {
		return nil, filer_pb.ErrNotFound
	}
	return entry, nil
}

func (s *fakeStore) ListEntries(ctx context.Context, dir string, start string, limit uint32) ([]*filer_pb.Entry, bool, error) {
	var entries []*filer_pb.Entry
	for _, entry := range s.entries {
		entries = append(entries, entry)
	}
	if len(entries) > int(limit) {
		return entries[:limit], false, nil
	}
	return entries, true, nil
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
