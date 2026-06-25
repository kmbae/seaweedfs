package swvfsfiler

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
)

type GRPCStore struct {
	Filers      []pb.ServerAddress
	DialOption  grpc.DialOption
	Collection  string
	Replication string
	TTLSeconds  int32
	DataCenter  string
	DiskType    string
}

func (s *GRPCStore) LookupEntry(ctx context.Context, fullPath string) (*filer_pb.Entry, error) {
	dir, name := splitFullPath(fullPath)
	var entry *filer_pb.Entry
	err := s.withFiler(ctx, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		})
		if err != nil {
			return err
		}
		entry = resp.Entry
		filer_pb.AfterEntryDeserialization(entry.GetChunks())
		return nil
	})
	return entry, err
}

func (s *GRPCStore) ListEntries(ctx context.Context, dir string, start string, limit uint32) ([]*filer_pb.Entry, bool, error) {
	var entries []*filer_pb.Entry
	eof := false
	err := s.withFiler(ctx, func(client filer_pb.SeaweedFilerClient) error {
		stream, err := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
			Directory:          cleanFullPath(dir),
			StartFromFileName:  start,
			InclusiveStartFrom: false,
			Limit:              limit,
		})
		if err != nil {
			return err
		}
		for {
			resp, err := stream.Recv()
			if errorsIsEOF(err) {
				eof = true
				return nil
			}
			if err != nil {
				return err
			}
			if resp.Entry != nil {
				filer_pb.AfterEntryDeserialization(resp.Entry.GetChunks())
				entries = append(entries, resp.Entry)
			}
		}
	})
	return entries, eof, err
}

func (s *GRPCStore) SaveEntry(ctx context.Context, fullPath string, entry *filer_pb.Entry) error {
	dir, name := splitFullPath(fullPath)
	entry.Name = name
	return s.withFiler(ctx, func(client filer_pb.SeaweedFilerClient) error {
		return filer_pb.CreateEntry(ctx, client, &filer_pb.CreateEntryRequest{
			Directory:                dir,
			Entry:                    entry,
			SkipCheckParentDirectory: true,
		})
	})
}

func (s *GRPCStore) DeleteEntry(ctx context.Context, fullPath string, recursive bool) error {
	dir, name := splitFullPath(fullPath)
	return s.withFiler(ctx, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
			Directory:          dir,
			Name:               name,
			IsDeleteData:       true,
			IsRecursive:        recursive,
			Signatures:         nil,
			IsFromOtherCluster: false,
		})
		if err != nil {
			return err
		}
		if resp.Error != "" {
			return fmt.Errorf("DeleteEntry %s: %s", fullPath, resp.Error)
		}
		return nil
	})
}

func (s *GRPCStore) AssignVolume(ctx context.Context, fullPath string, size uint64) (fileID, volumeServer string, err error) {
	err = s.withFiler(ctx, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.AssignVolume(ctx, &filer_pb.AssignVolumeRequest{
			Count:            1,
			Collection:       s.Collection,
			Replication:      s.Replication,
			TtlSec:           s.TTLSeconds,
			DataCenter:       s.DataCenter,
			DiskType:         s.DiskType,
			Path:             fullPath,
			ExpectedDataSize: size,
		})
		if err != nil {
			return err
		}
		if resp.Error != "" {
			return fmt.Errorf("AssignVolume: %s", resp.Error)
		}
		fileID = resp.FileId
		if resp.Location == nil || resp.Location.Url == "" {
			return fmt.Errorf("AssignVolume returned no volume location for %s", fileID)
		}
		volumeServer = "http://" + resp.Location.Url
		return nil
	})
	return fileID, volumeServer, err
}

func (s *GRPCStore) LookupFileID(ctx context.Context, fileID string) ([]string, error) {
	volumeID := strings.Split(fileID, ",")[0]
	var urls []string
	err := s.withFiler(ctx, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.LookupVolume(ctx, &filer_pb.LookupVolumeRequest{VolumeIds: []string{volumeID}})
		if err != nil {
			return err
		}
		locs := resp.LocationsMap[volumeID]
		if locs == nil || len(locs.Locations) == 0 {
			return fmt.Errorf("volume %s not found for file %s", volumeID, fileID)
		}
		for _, loc := range locs.Locations {
			host := loc.Url
			if host == "" {
				host = loc.PublicUrl
			}
			if host != "" {
				urls = append(urls, "http://"+host+"/"+fileID)
			}
		}
		return nil
	})
	return urls, err
}

func (s *GRPCStore) withFiler(ctx context.Context, fn func(filer_pb.SeaweedFilerClient) error) error {
	if s == nil || len(s.Filers) == 0 {
		return fmt.Errorf("no filer configured")
	}
	var lastErr error
	for _, filerAddr := range s.Filers {
		err := pb.WithGrpcFilerClient(false, 0, filerAddr, s.DialOption, fn)
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

func errorsIsEOF(err error) bool {
	return err == io.EOF
}
