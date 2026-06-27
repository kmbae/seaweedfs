package swvfsdaemon

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"seaweedfs-rdma-sidecar/pkg/swvfsproto"
)

func TestVolumeNativeRDMAReadDescriptorClientMarksAndReleasesLease(t *testing.T) {
	var (
		readPath        string
		releasedSession uint64
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case VolumeRDMAReadDescPath:
			readPath = r.URL.Path
			var req VolumeRDMAReadDescRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode read request: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if req.FileID != "3,01637037d6" || req.VolumeID != 3 || req.NeedleID == 0 || req.Size != 512 {
				t.Errorf("unexpected request: %+v", req)
			}
			writeJSON(w, VolumeRDMAReadDescResponse{
				Desc: swvfsproto.RDMADataDesc{
					RemoteAddr: 0xbeef,
					RKey:       77,
					Length:     512,
				},
				SessionID: 99,
			})
		case VolumeRDMAReleaseDescPath:
			var req VolumeRDMAReleaseDescRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode release request: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			releasedSession = req.SessionID
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := &VolumeNativeRDMAReadDescriptorClient{
		Client:       server.Client(),
		Timeout:      time.Second,
		ReleaseDelay: time.Hour,
		Stats:        NewStats(),
	}
	desc, _, err := client.ReadNeedleRDMA(context.Background(), NeedleReadDescriptorRequest{
		FileID:       "3,01637037d6",
		VolumeID:     3,
		NeedleID:     0x163703,
		Cookie:       0x7d6,
		VolumeServer: server.URL + "/3,01637037d6",
		Offset:       128,
		Size:         512,
	})
	if err != nil {
		t.Fatalf("ReadNeedleRDMA: %v", err)
	}
	if readPath != VolumeRDMAReadDescPath {
		t.Fatalf("read path = %q", readPath)
	}
	if desc.RemoteAddr != 0xbeef || desc.RKey != 77 || desc.Length != 512 {
		t.Fatalf("unexpected desc: %+v", desc)
	}
	if !IsNativeReadLease(desc.Reserved[0]) {
		t.Fatalf("lease was not marked as native: %#x", desc.Reserved[0])
	}
	if err := client.ReleaseReadDescriptor(context.Background(), desc.Reserved[0], 0, uint64(desc.Length)); err != nil {
		t.Fatalf("ReleaseReadDescriptor: %v", err)
	}
	if releasedSession != 99 {
		t.Fatalf("released session = %d", releasedSession)
	}
}

func TestVolumeNativeRDMAReadDescriptorClientMapsUnimplementedToNoSys(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not implemented", http.StatusNotImplemented)
	}))
	defer server.Close()

	client := &VolumeNativeRDMAReadDescriptorClient{Client: server.Client(), Timeout: time.Second}
	_, _, err := client.ReadNeedleRDMA(context.Background(), NeedleReadDescriptorRequest{
		FileID:       "3,01637037d6",
		VolumeID:     3,
		NeedleID:     1,
		VolumeServer: server.URL,
		Size:         512,
	})
	var errno ErrnoError
	if !errors.As(err, &errno) || errno.Errno != ErrnoNoSys {
		t.Fatalf("err = %v, want ErrnoNoSys", err)
	}
}
