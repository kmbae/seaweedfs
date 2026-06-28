package swvfsdaemon

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"syscall"
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

func TestVolumeNativeRDMAReadDescriptorClientConnectsNativePeer(t *testing.T) {
	var (
		localRequests   int
		connectRequests int
		readRequests    int
		postedLocal     RDMALocalEndpoint
	)
	remoteEndpoint := RDMALocalEndpoint{
		ConnectionID:  55,
		ABIVersion:    swvfsproto.RDMAABIVersion,
		KernelEnabled: true,
		EndpointReady: true,
		Device:        "mlx5_0",
		Port:          1,
		QPNum:         222,
		PSN:           0x222222,
		LID:           0x22,
		LinkLayer:     swvfsproto.RDMALinkInfiniBand,
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case VolumeRDMALocalPath:
			localRequests++
			writeJSON(w, remoteEndpoint)
		case VolumeRDMAConnectPath:
			connectRequests++
			if r.URL.Query().Get("sl") != "4" {
				t.Errorf("service level = %q", r.URL.Query().Get("sl"))
			}
			if r.URL.Query().Get("connection_id") != "55" {
				t.Errorf("connection_id = %q", r.URL.Query().Get("connection_id"))
			}
			if err := json.NewDecoder(r.Body).Decode(&postedLocal); err != nil {
				t.Errorf("decode connect request: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			writeJSON(w, map[string]bool{"connected": true})
		case VolumeRDMAReadDescPath:
			readRequests++
			var req VolumeRDMAReadDescRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode read desc request: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if req.ConnectionID != 55 {
				t.Errorf("read desc connection_id = %d", req.ConnectionID)
			}
			writeJSON(w, VolumeRDMAReadDescResponse{
				Desc: swvfsproto.RDMADataDesc{
					RemoteAddr: 0xbeef,
					RKey:       77,
					Length:     512,
				},
				SessionID: uint64(100 + readRequests),
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	control := &fakeRDMAControl{
		local: swvfsproto.RDMALocalInfo{
			ABIVersion: swvfsproto.RDMAABIVersion,
			Flags:      swvfsproto.RDMAFKernelEnabled | swvfsproto.RDMAFEndpointReady,
			Port:       1,
			QPNum:      111,
			PSN:        0x111111,
			LID:        0x11,
			LinkLayer:  swvfsproto.RDMALinkInfiniBand,
		},
	}
	client := &VolumeNativeRDMAReadDescriptorClient{
		Client:       server.Client(),
		Control:      control,
		Timeout:      time.Second,
		ReleaseDelay: time.Hour,
		ServiceLevel: 4,
		Stats:        NewStats(),
	}

	for i := 0; i < 2; i++ {
		desc, _, err := client.ReadNeedleRDMA(context.Background(), NeedleReadDescriptorRequest{
			FileID:       "3,01637037d6",
			VolumeID:     3,
			NeedleID:     0x163703,
			Cookie:       0x7d6,
			VolumeServer: server.URL + "/3,01637037d6",
			Size:         512,
		})
		if err != nil {
			t.Fatalf("ReadNeedleRDMA[%d]: %v", i, err)
		}
		if desc.RemoteAddr != 0xbeef || desc.RKey != 77 || desc.Length != 512 {
			t.Fatalf("unexpected desc[%d]: %+v", i, desc)
		}
		if desc.Reserved[1] != 55 {
			t.Fatalf("desc[%d] connection id = %d, want 55", i, desc.Reserved[1])
		}
	}
	if localRequests != 1 {
		t.Fatalf("local requests = %d, want 1", localRequests)
	}
	if connectRequests != 1 {
		t.Fatalf("connect requests = %d, want 1", connectRequests)
	}
	if readRequests != 2 {
		t.Fatalf("read requests = %d, want 2", readRequests)
	}
	if !control.connected || control.remote.QPN != remoteEndpoint.QPNum || control.remote.LID != remoteEndpoint.LID || control.remote.SL != 4 {
		t.Fatalf("kernel control was not connected to remote endpoint: connected=%v remote=%+v", control.connected, control.remote)
	}
	if control.remote.Reserved[0] != 55 {
		t.Fatalf("kernel connect connection id = %d, want 55", control.remote.Reserved[0])
	}
	if len(control.localForIDs) != 2 || control.localForIDs[0] != 55 || control.localForIDs[1] != 55 {
		t.Fatalf("local-for ids = %v, want [55 55]", control.localForIDs)
	}
	if postedLocal.QPNum != 111 || postedLocal.LID != 0x11 || postedLocal.ConnectionID != 55 {
		t.Fatalf("posted local endpoint = %+v", postedLocal)
	}
}

func TestVolumeNativePeerManagerRetriesEAGAINWithFreshLocal(t *testing.T) {
	var (
		localRequests   int
		connectRequests int
		postedLocal     RDMALocalEndpoint
	)
	remoteEndpoints := []RDMALocalEndpoint{
		{
			ConnectionID:  55,
			ABIVersion:    swvfsproto.RDMAABIVersion,
			KernelEnabled: true,
			EndpointReady: true,
			Device:        "mlx5_0",
			Port:          1,
			QPNum:         222,
			PSN:           0x222222,
			LID:           0x22,
			LinkLayer:     swvfsproto.RDMALinkInfiniBand,
		},
		{
			ConnectionID:  56,
			ABIVersion:    swvfsproto.RDMAABIVersion,
			KernelEnabled: true,
			EndpointReady: true,
			Device:        "mlx5_0",
			Port:          1,
			QPNum:         333,
			PSN:           0x333333,
			LID:           0x22,
			LinkLayer:     swvfsproto.RDMALinkInfiniBand,
		},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case VolumeRDMALocalPath:
			idx := localRequests
			localRequests++
			if idx >= len(remoteEndpoints) {
				idx = len(remoteEndpoints) - 1
			}
			writeJSON(w, remoteEndpoints[idx])
		case VolumeRDMAConnectPath:
			connectRequests++
			if r.URL.Query().Get("connection_id") != "56" {
				t.Errorf("connection_id = %q", r.URL.Query().Get("connection_id"))
			}
			if err := json.NewDecoder(r.Body).Decode(&postedLocal); err != nil {
				t.Errorf("decode connect request: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			writeJSON(w, map[string]bool{"connected": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	control := &fakeRDMAControl{
		locals: []swvfsproto.RDMALocalInfo{
			readyInfo(0x11, 111, 0x111111),
			readyInfo(0x11, 112, 0x111112),
		},
		connectErrs: []error{syscall.EAGAIN, nil},
	}
	manager := &VolumeNativePeerManager{
		Client:       server.Client(),
		Control:      control,
		ServiceLevel: 2,
		Stats:        NewStats(),
	}
	peer, err := manager.Ensure(context.Background(), server.URL+"/3,01637037d6")
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if peer.VolumeConnectionID != 56 {
		t.Fatalf("connection id = %d, want 56", peer.VolumeConnectionID)
	}
	if control.connectCalls != 2 || localRequests != 2 || connectRequests != 1 {
		t.Fatalf("calls connect/local/post = %d/%d/%d, want 2/2/1", control.connectCalls, localRequests, connectRequests)
	}
	if len(control.localForIDs) != 2 || control.localForIDs[0] != 55 || control.localForIDs[1] != 56 {
		t.Fatalf("local-for ids = %v, want [55 56]", control.localForIDs)
	}
	if control.remote.Reserved[0] != 56 {
		t.Fatalf("kernel retry connect connection id = %d, want 56", control.remote.Reserved[0])
	}
	if postedLocal.QPNum != 112 || postedLocal.PSN != 0x111112 || postedLocal.ConnectionID != 56 {
		t.Fatalf("posted stale local endpoint after retry: %+v", postedLocal)
	}
}

func TestFetchVolumeNativeStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != VolumeRDMAStatusPath {
			t.Errorf("path = %q", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		writeJSON(w, VolumeRDMAStatusResponse{
			ReadExporterConfigured: true,
			EndpointConfigured:     true,
			ABIVersion:             swvfsproto.RDMAABIVersion,
			LocalPath:              VolumeRDMALocalPath,
			ConnectPath:            VolumeRDMAConnectPath,
			ReadDescPath:           VolumeRDMAReadDescPath,
			ReleaseDescPath:        VolumeRDMAReleaseDescPath,
		})
	}))
	defer server.Close()

	status, err := FetchVolumeNativeStatus(context.Background(), server.Client(), server.URL+"/3,abc")
	if err != nil {
		t.Fatalf("FetchVolumeNativeStatus: %v", err)
	}
	if !status.ReadExporterConfigured || !status.EndpointConfigured || status.LocalPath != VolumeRDMALocalPath {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestPostVolumeNativeWriteCommitBatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != VolumeRDMAWriteCommitBatchPath {
			t.Errorf("path = %q", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		var req VolumeRDMAWriteCommitBatchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(req.Entries) != 2 || req.Entries[0].SessionID != 11 || req.Entries[1].Size != 8192 {
			t.Errorf("entries = %+v", req.Entries)
		}
		writeJSON(w, VolumeRDMAWriteCommitBatchResponse{
			Results: []VolumeRDMAWriteCommitResult{
				{SessionID: 11, FileID: "3,abc", Size: 4096, Source: "native-volume-rdma-write-desc"},
				{SessionID: 12, FileID: "3,def", Size: 8192, Source: "native-volume-rdma-write-desc"},
			},
		})
	}))
	defer server.Close()

	resp, err := PostVolumeNativeWriteCommitBatch(context.Background(), server.Client(), server.URL, VolumeRDMAWriteCommitBatchRequest{
		Entries: []VolumeRDMAWriteCommitRequest{
			{SessionID: 11, FileID: "3,abc", VolumeID: 3, NeedleID: 1, Cookie: 2, Size: 4096},
			{SessionID: 12, FileID: "3,def", VolumeID: 3, NeedleID: 2, Cookie: 2, Size: 8192},
		},
	})
	if err != nil {
		t.Fatalf("PostVolumeNativeWriteCommitBatch: %v", err)
	}
	if len(resp.Results) != 2 || resp.Results[0].FileID != "3,abc" || resp.Results[1].Size != 8192 {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestVolumeNativeRDMAReadDescriptorClientHandshakeFailureFallsBack(t *testing.T) {
	client := &VolumeNativeRDMAReadDescriptorClient{
		Control: &fakeRDMAControl{},
		Timeout: time.Second,
		Stats:   NewStats(),
	}
	_, _, err := client.ReadNeedleRDMA(context.Background(), NeedleReadDescriptorRequest{
		FileID:       "3,01637037d6",
		VolumeID:     3,
		NeedleID:     1,
		VolumeServer: "http://volume.example",
		Size:         512,
	})
	var errno ErrnoError
	if !errors.As(err, &errno) || errno.Errno != ErrnoNoSys {
		t.Fatalf("err = %v, want ErrnoNoSys", err)
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
