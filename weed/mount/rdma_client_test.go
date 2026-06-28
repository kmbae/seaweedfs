package mount

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	storage_needle "github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func mockLookupFn(fileId string) func(ctx context.Context, fileId string) ([]string, error) {
	return func(ctx context.Context, fid string) ([]string, error) {
		return []string{fmt.Sprintf("http://mock-volume:8080/%s", fid)}, nil
	}
}

func newTestClient(t *testing.T, handler http.Handler) (*RDMAMountClient, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	addr := strings.TrimPrefix(server.URL, "http://")

	client := &RDMAMountClient{
		sidecarAddr:   addr,
		mode:          rdmaMountModeSidecar,
		maxConcurrent: 64,
		timeout:       5 * time.Second,
		httpClient:    server.Client(),
		semaphore:     make(chan struct{}, 64),
		lookupFileIdFn: func(ctx context.Context, fileId string) ([]string, error) {
			return []string{fmt.Sprintf("http://mock-volume:8080/%s", fileId)}, nil
		},
	}
	return client, server
}

type fakeNativeRdmaRequester struct {
	local       weed_server.VolumeRdmaEndpointInfo
	readData    []byte
	localCalls  atomic.Int64
	connects    atomic.Int64
	readCalls   atomic.Int64
	lastReadID  atomic.Uint64
	lastConnID  atomic.Uint64
	lastReadLen atomic.Uint64
}

func (f *fakeNativeRdmaRequester) RequesterLocalEndpoint(ctx context.Context) (weed_server.VolumeRdmaEndpointInfo, error) {
	local, _, err := f.RequesterLocalEndpointFor(ctx, 0)
	return local, err
}

func (f *fakeNativeRdmaRequester) RequesterLocalEndpointFor(ctx context.Context, connectionID uint64) (weed_server.VolumeRdmaEndpointInfo, uint64, error) {
	f.localCalls.Add(1)
	if connectionID == 0 {
		connectionID = 77
	}
	local := f.local
	local.ConnectionID = connectionID
	return local, connectionID, nil
}

func (f *fakeNativeRdmaRequester) RequesterConnectEndpoint(ctx context.Context, remote weed_server.VolumeRdmaRemoteInfo) error {
	return f.RequesterConnectEndpointFor(ctx, 0, remote)
}

func (f *fakeNativeRdmaRequester) RequesterConnectEndpointFor(ctx context.Context, connectionID uint64, remote weed_server.VolumeRdmaRemoteInfo) error {
	f.connects.Add(1)
	f.lastConnID.Store(connectionID)
	return nil
}

func (f *fakeNativeRdmaRequester) ReadRemoteFor(ctx context.Context, connectionID uint64, desc weed_server.VolumeRdmaDataDesc, timeout time.Duration) ([]byte, error) {
	var out bytes.Buffer
	if err := f.ReadRemoteToFor(ctx, connectionID, desc, timeout, &out); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func (f *fakeNativeRdmaRequester) ReadRemoteToFor(ctx context.Context, connectionID uint64, desc weed_server.VolumeRdmaDataDesc, timeout time.Duration, dst io.Writer) error {
	f.readCalls.Add(1)
	f.lastReadID.Store(connectionID)
	f.lastReadLen.Store(uint64(desc.Length))
	if int(desc.Length) > len(f.readData) {
		return fmt.Errorf("fake requester has %d bytes, descriptor asks for %d", len(f.readData), desc.Length)
	}
	_, err := dst.Write(f.readData[:desc.Length])
	return err
}

func readyMountRdmaEndpoint(qpn uint32) weed_server.VolumeRdmaEndpointInfo {
	return weed_server.VolumeRdmaEndpointInfo{
		ABIVersion:    weed_server.VolumeRdmaABIVersion,
		KernelEnabled: true,
		EndpointReady: true,
		Device:        "mlx5_0",
		Port:          1,
		QPNum:         qpn,
		PSN:           0x123456,
		LID:           0x42,
		GIDIndex:      0,
		LinkLayer:     weed_server.VolumeRdmaLinkInfiniBand,
	}
}

func TestRDMAMountClient_HealthCheck(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "healthy",
				"rdma":   map[string]bool{"enabled": true, "connected": true},
			})
		})
		client, server := newTestClient(t, mux)
		defer server.Close()

		err := client.healthCheck()
		if err != nil {
			t.Fatalf("expected healthy, got error: %v", err)
		}
	})

	t.Run("unhealthy_status", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "degraded",
				"rdma":   map[string]bool{"enabled": true, "connected": false},
			})
		})
		client, server := newTestClient(t, mux)
		defer server.Close()

		err := client.healthCheck()
		if err == nil {
			t.Fatal("expected error for unhealthy status")
		}
		if !strings.Contains(err.Error(), "unhealthy") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("rdma_disabled", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "healthy",
				"rdma":   map[string]bool{"enabled": false, "connected": false},
			})
		})
		client, server := newTestClient(t, mux)
		defer server.Close()

		err := client.healthCheck()
		if err == nil {
			t.Fatal("expected error when RDMA is disabled")
		}
		if !strings.Contains(err.Error(), "not enabled") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("server_down", func(t *testing.T) {
		client := &RDMAMountClient{
			sidecarAddr:   "127.0.0.1:1",
			maxConcurrent: 1,
			timeout:       100 * time.Millisecond,
			httpClient:    &http.Client{Timeout: 100 * time.Millisecond},
			semaphore:     make(chan struct{}, 1),
		}
		err := client.healthCheck()
		if err == nil {
			t.Fatal("expected error when server is down")
		}
	})
}

func TestRDMAMountClient_ReadNeedle(t *testing.T) {
	t.Run("successful_read", func(t *testing.T) {
		testData := []byte("hello-rdma-world")
		mux := http.NewServeMux()
		mux.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				http.Error(w, "bad method", http.StatusMethodNotAllowed)
				return
			}
			fileID := r.URL.Query().Get("file_id")
			if fileID == "" {
				http.Error(w, "missing file_id", http.StatusBadRequest)
				return
			}
			w.Header().Set("X-Source", "rdma+local-volume")
			w.Header().Set("X-RDMA-Used", "true")
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Write(testData)
		})

		client, server := newTestClient(t, mux)
		defer server.Close()

		data, isRDMA, err := client.ReadNeedle(context.Background(), "3,abc123", 0, 4096)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !isRDMA {
			t.Fatal("expected isRDMA=true")
		}
		if string(data) != string(testData) {
			t.Fatalf("data mismatch: got %q, want %q", data, testData)
		}
		if client.successfulReads.Load() != 1 {
			t.Fatalf("expected 1 successful read, got %d", client.successfulReads.Load())
		}
	})

	t.Run("server_error", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "internal error", http.StatusInternalServerError)
		})
		client, server := newTestClient(t, mux)
		defer server.Close()

		_, _, err := client.ReadNeedle(context.Background(), "3,abc123", 0, 4096)
		if err == nil {
			t.Fatal("expected error on server error")
		}
		if client.failedReads.Load() != 1 {
			t.Fatalf("expected 1 failed read, got %d", client.failedReads.Load())
		}
	})

	t.Run("context_cancelled", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(2 * time.Second)
			w.Write([]byte("late"))
		})
		client, server := newTestClient(t, mux)
		defer server.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		_, _, err := client.ReadNeedle(ctx, "3,abc123", 0, 4096)
		if err == nil {
			t.Fatal("expected error on cancelled context")
		}
	})

	t.Run("non_rdma_source", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Source", "http-fallback")
			w.Header().Set("X-RDMA-Used", "false")
			w.Write([]byte("http-data"))
		})
		client, server := newTestClient(t, mux)
		defer server.Close()

		data, isRDMA, err := client.ReadNeedle(context.Background(), "3,abc123", 0, 4096)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if isRDMA {
			t.Fatal("expected isRDMA=false for http source")
		}
		if string(data) != "http-data" {
			t.Fatalf("unexpected data: %q", data)
		}
	})
}

func TestRDMAMountClient_NativeReadNeedleTo(t *testing.T) {
	fileID := storage_needle.NewFileId(7, 0x1234, 0x98765432).String()
	readPayload := []byte("native-rdma-read-payload")
	requester := &fakeNativeRdmaRequester{
		local:    readyMountRdmaEndpoint(200),
		readData: readPayload,
	}

	var providerConnects atomic.Int64
	var readDescCalls atomic.Int64
	var releases atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc(weed_server.VolumeRdmaNativeLocalPath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		local := readyMountRdmaEndpoint(100)
		local.ConnectionID = 11
		_ = json.NewEncoder(w).Encode(local)
	})
	mux.HandleFunc(weed_server.VolumeRdmaNativeConnectPath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		var peer weed_server.VolumeRdmaEndpointInfo
		if err := json.NewDecoder(r.Body).Decode(&peer); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !peer.ReadyForConnect() {
			http.Error(w, "peer not ready", http.StatusBadRequest)
			return
		}
		providerConnects.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]bool{"connected": true})
	})
	mux.HandleFunc(weed_server.VolumeRdmaNativeReadDescPath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		var req weed_server.VolumeRdmaReadRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.ConnectionID != 11 {
			http.Error(w, fmt.Sprintf("connection_id=%d", req.ConnectionID), http.StatusBadRequest)
			return
		}
		if req.FileID != fileID || req.VolumeID != 7 || req.NeedleID != 0x1234 || req.Cookie != 0x98765432 || req.Size != uint64(len(readPayload)) {
			http.Error(w, fmt.Sprintf("unexpected read request: %+v", req), http.StatusBadRequest)
			return
		}
		readDescCalls.Add(1)
		_ = json.NewEncoder(w).Encode(volumeRdmaReadDescResponse{
			Desc: weed_server.VolumeRdmaDataDesc{
				RemoteAddr: 0x100000,
				RKey:       99,
				Length:     uint32(req.Size),
			},
			ConnectionID: req.ConnectionID,
			SessionID:    uint64(700 + readDescCalls.Load()),
		})
	})
	mux.HandleFunc(weed_server.VolumeRdmaNativeReleaseDescPath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		var req volumeRdmaReleaseDescRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.SessionID == 0 {
			http.Error(w, "missing session", http.StatusBadRequest)
			return
		}
		releases.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]bool{"released": true})
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	client := newRDMAMountClientBase(rdmaMountModeNative, "", func(ctx context.Context, fid string) ([]string, error) {
		if fid != fileID {
			t.Fatalf("lookup fid = %s, want %s", fid, fileID)
		}
		return []string{server.URL + "/" + fid}, nil
	}, 4, 1000)
	client.httpClient = server.Client()
	client.nativeRequester = requester

	for i := 0; i < 2; i++ {
		var out bytes.Buffer
		n, isRDMA, err := client.ReadNeedleTo(context.Background(), fileID, 0, uint64(len(readPayload)), &out)
		if err != nil {
			t.Fatalf("ReadNeedleTo attempt %d: %v", i+1, err)
		}
		if !isRDMA {
			t.Fatalf("ReadNeedleTo attempt %d did not report RDMA", i+1)
		}
		if n != int64(len(readPayload)) || out.String() != string(readPayload) {
			t.Fatalf("attempt %d data mismatch: n=%d data=%q", i+1, n, out.String())
		}
	}

	if requester.localCalls.Load() != 1 || requester.connects.Load() != 1 || providerConnects.Load() != 1 {
		t.Fatalf("handshake was not cached: requesterLocal=%d requesterConnect=%d providerConnect=%d",
			requester.localCalls.Load(), requester.connects.Load(), providerConnects.Load())
	}
	if readDescCalls.Load() != 2 || requester.readCalls.Load() != 2 || releases.Load() != 2 {
		t.Fatalf("read/release counts: readDesc=%d requesterRead=%d releases=%d",
			readDescCalls.Load(), requester.readCalls.Load(), releases.Load())
	}
	if requester.lastReadID.Load() != 1 {
		t.Fatalf("requester read connection id = %d, want 1", requester.lastReadID.Load())
	}
	stats := client.GetStats()
	if stats["native_read_requests"].(int64) != 2 || stats["native_read_successes"].(int64) != 2 {
		t.Fatalf("unexpected native read stats: %+v", stats)
	}
	if stats["native_read_bytes"].(int64) != int64(len(readPayload))*2 || stats["rdma_bytes_read"].(int64) != int64(len(readPayload))*2 {
		t.Fatalf("unexpected native read bytes: %+v", stats)
	}
}

func TestRDMAMountClient_NativeReadNeedlesToUsesDescriptorBatch(t *testing.T) {
	fileID1 := storage_needle.NewFileId(7, 0x1234, 0x98765432).String()
	fileID2 := storage_needle.NewFileId(7, 0x1235, 0x98765432).String()
	requester := &fakeNativeRdmaRequester{
		local:    readyMountRdmaEndpoint(200),
		readData: []byte("abcdefghijklmnopqrstuvwxyz"),
	}

	var providerConnects atomic.Int64
	var readDescBatchCalls atomic.Int64
	var releaseBatchCalls atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc(weed_server.VolumeRdmaNativeLocalPath, func(w http.ResponseWriter, r *http.Request) {
		connectionID, err := strconv.ParseUint(r.URL.Query().Get("connection_id"), 10, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		local := readyMountRdmaEndpoint(100)
		local.ConnectionID = connectionID
		_ = json.NewEncoder(w).Encode(local)
	})
	mux.HandleFunc(weed_server.VolumeRdmaNativeConnectPath, func(w http.ResponseWriter, r *http.Request) {
		var peer weed_server.VolumeRdmaEndpointInfo
		if err := json.NewDecoder(r.Body).Decode(&peer); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !peer.ReadyForConnect() {
			http.Error(w, "peer not ready", http.StatusBadRequest)
			return
		}
		providerConnects.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]bool{"connected": true})
	})
	mux.HandleFunc(weed_server.VolumeRdmaNativeReadDescBatchPath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		var req weed_server.VolumeRdmaReadDescBatchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(req.Entries) != 2 {
			http.Error(w, fmt.Sprintf("entries=%d", len(req.Entries)), http.StatusBadRequest)
			return
		}
		for i, entry := range req.Entries {
			if entry.ConnectionID != uint64(i+1) || entry.VolumeID != 7 {
				http.Error(w, fmt.Sprintf("unexpected entry: %+v", entry), http.StatusBadRequest)
				return
			}
		}
		readDescBatchCalls.Add(1)
		_ = json.NewEncoder(w).Encode(volumeRdmaReadDescBatchResponse{
			Results: []volumeRdmaReadDescBatchResult{
				{
					Index:     0,
					Desc:      weed_server.VolumeRdmaDataDesc{RemoteAddr: 0x100000, RKey: 99, Length: uint32(req.Entries[0].Size)},
					SessionID: 900,
					Status:    http.StatusOK,
				},
				{
					Index:     1,
					Desc:      weed_server.VolumeRdmaDataDesc{RemoteAddr: 0x200000, RKey: 100, Length: uint32(req.Entries[1].Size)},
					SessionID: 901,
					Status:    http.StatusOK,
				},
			},
		})
	})
	mux.HandleFunc(weed_server.VolumeRdmaNativeReleaseDescBatchPath, func(w http.ResponseWriter, r *http.Request) {
		var req volumeRdmaReleaseDescBatchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(req.SessionIDs) != 2 || req.SessionIDs[0] != 900 || req.SessionIDs[1] != 901 {
			http.Error(w, fmt.Sprintf("unexpected sessions: %+v", req.SessionIDs), http.StatusBadRequest)
			return
		}
		releaseBatchCalls.Add(1)
		_ = json.NewEncoder(w).Encode(volumeRdmaReleaseDescBatchResponse{
			Results: []volumeRdmaReleaseDescBatchResult{
				{SessionID: 900, Released: true, Status: http.StatusOK},
				{SessionID: 901, Released: true, Status: http.StatusOK},
			},
		})
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	client := newRDMAMountClientBase(rdmaMountModeNative, "", func(ctx context.Context, fid string) ([]string, error) {
		return []string{server.URL + "/" + fid}, nil
	}, 4, 1000)
	client.httpClient = server.Client()
	client.nativeRequester = requester

	var out1, out2 bytes.Buffer
	n, isRDMA, err := client.ReadNeedlesTo(context.Background(), []RDMANeedleReadRequest{
		{FileID: fileID1, Size: 3, Dst: &out1},
		{FileID: fileID2, Size: 4, Dst: &out2},
	})
	if err != nil {
		t.Fatalf("ReadNeedlesTo: %v", err)
	}
	if !isRDMA {
		t.Fatal("expected RDMA batch read")
	}
	if n != 7 || out1.String() != "abc" || out2.String() != "abcd" {
		t.Fatalf("unexpected batch read output: n=%d out1=%q out2=%q", n, out1.String(), out2.String())
	}
	if requester.localCalls.Load() != 2 || requester.connects.Load() != 2 || providerConnects.Load() != 2 {
		t.Fatalf("parallel handshakes: requesterLocal=%d requesterConnect=%d providerConnect=%d",
			requester.localCalls.Load(), requester.connects.Load(), providerConnects.Load())
	}
	if readDescBatchCalls.Load() != 1 || requester.readCalls.Load() != 2 || releaseBatchCalls.Load() != 1 {
		t.Fatalf("batch counts: readDescBatch=%d requesterRead=%d releaseBatch=%d",
			readDescBatchCalls.Load(), requester.readCalls.Load(), releaseBatchCalls.Load())
	}
	stats := client.GetStats()
	if stats["native_read_requests"].(int64) != 2 || stats["native_read_successes"].(int64) != 2 || stats["native_read_bytes"].(int64) != 7 {
		t.Fatalf("unexpected native read stats: %+v", stats)
	}
}

func TestRDMAMountClient_WriteNeedle(t *testing.T) {
	t.Run("successful_write", func(t *testing.T) {
		var receivedData []byte
		var receivedFileID string

		mux := http.NewServeMux()
		mux.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "bad method", http.StatusMethodNotAllowed)
				return
			}
			receivedFileID = r.URL.Query().Get("file_id")
			var err error
			receivedData, err = io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "read body failed", http.StatusBadRequest)
				return
			}
			json.NewEncoder(w).Encode(RDMAWriteResponse{
				Success: true,
				IsRDMA:  true,
				Source:  "rdma+http-submit",
				FileID:  receivedFileID,
				Size:    len(receivedData),
			})
		})

		client, server := newTestClient(t, mux)
		defer server.Close()

		payload := []byte("write-test-data-12345")
		resp, err := client.WriteNeedle(context.Background(), "5,def456", payload, "http://vol:8080")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.Success {
			t.Fatal("expected success=true")
		}
		if !resp.IsRDMA {
			t.Fatal("expected isRDMA=true")
		}
		if string(receivedData) != string(payload) {
			t.Fatalf("data mismatch: got %q, want %q", receivedData, payload)
		}
		if receivedFileID != "5,def456" {
			t.Fatalf("fileID mismatch: got %q, want %q", receivedFileID, "5,def456")
		}
		if client.successfulWrites.Load() != 1 {
			t.Fatalf("expected 1 successful write, got %d", client.successfulWrites.Load())
		}
		if client.totalBytesWritten.Load() != int64(len(payload)) {
			t.Fatalf("expected %d bytes written, got %d", len(payload), client.totalBytesWritten.Load())
		}
	})

	t.Run("server_error", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "disk full", http.StatusInternalServerError)
		})
		client, server := newTestClient(t, mux)
		defer server.Close()

		_, err := client.WriteNeedle(context.Background(), "5,def456", []byte("data"), "http://vol:8080")
		if err == nil {
			t.Fatal("expected error on server error")
		}
		if client.failedWrites.Load() != 1 {
			t.Fatalf("expected 1 failed write, got %d", client.failedWrites.Load())
		}
	})

	t.Run("volume_lookup_write", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
			vs := r.URL.Query().Get("volume_server")
			json.NewEncoder(w).Encode(RDMAWriteResponse{
				Success: true,
				IsRDMA:  true,
				Source:  "rdma+http-submit",
				FileID:  r.URL.Query().Get("file_id"),
				Size:    10,
			})
			_ = vs
		})
		client, server := newTestClient(t, mux)
		defer server.Close()

		// Empty volumeServer triggers lookup
		resp, err := client.WriteNeedle(context.Background(), "5,def456", []byte("0123456789"), "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.Success {
			t.Fatal("expected success")
		}
	})
}

func TestRDMAMountClient_Concurrency(t *testing.T) {
	maxConcurrent := 4
	var activeCount atomic.Int32
	var maxSeen atomic.Int32

	mux := http.NewServeMux()
	mux.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
		cur := activeCount.Add(1)
		defer activeCount.Add(-1)

		for {
			old := maxSeen.Load()
			if cur <= old || maxSeen.CompareAndSwap(old, cur) {
				break
			}
		}

		time.Sleep(20 * time.Millisecond)
		w.Header().Set("X-Source", "rdma")
		w.Header().Set("X-RDMA-Used", "true")
		w.Write([]byte("ok"))
	})

	server := httptest.NewServer(mux)
	defer server.Close()
	addr := strings.TrimPrefix(server.URL, "http://")

	client := &RDMAMountClient{
		sidecarAddr:   addr,
		maxConcurrent: maxConcurrent,
		timeout:       5 * time.Second,
		httpClient:    server.Client(),
		semaphore:     make(chan struct{}, maxConcurrent),
		lookupFileIdFn: func(ctx context.Context, fileId string) ([]string, error) {
			return []string{fmt.Sprintf("http://mock:8080/%s", fileId)}, nil
		},
	}

	totalRequests := 20
	var wg sync.WaitGroup
	wg.Add(totalRequests)

	for i := 0; i < totalRequests; i++ {
		go func(idx int) {
			defer wg.Done()
			client.ReadNeedle(context.Background(), fmt.Sprintf("1,%d", idx), 0, 64)
		}(i)
	}

	wg.Wait()

	if maxSeen.Load() > int32(maxConcurrent) {
		t.Fatalf("concurrency exceeded: max seen %d, limit %d", maxSeen.Load(), maxConcurrent)
	}
	if client.totalRequests.Load() != int64(totalRequests) {
		t.Fatalf("expected %d total requests, got %d", totalRequests, client.totalRequests.Load())
	}
}

func TestRDMAMountClient_GetStats(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Source", "rdma")
		w.Header().Set("X-RDMA-Used", "true")
		w.Write([]byte("data"))
	})
	mux.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(RDMAWriteResponse{
			Success: true, IsRDMA: true, Source: "rdma", Size: 5,
		})
	})

	client, server := newTestClient(t, mux)
	defer server.Close()

	client.ReadNeedle(context.Background(), "1,abc", 0, 100)
	client.WriteNeedle(context.Background(), "2,def", []byte("hello"), "http://vol:8080")

	stats := client.GetStats()
	if stats["total_read_requests"].(int64) != 1 {
		t.Fatalf("unexpected read count: %v", stats["total_read_requests"])
	}
	if stats["total_write_requests"].(int64) != 1 {
		t.Fatalf("unexpected write count: %v", stats["total_write_requests"])
	}
	if stats["successful_reads"].(int64) != 1 {
		t.Fatalf("unexpected successful reads: %v", stats["successful_reads"])
	}
	if stats["sidecar_read_requests"].(int64) != 1 || stats["sidecar_read_successes"].(int64) != 1 {
		t.Fatalf("unexpected sidecar read stats: %+v", stats)
	}
	if stats["rdma_bytes_read"].(int64) != 4 || stats["sidecar_read_bytes"].(int64) != 4 {
		t.Fatalf("unexpected RDMA read bytes: %+v", stats)
	}
	client.RecordFallbackRead(32, io.EOF)
	stats = client.GetStats()
	if stats["fallback_read_requests"].(int64) != 1 || stats["fallback_read_successes"].(int64) != 1 || stats["fallback_read_bytes"].(int64) != 32 {
		t.Fatalf("unexpected fallback stats: %+v", stats)
	}
	if stats["successful_writes"].(int64) != 1 {
		t.Fatalf("unexpected successful writes: %v", stats["successful_writes"])
	}
}

func TestRDMAMountClient_LookupVolumeLocation(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		client := &RDMAMountClient{
			lookupFileIdFn: func(ctx context.Context, fileId string) ([]string, error) {
				return []string{"http://vol-server:8080/3,abc123"}, nil
			},
		}
		addr, err := client.lookupVolumeLocationByFileID(context.Background(), "3,abc123")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if addr != "http://vol-server:8080" {
			t.Fatalf("unexpected address: %s", addr)
		}
	})

	t.Run("no_locations", func(t *testing.T) {
		client := &RDMAMountClient{
			lookupFileIdFn: func(ctx context.Context, fileId string) ([]string, error) {
				return []string{}, nil
			},
		}
		_, err := client.lookupVolumeLocationByFileID(context.Background(), "3,abc123")
		if err == nil {
			t.Fatal("expected error for empty locations")
		}
	})

	t.Run("lookup_error", func(t *testing.T) {
		client := &RDMAMountClient{
			lookupFileIdFn: func(ctx context.Context, fileId string) ([]string, error) {
				return nil, fmt.Errorf("filer unavailable")
			},
		}
		_, err := client.lookupVolumeLocationByFileID(context.Background(), "3,abc123")
		if err == nil {
			t.Fatal("expected error on lookup failure")
		}
	})
}

func TestRDMAMountClient_IsHealthy(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "healthy",
			"rdma":   map[string]bool{"enabled": true, "connected": true},
		})
	})
	client, server := newTestClient(t, mux)
	defer server.Close()

	if !client.IsHealthy() {
		t.Fatal("expected IsHealthy() to return true")
	}
}
