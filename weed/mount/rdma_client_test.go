package mount

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
