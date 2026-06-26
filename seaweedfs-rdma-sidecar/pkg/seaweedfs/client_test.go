package seaweedfs

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sirupsen/logrus"
	"seaweedfs-rdma-sidecar/pkg/rdma"
)

func TestFetchNeedleDataRequiresDataSource(t *testing.T) {
	client := &SeaweedFSRDMAClient{
		logger: logrus.New(),
	}

	_, _, _, err := client.fetchNeedleData(t.Context(), &NeedleReadRequest{
		VolumeID:     1,
		NeedleID:     1,
		Cookie:       1,
		Offset:       0,
		Size:         16,
		VolumeServer: "",
	})
	if err == nil {
		t.Fatal("expected error when no data source is configured")
	}
}

func TestRemoteSourceDefaultsTCP(t *testing.T) {
	if got := remoteReadSource(""); got != "remote-tcp" {
		t.Fatalf("unexpected read source: %s", got)
	}
	if got := remoteWriteSource("UCX"); got != "remote-ucx-write" {
		t.Fatalf("unexpected write source: %s", got)
	}
}

func TestIsRealRdmaDefaultsFalse(t *testing.T) {
	c := rdma.NewClient(&rdma.Config{})
	if c.IsRealRdma() {
		t.Fatal("expected mock engine to report real_rdma=false")
	}
}

func TestHTTPFallbackSlicesFullNeedleBody(t *testing.T) {
	body := []byte("0123456789abcdef")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("offset") != "4" || r.URL.Query().Get("size") != "6" {
			t.Fatalf("unexpected query: %s", r.URL.RawQuery)
		}
		_, _ = w.Write(body)
	}))
	defer server.Close()

	client := &SeaweedFSRDMAClient{
		logger:          logrus.New(),
		volumeServerURL: server.URL,
	}
	got, err := client.httpFallback(t.Context(), &NeedleReadRequest{
		VolumeID: 1,
		NeedleID: 2,
		Cookie:   3,
		Offset:   4,
		Size:     6,
	})
	if err != nil {
		t.Fatalf("httpFallback: %v", err)
	}
	if want := []byte("456789"); !bytes.Equal(got, want) {
		t.Fatalf("fallback data = %q, want %q", got, want)
	}
}

func TestHTTPFallbackTruncatesPartialContent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 4-12/*")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("456789abc"))
	}))
	defer server.Close()

	client := &SeaweedFSRDMAClient{
		logger:          logrus.New(),
		volumeServerURL: server.URL,
	}
	got, err := client.httpFallback(t.Context(), &NeedleReadRequest{
		VolumeID: 1,
		NeedleID: 2,
		Cookie:   3,
		Offset:   4,
		Size:     6,
	})
	if err != nil {
		t.Fatalf("httpFallback: %v", err)
	}
	if want := []byte("456789"); !bytes.Equal(got, want) {
		t.Fatalf("fallback data = %q, want %q", got, want)
	}
}
