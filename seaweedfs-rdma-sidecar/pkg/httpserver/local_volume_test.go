package httpserver

import (
	"net/http/httptest"
	"testing"
)

func TestLocalReadRangeHeader(t *testing.T) {
	req := httptest.NewRequest("GET", "/local-volume/1,abc", nil)
	req.Header.Set("Range", "bytes=1024-2047")

	offset, size, ranged, err := localReadRange(req)
	if err != nil {
		t.Fatal(err)
	}
	if !ranged {
		t.Fatal("expected ranged read")
	}
	if offset != 1024 || size != 1024 {
		t.Fatalf("range = offset %d size %d", offset, size)
	}
}

func TestLocalReadRangeQuery(t *testing.T) {
	req := httptest.NewRequest("GET", "/local-volume/1,abc?offset=7&size=11", nil)

	offset, size, ranged, err := localReadRange(req)
	if err != nil {
		t.Fatal(err)
	}
	if ranged {
		t.Fatal("did not expect ranged read")
	}
	if offset != 7 || size != 11 {
		t.Fatalf("query range = offset %d size %d", offset, size)
	}
}
