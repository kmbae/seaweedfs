package swvfsdaemon

import (
	"context"
	"errors"
	"testing"
)

type fakePlane struct {
	readErr    error
	writeErr   error
	readCalls  int
	writeCalls int
	source     string
	usedRDMA   bool
}

func (f *fakePlane) ReadNeedle(ctx context.Context, req NeedleReadRequest) (*NeedleReadResult, error) {
	f.readCalls++
	if f.readErr != nil {
		return nil, f.readErr
	}
	return &NeedleReadResult{Data: []byte("ok"), UsedRDMA: f.usedRDMA, Source: f.source}, nil
}

func (f *fakePlane) WriteNeedle(ctx context.Context, req NeedleWriteRequest) (*NeedleWriteResult, error) {
	f.writeCalls++
	if f.writeErr != nil {
		return nil, f.writeErr
	}
	return &NeedleWriteResult{FileID: req.FileID, UsedRDMA: f.usedRDMA, Source: f.source}, nil
}

func TestRouterPrefersRDMARead(t *testing.T) {
	rdma := &fakePlane{source: "rdma", usedRDMA: true}
	tcp := &fakePlane{source: "tcp"}
	router := &Router{RDMA: rdma, Fallback: tcp, EnableReadRDMA: true, FallbackOnError: true}
	resp, err := router.ReadNeedle(context.Background(), NeedleReadRequest{FileID: "1,abc", PreferRDMA: true, Size: 1024})
	if err != nil {
		t.Fatalf("ReadNeedle: %v", err)
	}
	if !resp.UsedRDMA || rdma.readCalls != 1 || tcp.readCalls != 0 {
		t.Fatalf("unexpected routing: resp=%+v rdma=%d tcp=%d", resp, rdma.readCalls, tcp.readCalls)
	}
}

func TestRouterSkipsRDMAReadBelowThreshold(t *testing.T) {
	rdma := &fakePlane{source: "rdma", usedRDMA: true}
	tcp := &fakePlane{source: "tcp"}
	router := &Router{
		RDMA:            rdma,
		Fallback:        tcp,
		EnableReadRDMA:  true,
		ReadRDMAMinSize: 4096,
		FallbackOnError: true,
	}
	resp, err := router.ReadNeedle(context.Background(), NeedleReadRequest{FileID: "1,abc", PreferRDMA: true, Size: 1024})
	if err != nil {
		t.Fatalf("ReadNeedle: %v", err)
	}
	if resp.Source != "tcp" || rdma.readCalls != 0 || tcp.readCalls != 1 {
		t.Fatalf("unexpected threshold routing: resp=%+v rdma=%d tcp=%d", resp, rdma.readCalls, tcp.readCalls)
	}
}

func TestRouterUsesRDMAReadAtThreshold(t *testing.T) {
	rdma := &fakePlane{source: "rdma", usedRDMA: true}
	tcp := &fakePlane{source: "tcp"}
	router := &Router{
		RDMA:            rdma,
		Fallback:        tcp,
		EnableReadRDMA:  true,
		ReadRDMAMinSize: 4096,
		FallbackOnError: true,
	}
	resp, err := router.ReadNeedle(context.Background(), NeedleReadRequest{FileID: "1,abc", PreferRDMA: true, Size: 4096})
	if err != nil {
		t.Fatalf("ReadNeedle: %v", err)
	}
	if !resp.UsedRDMA || rdma.readCalls != 1 || tcp.readCalls != 0 {
		t.Fatalf("unexpected threshold routing: resp=%+v rdma=%d tcp=%d", resp, rdma.readCalls, tcp.readCalls)
	}
}

func TestRouterFallsBackOnReadError(t *testing.T) {
	rdma := &fakePlane{readErr: errors.New("rdma down")}
	tcp := &fakePlane{source: "tcp"}
	router := &Router{RDMA: rdma, Fallback: tcp, EnableReadRDMA: true, FallbackOnError: true}
	resp, err := router.ReadNeedle(context.Background(), NeedleReadRequest{FileID: "1,abc", PreferRDMA: true})
	if err != nil {
		t.Fatalf("ReadNeedle: %v", err)
	}
	if resp.Source != "tcp" || rdma.readCalls != 1 || tcp.readCalls != 1 {
		t.Fatalf("unexpected fallback: resp=%+v rdma=%d tcp=%d", resp, rdma.readCalls, tcp.readCalls)
	}
}

func TestRouterSkipsRDMAWriteBelowThreshold(t *testing.T) {
	rdma := &fakePlane{source: "rdma", usedRDMA: true}
	tcp := &fakePlane{source: "tcp"}
	router := &Router{
		RDMA:             rdma,
		Fallback:         tcp,
		EnableWriteRDMA:  true,
		WriteRDMAMinSize: 4096,
		FallbackOnError:  true,
	}
	resp, err := router.WriteNeedle(context.Background(), NeedleWriteRequest{FileID: "1,abc", PreferRDMA: true, Data: []byte("payload")})
	if err != nil {
		t.Fatalf("WriteNeedle: %v", err)
	}
	if resp.Source != "tcp" || rdma.writeCalls != 0 || tcp.writeCalls != 1 {
		t.Fatalf("unexpected threshold routing: resp=%+v rdma=%d tcp=%d", resp, rdma.writeCalls, tcp.writeCalls)
	}
}

func TestRouterUsesRDMAWriteAtThreshold(t *testing.T) {
	rdma := &fakePlane{source: "rdma", usedRDMA: true}
	tcp := &fakePlane{source: "tcp"}
	router := &Router{
		RDMA:             rdma,
		Fallback:         tcp,
		EnableWriteRDMA:  true,
		WriteRDMAMinSize: 7,
		FallbackOnError:  true,
	}
	resp, err := router.WriteNeedle(context.Background(), NeedleWriteRequest{FileID: "1,abc", PreferRDMA: true, Data: []byte("payload")})
	if err != nil {
		t.Fatalf("WriteNeedle: %v", err)
	}
	if !resp.UsedRDMA || rdma.writeCalls != 1 || tcp.writeCalls != 0 {
		t.Fatalf("unexpected threshold routing: resp=%+v rdma=%d tcp=%d", resp, rdma.writeCalls, tcp.writeCalls)
	}
}

func TestRouterPrefersRDMAWrite(t *testing.T) {
	rdma := &fakePlane{source: "rdma", usedRDMA: true}
	tcp := &fakePlane{source: "tcp"}
	router := &Router{RDMA: rdma, Fallback: tcp, EnableWriteRDMA: true, FallbackOnError: true}
	resp, err := router.WriteNeedle(context.Background(), NeedleWriteRequest{FileID: "1,abc", PreferRDMA: true, Data: []byte("payload")})
	if err != nil {
		t.Fatalf("WriteNeedle: %v", err)
	}
	if !resp.UsedRDMA || rdma.writeCalls != 1 || tcp.writeCalls != 0 {
		t.Fatalf("unexpected routing: resp=%+v rdma=%d tcp=%d", resp, rdma.writeCalls, tcp.writeCalls)
	}
}
