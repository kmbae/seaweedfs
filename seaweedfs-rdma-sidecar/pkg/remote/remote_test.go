package remote

import (
	"context"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
)

func TestIsLocalHost(t *testing.T) {
	cases := map[string]bool{
		"http://127.0.0.1:8444":  true,
		"http://localhost:8444":  true,
		"http://10.0.0.5:8444":   false,
		"http://volume-pod:8444": false,
	}
	for input, want := range cases {
		if got := IsLocalHost(input); got != want {
			t.Fatalf("IsLocalHost(%q) = %v, want %v", input, got, want)
		}
	}
}

func TestParseVolumeHost(t *testing.T) {
	host, err := ParseVolumeHost("http://100.64.1.10:8444")
	if err != nil {
		t.Fatal(err)
	}
	if host != "100.64.1.10" {
		t.Fatalf("host = %q", host)
	}
}

func TestReadNeedleResultReusesConnection(t *testing.T) {
	oldPool := pooledRemoteConns
	pooledRemoteConns = newRemoteConnectionPool(2)
	defer func() {
		pooledRemoteConns.closeIdle()
		pooledRemoteConns = oldPool
	}()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	var accepts atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			accepts.Add(1)
			go func(conn net.Conn) {
				defer conn.Close()
				for {
					var req NeedleReadRequest
					if err := readMsgpack(conn, &req); err != nil {
						return
					}
					resp := NeedleReadResponse{
						Success:   true,
						Data:      []byte("ok"),
						Size:      2,
						Transport: "tcp",
						Source:    "test",
					}
					if err := writeMsgpack(conn, &resp); err != nil {
						return
					}
				}
			}(conn)
		}
	}()

	host, portText, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}

	req := &NeedleReadRequest{VolumeID: 1, NeedleID: 2, Cookie: 3, Size: 2}
	for i := 0; i < 2; i++ {
		got, err := ReadNeedleResult(context.Background(), "http://"+host, uint16(port), req)
		if err != nil {
			t.Fatal(err)
		}
		if string(got.Data) != "ok" {
			t.Fatalf("data = %q", got.Data)
		}
	}

	if got := accepts.Load(); got != 1 {
		t.Fatalf("accepted connections = %d, want 1", got)
	}
}
