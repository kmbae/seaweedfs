package swvfsproto

import (
	"encoding/binary"
	"errors"
	"testing"
)

func TestRequestEncodeDecode(t *testing.T) {
	req := &Request{
		Header: RequestHeader{
			Tag:    42,
			Op:     OpWrite,
			Offset: 4096,
			Size:   3,
			Valid:  WriteFRDMAPreferred,
			Mode:   0644,
			UID:    1000,
			GID:    1001,
		},
		Path1: "/bench/file",
		Data:  []byte("abc"),
	}
	encoded, err := req.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := DecodeRequest(encoded)
	if err != nil {
		t.Fatalf("DecodeRequest: %v", err)
	}
	if got.Header.Tag != req.Header.Tag || got.Header.Offset != req.Header.Offset || got.Path1 != req.Path1 {
		t.Fatalf("decoded request mismatch: %+v", got)
	}
	if string(got.Data) != "abc" {
		t.Fatalf("data = %q", got.Data)
	}
	if !got.WriteRDMAPreferred() {
		t.Fatal("write RDMA hint was not decoded")
	}
}

func TestDecodeRequestRejectsBadLength(t *testing.T) {
	buf := make([]byte, RequestHeaderSize)
	binary.LittleEndian.PutUint32(buf[44:48], 10)
	if _, err := DecodeRequest(buf); !errors.Is(err, ErrBadLength) {
		t.Fatalf("expected ErrBadLength, got %v", err)
	}
}

func TestReplyEncode(t *testing.T) {
	reply := &Reply{
		Tag:   7,
		Error: 0,
		Attr: Attr{
			Ino:   99,
			Size:  5,
			Mode:  0100644,
			Nlink: 1,
		},
		Data: []byte("hello"),
	}
	encoded, err := reply.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) != ReplyHeaderSize+5 {
		t.Fatalf("encoded length = %d", len(encoded))
	}
	hdr, err := DecodeReplyHeader(encoded)
	if err != nil {
		t.Fatalf("DecodeReplyHeader: %v", err)
	}
	if hdr.Tag != 7 || hdr.Attr.Ino != 99 || hdr.Attr.Size != 5 {
		t.Fatalf("reply header mismatch: %+v", hdr)
	}
	if got := binary.LittleEndian.Uint32(encoded[92:96]); got != 5 {
		t.Fatalf("datalen = %d", got)
	}
}

func TestStatFSEncodeDecode(t *testing.T) {
	encoded := EncodeStatFS(StatFS{
		Blocks:  100,
		Bfree:   80,
		Bavail:  70,
		Files:   1000,
		Ffree:   900,
		Bsize:   4096,
		Namelen: 255,
	})
	if len(encoded) != StatFSSize {
		t.Fatalf("encoded length = %d", len(encoded))
	}
	got, err := DecodeStatFS(encoded)
	if err != nil {
		t.Fatalf("DecodeStatFS: %v", err)
	}
	if got.Blocks != 100 || got.Bfree != 80 || got.Bavail != 70 || got.Files != 1000 || got.Ffree != 900 || got.Bsize != 4096 || got.Namelen != 255 {
		t.Fatalf("statfs mismatch: %+v", got)
	}
}
