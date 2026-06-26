package ipc

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack/v5"
)

func TestStartWriteUsesSidebandForLargePayload(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	logger := logrus.New()
	logger.SetOutput(io.Discard)
	c := &Client{conn: clientConn, connected: true, logger: logger}

	payload := make([]byte, SidebandMinSize+1024)
	for i := range payload {
		payload[i] = byte(i)
	}

	serverErr := make(chan error, 1)
	go func() {
		msg, err := readTestMessage(serverConn)
		if err != nil {
			serverErr <- err
			return
		}
		if msg.Type != MsgStartWrite {
			serverErr <- errUnexpected("message type", msg.Type, MsgStartWrite)
			return
		}
		req, err := decodeTestData[StartWriteRequest](msg.Data)
		if err != nil {
			serverErr <- err
			return
		}
		if !req.DataSideband {
			serverErr <- errUnexpected("data_sideband", req.DataSideband, true)
			return
		}
		if len(req.Data) != 0 {
			serverErr <- errUnexpected("inline data length", len(req.Data), 0)
			return
		}
		if req.Size != uint64(len(payload)) {
			serverErr <- errUnexpected("sideband size", req.Size, uint64(len(payload)))
			return
		}
		got := make([]byte, len(payload))
		if _, err := io.ReadFull(serverConn, got); err != nil {
			serverErr <- err
			return
		}
		for i := range got {
			if got[i] != payload[i] {
				serverErr <- errUnexpected("sideband byte", got[i], payload[i])
				return
			}
		}
		resp := &IpcMessage{
			Type: MsgStartWriteResponse,
			Data: &StartWriteResponse{
				SessionID:     "s1",
				BytesBuffered: uint64(len(payload)),
				Success:       true,
			},
		}
		serverErr <- writeTestMessage(serverConn, resp)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.StartWrite(ctx, &StartWriteRequest{
		VolumeID: 1,
		NeedleID: 2,
		Cookie:   3,
		Size:     uint64(len(payload)),
		Data:     payload,
	})
	if err != nil {
		t.Fatalf("StartWrite failed: %v", err)
	}
	if resp.SessionID != "s1" || resp.BytesBuffered != uint64(len(payload)) {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("server side failed: %v", err)
	}
}

func TestCompleteReadReceivesSidebandPayload(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	logger := logrus.New()
	logger.SetOutput(io.Discard)
	c := &Client{conn: clientConn, connected: true, logger: logger}

	payload := make([]byte, SidebandMinSize+4096)
	for i := range payload {
		payload[i] = byte(255 - i)
	}

	serverErr := make(chan error, 1)
	go func() {
		msg, err := readTestMessage(serverConn)
		if err != nil {
			serverErr <- err
			return
		}
		if msg.Type != MsgCompleteRead {
			serverErr <- errUnexpected("message type", msg.Type, MsgCompleteRead)
			return
		}
		resp := &IpcMessage{
			Type: MsgCompleteReadResponse,
			Data: &CompleteReadResponse{
				Success:      true,
				DataSideband: true,
				DataSize:     uint64(len(payload)),
			},
		}
		if err := writeTestMessage(serverConn, resp); err != nil {
			serverErr <- err
			return
		}
		_, err = serverConn.Write(payload)
		serverErr <- err
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.CompleteRead(ctx, "s1", true, uint64(len(payload)), nil)
	if err != nil {
		t.Fatalf("CompleteRead failed: %v", err)
	}
	if len(resp.Data) != len(payload) {
		t.Fatalf("data length = %d, want %d", len(resp.Data), len(payload))
	}
	for i := range payload {
		if resp.Data[i] != payload[i] {
			t.Fatalf("data[%d] = %d, want %d", i, resp.Data[i], payload[i])
		}
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("server side failed: %v", err)
	}
}

func readTestMessage(conn net.Conn) (*IpcMessage, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return nil, err
	}
	frame := make([]byte, binary.LittleEndian.Uint32(lenBuf[:]))
	if _, err := io.ReadFull(conn, frame); err != nil {
		return nil, err
	}
	var msg IpcMessage
	if err := msgpack.Unmarshal(frame, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

func writeTestMessage(conn net.Conn, msg *IpcMessage) error {
	frame, err := msgpack.Marshal(msg)
	if err != nil {
		return err
	}
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(frame)))
	if _, err := conn.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err = conn.Write(frame)
	return err
}

func decodeTestData[T any](data interface{}) (*T, error) {
	frame, err := msgpack.Marshal(data)
	if err != nil {
		return nil, err
	}
	var out T
	if err := msgpack.Unmarshal(frame, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

type unexpectedValue struct {
	field string
	got   interface{}
	want  interface{}
}

func errUnexpected(field string, got, want interface{}) error {
	return unexpectedValue{field: field, got: got, want: want}
}

func (e unexpectedValue) Error() string {
	return e.field + " mismatch"
}
