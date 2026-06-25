// Package swvfsproto implements the seaweedvfs kernel <-> userspace ABI.
package swvfsproto

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	OpLookup    uint32 = 1
	OpReadDir   uint32 = 2
	OpRead      uint32 = 3
	OpCreate    uint32 = 4
	OpMkdir     uint32 = 5
	OpUnlink    uint32 = 6
	OpRmdir     uint32 = 7
	OpSetAttr   uint32 = 8
	OpWrite     uint32 = 9
	OpFlush     uint32 = 10
	OpRelease   uint32 = 11
	OpRename    uint32 = 12
	OpGetAttr   uint32 = 13
	OpSymlink   uint32 = 14
	OpReadLink  uint32 = 15
	OpGetXAttr  uint32 = 16
	OpSetXAttr  uint32 = 17
	OpListXAttr uint32 = 18
	OpLink      uint32 = 19
	OpMknod     uint32 = 20
	OpStatFS    uint32 = 21
	OpLock      uint32 = 22
)

const (
	ReadFRDMAPreferred  uint32 = 1 << 0
	WriteFRDMAPreferred uint32 = 1 << 0
)

const (
	PathMax    = 8192
	NameMax    = 255
	NameBuf    = 256
	MaxDirents = 32
	MaxWrite   = 1 << 20

	RequestHeaderSize = 88
	AttrSize          = 72
	ReplyHeaderSize   = 96
	DirentSize        = 336
	StatFSSize        = 48
)

var (
	ErrShortRequest = errors.New("short swvfs request")
	ErrShortReply   = errors.New("short swvfs reply")
	ErrBadLength    = errors.New("invalid swvfs payload length")
)

type RequestHeader struct {
	Tag       uint64
	Offset    uint64
	Size      uint64
	MtimeSec  int64
	AtimeSec  int64
	Op        uint32
	Plen1     uint32
	Plen2     uint32
	Dlen      uint32
	Valid     uint32
	Mode      uint32
	UID       uint32
	GID       uint32
	MtimeNsec uint32
	AtimeNsec uint32
	Pad0      uint32
	Pad1      uint32
}

type Request struct {
	Header RequestHeader
	Path1  string
	Path2  string
	Data   []byte
}

func (r *Request) ReadRDMAPreferred() bool {
	return r != nil && r.Header.Op == OpRead && r.Header.Valid&ReadFRDMAPreferred != 0
}

func (r *Request) WriteRDMAPreferred() bool {
	return r != nil && r.Header.Op == OpWrite && r.Header.Valid&WriteFRDMAPreferred != 0
}

func DecodeRequest(buf []byte) (*Request, error) {
	if len(buf) < RequestHeaderSize {
		return nil, fmt.Errorf("%w: got %d need %d", ErrShortRequest, len(buf), RequestHeaderSize)
	}
	h := decodeRequestHeader(buf[:RequestHeaderSize])
	if h.Plen1 > PathMax || h.Plen2 > PathMax || h.Dlen > MaxWrite {
		return nil, fmt.Errorf("%w: plen1=%d plen2=%d dlen=%d", ErrBadLength, h.Plen1, h.Plen2, h.Dlen)
	}
	total := uint64(RequestHeaderSize) + uint64(h.Plen1) + uint64(h.Plen2) + uint64(h.Dlen)
	if total > uint64(len(buf)) {
		return nil, fmt.Errorf("%w: got %d need %d", ErrBadLength, len(buf), total)
	}
	pos := RequestHeaderSize
	path1 := string(buf[pos : pos+int(h.Plen1)])
	pos += int(h.Plen1)
	path2 := string(buf[pos : pos+int(h.Plen2)])
	pos += int(h.Plen2)
	data := append([]byte(nil), buf[pos:pos+int(h.Dlen)]...)
	return &Request{Header: h, Path1: path1, Path2: path2, Data: data}, nil
}

func (r *Request) Encode() ([]byte, error) {
	if r == nil {
		return nil, errors.New("nil swvfs request")
	}
	if len(r.Path1) > PathMax || len(r.Path2) > PathMax || len(r.Data) > MaxWrite {
		return nil, fmt.Errorf("%w: path1=%d path2=%d data=%d", ErrBadLength, len(r.Path1), len(r.Path2), len(r.Data))
	}
	h := r.Header
	h.Plen1 = uint32(len(r.Path1))
	h.Plen2 = uint32(len(r.Path2))
	h.Dlen = uint32(len(r.Data))
	out := make([]byte, RequestHeaderSize+len(r.Path1)+len(r.Path2)+len(r.Data))
	encodeRequestHeader(out[:RequestHeaderSize], h)
	pos := RequestHeaderSize
	pos += copy(out[pos:], r.Path1)
	pos += copy(out[pos:], r.Path2)
	copy(out[pos:], r.Data)
	return out, nil
}

type Attr struct {
	Ino       uint64
	Size      uint64
	MtimeSec  int64
	CtimeSec  int64
	AtimeSec  int64
	Mode      uint32
	UID       uint32
	GID       uint32
	Nlink     uint32
	Rdev      uint32
	MtimeNsec uint32
	CtimeNsec uint32
	AtimeNsec uint32
}

type Dirent struct {
	Attr Attr
	Type uint32
	Name string
}

type StatFS struct {
	Blocks  uint64
	Bfree   uint64
	Bavail  uint64
	Files   uint64
	Ffree   uint64
	Bsize   uint32
	Namelen uint32
}

type Reply struct {
	Tag      uint64
	Attr     Attr
	Error    int32
	NEntries uint32
	EOF      uint32
	Data     []byte
	Dirents  []Dirent
}

func ErrorReply(tag uint64, errno int32) *Reply {
	if errno > 0 {
		errno = -errno
	}
	return &Reply{Tag: tag, Error: errno}
}

func (r *Reply) Encode() ([]byte, error) {
	if r == nil {
		return nil, errors.New("nil swvfs reply")
	}
	nentries := uint32(len(r.Dirents))
	if nentries > MaxDirents {
		return nil, fmt.Errorf("%w: too many dirents %d", ErrBadLength, nentries)
	}
	datalen := uint32(len(r.Data))
	payloadLen := len(r.Data) + len(r.Dirents)*DirentSize
	out := make([]byte, ReplyHeaderSize+payloadLen)
	binary.LittleEndian.PutUint64(out[0:8], r.Tag)
	encodeAttr(out[8:80], r.Attr)
	putI32(out[80:84], r.Error)
	binary.LittleEndian.PutUint32(out[84:88], nentries)
	binary.LittleEndian.PutUint32(out[88:92], r.EOF)
	binary.LittleEndian.PutUint32(out[92:96], datalen)
	pos := ReplyHeaderSize
	for _, d := range r.Dirents {
		encodeDirent(out[pos:pos+DirentSize], d)
		pos += DirentSize
	}
	copy(out[pos:], r.Data)
	return out, nil
}

func EncodeStatFS(st StatFS) []byte {
	out := make([]byte, StatFSSize)
	binary.LittleEndian.PutUint64(out[0:8], st.Blocks)
	binary.LittleEndian.PutUint64(out[8:16], st.Bfree)
	binary.LittleEndian.PutUint64(out[16:24], st.Bavail)
	binary.LittleEndian.PutUint64(out[24:32], st.Files)
	binary.LittleEndian.PutUint64(out[32:40], st.Ffree)
	binary.LittleEndian.PutUint32(out[40:44], st.Bsize)
	binary.LittleEndian.PutUint32(out[44:48], st.Namelen)
	return out
}

func DecodeStatFS(buf []byte) (StatFS, error) {
	if len(buf) < StatFSSize {
		return StatFS{}, fmt.Errorf("%w: statfs got %d need %d", ErrShortReply, len(buf), StatFSSize)
	}
	return StatFS{
		Blocks:  binary.LittleEndian.Uint64(buf[0:8]),
		Bfree:   binary.LittleEndian.Uint64(buf[8:16]),
		Bavail:  binary.LittleEndian.Uint64(buf[16:24]),
		Files:   binary.LittleEndian.Uint64(buf[24:32]),
		Ffree:   binary.LittleEndian.Uint64(buf[32:40]),
		Bsize:   binary.LittleEndian.Uint32(buf[40:44]),
		Namelen: binary.LittleEndian.Uint32(buf[44:48]),
	}, nil
}

func DecodeReplyHeader(buf []byte) (*Reply, error) {
	if len(buf) < ReplyHeaderSize {
		return nil, fmt.Errorf("%w: got %d need %d", ErrShortReply, len(buf), ReplyHeaderSize)
	}
	return &Reply{
		Tag:      binary.LittleEndian.Uint64(buf[0:8]),
		Attr:     decodeAttr(buf[8:80]),
		Error:    int32(binary.LittleEndian.Uint32(buf[80:84])),
		NEntries: binary.LittleEndian.Uint32(buf[84:88]),
		EOF:      binary.LittleEndian.Uint32(buf[88:92]),
	}, nil
}

func decodeRequestHeader(buf []byte) RequestHeader {
	return RequestHeader{
		Tag:       binary.LittleEndian.Uint64(buf[0:8]),
		Offset:    binary.LittleEndian.Uint64(buf[8:16]),
		Size:      binary.LittleEndian.Uint64(buf[16:24]),
		MtimeSec:  int64(binary.LittleEndian.Uint64(buf[24:32])),
		AtimeSec:  int64(binary.LittleEndian.Uint64(buf[32:40])),
		Op:        binary.LittleEndian.Uint32(buf[40:44]),
		Plen1:     binary.LittleEndian.Uint32(buf[44:48]),
		Plen2:     binary.LittleEndian.Uint32(buf[48:52]),
		Dlen:      binary.LittleEndian.Uint32(buf[52:56]),
		Valid:     binary.LittleEndian.Uint32(buf[56:60]),
		Mode:      binary.LittleEndian.Uint32(buf[60:64]),
		UID:       binary.LittleEndian.Uint32(buf[64:68]),
		GID:       binary.LittleEndian.Uint32(buf[68:72]),
		MtimeNsec: binary.LittleEndian.Uint32(buf[72:76]),
		AtimeNsec: binary.LittleEndian.Uint32(buf[76:80]),
		Pad0:      binary.LittleEndian.Uint32(buf[80:84]),
		Pad1:      binary.LittleEndian.Uint32(buf[84:88]),
	}
}

func encodeRequestHeader(buf []byte, h RequestHeader) {
	binary.LittleEndian.PutUint64(buf[0:8], h.Tag)
	binary.LittleEndian.PutUint64(buf[8:16], h.Offset)
	binary.LittleEndian.PutUint64(buf[16:24], h.Size)
	binary.LittleEndian.PutUint64(buf[24:32], uint64(h.MtimeSec))
	binary.LittleEndian.PutUint64(buf[32:40], uint64(h.AtimeSec))
	binary.LittleEndian.PutUint32(buf[40:44], h.Op)
	binary.LittleEndian.PutUint32(buf[44:48], h.Plen1)
	binary.LittleEndian.PutUint32(buf[48:52], h.Plen2)
	binary.LittleEndian.PutUint32(buf[52:56], h.Dlen)
	binary.LittleEndian.PutUint32(buf[56:60], h.Valid)
	binary.LittleEndian.PutUint32(buf[60:64], h.Mode)
	binary.LittleEndian.PutUint32(buf[64:68], h.UID)
	binary.LittleEndian.PutUint32(buf[68:72], h.GID)
	binary.LittleEndian.PutUint32(buf[72:76], h.MtimeNsec)
	binary.LittleEndian.PutUint32(buf[76:80], h.AtimeNsec)
	binary.LittleEndian.PutUint32(buf[80:84], h.Pad0)
	binary.LittleEndian.PutUint32(buf[84:88], h.Pad1)
}

func encodeAttr(buf []byte, a Attr) {
	binary.LittleEndian.PutUint64(buf[0:8], a.Ino)
	binary.LittleEndian.PutUint64(buf[8:16], a.Size)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(a.MtimeSec))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(a.CtimeSec))
	binary.LittleEndian.PutUint64(buf[32:40], uint64(a.AtimeSec))
	binary.LittleEndian.PutUint32(buf[40:44], a.Mode)
	binary.LittleEndian.PutUint32(buf[44:48], a.UID)
	binary.LittleEndian.PutUint32(buf[48:52], a.GID)
	binary.LittleEndian.PutUint32(buf[52:56], a.Nlink)
	binary.LittleEndian.PutUint32(buf[56:60], a.Rdev)
	binary.LittleEndian.PutUint32(buf[60:64], a.MtimeNsec)
	binary.LittleEndian.PutUint32(buf[64:68], a.CtimeNsec)
	binary.LittleEndian.PutUint32(buf[68:72], a.AtimeNsec)
}

func decodeAttr(buf []byte) Attr {
	return Attr{
		Ino:       binary.LittleEndian.Uint64(buf[0:8]),
		Size:      binary.LittleEndian.Uint64(buf[8:16]),
		MtimeSec:  int64(binary.LittleEndian.Uint64(buf[16:24])),
		CtimeSec:  int64(binary.LittleEndian.Uint64(buf[24:32])),
		AtimeSec:  int64(binary.LittleEndian.Uint64(buf[32:40])),
		Mode:      binary.LittleEndian.Uint32(buf[40:44]),
		UID:       binary.LittleEndian.Uint32(buf[44:48]),
		GID:       binary.LittleEndian.Uint32(buf[48:52]),
		Nlink:     binary.LittleEndian.Uint32(buf[52:56]),
		Rdev:      binary.LittleEndian.Uint32(buf[56:60]),
		MtimeNsec: binary.LittleEndian.Uint32(buf[60:64]),
		CtimeNsec: binary.LittleEndian.Uint32(buf[64:68]),
		AtimeNsec: binary.LittleEndian.Uint32(buf[68:72]),
	}
}

func encodeDirent(buf []byte, d Dirent) {
	encodeAttr(buf[0:AttrSize], d.Attr)
	binary.LittleEndian.PutUint32(buf[72:76], d.Type)
	name := []byte(d.Name)
	if len(name) > NameMax {
		name = name[:NameMax]
	}
	binary.LittleEndian.PutUint32(buf[76:80], uint32(len(name)))
	copy(buf[80:80+NameBuf], name)
}

func putI32(buf []byte, v int32) {
	binary.LittleEndian.PutUint32(buf, uint32(v))
}
