package swvfsproto

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
)

const (
	RDMAABIVersion     uint32 = 1
	RDMADeviceNameMax         = 64
	RDMAIOMax                 = 1 << 20
	RDMALinkUnknown    uint32 = 0
	RDMALinkInfiniBand uint32 = 1
	RDMALinkEthernet   uint32 = 2
)

const (
	RDMAFKernelEnabled   uint32 = 1 << 0
	RDMAFEndpointReady   uint32 = 1 << 1
	RDMAFGIDValid        uint32 = 1 << 2
	RDMAFQPConnected     uint32 = 1 << 3
	RDMAFUnsafeGlobalKey uint32 = 1 << 4
)

const (
	RDMARemoteFGIDValid    uint32 = 1 << 0
	RDMARemoteFGRHRequired uint32 = 1 << 1
)

const RDMADataDescSize = 48

type RDMALocalInfo struct {
	ABIVersion           uint32
	Flags                uint32
	Port                 uint32
	QPNum                uint32
	PSN                  uint32
	QPState              uint32
	LID                  uint32
	SMLID                uint32
	PortState            uint32
	ActiveMTU            uint32
	GIDIndex             uint32
	LinkLayer            uint32
	GID                  [16]byte
	Device               [RDMADeviceNameMax]byte
	DevicesSeen          uint64
	DevicesSelected      uint64
	DevicesRejected      uint64
	ResourceInits        uint64
	ResourceFailures     uint64
	ReadAttempts         uint64
	ReadFallbacks        uint64
	Connects             uint64
	ConnectFailures      uint64
	RDMAReadPosts        uint64
	RDMAReadCompletions  uint64
	RDMAReadFailures     uint64
	RDMAReadBytes        uint64
	RDMAWritePosts       uint64
	RDMAWriteCompletions uint64
	RDMAWriteFailures    uint64
	RDMAWriteBytes       uint64
	Reserved             [2]uint64
}

func (i RDMALocalInfo) KernelEnabled() bool {
	return i.Flags&RDMAFKernelEnabled != 0
}

func (i RDMALocalInfo) EndpointReady() bool {
	return i.Flags&RDMAFEndpointReady != 0
}

func (i RDMALocalInfo) Connected() bool {
	return i.Flags&RDMAFQPConnected != 0
}

func (i RDMALocalInfo) GIDValid() bool {
	return i.Flags&RDMAFGIDValid != 0
}

func (i RDMALocalInfo) DeviceName() string {
	n := 0
	for n < len(i.Device) && i.Device[n] != 0 {
		n++
	}
	return string(i.Device[:n])
}

func (i RDMALocalInfo) GIDHex() string {
	return hex.EncodeToString(i.GID[:])
}

type RDMARemoteInfo struct {
	ABIVersion uint32
	Flags      uint32
	QPN        uint32
	LID        uint32
	PSN        uint32
	Port       uint32
	GIDIndex   uint32
	SL         uint32
	GID        [16]byte
	Reserved   [8]uint64
}

type RDMADataDesc struct {
	RemoteAddr uint64
	RKey       uint32
	Length     uint32
	Reserved   [4]uint64
}

func EncodeRDMADataDesc(desc RDMADataDesc) []byte {
	out := make([]byte, RDMADataDescSize)
	binary.LittleEndian.PutUint64(out[0:8], desc.RemoteAddr)
	binary.LittleEndian.PutUint32(out[8:12], desc.RKey)
	binary.LittleEndian.PutUint32(out[12:16], desc.Length)
	for i, v := range desc.Reserved {
		off := 16 + i*8
		binary.LittleEndian.PutUint64(out[off:off+8], v)
	}
	return out
}

func DecodeRDMADataDesc(buf []byte) (RDMADataDesc, error) {
	if len(buf) < RDMADataDescSize {
		return RDMADataDesc{}, fmt.Errorf("%w: rdma desc got %d need %d", ErrShortReply, len(buf), RDMADataDescSize)
	}
	var desc RDMADataDesc
	desc.RemoteAddr = binary.LittleEndian.Uint64(buf[0:8])
	desc.RKey = binary.LittleEndian.Uint32(buf[8:12])
	desc.Length = binary.LittleEndian.Uint32(buf[12:16])
	for i := range desc.Reserved {
		off := 16 + i*8
		desc.Reserved[i] = binary.LittleEndian.Uint64(buf[off : off+8])
	}
	return desc, nil
}

func DecodeGIDHex(raw string) ([16]byte, bool) {
	var gid [16]byte
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return gid, false
	}
	decoded, err := hex.DecodeString(raw)
	if err != nil || len(decoded) != len(gid) {
		return gid, false
	}
	copy(gid[:], decoded)
	return gid, true
}
