package swvfsdaemon

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"

	"seaweedfs-rdma-sidecar/pkg/swvfsproto"
)

const (
	iocNRBits   = 8
	iocTypeBits = 8
	iocSizeBits = 14

	iocNRShift   = 0
	iocTypeShift = iocNRShift + iocNRBits
	iocSizeShift = iocTypeShift + iocTypeBits
	iocDirShift  = iocSizeShift + iocSizeBits

	iocWrite = 1
	iocRead  = 2

	swvfsIOCMagic = uintptr('w')
)

var (
	ioctlRDMAGetLocal    = ior(swvfsIOCMagic, 1, unsafe.Sizeof(swvfsproto.RDMALocalInfo{}))
	ioctlRDMAConnect     = iow(swvfsIOCMagic, 2, unsafe.Sizeof(swvfsproto.RDMARemoteInfo{}))
	ioctlRDMATestMRAlloc = iowr(swvfsIOCMagic, 5, unsafe.Sizeof(swvfsproto.RDMATestMR{}))
	ioctlRDMATestMRFree  = ioctlNoArg(swvfsIOCMagic, 6)
	ioctlRDMATestMRInfo  = ior(swvfsIOCMagic, 7, unsafe.Sizeof(swvfsproto.RDMATestMR{}))
	ioctlRDMATestMRRead  = iowr(swvfsIOCMagic, 8, unsafe.Sizeof(swvfsproto.RDMATestMR{}))
	ioctlRDMATestMRWrite = iowr(swvfsIOCMagic, 9, unsafe.Sizeof(swvfsproto.RDMATestMR{}))
	ioctlRDMAGetLocalFor = iowr(swvfsIOCMagic, 10, unsafe.Sizeof(swvfsproto.RDMALocalInfo{}))
)

type RDMAControl struct {
	file *os.File
}

func NewRDMAControl(file *os.File) *RDMAControl {
	return &RDMAControl{file: file}
}

func (c *RDMAControl) GetLocal() (swvfsproto.RDMALocalInfo, error) {
	var info swvfsproto.RDMALocalInfo
	if c == nil || c.file == nil {
		return info, fmt.Errorf("nil RDMA control device")
	}
	if err := ioctl(c.file.Fd(), ioctlRDMAGetLocal, uintptr(unsafe.Pointer(&info))); err != nil {
		return info, fmt.Errorf("SWVFS_IOC_RDMA_GET_LOCAL: %w", err)
	}
	return info, nil
}

func (c *RDMAControl) GetLocalFor(connectionID uint64) (swvfsproto.RDMALocalInfo, error) {
	var info swvfsproto.RDMALocalInfo
	if c == nil || c.file == nil {
		return info, fmt.Errorf("nil RDMA control device")
	}
	info.Reserved[0] = connectionID
	if err := ioctl(c.file.Fd(), ioctlRDMAGetLocalFor, uintptr(unsafe.Pointer(&info))); err != nil {
		return info, fmt.Errorf("SWVFS_IOC_RDMA_GET_LOCAL_FOR: %w", err)
	}
	return info, nil
}

func (c *RDMAControl) Connect(remote swvfsproto.RDMARemoteInfo) error {
	if c == nil || c.file == nil {
		return fmt.Errorf("nil RDMA control device")
	}
	if remote.ABIVersion == 0 {
		remote.ABIVersion = swvfsproto.RDMAABIVersion
	}
	if err := ioctl(c.file.Fd(), ioctlRDMAConnect, uintptr(unsafe.Pointer(&remote))); err != nil {
		return fmt.Errorf("SWVFS_IOC_RDMA_CONNECT: %w", err)
	}
	return nil
}

func (c *RDMAControl) TestMRAlloc(length uint32, pattern uint32) (swvfsproto.RDMATestMR, error) {
	mr := swvfsproto.RDMATestMR{
		ABIVersion: swvfsproto.RDMAABIVersion,
		Length:     length,
		Pattern:    pattern,
	}
	if c == nil || c.file == nil {
		return mr, fmt.Errorf("nil RDMA control device")
	}
	if length == 0 || length > swvfsproto.RDMAIOMax {
		return mr, fmt.Errorf("invalid RDMA test MR length %d", length)
	}
	if err := ioctl(c.file.Fd(), ioctlRDMATestMRAlloc, uintptr(unsafe.Pointer(&mr))); err != nil {
		return mr, fmt.Errorf("SWVFS_IOC_RDMA_TEST_MR_ALLOC: %w", err)
	}
	return mr, nil
}

func (c *RDMAControl) TestMRInfo(sessionID uint64) (swvfsproto.RDMATestMR, error) {
	mr := swvfsproto.RDMATestMR{
		SessionID: sessionID,
	}
	if c == nil || c.file == nil {
		return mr, fmt.Errorf("nil RDMA control device")
	}
	if err := ioctl(c.file.Fd(), ioctlRDMATestMRInfo, uintptr(unsafe.Pointer(&mr))); err != nil {
		return mr, fmt.Errorf("SWVFS_IOC_RDMA_TEST_MR_INFO: %w", err)
	}
	return mr, nil
}

func (c *RDMAControl) TestMRWrite(sessionID uint64, data []byte) (swvfsproto.RDMATestMR, error) {
	mr := swvfsproto.RDMATestMR{
		ABIVersion: swvfsproto.RDMAABIVersion,
		SessionID:  sessionID,
	}
	if c == nil || c.file == nil {
		return mr, fmt.Errorf("nil RDMA control device")
	}
	if len(data) == 0 || len(data) > swvfsproto.RDMAIOMax {
		return mr, fmt.Errorf("invalid RDMA test MR write length %d", len(data))
	}
	mr.UserAddr = uint64(uintptr(unsafe.Pointer(&data[0])))
	mr.UserLength = uint32(len(data))
	if err := ioctl(c.file.Fd(), ioctlRDMATestMRWrite, uintptr(unsafe.Pointer(&mr))); err != nil {
		return mr, fmt.Errorf("SWVFS_IOC_RDMA_TEST_MR_WRITE: %w", err)
	}
	return mr, nil
}

func (c *RDMAControl) TestMRRead(sessionID uint64, length uint32) ([]byte, swvfsproto.RDMATestMR, error) {
	mr := swvfsproto.RDMATestMR{
		ABIVersion: swvfsproto.RDMAABIVersion,
		SessionID:  sessionID,
	}
	if c == nil || c.file == nil {
		return nil, mr, fmt.Errorf("nil RDMA control device")
	}
	if length == 0 || length > swvfsproto.RDMAIOMax {
		return nil, mr, fmt.Errorf("invalid RDMA test MR read length %d", length)
	}
	buf := make([]byte, length)
	mr.UserAddr = uint64(uintptr(unsafe.Pointer(&buf[0])))
	mr.UserLength = length
	if err := ioctl(c.file.Fd(), ioctlRDMATestMRRead, uintptr(unsafe.Pointer(&mr))); err != nil {
		return nil, mr, fmt.Errorf("SWVFS_IOC_RDMA_TEST_MR_READ: %w", err)
	}
	return buf[:mr.UserLength], mr, nil
}

func (c *RDMAControl) TestMRFree(sessionID uint64) error {
	if c == nil || c.file == nil {
		return fmt.Errorf("nil RDMA control device")
	}
	mr := swvfsproto.RDMATestMR{
		SessionID: sessionID,
	}
	arg := uintptr(0)
	if sessionID != 0 {
		arg = uintptr(unsafe.Pointer(&mr))
	}
	if err := ioctl(c.file.Fd(), ioctlRDMATestMRFree, arg); err != nil {
		return fmt.Errorf("SWVFS_IOC_RDMA_TEST_MR_FREE: %w", err)
	}
	return nil
}

func ioctl(fd, req, arg uintptr) error {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, req, arg)
	if errno != 0 {
		return errno
	}
	return nil
}

func ior(typ uintptr, nr uintptr, size uintptr) uintptr {
	return ioc(iocRead, typ, nr, size)
}

func iow(typ uintptr, nr uintptr, size uintptr) uintptr {
	return ioc(iocWrite, typ, nr, size)
}

func iowr(typ uintptr, nr uintptr, size uintptr) uintptr {
	return ioc(iocRead|iocWrite, typ, nr, size)
}

func ioctlNoArg(typ uintptr, nr uintptr) uintptr {
	return ioc(0, typ, nr, 0)
}

func ioc(dir uintptr, typ uintptr, nr uintptr, size uintptr) uintptr {
	return (dir << iocDirShift) |
		(typ << iocTypeShift) |
		(nr << iocNRShift) |
		(size << iocSizeShift)
}
