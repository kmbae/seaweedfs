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
	ioctlRDMAGetLocal = ior(swvfsIOCMagic, 1, unsafe.Sizeof(swvfsproto.RDMALocalInfo{}))
	ioctlRDMAConnect  = iow(swvfsIOCMagic, 2, unsafe.Sizeof(swvfsproto.RDMARemoteInfo{}))
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

func ioc(dir uintptr, typ uintptr, nr uintptr, size uintptr) uintptr {
	return (dir << iocDirShift) |
		(typ << iocTypeShift) |
		(nr << iocNRShift) |
		(size << iocSizeShift)
}
