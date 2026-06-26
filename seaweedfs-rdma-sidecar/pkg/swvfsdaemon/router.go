// Package swvfsdaemon contains the userspace daemon plumbing for seaweedvfs.
package swvfsdaemon

import (
	"context"
	"errors"
	"fmt"
)

type NeedleReadRequest struct {
	FileID       string
	VolumeServer string
	RDMAServer   string
	Offset       uint64
	Size         uint64
	PreferRDMA   bool
}

type NeedleReadResult struct {
	Data     []byte
	UsedRDMA bool
	Source   string
}

type NeedleWriteRequest struct {
	FileID       string
	VolumeServer string
	RDMAServer   string
	Data         []byte
	PreferRDMA   bool
}

type NeedleWriteResult struct {
	FileID   string
	UsedRDMA bool
	Source   string
}

type NeedleDataPlane interface {
	ReadNeedle(context.Context, NeedleReadRequest) (*NeedleReadResult, error)
	WriteNeedle(context.Context, NeedleWriteRequest) (*NeedleWriteResult, error)
}

type Router struct {
	RDMA             NeedleDataPlane
	Fallback         NeedleDataPlane
	EnableReadRDMA   bool
	EnableWriteRDMA  bool
	ReadRDMAMinSize  uint64
	WriteRDMAMinSize uint64
	FallbackOnError  bool
}

func (r *Router) ReadNeedle(ctx context.Context, req NeedleReadRequest) (*NeedleReadResult, error) {
	if r.shouldUseRDMARead(req) {
		resp, err := r.RDMA.ReadNeedle(ctx, req)
		if err == nil {
			if resp.Source == "" {
				resp.Source = "rdma"
			}
			return resp, nil
		}
		if !r.FallbackOnError || r.Fallback == nil {
			return nil, err
		}
	}
	if r.Fallback == nil {
		return nil, errors.New("no fallback read data plane configured")
	}
	req.PreferRDMA = false
	resp, err := r.Fallback.ReadNeedle(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.Source == "" {
		resp.Source = "tcp"
	}
	return resp, nil
}

func (r *Router) WriteNeedle(ctx context.Context, req NeedleWriteRequest) (*NeedleWriteResult, error) {
	if r.shouldUseRDMAWrite(req) {
		resp, err := r.RDMA.WriteNeedle(ctx, req)
		if err == nil {
			if resp.Source == "" {
				resp.Source = "rdma"
			}
			return resp, nil
		}
		if !r.FallbackOnError || r.Fallback == nil {
			return nil, err
		}
	}
	if r.Fallback == nil {
		return nil, errors.New("no fallback write data plane configured")
	}
	req.PreferRDMA = false
	resp, err := r.Fallback.WriteNeedle(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.Source == "" {
		resp.Source = "tcp"
	}
	if resp.FileID == "" {
		resp.FileID = req.FileID
	}
	return resp, nil
}

func (r *Router) shouldUseRDMARead(req NeedleReadRequest) bool {
	if !req.PreferRDMA || !r.EnableReadRDMA || r.RDMA == nil {
		return false
	}
	return r.ReadRDMAMinSize == 0 || req.Size >= r.ReadRDMAMinSize
}

func (r *Router) shouldUseRDMAWrite(req NeedleWriteRequest) bool {
	if !req.PreferRDMA || !r.EnableWriteRDMA || r.RDMA == nil {
		return false
	}
	return r.WriteRDMAMinSize == 0 || uint64(len(req.Data)) >= r.WriteRDMAMinSize
}

func RequireRouter(router *Router) error {
	if router == nil {
		return fmt.Errorf("nil swvfs data-plane router")
	}
	if router.Fallback == nil && router.RDMA == nil {
		return fmt.Errorf("swvfs data-plane router has no backends")
	}
	return nil
}
