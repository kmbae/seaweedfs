// Package swvfsdaemon contains the userspace daemon plumbing for seaweedvfs.
package swvfsdaemon

import (
	"context"
	"errors"
	"fmt"
	"time"
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
	Stats            *Stats
}

func (r *Router) ReadNeedle(ctx context.Context, req NeedleReadRequest) (*NeedleReadResult, error) {
	start := time.Now()
	r.Stats.Inc("router_read_requests")
	r.Stats.Add("router_read_requested_bytes", req.Size)
	if r.shouldUseRDMARead(req) {
		r.Stats.Inc("router_read_rdma_attempts")
		resp, err := r.RDMA.ReadNeedle(ctx, req)
		if err == nil {
			if resp.Source == "" {
				resp.Source = "rdma"
			}
			r.Stats.Inc("router_read_rdma_success")
			r.Stats.Add("router_read_rdma_bytes", uint64(len(resp.Data)))
			r.Stats.Observe("router_read_rdma", time.Since(start))
			return resp, nil
		}
		r.Stats.Inc("router_read_rdma_errors")
		if !r.FallbackOnError || r.Fallback == nil {
			return nil, err
		}
		r.Stats.Inc("router_read_fallback_after_rdma_error")
	}
	if r.Fallback == nil {
		return nil, errors.New("no fallback read data plane configured")
	}
	req.PreferRDMA = false
	resp, err := r.Fallback.ReadNeedle(ctx, req)
	if err != nil {
		r.Stats.Inc("router_read_fallback_errors")
		return nil, err
	}
	if resp.Source == "" {
		resp.Source = "tcp"
	}
	r.Stats.Inc("router_read_fallback_success")
	r.Stats.Add("router_read_fallback_bytes", uint64(len(resp.Data)))
	r.Stats.Observe("router_read_fallback", time.Since(start))
	return resp, nil
}

func (r *Router) WriteNeedle(ctx context.Context, req NeedleWriteRequest) (*NeedleWriteResult, error) {
	start := time.Now()
	r.Stats.Inc("router_write_requests")
	r.Stats.Add("router_write_requested_bytes", uint64(len(req.Data)))
	if r.shouldUseRDMAWrite(req) {
		r.Stats.Inc("router_write_rdma_attempts")
		resp, err := r.RDMA.WriteNeedle(ctx, req)
		if err == nil {
			if resp.Source == "" {
				resp.Source = "rdma"
			}
			r.Stats.Inc("router_write_rdma_success")
			r.Stats.Add("router_write_rdma_bytes", uint64(len(req.Data)))
			r.Stats.Observe("router_write_rdma", time.Since(start))
			return resp, nil
		}
		r.Stats.Inc("router_write_rdma_errors")
		if !r.FallbackOnError || r.Fallback == nil {
			return nil, err
		}
		r.Stats.Inc("router_write_fallback_after_rdma_error")
	}
	if r.Fallback == nil {
		return nil, errors.New("no fallback write data plane configured")
	}
	req.PreferRDMA = false
	resp, err := r.Fallback.WriteNeedle(ctx, req)
	if err != nil {
		r.Stats.Inc("router_write_fallback_errors")
		return nil, err
	}
	if resp.Source == "" {
		resp.Source = "tcp"
	}
	if resp.FileID == "" {
		resp.FileID = req.FileID
	}
	r.Stats.Inc("router_write_fallback_success")
	r.Stats.Add("router_write_fallback_bytes", uint64(len(req.Data)))
	r.Stats.Observe("router_write_fallback", time.Since(start))
	return resp, nil
}

func (r *Router) shouldUseRDMARead(req NeedleReadRequest) bool {
	if !req.PreferRDMA || !r.EnableReadRDMA || r.RDMA == nil {
		r.Stats.Inc("router_read_rdma_policy_disabled")
		return false
	}
	if r.ReadRDMAMinSize != 0 && req.Size < r.ReadRDMAMinSize {
		r.Stats.Inc("router_read_rdma_policy_too_small")
		return false
	}
	return true
}

func (r *Router) shouldUseRDMAWrite(req NeedleWriteRequest) bool {
	if !req.PreferRDMA || !r.EnableWriteRDMA || r.RDMA == nil {
		r.Stats.Inc("router_write_rdma_policy_disabled")
		return false
	}
	if r.WriteRDMAMinSize != 0 && uint64(len(req.Data)) < r.WriteRDMAMinSize {
		r.Stats.Inc("router_write_rdma_policy_too_small")
		return false
	}
	return true
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
