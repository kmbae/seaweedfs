package swvfsfiler

import (
	"context"
	"fmt"

	"seaweedfs-rdma-sidecar/pkg/seaweedfs"
	"seaweedfs-rdma-sidecar/pkg/swvfsdaemon"
)

type SeaweedNeedlePlane struct {
	Client *seaweedfs.SeaweedFSRDMAClient
}

func (p *SeaweedNeedlePlane) ReadNeedle(ctx context.Context, req swvfsdaemon.NeedleReadRequest) (*swvfsdaemon.NeedleReadResult, error) {
	if p == nil || p.Client == nil {
		return nil, fmt.Errorf("nil SeaweedFS needle read client")
	}
	volumeID, needleID, cookie, err := ParseFileID(req.FileID)
	if err != nil {
		return nil, err
	}
	resp, err := p.Client.ReadNeedle(ctx, &seaweedfs.NeedleReadRequest{
		VolumeID:     volumeID,
		NeedleID:     needleID,
		Cookie:       cookie,
		Offset:       req.Offset,
		Size:         req.Size,
		VolumeServer: req.VolumeServer,
		RDMAServer:   req.RDMAServer,
	})
	if err != nil {
		return nil, err
	}
	return &swvfsdaemon.NeedleReadResult{
		Data:     resp.Data,
		UsedRDMA: resp.IsRDMA,
		Source:   resp.Source,
	}, nil
}

func (p *SeaweedNeedlePlane) WriteNeedle(ctx context.Context, req swvfsdaemon.NeedleWriteRequest) (*swvfsdaemon.NeedleWriteResult, error) {
	if p == nil || p.Client == nil {
		return nil, fmt.Errorf("nil SeaweedFS needle write client")
	}
	volumeID, needleID, cookie, err := ParseFileID(req.FileID)
	if err != nil {
		return nil, err
	}
	resp, err := p.Client.WriteNeedle(ctx, &seaweedfs.NeedleWriteRequest{
		VolumeID:     volumeID,
		NeedleID:     needleID,
		Cookie:       cookie,
		Data:         req.Data,
		VolumeServer: req.VolumeServer,
		RDMAServer:   req.RDMAServer,
	})
	if err != nil {
		return nil, err
	}
	return &swvfsdaemon.NeedleWriteResult{
		FileID:   resp.FileID,
		UsedRDMA: resp.IsRDMA,
		Source:   resp.Source,
	}, nil
}
