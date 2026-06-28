package weed_server

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	VolumeRdmaTransportSocket   = "socket"
	VolumeRdmaTransportEmbedded = "embedded"
)

var ErrVolumeRdmaEmbeddedUnsupported = errors.New("embedded volume RDMA transport is not supported by this build")

type VolumeRdmaTransportConfig struct {
	Transport              string
	EngineSocket           string
	EngineTimeout          time.Duration
	ReadExporter           VolumeRdmaReadExporterConfig
	Embedded               VolumeRdmaEmbeddedConfig
	EmbeddedFallbackSocket bool
}

type VolumeRdmaEmbeddedConfig struct {
	Device   string
	Port     uint32
	GIDIndex uint32
}

func (vs *VolumeServer) RdmaTransport() string {
	if vs == nil {
		return ""
	}
	return vs.rdmaTransport
}

func (vs *VolumeServer) ConfigureRdmaTransport(cfg VolumeRdmaTransportConfig) error {
	if vs == nil {
		return fmt.Errorf("volume server is nil")
	}
	switch strings.TrimSpace(cfg.Transport) {
	case VolumeRdmaTransportEmbedded:
		endpoint, registrar, err := NewEmbeddedVolumeRdmaTransport(cfg.Embedded)
		if err == nil {
			return vs.setRdmaTransport(VolumeRdmaTransportEmbedded, endpoint, registrar, cfg.ReadExporter)
		}
		if !errors.Is(err, ErrVolumeRdmaEmbeddedUnsupported) || !cfg.EmbeddedFallbackSocket || strings.TrimSpace(cfg.EngineSocket) == "" {
			return err
		}
		return vs.configureSocketRdmaTransport(cfg)
	case VolumeRdmaTransportSocket, "":
		return vs.configureSocketRdmaTransport(cfg)
	default:
		return fmt.Errorf("unknown volume RDMA transport %q", cfg.Transport)
	}
}

func (vs *VolumeServer) configureSocketRdmaTransport(cfg VolumeRdmaTransportConfig) error {
	socketPath := strings.TrimSpace(cfg.EngineSocket)
	if socketPath == "" {
		return fmt.Errorf("native RDMA engine socket path is required")
	}
	client := NewVolumeRdmaEngineClient(socketPath, cfg.EngineTimeout)
	return vs.setRdmaTransport(VolumeRdmaTransportSocket, client, client, cfg.ReadExporter)
}

func (vs *VolumeServer) setRdmaTransport(mode string, endpoint VolumeRdmaEndpoint, registrar VolumeRdmaReadRegistrar, cfg VolumeRdmaReadExporterConfig) error {
	if endpoint == nil {
		return fmt.Errorf("native RDMA endpoint is nil")
	}
	if registrar == nil {
		return fmt.Errorf("native RDMA read registrar is nil")
	}
	vs.SetRdmaEndpoint(endpoint)
	vs.SetRdmaReadExporter(NewVolumeStoreRdmaReadExporter(vs.store, registrar, cfg))
	vs.rdmaTransport = mode
	return nil
}
