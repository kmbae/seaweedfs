package weed_server

import (
	"errors"
	"testing"
	"time"
)

func TestConfigureRdmaTransportSocket(t *testing.T) {
	vs := &VolumeServer{}

	if err := vs.ConfigureRdmaTransport(VolumeRdmaTransportConfig{
		Transport:     VolumeRdmaTransportSocket,
		EngineSocket:  "/tmp/volume-rdma-engine.sock",
		EngineTimeout: time.Second,
	}); err != nil {
		t.Fatalf("ConfigureRdmaTransport socket: %v", err)
	}

	if vs.rdmaTransport != VolumeRdmaTransportSocket {
		t.Fatalf("transport = %q, want %q", vs.rdmaTransport, VolumeRdmaTransportSocket)
	}
	if _, ok := vs.rdmaEndpoint.(*VolumeRdmaEngineClient); !ok {
		t.Fatalf("endpoint = %T, want *VolumeRdmaEngineClient", vs.rdmaEndpoint)
	}
	if vs.rdmaReadExporter == nil {
		t.Fatal("read exporter was not configured")
	}
}

func TestConfigureRdmaTransportEmbeddedUnsupportedWithoutFallback(t *testing.T) {
	vs := &VolumeServer{}

	err := vs.ConfigureRdmaTransport(VolumeRdmaTransportConfig{
		Transport:              VolumeRdmaTransportEmbedded,
		EmbeddedFallbackSocket: false,
		EngineSocket:           "/tmp/volume-rdma-engine.sock",
	})
	if !errors.Is(err, ErrVolumeRdmaEmbeddedUnsupported) {
		t.Fatalf("error = %v, want ErrVolumeRdmaEmbeddedUnsupported", err)
	}
	if vs.rdmaEndpoint != nil || vs.rdmaReadExporter != nil || vs.rdmaTransport != "" {
		t.Fatalf("unexpected partial RDMA configuration: endpoint=%T exporter=%T transport=%q", vs.rdmaEndpoint, vs.rdmaReadExporter, vs.rdmaTransport)
	}
}

func TestConfigureRdmaTransportEmbeddedFallsBackToSocket(t *testing.T) {
	vs := &VolumeServer{}

	if err := vs.ConfigureRdmaTransport(VolumeRdmaTransportConfig{
		Transport:              VolumeRdmaTransportEmbedded,
		EmbeddedFallbackSocket: true,
		EngineSocket:           "/tmp/volume-rdma-engine.sock",
		Embedded: VolumeRdmaEmbeddedConfig{
			Device:   "mlx5_1",
			Port:     1,
			GIDIndex: 0,
		},
	}); err != nil {
		t.Fatalf("ConfigureRdmaTransport embedded fallback: %v", err)
	}
	if vs.rdmaTransport != VolumeRdmaTransportSocket {
		t.Fatalf("transport = %q, want fallback %q", vs.rdmaTransport, VolumeRdmaTransportSocket)
	}
	if _, ok := vs.rdmaEndpoint.(*VolumeRdmaEngineClient); !ok {
		t.Fatalf("endpoint = %T, want *VolumeRdmaEngineClient", vs.rdmaEndpoint)
	}
}
