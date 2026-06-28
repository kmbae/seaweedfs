//go:build !linux || !cgo || !seaweedfs_rdmaverbs

package weed_server

func NewEmbeddedVolumeRdmaTransport(cfg VolumeRdmaEmbeddedConfig) (VolumeRdmaEndpoint, VolumeRdmaReadRegistrar, error) {
	return nil, nil, ErrVolumeRdmaEmbeddedUnsupported
}
