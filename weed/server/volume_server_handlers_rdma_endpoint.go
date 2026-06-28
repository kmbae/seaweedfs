package weed_server

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	VolumeRdmaNativeStatusPath           = "/rdma/native/status"
	VolumeRdmaNativeLocalPath            = "/rdma/native/local"
	VolumeRdmaNativeConnectPath          = "/rdma/native/connect"
	VolumeRdmaNativeReadDescPath         = "/rdma/native/read-desc"
	VolumeRdmaNativeReleaseDescPath      = "/rdma/native/release-desc"
	VolumeRdmaNativeRequesterLocalPath   = "/rdma/native/requester-local"
	VolumeRdmaNativeRequesterConnectPath = "/rdma/native/requester-connect"
	VolumeRdmaNativeWritePath            = "/rdma/native/write"
	VolumeRdmaNativeWriteDescPath        = "/rdma/native/write-desc"
	VolumeRdmaNativeWriteCommitPath      = "/rdma/native/write-commit"
	VolumeRdmaNativeWriteAbortPath       = "/rdma/native/write-abort"

	VolumeRdmaABIVersion         uint32 = 1
	VolumeRdmaLinkUnknown        uint32 = 0
	VolumeRdmaLinkInfiniBand     uint32 = 1
	VolumeRdmaLinkEthernet       uint32 = 2
	VolumeRdmaRemoteFGIDValid    uint32 = 1 << 0
	VolumeRdmaRemoteFGRHRequired        = 1 << 1
)

type VolumeRdmaEndpoint interface {
	LocalEndpoint(context.Context) (VolumeRdmaEndpointInfo, error)
	ConnectEndpoint(context.Context, VolumeRdmaRemoteInfo) error
}

type VolumeRdmaConnectionEndpoint interface {
	LocalEndpointFor(context.Context, uint64) (VolumeRdmaEndpointInfo, uint64, error)
	ConnectEndpointFor(context.Context, uint64, VolumeRdmaRemoteInfo) error
}

type VolumeRdmaRequesterEndpoint interface {
	RequesterLocalEndpoint(context.Context) (VolumeRdmaEndpointInfo, error)
	RequesterLocalEndpointFor(context.Context, uint64) (VolumeRdmaEndpointInfo, uint64, error)
	RequesterConnectEndpoint(context.Context, VolumeRdmaRemoteInfo) error
	RequesterConnectEndpointFor(context.Context, uint64, VolumeRdmaRemoteInfo) error
	ReadRemoteFor(context.Context, uint64, VolumeRdmaDataDesc, time.Duration) ([]byte, error)
}

type VolumeRdmaEndpointInfo struct {
	ConnectionID    uint64 `json:"connection_id,omitempty"`
	ABIVersion      uint32 `json:"abi_version"`
	Flags           uint32 `json:"flags"`
	Device          string `json:"device"`
	Port            uint32 `json:"port"`
	QPNum           uint32 `json:"qp_num"`
	PSN             uint32 `json:"psn"`
	QPState         uint32 `json:"qp_state"`
	LID             uint32 `json:"lid"`
	SMLID           uint32 `json:"sm_lid"`
	PortState       uint32 `json:"port_state"`
	ActiveMTU       uint32 `json:"active_mtu"`
	GIDIndex        uint32 `json:"gid_index"`
	LinkLayer       uint32 `json:"link_layer"`
	GID             string `json:"gid"`
	KernelEnabled   bool   `json:"kernel_enabled"`
	EndpointReady   bool   `json:"endpoint_ready"`
	QPConnected     bool   `json:"qp_connected"`
	UnsafeGlobalKey bool   `json:"unsafe_global_rkey"`
}

type VolumeRdmaRemoteInfo struct {
	ABIVersion uint32    `json:"abi_version"`
	Flags      uint32    `json:"flags"`
	QPN        uint32    `json:"qpn"`
	LID        uint32    `json:"lid"`
	PSN        uint32    `json:"psn"`
	Port       uint32    `json:"port"`
	GIDIndex   uint32    `json:"gid_index"`
	SL         uint32    `json:"sl"`
	GID        [16]byte  `json:"gid"`
	Reserved   [8]uint64 `json:"reserved"`
}

type volumeRdmaNativeStatusResponse struct {
	ReadExporterConfigured bool   `json:"read_exporter_configured"`
	EndpointConfigured     bool   `json:"endpoint_configured"`
	ABIVersion             uint32 `json:"abi_version"`
	StatusPath             string `json:"status_path"`
	LocalPath              string `json:"local_path"`
	ConnectPath            string `json:"connect_path"`
	ReadDescPath           string `json:"read_desc_path"`
	ReleaseDescPath        string `json:"release_desc_path"`
	RequesterLocalPath     string `json:"requester_local_path"`
	RequesterConnectPath   string `json:"requester_connect_path"`
	WritePath              string `json:"write_path"`
	WriteDescPath          string `json:"write_desc_path"`
	WriteCommitPath        string `json:"write_commit_path"`
	WriteAbortPath         string `json:"write_abort_path"`
}

func (e VolumeRdmaEndpointInfo) ReadyForConnect() bool {
	return e.ABIVersion == VolumeRdmaABIVersion &&
		e.KernelEnabled &&
		e.EndpointReady &&
		e.QPNum != 0 &&
		e.PSN <= 0x00ffffff &&
		e.LID != 0
}

func (e VolumeRdmaEndpointInfo) RemoteInfo(serviceLevel uint32) (VolumeRdmaRemoteInfo, error) {
	var remote VolumeRdmaRemoteInfo
	if serviceLevel > 15 {
		return remote, fmt.Errorf("RDMA service level must be 0..15")
	}
	if !e.ReadyForConnect() {
		return remote, fmt.Errorf("RDMA endpoint is not ready: qpn=%d lid=%d flags=0x%x ready=%v enabled=%v", e.QPNum, e.LID, e.Flags, e.EndpointReady, e.KernelEnabled)
	}
	remote = VolumeRdmaRemoteInfo{
		ABIVersion: VolumeRdmaABIVersion,
		QPN:        e.QPNum,
		LID:        e.LID,
		PSN:        e.PSN,
		Port:       e.Port,
		GIDIndex:   e.GIDIndex,
		SL:         serviceLevel,
	}
	if gid, ok := decodeVolumeRdmaGIDHex(e.GID); ok {
		remote.GID = gid
		remote.Flags |= VolumeRdmaRemoteFGIDValid
	}
	if e.LinkLayer == VolumeRdmaLinkEthernet {
		remote.Flags |= VolumeRdmaRemoteFGRHRequired
	}
	return remote, nil
}

func (vs *VolumeServer) SetRdmaEndpoint(endpoint VolumeRdmaEndpoint) {
	if vs == nil {
		return
	}
	vs.rdmaEndpoint = endpoint
}

func (vs *VolumeServer) volumeRdmaStatusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJsonQuiet(w, r, http.StatusOK, volumeRdmaNativeStatusResponse{
		ReadExporterConfigured: vs != nil && vs.rdmaReadExporter != nil,
		EndpointConfigured:     vs != nil && vs.rdmaEndpoint != nil,
		ABIVersion:             VolumeRdmaABIVersion,
		StatusPath:             VolumeRdmaNativeStatusPath,
		LocalPath:              VolumeRdmaNativeLocalPath,
		ConnectPath:            VolumeRdmaNativeConnectPath,
		ReadDescPath:           VolumeRdmaNativeReadDescPath,
		ReleaseDescPath:        VolumeRdmaNativeReleaseDescPath,
		RequesterLocalPath:     VolumeRdmaNativeRequesterLocalPath,
		RequesterConnectPath:   VolumeRdmaNativeRequesterConnectPath,
		WritePath:              VolumeRdmaNativeWritePath,
		WriteDescPath:          VolumeRdmaNativeWriteDescPath,
		WriteCommitPath:        VolumeRdmaNativeWriteCommitPath,
		WriteAbortPath:         VolumeRdmaNativeWriteAbortPath,
	})
}

func (vs *VolumeServer) volumeRdmaLocalHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if vs == nil || vs.rdmaEndpoint == nil {
		http.Error(w, "native RDMA endpoint is not configured", http.StatusNotImplemented)
		return
	}
	connectionID, err := parseVolumeRdmaConnectionID(r)
	if err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	var local VolumeRdmaEndpointInfo
	if endpoint, ok := vs.rdmaEndpoint.(VolumeRdmaConnectionEndpoint); ok {
		local, connectionID, err = endpoint.LocalEndpointFor(r.Context(), connectionID)
		if connectionID != 0 {
			local.ConnectionID = connectionID
		}
	} else {
		local, err = vs.rdmaEndpoint.LocalEndpoint(r.Context())
	}
	if err != nil {
		writeJsonError(w, r, http.StatusServiceUnavailable, err)
		return
	}
	if !local.ReadyForConnect() {
		writeJsonError(w, r, http.StatusServiceUnavailable, fmt.Errorf("native RDMA endpoint is not ready"))
		return
	}
	writeJsonQuiet(w, r, http.StatusOK, local)
}

func (vs *VolumeServer) volumeRdmaConnectHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if vs == nil || vs.rdmaEndpoint == nil {
		http.Error(w, "native RDMA endpoint is not configured", http.StatusNotImplemented)
		return
	}
	var peer VolumeRdmaEndpointInfo
	if err := json.NewDecoder(r.Body).Decode(&peer); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	serviceLevel, err := parseVolumeRdmaServiceLevel(r)
	if err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	remote, err := peer.RemoteInfo(serviceLevel)
	if err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	connectionID, err := parseVolumeRdmaConnectionID(r)
	if err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	if endpoint, ok := vs.rdmaEndpoint.(VolumeRdmaConnectionEndpoint); ok {
		err = endpoint.ConnectEndpointFor(r.Context(), connectionID, remote)
	} else {
		err = vs.rdmaEndpoint.ConnectEndpoint(r.Context(), remote)
	}
	if err != nil {
		writeJsonError(w, r, http.StatusServiceUnavailable, err)
		return
	}
	writeJsonQuiet(w, r, http.StatusOK, map[string]bool{"connected": true})
}

func (vs *VolumeServer) volumeRdmaRequesterLocalHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	requester, ok := vs.rdmaRequesterEndpoint()
	if !ok {
		http.Error(w, "native RDMA requester endpoint is not configured", http.StatusNotImplemented)
		return
	}
	connectionID, err := parseVolumeRdmaConnectionID(r)
	if err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	local, connectionID, err := requester.RequesterLocalEndpointFor(r.Context(), connectionID)
	if connectionID != 0 {
		local.ConnectionID = connectionID
	}
	if err != nil {
		writeJsonError(w, r, http.StatusServiceUnavailable, err)
		return
	}
	if !local.ReadyForConnect() {
		writeJsonError(w, r, http.StatusServiceUnavailable, fmt.Errorf("native RDMA requester endpoint is not ready"))
		return
	}
	writeJsonQuiet(w, r, http.StatusOK, local)
}

func (vs *VolumeServer) volumeRdmaRequesterConnectHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	requester, ok := vs.rdmaRequesterEndpoint()
	if !ok {
		http.Error(w, "native RDMA requester endpoint is not configured", http.StatusNotImplemented)
		return
	}
	var peer VolumeRdmaEndpointInfo
	if err := json.NewDecoder(r.Body).Decode(&peer); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	serviceLevel, err := parseVolumeRdmaServiceLevel(r)
	if err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	remote, err := peer.RemoteInfo(serviceLevel)
	if err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	connectionID, err := parseVolumeRdmaConnectionID(r)
	if err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	if err := requester.RequesterConnectEndpointFor(r.Context(), connectionID, remote); err != nil {
		writeJsonError(w, r, http.StatusServiceUnavailable, err)
		return
	}
	writeJsonQuiet(w, r, http.StatusOK, map[string]bool{"connected": true})
}

func (vs *VolumeServer) rdmaRequesterEndpoint() (VolumeRdmaRequesterEndpoint, bool) {
	if vs == nil || vs.rdmaEndpoint == nil {
		return nil, false
	}
	requester, ok := vs.rdmaEndpoint.(VolumeRdmaRequesterEndpoint)
	return requester, ok
}

func parseVolumeRdmaConnectionID(r *http.Request) (uint64, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("connection_id"))
	if raw == "" {
		return 0, nil
	}
	connectionID, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("connection_id must be an unsigned integer")
	}
	return connectionID, nil
}

func parseVolumeRdmaServiceLevel(r *http.Request) (uint32, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("sl"))
	if raw == "" {
		return 0, nil
	}
	sl, err := strconv.ParseUint(raw, 10, 32)
	if err != nil || sl > 15 {
		return 0, fmt.Errorf("RDMA service level must be 0..15")
	}
	return uint32(sl), nil
}

func decodeVolumeRdmaGIDHex(raw string) ([16]byte, bool) {
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
