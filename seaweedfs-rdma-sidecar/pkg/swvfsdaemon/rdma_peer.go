package swvfsdaemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"seaweedfs-rdma-sidecar/pkg/swvfsproto"
)

const (
	RDMAPeerLocalPath       = "/rdma/local"
	RDMAPeerConnectPath     = "/rdma/connect"
	RDMAPeerReadDescPath    = "/rdma/read-desc"
	RDMAPeerReleaseDescPath = "/rdma/release-desc"
	RDMAPeerWritePrepare    = "/rdma/write-prepare"
	RDMAPeerWriteCommit     = "/rdma/write-commit"
	RDMAPeerWriteAbort      = "/rdma/write-abort"
	RDMAPeerWriteFlush      = "/rdma/write-flush"
)

var ErrRDMAPeerUnpaired = errors.New("no deterministic RDMA pair selected")

type RDMALocalProvider interface {
	GetLocal() (swvfsproto.RDMALocalInfo, error)
}

type RDMAPeerConnectorControl interface {
	RDMALocalProvider
	Connect(swvfsproto.RDMARemoteInfo) error
}

type RDMALocalEndpoint struct {
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

func RDMALocalEndpointFromInfo(info swvfsproto.RDMALocalInfo) RDMALocalEndpoint {
	return RDMALocalEndpoint{
		ABIVersion:      info.ABIVersion,
		Flags:           info.Flags,
		Device:          info.DeviceName(),
		Port:            info.Port,
		QPNum:           info.QPNum,
		PSN:             info.PSN,
		QPState:         info.QPState,
		LID:             info.LID,
		SMLID:           info.SMLID,
		PortState:       info.PortState,
		ActiveMTU:       info.ActiveMTU,
		GIDIndex:        info.GIDIndex,
		LinkLayer:       info.LinkLayer,
		GID:             info.GIDHex(),
		KernelEnabled:   info.KernelEnabled(),
		EndpointReady:   info.EndpointReady(),
		QPConnected:     info.Connected(),
		UnsafeGlobalKey: info.Flags&swvfsproto.RDMAFUnsafeGlobalKey != 0,
	}
}

func (e RDMALocalEndpoint) ReadyForConnect() bool {
	return e.ABIVersion == swvfsproto.RDMAABIVersion &&
		e.KernelEnabled &&
		e.EndpointReady &&
		e.QPNum != 0 &&
		e.PSN <= 0x00ffffff &&
		e.LID != 0
}

func (e RDMALocalEndpoint) PeerKey() string {
	return fmt.Sprintf("%08x:%08x:%08x:%s", e.LID, e.QPNum, e.PSN, strings.ToLower(e.GID))
}

func (e RDMALocalEndpoint) StablePeerKey() string {
	return fmt.Sprintf("%08x:%08x:%s", e.LID, e.Port, strings.ToLower(e.GID))
}

func (e RDMALocalEndpoint) SamePeer(other RDMALocalEndpoint) bool {
	return e.PeerKey() == other.PeerKey()
}

func (e RDMALocalEndpoint) RemoteInfo(serviceLevel uint32) (swvfsproto.RDMARemoteInfo, error) {
	var remote swvfsproto.RDMARemoteInfo
	if serviceLevel > 15 {
		return remote, fmt.Errorf("RDMA service level must be 0..15")
	}
	if !e.ReadyForConnect() {
		return remote, fmt.Errorf("remote RDMA endpoint is not ready: qpn=%d lid=%d flags=0x%x", e.QPNum, e.LID, e.Flags)
	}
	remote = swvfsproto.RDMARemoteInfo{
		ABIVersion: swvfsproto.RDMAABIVersion,
		QPN:        e.QPNum,
		LID:        e.LID,
		PSN:        e.PSN,
		Port:       e.Port,
		GIDIndex:   e.GIDIndex,
		SL:         serviceLevel,
	}
	if gid, ok := swvfsproto.DecodeGIDHex(e.GID); ok {
		remote.GID = gid
		remote.Flags |= swvfsproto.RDMARemoteFGIDValid
	}
	if e.LinkLayer == swvfsproto.RDMALinkEthernet {
		remote.Flags |= swvfsproto.RDMARemoteFGRHRequired
	}
	return remote, nil
}

type RDMAPeerControlServer struct {
	Control     RDMAPeerConnectorControl
	ReadStager  RDMAReadDescriptorStager
	WriteStager RDMAWriteDescriptorBackend
	Stats       *Stats
}

func (s *RDMAPeerControlServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(RDMAPeerLocalPath, s.handleLocal)
	mux.HandleFunc(RDMAPeerConnectPath, s.handleConnect)
	mux.HandleFunc(RDMAPeerReadDescPath, s.handleReadDesc)
	mux.HandleFunc(RDMAPeerReleaseDescPath, s.handleReleaseDesc)
	mux.HandleFunc(RDMAPeerWritePrepare, s.handleWritePrepare)
	mux.HandleFunc(RDMAPeerWriteCommit, s.handleWriteCommit)
	mux.HandleFunc(RDMAPeerWriteAbort, s.handleWriteAbort)
	mux.HandleFunc(RDMAPeerWriteFlush, s.handleWriteFlush)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	if s != nil && s.Stats != nil {
		mux.Handle("/metrics", s.Stats)
	}
	return mux
}

func (s *RDMAPeerControlServer) handleLocal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.Stats.Inc("peer_control_local_requests")
	info, err := s.Control.GetLocal()
	if err != nil {
		s.Stats.Inc("peer_control_local_errors")
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	s.Stats.Inc("peer_control_local_success")
	writeJSON(w, RDMALocalEndpointFromInfo(info))
}

func (s *RDMAPeerControlServer) handleConnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.Stats.Inc("peer_control_connect_requests")
	var endpoint RDMALocalEndpoint
	if err := json.NewDecoder(r.Body).Decode(&endpoint); err != nil {
		s.Stats.Inc("peer_control_connect_bad_request")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	sl := uint32(0)
	if raw := r.URL.Query().Get("sl"); raw != "" {
		if _, err := fmt.Sscanf(raw, "%d", &sl); err != nil {
			s.Stats.Inc("peer_control_connect_bad_request")
			http.Error(w, "invalid service level", http.StatusBadRequest)
			return
		}
	}
	remote, err := endpoint.RemoteInfo(sl)
	if err != nil {
		s.Stats.Inc("peer_control_connect_bad_request")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.Control.Connect(remote); err != nil {
		s.Stats.Inc("peer_control_connect_errors")
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	s.Stats.Inc("peer_control_connect_success")
	writeJSON(w, map[string]any{"connected": true})
}

type RDMAPeerReadDescRequest struct {
	Path   string `json:"path"`
	Offset uint64 `json:"offset"`
	Size   uint64 `json:"size"`
}

type RDMAPeerReadDescResponse struct {
	Desc      swvfsproto.RDMADataDesc `json:"desc"`
	Attr      *swvfsproto.Attr        `json:"attr,omitempty"`
	SessionID uint64                  `json:"session_id,omitempty"`
}

type RDMAPeerReleaseDescRequest struct {
	SessionID uint64 `json:"session_id"`
}

type RDMAPeerWritePrepareRequest struct {
	Path   string `json:"path"`
	Offset uint64 `json:"offset"`
	Size   uint64 `json:"size"`
}

type RDMAPeerWritePrepareResponse struct {
	Desc      swvfsproto.RDMADataDesc `json:"desc"`
	Attr      *swvfsproto.Attr        `json:"attr,omitempty"`
	SessionID uint64                  `json:"session_id,omitempty"`
}

type RDMAPeerWriteCommitRequest struct {
	SessionID uint64 `json:"session_id"`
	Path      string `json:"path"`
	Offset    uint64 `json:"offset"`
	Size      uint64 `json:"size"`
}

type RDMAPeerWriteCommitResponse struct {
	Attr *swvfsproto.Attr `json:"attr,omitempty"`
}

type RDMAPeerWriteFlushRequest struct {
	Path string `json:"path"`
}

type RDMAPeerWriteFlushResponse struct {
	Attr *swvfsproto.Attr `json:"attr,omitempty"`
}

func (s *RDMAPeerControlServer) handleReadDesc(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.Stats.Inc("peer_control_read_desc_requests")
	if s.ReadStager == nil {
		s.Stats.Inc("peer_control_read_desc_not_configured")
		http.Error(w, "rdma read descriptor staging is not configured", http.StatusNotImplemented)
		return
	}
	var req RDMAPeerReadDescRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.Stats.Inc("peer_control_read_desc_bad_request")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Path == "" {
		s.Stats.Inc("peer_control_read_desc_bad_request")
		http.Error(w, "path is required", http.StatusBadRequest)
		return
	}
	lease, err := s.ReadStager.StageReadRDMA(r.Context(), req.Path, req.Offset, req.Size)
	if err != nil {
		s.Stats.Inc("peer_control_read_desc_errors")
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if lease == nil {
		s.Stats.Inc("peer_control_read_desc_errors")
		http.Error(w, "rdma read descriptor stager returned no descriptor", http.StatusServiceUnavailable)
		return
	}
	s.Stats.Inc("peer_control_read_desc_success")
	s.Stats.Add("peer_control_read_desc_bytes", uint64(lease.Desc.Length))
	writeJSON(w, RDMAPeerReadDescResponse{Desc: lease.Desc, Attr: lease.Attr, SessionID: lease.SessionID})
}

func (s *RDMAPeerControlServer) handleReleaseDesc(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.Stats.Inc("peer_control_release_desc_requests")
	if s.ReadStager == nil {
		s.Stats.Inc("peer_control_release_desc_not_configured")
		http.Error(w, "rdma read descriptor staging is not configured", http.StatusNotImplemented)
		return
	}
	var req RDMAPeerReleaseDescRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.Stats.Inc("peer_control_release_desc_bad_request")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.SessionID == 0 {
		s.Stats.Inc("peer_control_release_desc_bad_request")
		http.Error(w, "session_id is required", http.StatusBadRequest)
		return
	}
	if err := s.ReadStager.ReleaseReadRDMA(r.Context(), req.SessionID); err != nil {
		s.Stats.Inc("peer_control_release_desc_errors")
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	s.Stats.Inc("peer_control_release_desc_success")
	writeJSON(w, map[string]any{"released": true})
}

func (s *RDMAPeerControlServer) handleWritePrepare(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.Stats.Inc("peer_control_write_prepare_requests")
	if s.WriteStager == nil {
		s.Stats.Inc("peer_control_write_prepare_not_configured")
		http.Error(w, "rdma write descriptor staging is not configured", http.StatusNotImplemented)
		return
	}
	var req RDMAPeerWritePrepareRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.Stats.Inc("peer_control_write_prepare_bad_request")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Path == "" || req.Size == 0 {
		s.Stats.Inc("peer_control_write_prepare_bad_request")
		http.Error(w, "path and size are required", http.StatusBadRequest)
		return
	}
	desc, attr, err := s.WriteStager.PrepareWriteRDMA(r.Context(), req.Path, req.Offset, req.Size)
	if err != nil {
		s.Stats.Inc("peer_control_write_prepare_errors")
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if desc == nil {
		s.Stats.Inc("peer_control_write_prepare_errors")
		http.Error(w, "rdma write descriptor stager returned no descriptor", http.StatusServiceUnavailable)
		return
	}
	s.Stats.Inc("peer_control_write_prepare_success")
	s.Stats.Add("peer_control_write_prepare_bytes", uint64(desc.Length))
	writeJSON(w, RDMAPeerWritePrepareResponse{Desc: *desc, Attr: attr, SessionID: desc.Reserved[0]})
}

func (s *RDMAPeerControlServer) handleWriteCommit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.Stats.Inc("peer_control_write_commit_requests")
	if s.WriteStager == nil {
		s.Stats.Inc("peer_control_write_commit_not_configured")
		http.Error(w, "rdma write descriptor staging is not configured", http.StatusNotImplemented)
		return
	}
	var req RDMAPeerWriteCommitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.Stats.Inc("peer_control_write_commit_bad_request")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Path == "" || req.Size == 0 {
		s.Stats.Inc("peer_control_write_commit_bad_request")
		http.Error(w, "path and size are required", http.StatusBadRequest)
		return
	}
	var (
		attr *swvfsproto.Attr
		err  error
	)
	if committer, ok := s.WriteStager.(interface {
		CommitWriteRDMASession(context.Context, uint64, string, uint64, uint64) (*swvfsproto.Attr, error)
	}); ok {
		attr, err = committer.CommitWriteRDMASession(r.Context(), req.SessionID, req.Path, req.Offset, req.Size)
	} else {
		attr, err = s.WriteStager.CommitWriteRDMA(r.Context(), req.Path, req.Offset, req.Size)
	}
	if err != nil {
		s.Stats.Inc("peer_control_write_commit_errors")
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	s.Stats.Inc("peer_control_write_commit_success")
	s.Stats.Add("peer_control_write_commit_bytes", req.Size)
	writeJSON(w, RDMAPeerWriteCommitResponse{Attr: attr})
}

func (s *RDMAPeerControlServer) handleWriteAbort(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.Stats.Inc("peer_control_write_abort_requests")
	if s.WriteStager == nil {
		s.Stats.Inc("peer_control_write_abort_not_configured")
		http.Error(w, "rdma write descriptor staging is not configured", http.StatusNotImplemented)
		return
	}
	var req RDMAPeerWriteCommitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.Stats.Inc("peer_control_write_abort_bad_request")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.SessionID == 0 {
		s.Stats.Inc("peer_control_write_abort_bad_request")
		http.Error(w, "session_id is required", http.StatusBadRequest)
		return
	}
	if aborter, ok := s.WriteStager.(interface {
		AbortWriteRDMASession(context.Context, uint64) error
	}); ok {
		if err := aborter.AbortWriteRDMASession(r.Context(), req.SessionID); err != nil {
			s.Stats.Inc("peer_control_write_abort_errors")
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
	}
	s.Stats.Inc("peer_control_write_abort_success")
	writeJSON(w, map[string]any{"aborted": true})
}

func (s *RDMAPeerControlServer) handleWriteFlush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.Stats.Inc("peer_control_write_flush_requests")
	if s.WriteStager == nil {
		s.Stats.Inc("peer_control_write_flush_not_configured")
		http.Error(w, "rdma write descriptor staging is not configured", http.StatusNotImplemented)
		return
	}
	var req RDMAPeerWriteFlushRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.Stats.Inc("peer_control_write_flush_bad_request")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Path == "" {
		s.Stats.Inc("peer_control_write_flush_bad_request")
		http.Error(w, "path is required", http.StatusBadRequest)
		return
	}
	flusher, ok := s.WriteStager.(interface {
		FlushFile(context.Context, string) (*swvfsproto.Attr, error)
	})
	if !ok {
		s.Stats.Inc("peer_control_write_flush_not_configured")
		http.Error(w, "rdma write descriptor flush is not configured", http.StatusNotImplemented)
		return
	}
	attr, err := flusher.FlushFile(r.Context(), req.Path)
	if err != nil {
		s.Stats.Inc("peer_control_write_flush_errors")
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	s.Stats.Inc("peer_control_write_flush_success")
	writeJSON(w, RDMAPeerWriteFlushResponse{Attr: attr})
}

func FetchRDMAPeerEndpoint(ctx context.Context, client *http.Client, rawURL string) (RDMALocalEndpoint, error) {
	var endpoint RDMALocalEndpoint
	reqURL, err := normalizeRDMAPeerURL(rawURL, RDMAPeerLocalPath)
	if err != nil {
		return endpoint, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return endpoint, err
	}
	resp, err := httpClient(client).Do(req)
	if err != nil {
		return endpoint, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return endpoint, fmt.Errorf("GET %s returned %s", reqURL, resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(&endpoint); err != nil {
		return endpoint, err
	}
	return endpoint, nil
}

func PostRDMAPeerConnect(ctx context.Context, client *http.Client, rawURL string, local RDMALocalEndpoint, serviceLevel uint32) error {
	reqURL, err := normalizeRDMAPeerURL(rawURL, RDMAPeerConnectPath)
	if err != nil {
		return err
	}
	if strings.Contains(reqURL, "?") {
		reqURL += fmt.Sprintf("&sl=%d", serviceLevel)
	} else {
		reqURL += fmt.Sprintf("?sl=%d", serviceLevel)
	}
	body, err := json.Marshal(local)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient(client).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("POST %s returned %s", reqURL, resp.Status)
	}
	return nil
}

func PostRDMAPeerReadDesc(ctx context.Context, client *http.Client, rawURL string, path string, offset, size uint64) (*swvfsproto.RDMADataDesc, *swvfsproto.Attr, uint64, error) {
	reqURL, err := normalizeRDMAPeerURL(rawURL, RDMAPeerReadDescPath)
	if err != nil {
		return nil, nil, 0, err
	}
	body, err := json.Marshal(RDMAPeerReadDescRequest{
		Path:   path,
		Offset: offset,
		Size:   size,
	})
	if err != nil {
		return nil, nil, 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(body))
	if err != nil {
		return nil, nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient(client).Do(req)
	if err != nil {
		return nil, nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, nil, 0, fmt.Errorf("POST %s returned %s", reqURL, resp.Status)
	}
	var out RDMAPeerReadDescResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, nil, 0, err
	}
	return &out.Desc, out.Attr, out.SessionID, nil
}

func PostRDMAPeerReleaseDesc(ctx context.Context, client *http.Client, rawURL string, sessionID uint64) error {
	reqURL, err := normalizeRDMAPeerURL(rawURL, RDMAPeerReleaseDescPath)
	if err != nil {
		return err
	}
	body, err := json.Marshal(RDMAPeerReleaseDescRequest{SessionID: sessionID})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient(client).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("POST %s returned %s", reqURL, resp.Status)
	}
	return nil
}

func PostRDMAPeerWritePrepare(ctx context.Context, client *http.Client, rawURL string, path string, offset, size uint64) (*swvfsproto.RDMADataDesc, *swvfsproto.Attr, uint64, error) {
	reqURL, err := normalizeRDMAPeerURL(rawURL, RDMAPeerWritePrepare)
	if err != nil {
		return nil, nil, 0, err
	}
	body, err := json.Marshal(RDMAPeerWritePrepareRequest{
		Path:   path,
		Offset: offset,
		Size:   size,
	})
	if err != nil {
		return nil, nil, 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(body))
	if err != nil {
		return nil, nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient(client).Do(req)
	if err != nil {
		return nil, nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, nil, 0, fmt.Errorf("POST %s returned %s", reqURL, resp.Status)
	}
	var out RDMAPeerWritePrepareResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, nil, 0, err
	}
	return &out.Desc, out.Attr, out.SessionID, nil
}

func PostRDMAPeerWriteCommit(ctx context.Context, client *http.Client, rawURL string, sessionID uint64, path string, offset, size uint64) (*swvfsproto.Attr, error) {
	reqURL, err := normalizeRDMAPeerURL(rawURL, RDMAPeerWriteCommit)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(RDMAPeerWriteCommitRequest{
		SessionID: sessionID,
		Path:      path,
		Offset:    offset,
		Size:      size,
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient(client).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("POST %s returned %s", reqURL, resp.Status)
	}
	var out RDMAPeerWriteCommitResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.Attr, nil
}

func PostRDMAPeerWriteAbort(ctx context.Context, client *http.Client, rawURL string, sessionID uint64) error {
	reqURL, err := normalizeRDMAPeerURL(rawURL, RDMAPeerWriteAbort)
	if err != nil {
		return err
	}
	body, err := json.Marshal(RDMAPeerWriteCommitRequest{SessionID: sessionID})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient(client).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("POST %s returned %s", reqURL, resp.Status)
	}
	return nil
}

func PostRDMAPeerWriteFlush(ctx context.Context, client *http.Client, rawURL string, path string) (*swvfsproto.Attr, error) {
	reqURL, err := normalizeRDMAPeerURL(rawURL, RDMAPeerWriteFlush)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(RDMAPeerWriteFlushRequest{Path: path})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient(client).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("POST %s returned %s", reqURL, resp.Status)
	}
	var out RDMAPeerWriteFlushResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.Attr, nil
}

func ExpandRDMAPeerURLs(ctx context.Context, rawEndpoints []string) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, raw := range rawEndpoints {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		expanded := expandOnePeerURL(ctx, raw)
		for _, item := range expanded {
			if _, ok := seen[item]; ok {
				continue
			}
			seen[item] = struct{}{}
			out = append(out, item)
		}
	}
	sort.Strings(out)
	return out
}

func SelectRDMAPairedPeer(local RDMALocalEndpoint, peers []RDMALocalEndpoint) (RDMALocalEndpoint, bool) {
	if !local.ReadyForConnect() {
		return RDMALocalEndpoint{}, false
	}
	all := []RDMALocalEndpoint{local}
	for _, peer := range peers {
		if peer.ReadyForConnect() && !peer.SamePeer(local) {
			all = append(all, peer)
		}
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i].PeerKey() < all[j].PeerKey()
	})
	for idx, peer := range all {
		if !peer.SamePeer(local) {
			continue
		}
		if idx%2 == 0 {
			if idx+1 < len(all) {
				return all[idx+1], true
			}
			return RDMALocalEndpoint{}, false
		}
		return all[idx-1], true
	}
	return RDMALocalEndpoint{}, false
}

func ShouldInitiateRDMAPeerConnect(local, peer RDMALocalEndpoint) bool {
	localKey := local.StablePeerKey()
	peerKey := peer.StablePeerKey()
	if localKey == peerKey {
		return local.PeerKey() < peer.PeerKey()
	}
	return localKey < peerKey
}

func normalizeRDMAPeerURL(raw string, defaultPath string) (string, error) {
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if u.Scheme == "" {
		u.Scheme = "http"
	}
	if u.Path == "" || u.Path == "/" || u.Path == RDMAPeerLocalPath ||
		u.Path == RDMAPeerConnectPath || u.Path == RDMAPeerReadDescPath ||
		u.Path == RDMAPeerReleaseDescPath {
		u.Path = defaultPath
	}
	return u.String(), nil
}

func expandOnePeerURL(ctx context.Context, raw string) []string {
	normalized, err := normalizeRDMAPeerURL(raw, RDMAPeerLocalPath)
	if err != nil {
		return []string{raw}
	}
	u, err := url.Parse(normalized)
	if err != nil {
		return []string{normalized}
	}
	host := u.Hostname()
	if host == "" || net.ParseIP(host) != nil {
		return []string{normalized}
	}
	ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
	if err != nil || len(ips) == 0 {
		return []string{normalized}
	}
	var out []string
	for _, ip := range ips {
		clone := *u
		if port := u.Port(); port != "" {
			clone.Host = net.JoinHostPort(ip.String(), port)
		} else {
			clone.Host = ip.String()
		}
		out = append(out, clone.String())
	}
	sort.Strings(out)
	return out
}

func httpClient(client *http.Client) *http.Client {
	if client != nil {
		return client
	}
	return &http.Client{Timeout: 5 * time.Second}
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
