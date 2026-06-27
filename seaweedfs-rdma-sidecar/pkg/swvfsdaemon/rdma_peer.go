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
	RDMAPeerLocalPath    = "/rdma/local"
	RDMAPeerConnectPath  = "/rdma/connect"
	RDMAPeerReadDescPath = "/rdma/read-desc"
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
	Control    RDMAPeerConnectorControl
	ReadStager RDMAReadDescriptorStager
}

func (s *RDMAPeerControlServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(RDMAPeerLocalPath, s.handleLocal)
	mux.HandleFunc(RDMAPeerConnectPath, s.handleConnect)
	mux.HandleFunc(RDMAPeerReadDescPath, s.handleReadDesc)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func (s *RDMAPeerControlServer) handleLocal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	info, err := s.Control.GetLocal()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, RDMALocalEndpointFromInfo(info))
}

func (s *RDMAPeerControlServer) handleConnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var endpoint RDMALocalEndpoint
	if err := json.NewDecoder(r.Body).Decode(&endpoint); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	sl := uint32(0)
	if raw := r.URL.Query().Get("sl"); raw != "" {
		if _, err := fmt.Sscanf(raw, "%d", &sl); err != nil {
			http.Error(w, "invalid service level", http.StatusBadRequest)
			return
		}
	}
	remote, err := endpoint.RemoteInfo(sl)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.Control.Connect(remote); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, map[string]any{"connected": true})
}

type RDMAPeerReadDescRequest struct {
	Path   string `json:"path"`
	Offset uint64 `json:"offset"`
	Size   uint64 `json:"size"`
}

type RDMAPeerReadDescResponse struct {
	Desc swvfsproto.RDMADataDesc `json:"desc"`
	Attr *swvfsproto.Attr        `json:"attr,omitempty"`
}

func (s *RDMAPeerControlServer) handleReadDesc(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.ReadStager == nil {
		http.Error(w, "rdma read descriptor staging is not configured", http.StatusNotImplemented)
		return
	}
	var req RDMAPeerReadDescRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Path == "" {
		http.Error(w, "path is required", http.StatusBadRequest)
		return
	}
	desc, attr, err := s.ReadStager.StageReadRDMA(r.Context(), req.Path, req.Offset, req.Size)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if desc == nil {
		http.Error(w, "rdma read descriptor stager returned no descriptor", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, RDMAPeerReadDescResponse{Desc: *desc, Attr: attr})
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

func PostRDMAPeerReadDesc(ctx context.Context, client *http.Client, rawURL string, path string, offset, size uint64) (*swvfsproto.RDMADataDesc, *swvfsproto.Attr, error) {
	reqURL, err := normalizeRDMAPeerURL(rawURL, RDMAPeerReadDescPath)
	if err != nil {
		return nil, nil, err
	}
	body, err := json.Marshal(RDMAPeerReadDescRequest{
		Path:   path,
		Offset: offset,
		Size:   size,
	})
	if err != nil {
		return nil, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient(client).Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, nil, fmt.Errorf("POST %s returned %s", reqURL, resp.Status)
	}
	var out RDMAPeerReadDescResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, nil, err
	}
	return &out.Desc, out.Attr, nil
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
	if u.Path == "" || u.Path == "/" || u.Path == RDMAPeerLocalPath || u.Path == RDMAPeerConnectPath {
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
