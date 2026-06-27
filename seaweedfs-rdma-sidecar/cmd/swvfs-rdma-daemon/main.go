package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"seaweedfs-rdma-sidecar/pkg/seaweedfs"
	"seaweedfs-rdma-sidecar/pkg/swvfsdaemon"
	"seaweedfs-rdma-sidecar/pkg/swvfsfiler"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultRDMAMinSize = 8 << 20

var (
	devPath           string
	filerAddresses    string
	engineSocket      string
	enableReadRDMA    bool
	enableWriteRDMA   bool
	enablePayloadRDMA bool
	readRDMAMinSize   uint64
	writeRDMAMinSize  uint64
	forceRDMA         bool
	fallbackOnError   bool
	rdmaControlListen string
	rdmaPeerEndpoints string
	rdmaPeerMinCount  int
	rdmaPeerSL        uint32
	rdmaPeerInterval  time.Duration
	rdmaPeerTimeout   time.Duration
	maxConnections    int
	timeout           time.Duration
	collection        string
	replication       string
	dataCenter        string
	diskType          string
	debug             bool
)

func main() {
	root := &cobra.Command{
		Use:   "swvfs-rdma-daemon",
		Short: "Experimental RDMA-aware userspace daemon for seaweedvfs",
		Long: `Experimental replacement daemon for seaweedvfs. It speaks the
/dev/seaweedvfs ABI, serves basic metadata plus READ and WRITE requests through
the SeaweedFS filer, and chooses the RDMA data plane when the kernel request
carries RDMA preference hints.`,
		RunE: run,
	}
	root.Flags().StringVar(&devPath, "dev", "/dev/seaweedvfs", "seaweedvfs character device")
	root.Flags().StringVar(&filerAddresses, "filer", "127.0.0.1:8888", "comma-separated filer HTTP addresses; use host:http.grpc for an explicit gRPC port")
	root.Flags().StringVar(&engineSocket, "engine-socket", "/tmp/rdma-engine.sock", "RDMA engine Unix socket")
	root.Flags().BoolVar(&enableReadRDMA, "enable-read-rdma", false, "prefer RDMA for READ requests carrying the kernel RDMA hint")
	root.Flags().BoolVar(&enableWriteRDMA, "enable-write-rdma", false, "prefer RDMA for WRITE requests carrying the kernel RDMA hint")
	root.Flags().BoolVar(&enablePayloadRDMA, "enable-payload-rdma", false, "enable real payload RDMA in the SeaweedFS data plane")
	root.Flags().Uint64Var(&readRDMAMinSize, "rdma-read-min-size", defaultRDMAMinSize, "minimum READ size in bytes before RDMA is considered; set 0 to allow all hinted reads")
	root.Flags().Uint64Var(&writeRDMAMinSize, "rdma-write-min-size", defaultRDMAMinSize, "minimum WRITE size in bytes before RDMA is considered; set 0 to allow all hinted writes")
	root.Flags().BoolVar(&forceRDMA, "force-rdma", false, "prefer RDMA for READ and WRITE even when the kernel request has no RDMA hint")
	root.Flags().BoolVar(&fallbackOnError, "fallback-on-error", true, "fall back to TCP/HTTP when RDMA is unavailable")
	root.Flags().StringVar(&rdmaControlListen, "rdma-control-listen", "", "listen address for kernel RDMA peer-control HTTP API; empty disables it")
	root.Flags().StringVar(&rdmaPeerEndpoints, "rdma-peer-endpoints", "", "comma-separated peer-control URLs used for automatic kernel RDMA QP handshake")
	root.Flags().IntVar(&rdmaPeerMinCount, "rdma-peer-min-count", 0, "minimum total ready RDMA peer-control endpoints, including self, before selecting a deterministic peer")
	root.Flags().Uint32Var(&rdmaPeerSL, "rdma-peer-service-level", 0, "InfiniBand service level for automatic peer connections")
	root.Flags().DurationVar(&rdmaPeerInterval, "rdma-peer-connect-interval", 5*time.Second, "retry interval for automatic kernel RDMA peer connection")
	root.Flags().DurationVar(&rdmaPeerTimeout, "rdma-peer-connect-timeout", 5*time.Second, "per-attempt timeout for automatic kernel RDMA peer connection")
	root.Flags().IntVar(&maxConnections, "max-connections", 8, "maximum RDMA engine IPC connections")
	root.Flags().DurationVar(&timeout, "timeout", 30*time.Second, "SeaweedFS/RDMA operation timeout")
	root.Flags().StringVar(&collection, "collection", "", "SeaweedFS collection for new writes")
	root.Flags().StringVar(&replication, "replication", "", "SeaweedFS replication for new writes")
	root.Flags().StringVar(&dataCenter, "data-center", "", "preferred SeaweedFS data center for new writes")
	root.Flags().StringVar(&diskType, "disk-type", "", "SeaweedFS disk type for new writes")
	root.Flags().BoolVar(&debug, "debug", false, "enable debug logging")

	if err := root.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {
	logger := logrus.New()
	if debug {
		logger.SetLevel(logrus.DebugLevel)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	filers := pb.ServerAddresses(filerAddresses).ToAddresses()
	store := &swvfsfiler.GRPCStore{
		Filers:      filers,
		DialOption:  dialOption,
		Collection:  collection,
		Replication: replication,
		DataCenter:  dataCenter,
		DiskType:    diskType,
	}

	rdmaClient, err := newSeaweedClient(logger, true)
	if err != nil {
		return err
	}
	if enableReadRDMA || enableWriteRDMA {
		if err := startClientWithRetry(ctx, rdmaClient, logger); err != nil {
			return err
		}
		defer rdmaClient.Stop()
	}

	fallbackClient, err := newSeaweedClient(logger, false)
	if err != nil {
		return err
	}
	defer fallbackClient.Stop()

	router := &swvfsdaemon.Router{
		RDMA:             &swvfsfiler.SeaweedNeedlePlane{Client: rdmaClient},
		Fallback:         &swvfsfiler.SeaweedNeedlePlane{Client: fallbackClient},
		EnableReadRDMA:   enableReadRDMA,
		EnableWriteRDMA:  enableWriteRDMA,
		ReadRDMAMinSize:  readRDMAMinSize,
		WriteRDMAMinSize: writeRDMAMinSize,
		FallbackOnError:  fallbackOnError,
	}
	if err := swvfsdaemon.RequireRouter(router); err != nil {
		return err
	}

	file, err := os.OpenFile(devPath, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open %s: %w", devPath, err)
	}
	defer file.Close()
	rdmaControl := swvfsdaemon.NewRDMAControl(file)
	peerList := splitCSV(rdmaPeerEndpoints)
	backend := &swvfsfiler.Backend{
		Store:  store,
		Router: router,
	}
	if len(peerList) > 0 {
		backend.ReadDescriptorBackend = &swvfsdaemon.RemoteRDMAReadDescriptorClient{
			Control: rdmaControl,
			Peers:   peerList,
			Timeout: rdmaPeerTimeout,
		}
	}
	readStager := &swvfsdaemon.KernelMRReadStager{
		Control: rdmaControl,
		Reader:  backend,
	}

	logger.WithFields(logrus.Fields{
		"dev":               devPath,
		"filers":            filerAddresses,
		"read_rdma":         enableReadRDMA,
		"write_rdma":        enableWriteRDMA,
		"payload_rdma":      enablePayloadRDMA,
		"rdma_read_min":     readRDMAMinSize,
		"rdma_write_min":    writeRDMAMinSize,
		"force_rdma":        forceRDMA,
		"fallback_on_error": fallbackOnError,
		"rdma_control":      rdmaControlListen,
		"rdma_peers":        rdmaPeerEndpoints,
	}).Info("starting swvfs RDMA daemon")

	if rdmaControlListen != "" {
		server := &http.Server{
			Addr:    rdmaControlListen,
			Handler: (&swvfsdaemon.RDMAPeerControlServer{Control: rdmaControl, ReadStager: readStager}).Handler(),
		}
		go func() {
			logger.WithField("addr", rdmaControlListen).Info("starting RDMA peer-control server")
			if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.WithError(err).Warn("RDMA peer-control server stopped")
			}
		}()
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdownCtx)
		}()
	}

	if len(peerList) > 0 {
		go runRDMAPeerConnector(ctx, rdmaControl, peerList, logger)
	}

	handler := &swvfsdaemon.Handler{
		ForceReadRDMA:  forceRDMA,
		ForceWriteRDMA: forceRDMA,
		Backend:        backend,
	}
	device := &swvfsdaemon.LegacyDevice{RW: file, Handler: handler}
	err = device.Serve(ctx)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func runRDMAPeerConnector(ctx context.Context, control *swvfsdaemon.RDMAControl, peers []string, logger *logrus.Logger) {
	if rdmaPeerInterval <= 0 {
		rdmaPeerInterval = 5 * time.Second
	}
	for {
		err := connectRDMAPeersOnce(ctx, control, peers, logger)
		if err == nil {
			return
		}
		if errors.Is(err, swvfsdaemon.ErrRDMAPeerUnpaired) {
			logger.WithError(err).Info("RDMA peer handshake skipped")
			return
		}
		logger.WithError(err).Warn("RDMA peer handshake not ready; retrying")
		select {
		case <-ctx.Done():
			return
		case <-time.After(rdmaPeerInterval):
		}
	}
}

func connectRDMAPeersOnce(ctx context.Context, control *swvfsdaemon.RDMAControl, peers []string, logger *logrus.Logger) error {
	timeout := rdmaPeerTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	localInfo, err := control.GetLocal()
	if err != nil {
		return err
	}
	local := swvfsdaemon.RDMALocalEndpointFromInfo(localInfo)
	if !local.ReadyForConnect() {
		return fmt.Errorf("local RDMA endpoint is not ready: qpn=%d lid=%d flags=0x%x", local.QPNum, local.LID, local.Flags)
	}

	client := &http.Client{Timeout: timeout}
	urls := swvfsdaemon.ExpandRDMAPeerURLs(attemptCtx, peers)
	type fetchedPeer struct {
		URL      string
		Endpoint swvfsdaemon.RDMALocalEndpoint
	}
	fetched := make([]fetchedPeer, 0, len(urls))
	for _, peerURL := range urls {
		endpoint, err := swvfsdaemon.FetchRDMAPeerEndpoint(attemptCtx, client, peerURL)
		if err != nil {
			logger.WithError(err).WithField("peer", peerURL).Debug("RDMA peer endpoint fetch failed")
			continue
		}
		if endpoint.SamePeer(local) {
			continue
		}
		fetched = append(fetched, fetchedPeer{URL: peerURL, Endpoint: endpoint})
	}
	if len(fetched) == 0 {
		return fmt.Errorf("no RDMA peer endpoints were reachable")
	}
	if rdmaPeerMinCount > 0 && len(fetched)+1 < rdmaPeerMinCount {
		return fmt.Errorf("only %d/%d RDMA peer endpoints are ready", len(fetched)+1, rdmaPeerMinCount)
	}

	endpoints := make([]swvfsdaemon.RDMALocalEndpoint, 0, len(fetched))
	for _, peer := range fetched {
		endpoints = append(endpoints, peer.Endpoint)
	}
	selected, ok := swvfsdaemon.SelectRDMAPairedPeer(local, endpoints)
	if !ok {
		return fmt.Errorf("%w for local qpn=%d lid=%d", swvfsdaemon.ErrRDMAPeerUnpaired, local.QPNum, local.LID)
	}
	remote, err := selected.RemoteInfo(rdmaPeerSL)
	if err != nil {
		return err
	}
	for _, peer := range fetched {
		if !peer.Endpoint.SamePeer(selected) {
			continue
		}
		if err := swvfsdaemon.PostRDMAPeerConnect(attemptCtx, client, peer.URL, local, rdmaPeerSL); err != nil {
			return err
		}
		if err := control.Connect(remote); err != nil {
			return err
		}
		logger.WithFields(logrus.Fields{
			"local_qpn":  local.QPNum,
			"local_lid":  local.LID,
			"peer_qpn":   selected.QPNum,
			"peer_lid":   selected.LID,
			"peer_url":   peer.URL,
			"service_lv": rdmaPeerSL,
		}).Info("kernel RDMA peer handshake completed")
		return nil
	}
	return fmt.Errorf("selected RDMA peer disappeared before connect post")
}

func newSeaweedClient(logger *logrus.Logger, enabled bool) (*seaweedfs.SeaweedFSRDMAClient, error) {
	return seaweedfs.NewSeaweedFSRDMAClient(&seaweedfs.Config{
		RDMASocketPath:    engineSocket,
		Enabled:           enabled,
		EnablePayloadRDMA: enablePayloadRDMA,
		DefaultTimeout:    timeout,
		Logger:            logger,
		EnablePooling:     true,
		MaxConnections:    maxConnections,
	})
}

func startClientWithRetry(ctx context.Context, client *seaweedfs.SeaweedFSRDMAClient, logger *logrus.Logger) error {
	var lastErr error
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := client.Start(attemptCtx)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return lastErr
			}
			return ctx.Err()
		case <-time.After(time.Second):
			logger.WithError(err).Warn("RDMA engine is not ready yet; retrying")
		}
	}
}
