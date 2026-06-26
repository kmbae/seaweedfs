package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
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
	}).Info("starting swvfs RDMA daemon")

	handler := &swvfsdaemon.Handler{
		ForceReadRDMA:  forceRDMA,
		ForceWriteRDMA: forceRDMA,
		Backend: &swvfsfiler.Backend{
			Store:  store,
			Router: router,
		},
	}
	device := &swvfsdaemon.LegacyDevice{RW: file, Handler: handler}
	err = device.Serve(ctx)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
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
