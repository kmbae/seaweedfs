// Package main provides the main RDMA sidecar service that integrates with SeaweedFS
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"seaweedfs-rdma-sidecar/pkg/httpserver"
	"seaweedfs-rdma-sidecar/pkg/rdma"
	"seaweedfs-rdma-sidecar/pkg/remote"
	"seaweedfs-rdma-sidecar/pkg/seaweedfs"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	port                   int
	engineSocket           string
	nativeEngineSocket     string
	volumeServerURL        string
	volumeDataDir          string
	volumeIdxDir           string
	volumeCollection       string
	enableRDMA             bool
	enablePayloadRDMA      bool
	enableNativeVolumeRDMA bool
	enableZeroCopy         bool
	tempDir                string
	maxConnections         int
	nativeServiceLevel     uint32
	debug                  bool
	timeout                time.Duration
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "rdma-sidecar",
		Short: "SeaweedFS RDMA acceleration sidecar",
		Long: `RDMA sidecar that accelerates SeaweedFS read/write operations using a Rust RDMA engine.

Mount clients call GET /read with file_id, offset, size, and volume_server parameters.
Mount clients call POST /write with file_id and volume_server query params, body = raw data.
When the engine runs in mock mode, needle data is loaded from the local volume directory
(if configured) or via HTTP fallback to the volume server.`,
		RunE: runSidecar,
	}

	rootCmd.Flags().IntVarP(&port, "port", "p", 8081, "HTTP server port")
	rootCmd.Flags().StringVarP(&engineSocket, "engine-socket", "e", "/tmp/rdma-engine.sock", "Path to RDMA engine Unix socket")
	rootCmd.Flags().StringVar(&nativeEngineSocket, "native-engine-socket", "/tmp/volume-rdma-engine.sock", "Path to native verbs volume RDMA engine Unix socket")
	rootCmd.Flags().StringVarP(&volumeServerURL, "volume-server", "v", "http://127.0.0.1:8444", "Default SeaweedFS volume server URL for HTTP fallback")
	rootCmd.Flags().StringVar(&volumeDataDir, "volume-data-dir", "", "Local volume data directory shared with the volume server (e.g. /data)")
	rootCmd.Flags().StringVar(&volumeIdxDir, "volume-idx-dir", "", "Local volume index directory (defaults to volume-data-dir)")
	rootCmd.Flags().StringVar(&volumeCollection, "volume-collection", "", "Volume collection name when using local reads")
	rootCmd.Flags().BoolVar(&enableRDMA, "enable-rdma", true, "Enable RDMA engine session coordination")
	rootCmd.Flags().BoolVar(&enablePayloadRDMA, "enable-payload-rdma", false, "Enable experimental RDMA payload transfer")
	rootCmd.Flags().BoolVar(&enableNativeVolumeRDMA, "enable-native-volume-rdma", false, "Enable native verbs volume-server RDMA READ path")
	rootCmd.Flags().BoolVar(&enableZeroCopy, "enable-zerocopy", true, "Enable zero-copy temp file optimization")
	rootCmd.Flags().StringVar(&tempDir, "temp-dir", "/tmp/rdma-cache", "Temp directory for zero-copy files")
	rootCmd.Flags().IntVar(&maxConnections, "max-connections", 8, "Maximum RDMA engine IPC connections")
	rootCmd.Flags().Uint32Var(&nativeServiceLevel, "native-rdma-service-level", 0, "RDMA service level for native volume RDMA QP handshakes")
	rootCmd.Flags().BoolVarP(&debug, "debug", "d", false, "Enable debug logging")
	rootCmd.Flags().DurationVarP(&timeout, "timeout", "t", 30*time.Second, "RDMA operation timeout")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runSidecar(cmd *cobra.Command, args []string) error {
	logger := logrus.New()
	if debug {
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetLevel(logrus.InfoLevel)
	}

	logger.WithFields(logrus.Fields{
		"port":                      port,
		"engine_socket":             engineSocket,
		"native_engine_socket":      nativeEngineSocket,
		"volume_server_url":         volumeServerURL,
		"volume_data_dir":           volumeDataDir,
		"enable_rdma":               enableRDMA,
		"enable_payload_rdma":       enablePayloadRDMA,
		"enable_native_volume_rdma": enableNativeVolumeRDMA,
		"max_connections":           maxConnections,
	}).Info("Starting SeaweedFS RDMA sidecar")

	sfClient, err := seaweedfs.NewSeaweedFSRDMAClient(&seaweedfs.Config{
		RDMASocketPath:         engineSocket,
		NativeEngineSocketPath: nativeEngineSocket,
		VolumeServerURL:        volumeServerURL,
		Enabled:                enableRDMA,
		EnablePayloadRDMA:      enablePayloadRDMA,
		EnableNativeVolumeRDMA: enableNativeVolumeRDMA,
		NativeRDMAServiceLevel: nativeServiceLevel,
		DefaultTimeout:         timeout,
		Logger:                 logger,
		UseZeroCopy:            enableZeroCopy,
		TempDir:                tempDir,
		EnablePooling:          true,
		MaxConnections:         maxConnections,
		VolumeDataDir:          volumeDataDir,
		VolumeIdxDir:           volumeIdxDir,
		VolumeCollection:       volumeCollection,
	})
	if err != nil {
		return fmt.Errorf("create seaweedfs client: %w", err)
	}

	startCtx, startCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer startCancel()
	if err := startClientWithRetry(startCtx, sfClient, logger); err != nil {
		return fmt.Errorf("start seaweedfs client: %w", err)
	}
	defer sfClient.Stop()

	healthRdma := sfClient.RDMAClient()

	mux := http.NewServeMux()
	mux.Handle("/read", &httpserver.ReadHandler{Client: sfClient, Logger: logger, Timeout: timeout})
	mux.Handle("/write", &httpserver.WriteHandler{Client: sfClient, Logger: logger, Timeout: timeout})
	mux.Handle("/local-volume/", &httpserver.LocalVolumeHandler{Client: sfClient, Logger: logger, Timeout: timeout, VolumeServerURL: volumeServerURL})
	mux.HandleFunc("/health", healthHandler(logger, healthRdma))
	mux.HandleFunc("/rdma/worker-address", workerAddressHandler(healthRdma))
	mux.HandleFunc("/rdma/capabilities", capabilitiesHandler(healthRdma))
	mux.HandleFunc("/rdma/ping", pingHandler(logger, healthRdma))

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		logger.WithField("port", port).Info("HTTP server starting")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.WithError(err).Fatal("HTTP server failed")
		}
	}()

	<-sigChan
	logger.Info("Shutting down sidecar")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	return server.Shutdown(shutdownCtx)
}

func startClientWithRetry(ctx context.Context, sfClient *seaweedfs.SeaweedFSRDMAClient, logger *logrus.Logger) error {
	var lastErr error
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := sfClient.Start(attemptCtx)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err

		select {
		case <-ctx.Done():
			return lastErr
		case <-time.After(time.Second):
			logger.WithError(err).Warn("RDMA engine is not ready yet; retrying")
		}
	}
}

func healthHandler(logger *logrus.Logger, rdmaClient *rdma.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		status := "healthy"
		latency := ""
		connected := rdmaClient != nil && rdmaClient.IsConnected()
		if connected {
			ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			defer cancel()
			d, err := rdmaClient.Ping(ctx)
			if err != nil {
				http.Error(w, "RDMA engine ping failed", http.StatusServiceUnavailable)
				return
			}
			latency = d.String()
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":                status,
			"rdma_engine_connected": connected,
			"rdma_engine_latency":   latency,
			"real_rdma":             connected && rdmaClient.IsRealRdma(),
			"timestamp":             time.Now().Format(time.RFC3339),
		})
	}
}

func capabilitiesHandler(rdmaClient *rdma.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if rdmaClient == nil {
			http.Error(w, "RDMA client not available", http.StatusServiceUnavailable)
			return
		}
		caps := rdmaClient.GetCapabilities()
		if caps == nil {
			http.Error(w, "No capabilities available", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(caps)
	}
}

func workerAddressHandler(rdmaClient *rdma.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if rdmaClient == nil {
			http.Error(w, "RDMA client not available", http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		info := map[string]interface{}{
			"worker_address_b64": "",
			"listen_port":        remote.DefaultRemotePort,
			"real_rdma":          rdmaClient.IsRealRdma(),
		}
		if rdmaClient.IsConnected() {
			if wa, err := rdmaClient.GetWorkerAddress(ctx); err == nil {
				info["worker_address_b64"] = wa.WorkerAddressB64
				info["listen_port"] = wa.ListenPort
				info["real_rdma"] = wa.RealRdma
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(info)
	}
}

func pingHandler(logger *logrus.Logger, rdmaClient *rdma.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if rdmaClient == nil || !rdmaClient.IsConnected() {
			http.Error(w, "RDMA client not connected", http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		start := time.Now()
		latency, err := rdmaClient.Ping(ctx)
		if err != nil {
			http.Error(w, fmt.Sprintf("Ping failed: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":        true,
			"engine_latency": latency.String(),
			"total_latency":  time.Since(start).String(),
			"timestamp":      time.Now().Format(time.RFC3339),
		})
	}
}
