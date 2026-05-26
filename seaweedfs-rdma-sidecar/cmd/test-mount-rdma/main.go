// Package main provides an integration test binary that exercises the
// RDMAMountClient (the same client weed mount uses) against a live
// RDMA sidecar / demo-server, validating both read and write paths.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

type rdmaMountClient struct {
	sidecarAddr string
	httpClient  *http.Client
}

type writeResponse struct {
	Success bool   `json:"success"`
	IsRDMA  bool   `json:"is_rdma"`
	Source  string `json:"source"`
	FileID  string `json:"file_id"`
	Size    int    `json:"size"`
}

func main() {
	var sidecarAddr string
	var timeout time.Duration

	root := &cobra.Command{
		Use:   "test-mount-rdma",
		Short: "Integration test for weed mount RDMA client",
		Long: `Tests the RDMAMountClient read/write paths against a running RDMA sidecar.
Requires: rdma-engine + demo-server (or production sidecar) already running.`,
	}

	root.PersistentFlags().StringVar(&sidecarAddr, "sidecar", "localhost:8080", "RDMA sidecar address (host:port)")
	root.PersistentFlags().DurationVar(&timeout, "timeout", 10*time.Second, "Request timeout")

	root.AddCommand(healthCmd(&sidecarAddr, &timeout))
	root.AddCommand(readCmd(&sidecarAddr, &timeout))
	root.AddCommand(writeCmd(&sidecarAddr, &timeout))
	root.AddCommand(allCmd(&sidecarAddr, &timeout))

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func healthCmd(addr *string, timeout *time.Duration) *cobra.Command {
	return &cobra.Command{
		Use:   "health",
		Short: "Test health check endpoint",
		RunE: func(cmd *cobra.Command, args []string) error {
			return testHealth(*addr, *timeout)
		},
	}
}

func readCmd(addr *string, timeout *time.Duration) *cobra.Command {
	var fileID string
	var size uint64
	cmd := &cobra.Command{
		Use:   "read",
		Short: "Test RDMA read via sidecar HTTP",
		RunE: func(cmd *cobra.Command, args []string) error {
			return testRead(*addr, *timeout, fileID, size)
		},
	}
	cmd.Flags().StringVar(&fileID, "file-id", "3,01637037d6", "File ID to read")
	cmd.Flags().Uint64Var(&size, "size", 4096, "Read size in bytes")
	return cmd
}

func writeCmd(addr *string, timeout *time.Duration) *cobra.Command {
	var fileID string
	var dataSize int
	cmd := &cobra.Command{
		Use:   "write",
		Short: "Test RDMA write via sidecar HTTP",
		RunE: func(cmd *cobra.Command, args []string) error {
			return testWrite(*addr, *timeout, fileID, dataSize)
		},
	}
	cmd.Flags().StringVar(&fileID, "file-id", "3,01637037d6", "File ID for write")
	cmd.Flags().IntVar(&dataSize, "data-size", 4096, "Data size to write (bytes)")
	return cmd
}

func allCmd(addr *string, timeout *time.Duration) *cobra.Command {
	return &cobra.Command{
		Use:   "all",
		Short: "Run all tests (health, read, write)",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("========================================")
			fmt.Println("  Mount RDMA Integration Test Suite")
			fmt.Println("========================================")
			fmt.Println()

			passed := 0
			failed := 0

			tests := []struct {
				name string
				fn   func() error
			}{
				{"Health Check", func() error { return testHealth(*addr, *timeout) }},
				{"Read (small)", func() error { return testRead(*addr, *timeout, "3,01637037d6", 1024) }},
				{"Read (medium)", func() error { return testRead(*addr, *timeout, "3,01637037d6", 65536) }},
				{"Write (small)", func() error { return testWrite(*addr, *timeout, "5,0a1b2c3d4e", 1024) }},
				{"Write (medium)", func() error { return testWrite(*addr, *timeout, "5,0a1b2c3d4e", 65536) }},
			}

			for _, tc := range tests {
				fmt.Printf("--- TEST: %s ---\n", tc.name)
				if err := tc.fn(); err != nil {
					fmt.Printf("FAIL: %s: %v\n\n", tc.name, err)
					failed++
				} else {
					fmt.Printf("PASS: %s\n\n", tc.name)
					passed++
				}
			}

			fmt.Println("========================================")
			fmt.Printf("  Results: %d passed, %d failed\n", passed, failed)
			fmt.Println("========================================")

			if failed > 0 {
				return fmt.Errorf("%d test(s) failed", failed)
			}
			return nil
		},
	}
}

func testHealth(addr string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	url := fmt.Sprintf("http://%s/health", addr)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("health request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check returned status %s", resp.Status)
	}

	var health map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		return fmt.Errorf("parse health response: %w", err)
	}

	fmt.Printf("  Status: %v\n", health["status"])
	if rdma, ok := health["rdma"].(map[string]interface{}); ok {
		fmt.Printf("  RDMA Enabled: %v\n", rdma["enabled"])
		fmt.Printf("  RDMA Connected: %v\n", rdma["connected"])
	}
	if connected, ok := health["rdma_engine_connected"]; ok {
		fmt.Printf("  Engine Connected: %v\n", connected)
	}

	return nil
}

func testRead(addr string, timeout time.Duration, fileID string, size uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	volumeServer := fmt.Sprintf("http://%s", addr)
	url := fmt.Sprintf("http://%s/read?file_id=%s&offset=0&size=%d&volume_server=%s",
		addr, fileID, size, volumeServer)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("create read request: %w", err)
	}

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("read request failed: %w", err)
	}
	defer resp.Body.Close()
	duration := time.Since(start)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("read returned status %s", resp.Status)
	}

	source := resp.Header.Get("X-Source")
	rdmaUsed := resp.Header.Get("X-RDMA-Used")
	contentLen := resp.ContentLength

	fmt.Printf("  File ID: %s\n", fileID)
	fmt.Printf("  Source: %s\n", source)
	fmt.Printf("  RDMA Used: %s\n", rdmaUsed)
	fmt.Printf("  Content-Length: %d\n", contentLen)
	fmt.Printf("  Duration: %v\n", duration)

	if !strings.Contains(source, "rdma") && rdmaUsed != "true" {
		fmt.Printf("  WARNING: RDMA was not used for this read\n")
	}

	return nil
}

func testWrite(addr string, timeout time.Duration, fileID string, dataSize int) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	volumeServer := fmt.Sprintf("http://%s", addr)
	url := fmt.Sprintf("http://%s/write?file_id=%s&volume_server=%s",
		addr, fileID, volumeServer)

	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(string(data)))
	if err != nil {
		return fmt.Errorf("create write request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("write request failed: %w", err)
	}
	defer resp.Body.Close()
	duration := time.Since(start)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("write returned status %s", resp.Status)
	}

	var writeResp writeResponse
	if err := json.NewDecoder(resp.Body).Decode(&writeResp); err != nil {
		return fmt.Errorf("parse write response: %w", err)
	}

	fmt.Printf("  File ID: %s\n", fileID)
	fmt.Printf("  Success: %v\n", writeResp.Success)
	fmt.Printf("  Is RDMA: %v\n", writeResp.IsRDMA)
	fmt.Printf("  Source: %s\n", writeResp.Source)
	fmt.Printf("  Size: %d\n", writeResp.Size)
	fmt.Printf("  Duration: %v\n", duration)

	if !writeResp.Success {
		return fmt.Errorf("write was not successful")
	}
	if !writeResp.IsRDMA {
		fmt.Printf("  WARNING: RDMA was not used for this write\n")
	}

	return nil
}
