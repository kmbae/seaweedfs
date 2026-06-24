package seaweedfs

import (
	"testing"

	"github.com/sirupsen/logrus"
	"seaweedfs-rdma-sidecar/pkg/rdma"
)

func TestFetchNeedleDataRequiresDataSource(t *testing.T) {
	client := &SeaweedFSRDMAClient{
		logger: logrus.New(),
	}

	_, _, _, err := client.fetchNeedleData(t.Context(), &NeedleReadRequest{
		VolumeID:     1,
		NeedleID:     1,
		Cookie:       1,
		Offset:       0,
		Size:         16,
		VolumeServer: "",
	})
	if err == nil {
		t.Fatal("expected error when no data source is configured")
	}
}

func TestRemoteSourceDefaultsTCP(t *testing.T) {
	if got := remoteReadSource(""); got != "remote-tcp" {
		t.Fatalf("unexpected read source: %s", got)
	}
	if got := remoteWriteSource("UCX"); got != "remote-ucx-write" {
		t.Fatalf("unexpected write source: %s", got)
	}
}

func TestIsRealRdmaDefaultsFalse(t *testing.T) {
	c := rdma.NewClient(&rdma.Config{})
	if c.IsRealRdma() {
		t.Fatal("expected mock engine to report real_rdma=false")
	}
}
