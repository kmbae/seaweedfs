# SeaweedFS RDMA Sidecar on Mellanox SR-IOV (NVIDIA Network Operator)

This guide matches clusters configured with:

- `NicClusterPolicy` + DOCA OFED driver
- `SriovNetworkNodePolicy` with `resourceName: mlnxnics`
- `SriovIBNetwork` named `sriov-ib-network`
- SR-IOV resource: `nvidia.com/mlnxnics`

## Architecture

Each volume pod runs three data-plane containers:

1. **volume** — SeaweedFS volume server (`/data0`, HTTP `:8444`)
2. **rdma-engine** (initContainer) — Rust UCX engine, Unix socket under `/tmp/rdma`
3. **rdma-sidecar** — Go HTTP sidecar (`GET /read`) for mount clients

Needle bytes are loaded from the shared `/data0` mount (local-volume path) or HTTP fallback to `127.0.0.1:8444`. The UCX engine coordinates RDMA sessions; when built with `--features real-ucx` and UCX libraries are present, `real_rdma=true` is reported in capabilities.

## Build images

```bash
cd seaweedfs-rdma-sidecar

# Go sidecar
docker build -f Dockerfile.sidecar -t seaweedfs-rdma-sidecar:latest .

# Rust engine (mock, default)
docker build -f Dockerfile.rdma-engine -t seaweedfs-rdma-engine:latest .

# Rust engine with UCX (Mellanox nodes with libucp)
docker build -f Dockerfile.rdma-engine \
  --build-arg CARGO_FEATURES=real-ucx \
  -t seaweedfs-rdma-engine:ucx .
```

## Operator install

```bash
helm install seaweedfs-operator ./deploy/helm -n seaweedfs-system --create-namespace
kubectl apply -f ../seaweedfs-operator/config/samples/seaweed_v1_seaweed_with_rdma_mellanox.yaml
```

## Mount client

```bash
weed mount \
  -filer=seaweed-rdma-filer:8888 \
  -dir=/mnt/sw \
  -rdma.enabled=true \
  -rdma.sidecar=<volume-pod-ip>:8081
```

## Verify SR-IOV IB (same as your test pod)

```bash
microk8s kubectl apply -f deploy/k8s/sriov-ib-test-pod.yaml
microk8s kubectl exec -it sriov-ib-test-pod -- ibv_devinfo
```

## Notes

- Pod annotation `k8s.v1.cni.cncf.io/networks: sriov-ib-network` attaches the IB interface.
- Request `nvidia.com/mlnxnics: 1` on the **rdma-engine** initContainer (and optionally sidecar if it needs IB).
- Sidecar `volume-data-dir` must match the operator volume mount (`/data0` for single-disk clusters).
- Remote peer RDMA (client ↔ volume over IB) requires additional UCX endpoint wiring; local reads work today via shared volume + HTTP fallback.
