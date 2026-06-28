//go:build linux && cgo && seaweedfs_rdmaverbs

package weed_server

/*
#cgo LDFLAGS: -libverbs
#include <errno.h>
#include <infiniband/verbs.h>
#include <stdint.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define SWRDMA_ABI_VERSION 1
#define SWRDMA_LINK_INFINIBAND 1
#define SWRDMA_LINK_ETHERNET 2
#define SWRDMA_REMOTE_F_GID_VALID (1U << 0)
#define SWRDMA_REMOTE_F_GRH_REQUIRED (1U << 1)
#define SWRDMA_MR_READ 1
#define SWRDMA_MR_WRITE 2
#define SWRDMA_MR_LOCAL 3
#define SWRDMA_MAX_READ_PIPELINE 16

typedef struct swrdma_endpoint {
	uint32_t abi_version;
	uint32_t flags;
	uint32_t port;
	uint32_t qp_num;
	uint32_t psn;
	uint32_t qp_state;
	uint32_t lid;
	uint32_t sm_lid;
	uint32_t port_state;
	uint32_t active_mtu;
	uint32_t gid_index;
	uint32_t link_layer;
	uint8_t gid[16];
	int kernel_enabled;
	int endpoint_ready;
	int qp_connected;
	int unsafe_global_rkey;
	char device[64];
} swrdma_endpoint;

typedef struct swrdma_remote {
	uint32_t abi_version;
	uint32_t flags;
	uint32_t qpn;
	uint32_t lid;
	uint32_t psn;
	uint32_t port;
	uint32_t gid_index;
	uint32_t sl;
	uint8_t gid[16];
} swrdma_remote;

typedef struct swrdma_conn {
	struct ibv_context *context;
	struct ibv_pd *pd;
	struct ibv_cq *cq;
	struct ibv_qp *qp;
	uint8_t port;
	int gid_index;
	uint32_t psn;
	uint32_t qp_state;
	uint32_t max_rd_atomic;
	uint32_t max_dest_rd_atomic;
	int connected;
	swrdma_endpoint endpoint;
} swrdma_conn;

typedef struct swrdma_mr {
	void *addr;
	size_t length;
	uint32_t lkey;
	uint32_t rkey;
	struct ibv_mr *mr;
} swrdma_mr;

static void swrdma_close_conn(swrdma_conn *conn);
static void swrdma_free_mr(swrdma_mr *mr);

static uint32_t swrdma_min_nonzero_u32(uint32_t a, uint32_t b) {
	if (a == 0) {
		return 1;
	}
	if (b == 0) {
		return 1;
	}
	return a < b ? a : b;
}

static void swrdma_set_error(char *err, size_t err_len, const char *fmt, ...) {
	if (err == NULL || err_len == 0) {
		return;
	}
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(err, err_len, fmt, ap);
	va_end(ap);
	err[err_len - 1] = '\0';
}

static uint32_t swrdma_map_link_layer(uint8_t link_layer) {
	if (link_layer == IBV_LINK_LAYER_INFINIBAND) {
		return SWRDMA_LINK_INFINIBAND;
	}
	if (link_layer == IBV_LINK_LAYER_ETHERNET) {
		return SWRDMA_LINK_ETHERNET;
	}
	return 0;
}

static int swrdma_gid_has_value(const uint8_t gid[16]) {
	for (int i = 0; i < 16; i++) {
		if (gid[i] != 0) {
			return 1;
		}
	}
	return 0;
}

static int swrdma_endpoint_ready(swrdma_endpoint *ep) {
	if (ep->port_state != IBV_PORT_ACTIVE || ep->qp_num == 0) {
		return 0;
	}
	if (ep->link_layer == SWRDMA_LINK_INFINIBAND) {
		return ep->lid != 0;
	}
	if (ep->link_layer == SWRDMA_LINK_ETHERNET) {
		return swrdma_gid_has_value(ep->gid);
	}
	return 0;
}

static void swrdma_fill_endpoint(swrdma_conn *conn, const char *device_name, struct ibv_port_attr *port_attr, union ibv_gid *gid) {
	memset(&conn->endpoint, 0, sizeof(conn->endpoint));
	conn->endpoint.abi_version = SWRDMA_ABI_VERSION;
	conn->endpoint.port = conn->port;
	conn->endpoint.qp_num = conn->qp ? conn->qp->qp_num : 0;
	conn->endpoint.psn = conn->psn;
	conn->endpoint.qp_state = conn->qp_state;
	conn->endpoint.lid = port_attr->lid;
	conn->endpoint.sm_lid = port_attr->sm_lid;
	conn->endpoint.port_state = port_attr->state;
	conn->endpoint.active_mtu = port_attr->active_mtu;
	conn->endpoint.gid_index = conn->gid_index;
	conn->endpoint.link_layer = swrdma_map_link_layer(port_attr->link_layer);
	memcpy(conn->endpoint.gid, gid->raw, 16);
	conn->endpoint.kernel_enabled = 1;
	conn->endpoint.qp_connected = conn->connected;
	conn->endpoint.unsafe_global_rkey = 0;
	snprintf(conn->endpoint.device, sizeof(conn->endpoint.device), "%s", device_name ? device_name : "");
	conn->endpoint.endpoint_ready = swrdma_endpoint_ready(&conn->endpoint);
}

static int swrdma_query_endpoint(swrdma_conn *conn, const char *device_name, char *err, size_t err_len) {
	struct ibv_port_attr port_attr;
	memset(&port_attr, 0, sizeof(port_attr));
	if (ibv_query_port(conn->context, conn->port, &port_attr) != 0) {
		swrdma_set_error(err, err_len, "ibv_query_port(%u) failed: %s", conn->port, strerror(errno));
		return -1;
	}
	union ibv_gid gid;
	memset(&gid, 0, sizeof(gid));
	if (ibv_query_gid(conn->context, conn->port, conn->gid_index, &gid) != 0) {
		memset(&gid, 0, sizeof(gid));
	}
	swrdma_fill_endpoint(conn, device_name, &port_attr, &gid);
	return 0;
}

static int swrdma_modify_init(swrdma_conn *conn, char *err, size_t err_len) {
	struct ibv_qp_attr attr;
	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_INIT;
	attr.pkey_index = 0;
	attr.port_num = conn->port;
	attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
	int mask = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
	if (ibv_modify_qp(conn->qp, &attr, mask) != 0) {
		swrdma_set_error(err, err_len, "ibv_modify_qp INIT failed: %s", strerror(errno));
		return -1;
	}
	conn->qp_state = IBV_QPS_INIT;
	conn->endpoint.qp_state = conn->qp_state;
	return 0;
}

static swrdma_conn *swrdma_open_conn(const char *preferred_device, uint8_t port, int gid_index, uint32_t psn, char *err, size_t err_len) {
	int num_devices = 0;
	struct ibv_device **devices = ibv_get_device_list(&num_devices);
	if (devices == NULL || num_devices == 0) {
		swrdma_set_error(err, err_len, "no RDMA devices found");
		return NULL;
	}

	int want_auto = preferred_device == NULL || preferred_device[0] == '\0' || strcmp(preferred_device, "auto") == 0;
	swrdma_conn *conn = NULL;
	const char *chosen_name = NULL;

	for (int i = 0; i < num_devices; i++) {
		const char *name = ibv_get_device_name(devices[i]);
		if (!want_auto && strcmp(preferred_device, name) != 0) {
			continue;
		}
		struct ibv_context *context = ibv_open_device(devices[i]);
		if (context == NULL) {
			continue;
		}
		struct ibv_port_attr port_attr;
		memset(&port_attr, 0, sizeof(port_attr));
		if (ibv_query_port(context, port, &port_attr) != 0) {
			ibv_close_device(context);
			continue;
		}
		if (port_attr.state != IBV_PORT_ACTIVE) {
			ibv_close_device(context);
			if (!want_auto) {
				swrdma_set_error(err, err_len, "RDMA device %s port %u is not active (state=%u)", name, port, port_attr.state);
				break;
			}
			continue;
		}

		conn = (swrdma_conn *)calloc(1, sizeof(swrdma_conn));
		if (conn == NULL) {
			ibv_close_device(context);
			swrdma_set_error(err, err_len, "calloc swrdma_conn failed");
			break;
			}
			conn->context = context;
			conn->port = port;
			conn->gid_index = gid_index;
			conn->psn = psn & 0x00ffffff;
			if (conn->psn == 0) {
				conn->psn = 1;
			}
			struct ibv_device_attr device_attr;
			memset(&device_attr, 0, sizeof(device_attr));
			if (ibv_query_device(context, &device_attr) != 0) {
				free(conn);
				conn = NULL;
				ibv_close_device(context);
				if (!want_auto) {
					swrdma_set_error(err, err_len, "ibv_query_device(%s) failed: %s", name, strerror(errno));
					break;
				}
				continue;
			}
			conn->max_rd_atomic = swrdma_min_nonzero_u32((uint32_t)device_attr.max_qp_rd_atom, SWRDMA_MAX_READ_PIPELINE);
			conn->max_dest_rd_atomic = swrdma_min_nonzero_u32((uint32_t)device_attr.max_qp_init_rd_atom, SWRDMA_MAX_READ_PIPELINE);
			if (conn->max_rd_atomic == 0) {
				conn->max_rd_atomic = 1;
			}
			if (conn->max_dest_rd_atomic == 0) {
				conn->max_dest_rd_atomic = 1;
			}
			chosen_name = name;
			break;
		}

	if (conn == NULL) {
		if (err != NULL && err[0] == '\0') {
			if (want_auto) {
				swrdma_set_error(err, err_len, "no active RDMA device found on port %u", port);
			} else {
				swrdma_set_error(err, err_len, "RDMA device %s not found", preferred_device);
			}
		}
		ibv_free_device_list(devices);
		return NULL;
	}

	conn->pd = ibv_alloc_pd(conn->context);
	if (conn->pd == NULL) {
		swrdma_set_error(err, err_len, "ibv_alloc_pd failed: %s", strerror(errno));
		swrdma_close_conn(conn);
		ibv_free_device_list(devices);
		return NULL;
	}
	conn->cq = ibv_create_cq(conn->context, 128, NULL, NULL, 0);
	if (conn->cq == NULL) {
		swrdma_set_error(err, err_len, "ibv_create_cq failed: %s", strerror(errno));
		swrdma_close_conn(conn);
		ibv_free_device_list(devices);
		return NULL;
	}
	struct ibv_qp_init_attr qp_attr;
	memset(&qp_attr, 0, sizeof(qp_attr));
	qp_attr.send_cq = conn->cq;
	qp_attr.recv_cq = conn->cq;
	qp_attr.qp_type = IBV_QPT_RC;
	qp_attr.cap.max_send_wr = 128;
	qp_attr.cap.max_recv_wr = 1;
	qp_attr.cap.max_send_sge = 1;
	qp_attr.cap.max_recv_sge = 1;
	conn->qp = ibv_create_qp(conn->pd, &qp_attr);
	if (conn->qp == NULL) {
		swrdma_set_error(err, err_len, "ibv_create_qp failed: %s", strerror(errno));
		swrdma_close_conn(conn);
		ibv_free_device_list(devices);
		return NULL;
	}
	if (swrdma_modify_init(conn, err, err_len) != 0) {
		swrdma_close_conn(conn);
		ibv_free_device_list(devices);
		return NULL;
	}
	if (swrdma_query_endpoint(conn, chosen_name, err, err_len) != 0) {
		swrdma_close_conn(conn);
		ibv_free_device_list(devices);
		return NULL;
	}
	ibv_free_device_list(devices);
	return conn;
}

static void swrdma_close_conn(swrdma_conn *conn) {
	if (conn == NULL) {
		return;
	}
	if (conn->qp != NULL) {
		ibv_destroy_qp(conn->qp);
	}
	if (conn->cq != NULL) {
		ibv_destroy_cq(conn->cq);
	}
	if (conn->pd != NULL) {
		ibv_dealloc_pd(conn->pd);
	}
	if (conn->context != NULL) {
		ibv_close_device(conn->context);
	}
	free(conn);
}

static int swrdma_requires_grh(swrdma_conn *conn, swrdma_remote *remote) {
	return conn->endpoint.link_layer == SWRDMA_LINK_ETHERNET || (remote->flags & SWRDMA_REMOTE_F_GRH_REQUIRED) != 0;
}

static int swrdma_connect(swrdma_conn *conn, swrdma_remote *remote, char *err, size_t err_len) {
	if (conn == NULL || remote == NULL) {
		swrdma_set_error(err, err_len, "connect requires connection and remote endpoint");
		return -1;
	}
	if (remote->abi_version != SWRDMA_ABI_VERSION || remote->qpn == 0 || remote->psn > 0x00ffffff || remote->port == 0 || remote->sl > 15) {
		swrdma_set_error(err, err_len, "invalid remote RDMA endpoint metadata");
		return -1;
	}
	if (conn->endpoint.link_layer == SWRDMA_LINK_INFINIBAND && remote->lid == 0) {
		swrdma_set_error(err, err_len, "remote LID is required for InfiniBand");
		return -1;
	}
	if (swrdma_requires_grh(conn, remote) && ((remote->flags & SWRDMA_REMOTE_F_GID_VALID) == 0 || !swrdma_gid_has_value(remote->gid))) {
		swrdma_set_error(err, err_len, "remote GID is required for GRH path");
		return -1;
	}

	struct ibv_qp_attr attr;
	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_RTR;
	attr.path_mtu = conn->endpoint.active_mtu >= IBV_MTU_256 && conn->endpoint.active_mtu <= IBV_MTU_4096 ? conn->endpoint.active_mtu : IBV_MTU_1024;
	attr.dest_qp_num = remote->qpn;
	attr.rq_psn = remote->psn;
	attr.max_dest_rd_atomic = conn->max_dest_rd_atomic;
	attr.min_rnr_timer = 12;
	attr.ah_attr.dlid = remote->lid;
	attr.ah_attr.sl = remote->sl;
	attr.ah_attr.src_path_bits = 0;
	attr.ah_attr.static_rate = 0;
	attr.ah_attr.port_num = conn->port;
	if (swrdma_requires_grh(conn, remote)) {
		attr.ah_attr.is_global = 1;
		memcpy(attr.ah_attr.grh.dgid.raw, remote->gid, 16);
		attr.ah_attr.grh.flow_label = 0;
		attr.ah_attr.grh.sgid_index = conn->gid_index;
		attr.ah_attr.grh.hop_limit = 64;
		attr.ah_attr.grh.traffic_class = 0;
	}
	int rtr_mask = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
	if (ibv_modify_qp(conn->qp, &attr, rtr_mask) != 0) {
		swrdma_set_error(err, err_len, "ibv_modify_qp RTR failed: %s", strerror(errno));
		return -1;
	}
	conn->qp_state = IBV_QPS_RTR;

	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_RTS;
	attr.timeout = 14;
	attr.retry_cnt = 7;
	attr.rnr_retry = 7;
	attr.sq_psn = conn->psn;
	attr.max_rd_atomic = conn->max_rd_atomic;
	int rts_mask = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
	if (ibv_modify_qp(conn->qp, &attr, rts_mask) != 0) {
		swrdma_set_error(err, err_len, "ibv_modify_qp RTS failed: %s", strerror(errno));
		return -1;
	}
	conn->qp_state = IBV_QPS_RTS;
	conn->connected = 1;
	conn->endpoint.qp_state = conn->qp_state;
	conn->endpoint.qp_connected = 1;
	return 0;
}

static swrdma_mr *swrdma_alloc_mr(swrdma_conn *conn, size_t length, int kind, char *err, size_t err_len) {
	if (conn == NULL || conn->pd == NULL) {
		swrdma_set_error(err, err_len, "RDMA connection is not open");
		return NULL;
	}
	if (length == 0) {
		swrdma_set_error(err, err_len, "MR length must be greater than zero");
		return NULL;
	}
	void *addr = NULL;
	int rc = posix_memalign(&addr, 4096, length);
	if (rc != 0 || addr == NULL) {
		swrdma_set_error(err, err_len, "posix_memalign(%zu) failed: %s", length, strerror(rc));
		return NULL;
	}
	memset(addr, 0, length);
	int access = IBV_ACCESS_LOCAL_WRITE;
	if (kind == SWRDMA_MR_READ) {
		access |= IBV_ACCESS_REMOTE_READ;
	} else if (kind == SWRDMA_MR_WRITE) {
		access |= IBV_ACCESS_REMOTE_WRITE;
	} else if (kind == SWRDMA_MR_LOCAL) {
		access |= IBV_ACCESS_LOCAL_WRITE;
	} else {
		free(addr);
		swrdma_set_error(err, err_len, "unknown MR kind %d", kind);
		return NULL;
	}
	struct ibv_mr *mr = ibv_reg_mr(conn->pd, addr, length, access);
	if (mr == NULL) {
		swrdma_set_error(err, err_len, "ibv_reg_mr(%zu) failed: %s", length, strerror(errno));
		free(addr);
		return NULL;
	}
	swrdma_mr *out = (swrdma_mr *)calloc(1, sizeof(swrdma_mr));
	if (out == NULL) {
		ibv_dereg_mr(mr);
		free(addr);
		swrdma_set_error(err, err_len, "calloc swrdma_mr failed");
		return NULL;
	}
	out->addr = addr;
	out->length = length;
	out->lkey = mr->lkey;
	out->rkey = mr->rkey;
	out->mr = mr;
	return out;
}

static uint64_t swrdma_now_ms(void) {
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return ((uint64_t)ts.tv_sec * 1000ULL) + ((uint64_t)ts.tv_nsec / 1000000ULL);
}

static int swrdma_poll_completion(swrdma_conn *conn, uint64_t wr_id, uint64_t timeout_ms, enum ibv_wc_opcode expected_opcode, char *err, size_t err_len) {
	uint64_t deadline = swrdma_now_ms() + (timeout_ms == 0 ? 5000 : timeout_ms);
	struct timespec sleep_time;
	sleep_time.tv_sec = 0;
	sleep_time.tv_nsec = 50000;
	for (;;) {
		struct ibv_wc wc;
		memset(&wc, 0, sizeof(wc));
		int n = ibv_poll_cq(conn->cq, 1, &wc);
		if (n < 0) {
			swrdma_set_error(err, err_len, "ibv_poll_cq failed");
			return -1;
		}
		if (n > 0) {
			if (wc.wr_id != wr_id) {
				swrdma_set_error(err, err_len, "unexpected RDMA completion wr_id=%llu expected=%llu", (unsigned long long)wc.wr_id, (unsigned long long)wr_id);
				return -1;
			}
			if (wc.status != IBV_WC_SUCCESS) {
				swrdma_set_error(err, err_len, "RDMA completion failed status=%d vendor_err=%u", wc.status, wc.vendor_err);
				return -1;
			}
			if (wc.opcode != expected_opcode) {
				swrdma_set_error(err, err_len, "unexpected RDMA completion opcode=%d expected=%d", wc.opcode, expected_opcode);
				return -1;
			}
			return 0;
		}
		if (swrdma_now_ms() > deadline) {
			swrdma_set_error(err, err_len, "timed out waiting for RDMA completion wr_id=%llu", (unsigned long long)wr_id);
			return -1;
		}
		nanosleep(&sleep_time, NULL);
	}
}

static int swrdma_poll_completion_any(swrdma_conn *conn, uint64_t timeout_ms, enum ibv_wc_opcode expected_opcode, uint64_t *wr_id, char *err, size_t err_len) {
	uint64_t deadline = swrdma_now_ms() + (timeout_ms == 0 ? 5000 : timeout_ms);
	struct timespec sleep_time;
	sleep_time.tv_sec = 0;
	sleep_time.tv_nsec = 50000;
	for (;;) {
		struct ibv_wc wc;
		memset(&wc, 0, sizeof(wc));
		int n = ibv_poll_cq(conn->cq, 1, &wc);
		if (n < 0) {
			swrdma_set_error(err, err_len, "ibv_poll_cq failed");
			return -1;
		}
		if (n > 0) {
			if (wc.status != IBV_WC_SUCCESS) {
				swrdma_set_error(err, err_len, "RDMA completion failed wr_id=%llu status=%d vendor_err=%u", (unsigned long long)wc.wr_id, wc.status, wc.vendor_err);
				return -1;
			}
			if (wc.opcode != expected_opcode) {
				swrdma_set_error(err, err_len, "unexpected RDMA completion wr_id=%llu opcode=%d expected=%d", (unsigned long long)wc.wr_id, wc.opcode, expected_opcode);
				return -1;
			}
			if (wr_id != NULL) {
				*wr_id = wc.wr_id;
			}
			return 0;
		}
		if (swrdma_now_ms() > deadline) {
			swrdma_set_error(err, err_len, "timed out waiting for RDMA completion");
			return -1;
		}
		nanosleep(&sleep_time, NULL);
	}
}

static int swrdma_post_read_remote_into(swrdma_conn *conn, swrdma_mr *local, uint64_t remote_addr, uint32_t rkey, size_t length, uint64_t wr_id, char *err, size_t err_len) {
	if (conn == NULL || conn->qp == NULL) {
		swrdma_set_error(err, err_len, "RDMA connection is not open");
		return -1;
	}
	if (!conn->connected || conn->qp_state != IBV_QPS_RTS) {
		swrdma_set_error(err, err_len, "RDMA READ requires connected RTS QP");
		return -1;
	}
	if (remote_addr == 0 || length == 0) {
		swrdma_set_error(err, err_len, "RDMA READ requires non-empty remote descriptor");
		return -1;
	}
	if (local == NULL || local->mr == NULL || local->addr == NULL || local->length < length) {
		swrdma_set_error(err, err_len, "RDMA READ local MR is too small");
		return -1;
	}
	if (length > UINT32_MAX) {
		swrdma_set_error(err, err_len, "RDMA READ length is too large: %zu", length);
		return -1;
	}

	struct ibv_sge sge;
	memset(&sge, 0, sizeof(sge));
	sge.addr = (uintptr_t)local->addr;
	sge.length = (uint32_t)length;
	sge.lkey = local->lkey;

	struct ibv_send_wr wr;
	memset(&wr, 0, sizeof(wr));
	struct ibv_send_wr *bad_wr = NULL;
	wr.wr_id = wr_id;
	wr.sg_list = &sge;
	wr.num_sge = 1;
	wr.opcode = IBV_WR_RDMA_READ;
	wr.send_flags = IBV_SEND_SIGNALED;
	wr.wr.rdma.remote_addr = remote_addr;
	wr.wr.rdma.rkey = rkey;

	if (ibv_post_send(conn->qp, &wr, &bad_wr) != 0) {
		swrdma_set_error(err, err_len, "ibv_post_send RDMA_READ failed: %s", strerror(errno));
		return -1;
	}
	return 0;
}

static int swrdma_read_remote_into(swrdma_conn *conn, swrdma_mr *local, uint64_t remote_addr, uint32_t rkey, size_t length, uint64_t timeout_ms, char *err, size_t err_len) {
	uint64_t wr_id = (uint64_t)(uintptr_t)local;
	if (swrdma_post_read_remote_into(conn, local, remote_addr, rkey, length, wr_id, err, err_len) != 0) {
		return -1;
	}
	if (swrdma_poll_completion(conn, wr_id, timeout_ms, IBV_WC_RDMA_READ, err, err_len) != 0) {
		return -1;
	}
	return 0;
}

static swrdma_mr *swrdma_read_remote(swrdma_conn *conn, uint64_t remote_addr, uint32_t rkey, size_t length, uint64_t timeout_ms, char *err, size_t err_len) {
	swrdma_mr *local = swrdma_alloc_mr(conn, length, SWRDMA_MR_LOCAL, err, err_len);
	if (local == NULL) {
		return NULL;
	}
	if (swrdma_read_remote_into(conn, local, remote_addr, rkey, length, timeout_ms, err, err_len) != 0) {
		swrdma_free_mr(local);
		return NULL;
	}
	return local;
}

static void swrdma_free_mr(swrdma_mr *mr) {
	if (mr == NULL) {
		return;
	}
	if (mr->mr != NULL) {
		ibv_dereg_mr(mr->mr);
	}
	if (mr->addr != NULL) {
		free(mr->addr);
	}
	free(mr);
}
*/
import "C"

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"time"
	"unsafe"

	"github.com/seaweedfs/seaweedfs/weed/stats"
)

const (
	embeddedVolumeRdmaDefaultPSN = 0xabcdef
	embeddedVolumeRdmaErrLen     = 512

	embeddedVolumeRdmaReadMRPoolLimit    = volumeRdmaPipelineDepth
	embeddedVolumeRdmaSessionMRPoolLimit = 8
	embeddedVolumeRdmaReadMRMinSize      = 64 << 10
	embeddedVolumeRdmaReadMRAlign        = 1 << 20
)

type embeddedVolumeRdmaTransport struct {
	cfg VolumeRdmaEmbeddedConfig

	mu               sync.Mutex
	nextConnectionID uint64
	nextSessionID    uint64
	connections      map[uint64]*embeddedVolumeRdmaConnection
	sessions         map[uint64]*embeddedVolumeRdmaBuffer
	closed           bool
}

type embeddedVolumeRdmaConnection struct {
	mu        sync.Mutex
	id        uint64
	handle    *C.swrdma_conn
	endpoint  VolumeRdmaEndpointInfo
	remote    VolumeRdmaRemoteInfo
	connected bool

	readMRPool    map[uint64][]*C.swrdma_mr
	sessionMRPool map[embeddedVolumeRdmaMRPoolKey][]*C.swrdma_mr
}

type embeddedVolumeRdmaBuffer struct {
	owner        *embeddedVolumeRdmaTransport
	sessionID    uint64
	connectionID uint64
	mr           *C.swrdma_mr
	poolKey      embeddedVolumeRdmaMRPoolKey
	desc         VolumeRdmaDataDesc
}

type embeddedVolumeRdmaMRPoolKey struct {
	Kind int
	Size uint64
}

type embeddedVolumeRdmaReadChunk struct {
	index   int
	length  uint32
	poolKey uint64
	mr      *C.swrdma_mr
	done    bool
}

func NewEmbeddedVolumeRdmaTransport(cfg VolumeRdmaEmbeddedConfig) (VolumeRdmaEndpoint, VolumeRdmaReadRegistrar, error) {
	if cfg.Port == 0 {
		cfg.Port = 1
	}
	if cfg.Device == "" {
		cfg.Device = "auto"
	}
	t := &embeddedVolumeRdmaTransport{
		cfg:              cfg,
		nextConnectionID: 1,
		nextSessionID:    1,
		connections:      make(map[uint64]*embeddedVolumeRdmaConnection),
		sessions:         make(map[uint64]*embeddedVolumeRdmaBuffer),
	}
	if _, _, err := t.LocalEndpointFor(context.Background(), 0); err != nil {
		return nil, nil, err
	}
	return t, t, nil
}

func (t *embeddedVolumeRdmaTransport) LocalEndpoint(ctx context.Context) (VolumeRdmaEndpointInfo, error) {
	endpoint, _, err := t.LocalEndpointFor(ctx, 1)
	return endpoint, err
}

func (t *embeddedVolumeRdmaTransport) LocalEndpointFor(ctx context.Context, connectionID uint64) (VolumeRdmaEndpointInfo, uint64, error) {
	if err := ctx.Err(); err != nil {
		return VolumeRdmaEndpointInfo{}, 0, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	conn, err := t.ensureConnectionLocked(connectionID)
	if err != nil {
		return VolumeRdmaEndpointInfo{}, 0, err
	}
	endpoint := conn.endpoint
	endpoint.ConnectionID = conn.id
	return endpoint, conn.id, nil
}

func (t *embeddedVolumeRdmaTransport) ConnectEndpoint(ctx context.Context, remote VolumeRdmaRemoteInfo) error {
	return t.ConnectEndpointFor(ctx, 1, remote)
}

func (t *embeddedVolumeRdmaTransport) ConnectEndpointFor(ctx context.Context, connectionID uint64, remote VolumeRdmaRemoteInfo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if connectionID == 0 {
		return fmt.Errorf("embedded RDMA connect requires connection_id")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	conn, err := t.ensureConnectionLocked(connectionID)
	if err != nil {
		return err
	}
	if conn.connected {
		if conn.remote == remote {
			return nil
		}
		return fmt.Errorf("embedded RDMA connection %d is already connected", connectionID)
	}
	var cRemote C.swrdma_remote
	fillEmbeddedCRemote(&cRemote, remote)
	var errBuf [embeddedVolumeRdmaErrLen]C.char
	if C.swrdma_connect(conn.handle, &cRemote, &errBuf[0], C.size_t(len(errBuf))) != 0 {
		return fmt.Errorf("embedded RDMA connect connection_id=%d: %s", connectionID, cErrorString(&errBuf[0]))
	}
	conn.connected = true
	conn.remote = remote
	conn.endpoint.QPConnected = true
	conn.endpoint.QPState = uint32(C.IBV_QPS_RTS)
	return nil
}

func (t *embeddedVolumeRdmaTransport) RequesterLocalEndpoint(ctx context.Context) (VolumeRdmaEndpointInfo, error) {
	endpoint, _, err := t.RequesterLocalEndpointFor(ctx, 1)
	return endpoint, err
}

func (t *embeddedVolumeRdmaTransport) RequesterLocalEndpointFor(ctx context.Context, connectionID uint64) (VolumeRdmaEndpointInfo, uint64, error) {
	return t.LocalEndpointFor(ctx, connectionID)
}

func (t *embeddedVolumeRdmaTransport) RequesterConnectEndpoint(ctx context.Context, remote VolumeRdmaRemoteInfo) error {
	return t.RequesterConnectEndpointFor(ctx, 1, remote)
}

func (t *embeddedVolumeRdmaTransport) RequesterConnectEndpointFor(ctx context.Context, connectionID uint64, remote VolumeRdmaRemoteInfo) error {
	return t.ConnectEndpointFor(ctx, connectionID, remote)
}

func (t *embeddedVolumeRdmaTransport) ReadRemoteFor(ctx context.Context, connectionID uint64, desc VolumeRdmaDataDesc, timeout time.Duration) ([]byte, error) {
	mr, poolKey, err := t.readRemoteMR(ctx, connectionID, desc, timeout)
	if err != nil {
		return nil, err
	}
	defer t.releaseReadMR(connectionID, poolKey, mr, true)
	data := C.GoBytes(mr.addr, C.int(desc.Length))
	if uint32(len(data)) < desc.Length {
		return nil, fmt.Errorf("embedded RDMA read_remote returned %d bytes for %d byte descriptor", len(data), desc.Length)
	}
	return data[:int(desc.Length)], nil
}

func (t *embeddedVolumeRdmaTransport) ReadRemoteToFor(ctx context.Context, connectionID uint64, desc VolumeRdmaDataDesc, timeout time.Duration, dst io.Writer) error {
	if dst == nil {
		return fmt.Errorf("embedded RDMA read_remote_to requires destination writer")
	}
	return t.readRemotePipelineTo(ctx, connectionID, desc, timeout, dst)
}

func (t *embeddedVolumeRdmaTransport) connectedConnection(ctx context.Context, connectionID uint64, op string) (*embeddedVolumeRdmaConnection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if connectionID == 0 {
		return nil, fmt.Errorf("embedded RDMA %s requires connection_id", op)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	conn, err := t.ensureConnectionLocked(connectionID)
	if err != nil {
		return nil, err
	}
	if !conn.connected {
		return nil, fmt.Errorf("embedded RDMA connection %d is not connected", connectionID)
	}
	return conn, nil
}

func (t *embeddedVolumeRdmaTransport) readRemoteMR(ctx context.Context, connectionID uint64, desc VolumeRdmaDataDesc, timeout time.Duration) (*C.swrdma_mr, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if connectionID == 0 {
		return nil, 0, fmt.Errorf("embedded RDMA read_remote requires connection_id")
	}
	if desc.RemoteAddr == 0 || desc.Length == 0 {
		return nil, 0, fmt.Errorf("embedded RDMA read_remote requires non-empty descriptor")
	}
	if desc.Length > volumeRdmaEngineMaxFrameSize {
		return nil, 0, fmt.Errorf("embedded RDMA read_remote frame too large: %d bytes", desc.Length)
	}
	timeoutMs := uint64(timeout.Milliseconds())
	if timeoutMs == 0 && timeout > 0 {
		timeoutMs = 1
	}
	poolKey := embeddedVolumeRdmaReadMRPoolKey(desc.Length)

	conn, err := t.connectedConnection(ctx, connectionID, "read_remote")
	if err != nil {
		return nil, 0, err
	}
	conn.mu.Lock()
	defer conn.mu.Unlock()
	var errBuf [embeddedVolumeRdmaErrLen]C.char
	mr, err := t.acquireReadMRLocked(conn, poolKey)
	if err != nil {
		return nil, 0, err
	}
	if C.swrdma_read_remote_into(
		conn.handle,
		mr,
		C.uint64_t(desc.RemoteAddr),
		C.uint32_t(desc.RKey),
		C.size_t(desc.Length),
		C.uint64_t(timeoutMs),
		&errBuf[0],
		C.size_t(len(errBuf)),
	) != 0 {
		C.swrdma_free_mr(mr)
		return nil, 0, fmt.Errorf("embedded RDMA read_remote: %s", cErrorString(&errBuf[0]))
	}
	return mr, poolKey, nil
}

func (t *embeddedVolumeRdmaTransport) readRemotePipelineTo(ctx context.Context, connectionID uint64, desc VolumeRdmaDataDesc, timeout time.Duration, dst io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if connectionID == 0 {
		return fmt.Errorf("embedded RDMA read_remote pipeline requires connection_id")
	}
	if desc.RemoteAddr == 0 || desc.Length == 0 {
		return fmt.Errorf("embedded RDMA read_remote pipeline requires non-empty descriptor")
	}
	if desc.Length > volumeRdmaEngineMaxFrameSize {
		return fmt.Errorf("embedded RDMA read_remote pipeline frame too large: %d bytes", desc.Length)
	}
	chunks := planVolumeRdmaChunks(desc.Length, volumeRdmaPipelineChunkSize)
	if len(chunks) == 0 {
		return nil
	}
	timeoutMs := uint64(timeout.Milliseconds())
	if timeoutMs == 0 && timeout > 0 {
		timeoutMs = 1
	}

	conn, err := t.connectedConnection(ctx, connectionID, "read_remote pipeline")
	if err != nil {
		return err
	}
	conn.mu.Lock()
	defer conn.mu.Unlock()

	inFlight := make(map[uint64]*embeddedVolumeRdmaReadChunk, volumeRdmaPipelineDepth)
	completed := make([]*embeddedVolumeRdmaReadChunk, len(chunks))
	defer stats.VolumeServerRdmaInFlightWRs.WithLabelValues("read_remote").Set(0)

	nextPost := 0
	nextWrite := 0
	completedCount := 0
	var writeErr error

	cleanup := func(reusable bool) {
		for _, ch := range completed {
			if ch != nil && ch.mr != nil {
				t.releaseReadMRLocked(conn, ch.poolKey, ch.mr, reusable && ch.done)
				ch.mr = nil
			}
		}
		for _, ch := range inFlight {
			if ch != nil && ch.mr != nil {
				t.releaseReadMRLocked(conn, ch.poolKey, ch.mr, false)
				ch.mr = nil
			}
		}
	}

	for (writeErr == nil && nextPost < len(chunks)) || len(inFlight) > 0 {
		if err := ctx.Err(); err != nil {
			stats.VolumeServerRdmaErrors.WithLabelValues("read_remote_pipeline").Inc()
			cleanup(false)
			return err
		}

		for writeErr == nil && nextPost < len(chunks) && len(inFlight) < volumeRdmaPipelineDepth {
			chunk := chunks[nextPost]
			if desc.RemoteAddr+chunk.Offset < desc.RemoteAddr {
				stats.VolumeServerRdmaErrors.WithLabelValues("read_remote_pipeline").Inc()
				cleanup(false)
				return fmt.Errorf("embedded RDMA read_remote pipeline address overflow")
			}
			poolKey := embeddedVolumeRdmaReadMRPoolKey(chunk.Length)
			mr, err := t.acquireReadMRLocked(conn, poolKey)
			if err != nil {
				stats.VolumeServerRdmaErrors.WithLabelValues("read_remote_pipeline").Inc()
				cleanup(false)
				return err
			}
			wrID := uint64(chunk.Index + 1)
			var errBuf [embeddedVolumeRdmaErrLen]C.char
			if C.swrdma_post_read_remote_into(
				conn.handle,
				mr,
				C.uint64_t(desc.RemoteAddr+chunk.Offset),
				C.uint32_t(desc.RKey),
				C.size_t(chunk.Length),
				C.uint64_t(wrID),
				&errBuf[0],
				C.size_t(len(errBuf)),
			) != 0 {
				t.releaseReadMRLocked(conn, poolKey, mr, false)
				stats.VolumeServerRdmaErrors.WithLabelValues("read_remote_pipeline").Inc()
				cleanup(false)
				return fmt.Errorf("embedded RDMA post read_remote pipeline: %s", cErrorString(&errBuf[0]))
			}
			readChunk := &embeddedVolumeRdmaReadChunk{
				index:   chunk.Index,
				length:  chunk.Length,
				poolKey: poolKey,
				mr:      mr,
			}
			inFlight[wrID] = readChunk
			completed[chunk.Index] = readChunk
			nextPost++
			stats.VolumeServerRdmaInFlightWRs.WithLabelValues("read_remote").Set(float64(len(inFlight)))
		}

		if len(inFlight) == 0 {
			break
		}

		var wrID C.uint64_t
		var errBuf [embeddedVolumeRdmaErrLen]C.char
		start := time.Now()
		if C.swrdma_poll_completion_any(
			conn.handle,
			C.uint64_t(timeoutMs),
			C.enum_ibv_wc_opcode(C.IBV_WC_RDMA_READ),
			&wrID,
			&errBuf[0],
			C.size_t(len(errBuf)),
		) != 0 {
			stats.VolumeServerRdmaCompletionLatencySeconds.WithLabelValues("read_remote").Observe(time.Since(start).Seconds())
			stats.VolumeServerRdmaErrors.WithLabelValues("read_remote_pipeline").Inc()
			cleanup(false)
			return fmt.Errorf("embedded RDMA poll read_remote pipeline: %s", cErrorString(&errBuf[0]))
		}
		stats.VolumeServerRdmaCompletionLatencySeconds.WithLabelValues("read_remote").Observe(time.Since(start).Seconds())

		chunk := inFlight[uint64(wrID)]
		if chunk == nil {
			stats.VolumeServerRdmaErrors.WithLabelValues("read_remote_pipeline").Inc()
			cleanup(false)
			return fmt.Errorf("embedded RDMA read_remote pipeline completed unknown wr_id=%d", uint64(wrID))
		}
		delete(inFlight, uint64(wrID))
		chunk.done = true
		completedCount++
		stats.VolumeServerRdmaInFlightWRs.WithLabelValues("read_remote").Set(float64(len(inFlight)))
		stats.VolumeServerRdmaTransferBytes.WithLabelValues("read_remote").Add(float64(chunk.length))
		stats.VolumeServerRdmaTransferChunks.WithLabelValues("read_remote").Inc()

		for writeErr == nil && nextWrite < len(completed) {
			ready := completed[nextWrite]
			if ready == nil || !ready.done {
				break
			}
			payload := unsafe.Slice((*byte)(ready.mr.addr), int(ready.length))
			n, err := dst.Write(payload)
			if err == nil && n != len(payload) {
				err = io.ErrShortWrite
			}
			t.releaseReadMRLocked(conn, ready.poolKey, ready.mr, true)
			ready.mr = nil
			completed[nextWrite] = nil
			nextWrite++
			if err != nil {
				writeErr = err
				stats.VolumeServerRdmaErrors.WithLabelValues("read_remote_write").Inc()
				break
			}
		}

		if completedCount == len(chunks) && len(inFlight) == 0 {
			break
		}
	}

	if writeErr != nil {
		cleanup(true)
		return writeErr
	}
	cleanup(true)
	return nil
}

func (t *embeddedVolumeRdmaTransport) RegisterReadBuffer(ctx context.Context, data []byte) (VolumeRdmaRegisteredBuffer, error) {
	return t.RegisterReadBufferFor(ctx, 1, data)
}

func (t *embeddedVolumeRdmaTransport) RegisterReadBufferFor(ctx context.Context, connectionID uint64, data []byte) (VolumeRdmaRegisteredBuffer, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("embedded RDMA register_read requires data")
	}
	if uint64(len(data)) > volumeRdmaEngineMaxFrameSize {
		return nil, fmt.Errorf("embedded RDMA register_read frame too large: %d bytes", len(data))
	}
	buffer, err := t.allocateSessionBuffer(ctx, connectionID, uint64(len(data)), C.SWRDMA_MR_READ)
	if err != nil {
		return nil, err
	}
	copy(buffer.bytes(), data)
	return buffer, nil
}

func (t *embeddedVolumeRdmaTransport) RegisterReadStreamFor(ctx context.Context, connectionID uint64, size uint64, writeData func(io.Writer) error) (VolumeRdmaRegisteredBuffer, error) {
	if size == 0 {
		return nil, fmt.Errorf("embedded RDMA register_read_stream requires data")
	}
	if size > volumeRdmaEngineMaxFrameSize {
		return nil, fmt.Errorf("embedded RDMA register_read_stream frame too large: %d bytes", size)
	}
	if writeData == nil {
		return nil, fmt.Errorf("embedded RDMA register_read_stream requires writer")
	}
	buffer, err := t.allocateSessionBuffer(ctx, connectionID, size, C.SWRDMA_MR_READ)
	if err != nil {
		return nil, err
	}
	writer := &embeddedRdmaExactBufferWriter{dst: buffer.bytes()[:int(size)]}
	if err := writeData(writer); err != nil {
		_ = buffer.Release(context.Background())
		return nil, err
	}
	if writer.off != len(writer.dst) {
		_ = buffer.Release(context.Background())
		return nil, fmt.Errorf("embedded RDMA register_read_stream short write: wrote %d of %d bytes", writer.off, len(writer.dst))
	}
	return buffer, nil
}

func (t *embeddedVolumeRdmaTransport) RegisterWriteBufferFor(ctx context.Context, connectionID uint64, size uint64) (VolumeRdmaRegisteredBuffer, error) {
	if size == 0 {
		return nil, fmt.Errorf("embedded RDMA register_write requires size")
	}
	if size > volumeRdmaEngineMaxFrameSize {
		return nil, fmt.Errorf("embedded RDMA register_write frame too large: %d bytes", size)
	}
	return t.allocateSessionBuffer(ctx, connectionID, size, C.SWRDMA_MR_WRITE)
}

func (t *embeddedVolumeRdmaTransport) ReadRegisteredBuffer(ctx context.Context, sessionID uint64, size uint64) ([]byte, error) {
	var out []byte
	err := t.ReadRegisteredBufferTo(ctx, sessionID, size, writerFunc(func(payload []byte) (int, error) {
		out = append(out, payload...)
		return len(payload), nil
	}))
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (t *embeddedVolumeRdmaTransport) ReadRegisteredBufferTo(ctx context.Context, sessionID uint64, size uint64, dst io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if sessionID == 0 {
		return fmt.Errorf("embedded RDMA read_registered requires session_id")
	}
	if size == 0 {
		return fmt.Errorf("embedded RDMA read_registered requires size")
	}
	if dst == nil {
		return fmt.Errorf("embedded RDMA read_registered requires destination writer")
	}
	t.mu.Lock()
	buffer, ok := t.sessions[sessionID]
	if !ok {
		t.mu.Unlock()
		return fmt.Errorf("unknown embedded RDMA session_id %d", sessionID)
	}
	if size > uint64(buffer.mr.length) {
		t.mu.Unlock()
		return fmt.Errorf("embedded RDMA read_registered size %d exceeds buffer length %d", size, uint64(buffer.mr.length))
	}
	payload := buffer.bytes()[:int(size)]
	n, err := dst.Write(payload)
	if err == nil && n != len(payload) {
		err = io.ErrShortWrite
	}
	t.mu.Unlock()
	return err
}

func (t *embeddedVolumeRdmaTransport) ReleaseSession(ctx context.Context, sessionID uint64) error {
	if sessionID == 0 {
		return nil
	}
	t.mu.Lock()
	buffer, ok := t.sessions[sessionID]
	if ok {
		delete(t.sessions, sessionID)
	}
	if ok {
		conn := t.connections[buffer.connectionID]
		if t.closed || conn == nil {
			C.swrdma_free_mr(buffer.mr)
			stats.VolumeServerRdmaMRPoolOps.WithLabelValues("session", "free").Inc()
		} else {
			conn.mu.Lock()
			t.releaseSessionMRLocked(conn, buffer.poolKey, buffer.mr, true)
			conn.mu.Unlock()
		}
		buffer.mr = nil
	}
	t.mu.Unlock()
	return nil
}

func (t *embeddedVolumeRdmaTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	sessions := t.sessions
	connections := t.connections
	t.sessions = make(map[uint64]*embeddedVolumeRdmaBuffer)
	t.connections = make(map[uint64]*embeddedVolumeRdmaConnection)
	t.mu.Unlock()

	for _, buffer := range sessions {
		C.swrdma_free_mr(buffer.mr)
		buffer.mr = nil
	}
	for _, conn := range connections {
		conn.mu.Lock()
		for _, pool := range conn.readMRPool {
			for _, mr := range pool {
				C.swrdma_free_mr(mr)
			}
		}
		for _, pool := range conn.sessionMRPool {
			for _, mr := range pool {
				C.swrdma_free_mr(mr)
			}
		}
		C.swrdma_close_conn(conn.handle)
		conn.handle = nil
		conn.connected = false
		conn.mu.Unlock()
	}
	return nil
}

func (t *embeddedVolumeRdmaTransport) acquireReadMRLocked(conn *embeddedVolumeRdmaConnection, poolKey uint64) (*C.swrdma_mr, error) {
	if conn == nil || conn.handle == nil {
		return nil, fmt.Errorf("embedded RDMA connection is not open")
	}
	if conn.readMRPool == nil {
		conn.readMRPool = make(map[uint64][]*C.swrdma_mr)
	}
	pool := conn.readMRPool[poolKey]
	if len(pool) > 0 {
		last := len(pool) - 1
		mr := pool[last]
		conn.readMRPool[poolKey] = pool[:last]
		stats.VolumeServerRdmaMRPoolOps.WithLabelValues("read", "hit").Inc()
		return mr, nil
	}
	var errBuf [embeddedVolumeRdmaErrLen]C.char
	mr := C.swrdma_alloc_mr(conn.handle, C.size_t(poolKey), C.SWRDMA_MR_LOCAL, &errBuf[0], C.size_t(len(errBuf)))
	if mr == nil {
		stats.VolumeServerRdmaMRPoolOps.WithLabelValues("read", "miss_error").Inc()
		return nil, fmt.Errorf("embedded RDMA allocate read MR: %s", cErrorString(&errBuf[0]))
	}
	stats.VolumeServerRdmaMRPoolOps.WithLabelValues("read", "miss").Inc()
	return mr, nil
}

func (t *embeddedVolumeRdmaTransport) releaseReadMR(connectionID uint64, poolKey uint64, mr *C.swrdma_mr, reusable bool) {
	if mr == nil {
		return
	}
	if !reusable || poolKey == 0 {
		C.swrdma_free_mr(mr)
		stats.VolumeServerRdmaMRPoolOps.WithLabelValues("read", "free").Inc()
		return
	}
	t.mu.Lock()
	conn := t.connections[connectionID]
	if t.closed || conn == nil {
		t.mu.Unlock()
		C.swrdma_free_mr(mr)
		stats.VolumeServerRdmaMRPoolOps.WithLabelValues("read", "free").Inc()
		return
	}
	conn.mu.Lock()
	t.mu.Unlock()
	defer conn.mu.Unlock()
	t.releaseReadMRLocked(conn, poolKey, mr, reusable)
}

func (t *embeddedVolumeRdmaTransport) releaseReadMRLocked(conn *embeddedVolumeRdmaConnection, poolKey uint64, mr *C.swrdma_mr, reusable bool) {
	if mr == nil {
		return
	}
	if !reusable || poolKey == 0 || conn == nil {
		C.swrdma_free_mr(mr)
		stats.VolumeServerRdmaMRPoolOps.WithLabelValues("read", "free").Inc()
		return
	}
	if conn.readMRPool == nil {
		conn.readMRPool = make(map[uint64][]*C.swrdma_mr)
	}
	if len(conn.readMRPool[poolKey]) >= embeddedVolumeRdmaReadMRPoolLimit {
		C.swrdma_free_mr(mr)
		stats.VolumeServerRdmaMRPoolOps.WithLabelValues("read", "drop").Inc()
		return
	}
	conn.readMRPool[poolKey] = append(conn.readMRPool[poolKey], mr)
	stats.VolumeServerRdmaMRPoolOps.WithLabelValues("read", "put").Inc()
}

func embeddedVolumeRdmaReadMRPoolKey(length uint32) uint64 {
	size := uint64(length)
	if size < embeddedVolumeRdmaReadMRMinSize {
		size = embeddedVolumeRdmaReadMRMinSize
	}
	if size <= embeddedVolumeRdmaReadMRAlign {
		return size
	}
	return ((size + embeddedVolumeRdmaReadMRAlign - 1) / embeddedVolumeRdmaReadMRAlign) * embeddedVolumeRdmaReadMRAlign
}

func (t *embeddedVolumeRdmaTransport) allocateSessionBuffer(ctx context.Context, connectionID uint64, size uint64, kind C.int) (*embeddedVolumeRdmaBuffer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	conn, err := t.ensureConnectionLocked(connectionID)
	if err != nil {
		return nil, err
	}
	if !conn.connected {
		return nil, fmt.Errorf("embedded RDMA connection %d is not connected", conn.id)
	}
	sessionID, err := t.nextSessionIDLocked()
	if err != nil {
		return nil, err
	}
	poolKey := embeddedVolumeRdmaSessionMRPoolKey(kind, size)
	conn.mu.Lock()
	mr, err := t.acquireSessionMRLocked(conn, poolKey)
	conn.mu.Unlock()
	if err != nil {
		return nil, err
	}
	buffer := &embeddedVolumeRdmaBuffer{
		owner:        t,
		sessionID:    sessionID,
		connectionID: conn.id,
		mr:           mr,
		poolKey:      poolKey,
		desc: VolumeRdmaDataDesc{
			RemoteAddr: uint64(uintptr(mr.addr)),
			RKey:       uint32(mr.rkey),
			Length:     uint32(size),
			Reserved:   [4]uint64{sessionID, conn.id, 0, 0},
		},
	}
	t.sessions[sessionID] = buffer
	return buffer, nil
}

func (t *embeddedVolumeRdmaTransport) acquireSessionMRLocked(conn *embeddedVolumeRdmaConnection, poolKey embeddedVolumeRdmaMRPoolKey) (*C.swrdma_mr, error) {
	if conn == nil || conn.handle == nil {
		return nil, fmt.Errorf("embedded RDMA connection is not open")
	}
	if conn.sessionMRPool == nil {
		conn.sessionMRPool = make(map[embeddedVolumeRdmaMRPoolKey][]*C.swrdma_mr)
	}
	pool := conn.sessionMRPool[poolKey]
	if len(pool) > 0 {
		last := len(pool) - 1
		mr := pool[last]
		conn.sessionMRPool[poolKey] = pool[:last]
		stats.VolumeServerRdmaMRPoolOps.WithLabelValues("session", "hit").Inc()
		return mr, nil
	}
	var errBuf [embeddedVolumeRdmaErrLen]C.char
	mr := C.swrdma_alloc_mr(conn.handle, C.size_t(poolKey.Size), C.int(poolKey.Kind), &errBuf[0], C.size_t(len(errBuf)))
	if mr == nil {
		stats.VolumeServerRdmaMRPoolOps.WithLabelValues("session", "miss_error").Inc()
		return nil, fmt.Errorf("embedded RDMA allocate MR: %s", cErrorString(&errBuf[0]))
	}
	stats.VolumeServerRdmaMRPoolOps.WithLabelValues("session", "miss").Inc()
	return mr, nil
}

func (t *embeddedVolumeRdmaTransport) releaseSessionMRLocked(conn *embeddedVolumeRdmaConnection, poolKey embeddedVolumeRdmaMRPoolKey, mr *C.swrdma_mr, reusable bool) {
	if mr == nil {
		return
	}
	if !reusable || poolKey.Size == 0 || conn == nil {
		C.swrdma_free_mr(mr)
		stats.VolumeServerRdmaMRPoolOps.WithLabelValues("session", "free").Inc()
		return
	}
	if conn.sessionMRPool == nil {
		conn.sessionMRPool = make(map[embeddedVolumeRdmaMRPoolKey][]*C.swrdma_mr)
	}
	if len(conn.sessionMRPool[poolKey]) >= embeddedVolumeRdmaSessionMRPoolLimit {
		C.swrdma_free_mr(mr)
		stats.VolumeServerRdmaMRPoolOps.WithLabelValues("session", "drop").Inc()
		return
	}
	conn.sessionMRPool[poolKey] = append(conn.sessionMRPool[poolKey], mr)
	stats.VolumeServerRdmaMRPoolOps.WithLabelValues("session", "put").Inc()
}

func embeddedVolumeRdmaSessionMRPoolKey(kind C.int, size uint64) embeddedVolumeRdmaMRPoolKey {
	return embeddedVolumeRdmaMRPoolKey{
		Kind: int(kind),
		Size: embeddedVolumeRdmaReadMRPoolKey(uint32(size)),
	}
}

func (t *embeddedVolumeRdmaTransport) ensureConnectionLocked(connectionID uint64) (*embeddedVolumeRdmaConnection, error) {
	if t.closed {
		return nil, fmt.Errorf("embedded RDMA transport is closed")
	}
	if connectionID == 0 {
		var err error
		connectionID, err = t.nextConnectionIDLocked()
		if err != nil {
			return nil, err
		}
	}
	if conn := t.connections[connectionID]; conn != nil {
		return conn, nil
	}
	device := C.CString(t.cfg.Device)
	defer C.free(unsafe.Pointer(device))
	var errBuf [embeddedVolumeRdmaErrLen]C.char
	handle := C.swrdma_open_conn(device, C.uint8_t(t.cfg.Port), C.int(t.cfg.GIDIndex), C.uint32_t(embeddedVolumeRdmaDefaultPSN+uint32(connectionID)), &errBuf[0], C.size_t(len(errBuf)))
	if handle == nil {
		return nil, fmt.Errorf("embedded RDMA open device=%s port=%d gid_index=%d: %s", t.cfg.Device, t.cfg.Port, t.cfg.GIDIndex, cErrorString(&errBuf[0]))
	}
	conn := &embeddedVolumeRdmaConnection{
		id:            connectionID,
		handle:        handle,
		endpoint:      endpointInfoFromC(&handle.endpoint),
		readMRPool:    make(map[uint64][]*C.swrdma_mr),
		sessionMRPool: make(map[embeddedVolumeRdmaMRPoolKey][]*C.swrdma_mr),
	}
	conn.endpoint.ConnectionID = connectionID
	t.connections[connectionID] = conn
	return conn, nil
}

func (t *embeddedVolumeRdmaTransport) nextConnectionIDLocked() (uint64, error) {
	connectionID := t.nextConnectionID
	if connectionID == 0 {
		return 0, fmt.Errorf("embedded RDMA connection id overflow")
	}
	t.nextConnectionID++
	return connectionID, nil
}

func (t *embeddedVolumeRdmaTransport) nextSessionIDLocked() (uint64, error) {
	sessionID := t.nextSessionID
	if sessionID == 0 {
		return 0, fmt.Errorf("embedded RDMA session id overflow")
	}
	t.nextSessionID++
	return sessionID, nil
}

func (b *embeddedVolumeRdmaBuffer) Descriptor() VolumeRdmaDataDesc {
	if b == nil {
		return VolumeRdmaDataDesc{}
	}
	return b.desc
}

func (b *embeddedVolumeRdmaBuffer) SessionID() uint64 {
	if b == nil {
		return 0
	}
	return b.sessionID
}

func (b *embeddedVolumeRdmaBuffer) Release(ctx context.Context) error {
	if b == nil || b.owner == nil {
		return nil
	}
	return b.owner.ReleaseSession(ctx, b.sessionID)
}

func (b *embeddedVolumeRdmaBuffer) bytes() []byte {
	if b == nil || b.mr == nil || b.mr.addr == nil || b.mr.length == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(b.mr.addr), int(b.mr.length))
}

type embeddedRdmaExactBufferWriter struct {
	dst []byte
	off int
}

func (w *embeddedRdmaExactBufferWriter) Write(payload []byte) (int, error) {
	if len(payload) > len(w.dst)-w.off {
		return 0, fmt.Errorf("embedded RDMA stream payload exceeds registered buffer: write=%d remaining=%d", len(payload), len(w.dst)-w.off)
	}
	copy(w.dst[w.off:], payload)
	w.off += len(payload)
	return len(payload), nil
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(payload []byte) (int, error) {
	return f(payload)
}

func endpointInfoFromC(ep *C.swrdma_endpoint) VolumeRdmaEndpointInfo {
	gid := make([]byte, 16)
	for i := range gid {
		gid[i] = byte(ep.gid[i])
	}
	return VolumeRdmaEndpointInfo{
		ABIVersion:      uint32(ep.abi_version),
		Flags:           uint32(ep.flags),
		Device:          C.GoString((*C.char)(unsafe.Pointer(&ep.device[0]))),
		Port:            uint32(ep.port),
		QPNum:           uint32(ep.qp_num),
		PSN:             uint32(ep.psn),
		QPState:         uint32(ep.qp_state),
		LID:             uint32(ep.lid),
		SMLID:           uint32(ep.sm_lid),
		PortState:       uint32(ep.port_state),
		ActiveMTU:       uint32(ep.active_mtu),
		GIDIndex:        uint32(ep.gid_index),
		LinkLayer:       uint32(ep.link_layer),
		GID:             hex.EncodeToString(gid),
		KernelEnabled:   ep.kernel_enabled != 0,
		EndpointReady:   ep.endpoint_ready != 0,
		QPConnected:     ep.qp_connected != 0,
		UnsafeGlobalKey: ep.unsafe_global_rkey != 0,
	}
}

func fillEmbeddedCRemote(dst *C.swrdma_remote, src VolumeRdmaRemoteInfo) {
	*dst = C.swrdma_remote{}
	dst.abi_version = C.uint32_t(src.ABIVersion)
	dst.flags = C.uint32_t(src.Flags)
	dst.qpn = C.uint32_t(src.QPN)
	dst.lid = C.uint32_t(src.LID)
	dst.psn = C.uint32_t(src.PSN)
	dst.port = C.uint32_t(src.Port)
	dst.gid_index = C.uint32_t(src.GIDIndex)
	dst.sl = C.uint32_t(src.SL)
	for i := range src.GID {
		dst.gid[i] = C.uint8_t(src.GID[i])
	}
}

func cErrorString(err *C.char) string {
	if err == nil {
		return "unknown error"
	}
	msg := C.GoString(err)
	if msg == "" {
		return "unknown error"
	}
	return msg
}
