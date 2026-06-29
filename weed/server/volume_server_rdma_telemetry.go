package weed_server

import (
	"sync/atomic"
	"time"
)

type volumeRdmaTelemetry struct {
	readDescRequests  atomic.Int64
	readDescSuccesses atomic.Int64
	readDescFailures  atomic.Int64
	readDescBytes     atomic.Int64
	readDescLatencyNs atomic.Int64

	releaseDescRequests  atomic.Int64
	releaseDescSuccesses atomic.Int64
	releaseDescFailures  atomic.Int64
	releaseDescLatencyNs atomic.Int64

	writeRequests  atomic.Int64
	writeSuccesses atomic.Int64
	writeFailures  atomic.Int64
	writeBytes     atomic.Int64
	writeLatencyNs atomic.Int64

	writeDescRequests  atomic.Int64
	writeDescSuccesses atomic.Int64
	writeDescFailures  atomic.Int64
	writeDescBytes     atomic.Int64
	writeDescLatencyNs atomic.Int64

	writeCommitRequests  atomic.Int64
	writeCommitSuccesses atomic.Int64
	writeCommitFailures  atomic.Int64
	writeCommitBytes     atomic.Int64
	writeCommitLatencyNs atomic.Int64

	writeCommitBatchRequests          atomic.Int64
	writeCommitBatchEntries           atomic.Int64
	writeCommitBatchEntrySuccesses    atomic.Int64
	writeCommitBatchEntryFailures     atomic.Int64
	writeCommitBatchBytes             atomic.Int64
	writeCommitBatchLatencyNs         atomic.Int64
	writeCommitBatchDecodeLatencyNs   atomic.Int64
	writeCommitBatchValidateLatencyNs atomic.Int64
	writeCommitBatchStorageLatencyNs  atomic.Int64
	writeCommitBatchStorageRequests   atomic.Int64
	writeCommitBatchStorageFallbacks  atomic.Int64

	writeAbortRequests  atomic.Int64
	writeAbortSuccesses atomic.Int64
	writeAbortFailures  atomic.Int64
	writeAbortLatencyNs atomic.Int64
}

func (s *volumeRdmaTelemetry) snapshot() map[string]int64 {
	if s == nil {
		return map[string]int64{}
	}
	return map[string]int64{
		"read_desc_requests":  s.readDescRequests.Load(),
		"read_desc_successes": s.readDescSuccesses.Load(),
		"read_desc_failures":  s.readDescFailures.Load(),
		"read_desc_bytes":     s.readDescBytes.Load(),
		"read_desc_avg_ns":    avgNs(s.readDescLatencyNs.Load(), s.readDescRequests.Load()),

		"release_desc_requests":  s.releaseDescRequests.Load(),
		"release_desc_successes": s.releaseDescSuccesses.Load(),
		"release_desc_failures":  s.releaseDescFailures.Load(),
		"release_desc_avg_ns":    avgNs(s.releaseDescLatencyNs.Load(), s.releaseDescRequests.Load()),

		"write_requests":  s.writeRequests.Load(),
		"write_successes": s.writeSuccesses.Load(),
		"write_failures":  s.writeFailures.Load(),
		"write_bytes":     s.writeBytes.Load(),
		"write_avg_ns":    avgNs(s.writeLatencyNs.Load(), s.writeRequests.Load()),

		"write_desc_requests":  s.writeDescRequests.Load(),
		"write_desc_successes": s.writeDescSuccesses.Load(),
		"write_desc_failures":  s.writeDescFailures.Load(),
		"write_desc_bytes":     s.writeDescBytes.Load(),
		"write_desc_avg_ns":    avgNs(s.writeDescLatencyNs.Load(), s.writeDescRequests.Load()),

		"write_commit_requests":  s.writeCommitRequests.Load(),
		"write_commit_successes": s.writeCommitSuccesses.Load(),
		"write_commit_failures":  s.writeCommitFailures.Load(),
		"write_commit_bytes":     s.writeCommitBytes.Load(),
		"write_commit_avg_ns":    avgNs(s.writeCommitLatencyNs.Load(), s.writeCommitRequests.Load()),

		"write_commit_batch_requests":          s.writeCommitBatchRequests.Load(),
		"write_commit_batch_entries":           s.writeCommitBatchEntries.Load(),
		"write_commit_batch_entry_successes":   s.writeCommitBatchEntrySuccesses.Load(),
		"write_commit_batch_entry_failures":    s.writeCommitBatchEntryFailures.Load(),
		"write_commit_batch_bytes":             s.writeCommitBatchBytes.Load(),
		"write_commit_batch_avg_ns":            avgNs(s.writeCommitBatchLatencyNs.Load(), s.writeCommitBatchRequests.Load()),
		"write_commit_batch_decode_avg_ns":     avgNs(s.writeCommitBatchDecodeLatencyNs.Load(), s.writeCommitBatchRequests.Load()),
		"write_commit_batch_validate_avg_ns":   avgNs(s.writeCommitBatchValidateLatencyNs.Load(), s.writeCommitBatchRequests.Load()),
		"write_commit_batch_storage_avg_ns":    avgNs(s.writeCommitBatchStorageLatencyNs.Load(), s.writeCommitBatchStorageRequests.Load()),
		"write_commit_batch_storage_requests":  s.writeCommitBatchStorageRequests.Load(),
		"write_commit_batch_storage_fallbacks": s.writeCommitBatchStorageFallbacks.Load(),

		"write_abort_requests":  s.writeAbortRequests.Load(),
		"write_abort_successes": s.writeAbortSuccesses.Load(),
		"write_abort_failures":  s.writeAbortFailures.Load(),
		"write_abort_avg_ns":    avgNs(s.writeAbortLatencyNs.Load(), s.writeAbortRequests.Load()),
	}
}

func avgNs(totalNs int64, count int64) int64 {
	if count <= 0 {
		return 0
	}
	return totalNs / count
}

func recordLatency(counter *atomic.Int64, start time.Time) {
	counter.Add(time.Since(start).Nanoseconds())
}
