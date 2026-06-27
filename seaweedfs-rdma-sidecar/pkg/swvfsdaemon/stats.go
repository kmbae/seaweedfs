package swvfsdaemon

import (
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Stats struct {
	start    time.Time
	counters sync.Map
}

func NewStats() *Stats {
	return &Stats{start: time.Now()}
}

func (s *Stats) Inc(name string) {
	s.Add(name, 1)
}

func (s *Stats) Add(name string, delta uint64) {
	if s == nil || name == "" {
		return
	}
	value, _ := s.counters.LoadOrStore(name, new(atomic.Uint64))
	value.(*atomic.Uint64).Add(delta)
}

func (s *Stats) Observe(name string, duration time.Duration) {
	if s == nil || name == "" {
		return
	}
	s.Inc(name + "_ops")
	if duration > 0 {
		s.Add(name+"_ns", uint64(duration.Nanoseconds()))
	}
}

func (s *Stats) Snapshot() map[string]uint64 {
	out := make(map[string]uint64)
	if s == nil {
		return out
	}
	out["uptime_seconds"] = uint64(time.Since(s.start).Seconds())
	s.counters.Range(func(key, value any) bool {
		name, ok := key.(string)
		if !ok {
			return true
		}
		counter, ok := value.(*atomic.Uint64)
		if !ok {
			return true
		}
		out[name] = counter.Load()
		return true
	})
	return out
}

func (s *Stats) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, map[string]any{
		"counters": sortedSnapshot(s.Snapshot()),
	})
}

func sortedSnapshot(snapshot map[string]uint64) []map[string]any {
	keys := make([]string, 0, len(snapshot))
	for key := range snapshot {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]map[string]any, 0, len(keys))
	for _, key := range keys {
		out = append(out, map[string]any{
			"name":  key,
			"value": snapshot[key],
		})
	}
	return out
}

func writeJSONStatus(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
