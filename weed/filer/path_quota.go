package filer

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var ErrPathQuotaExceeded = errors.New("path quota exceeded")

type PathQuotaExceededError struct {
	Path      string
	Prefix    string
	UsedBytes uint64
	NewBytes  uint64
	Limit     uint64
}

func (e *PathQuotaExceededError) Error() string {
	return fmt.Sprintf(
		"%v for %s (prefix=%s used=%d new=%d limit=%d)",
		ErrPathQuotaExceeded, e.Path, e.Prefix, e.UsedBytes, e.NewBytes, e.Limit,
	)
}

func (e *PathQuotaExceededError) Unwrap() error {
	return ErrPathQuotaExceeded
}

func (f *Filer) enforcePathQuota(ctx context.Context, entry, oldEntry *Entry) error {
	if entry == nil || entry.IsDirectory() || f == nil || f.FilerConf == nil {
		return nil
	}

	rule := f.FilerConf.MatchQuotaRule(string(entry.FullPath))
	if rule == nil || rule.GetQuotaBytes() == 0 {
		return nil
	}

	usedBytes, err := f.pathUsageBytes(ctx, util.FullPath(rule.GetLocationPrefix()))
	if err != nil {
		return err
	}

	newBytes := entry.Size()
	var oldBytes uint64
	if oldEntry != nil && !oldEntry.IsDirectory() {
		oldBytes = oldEntry.Size()
	}

	adjustedUsed := usedBytes
	if adjustedUsed >= oldBytes {
		adjustedUsed -= oldBytes
	} else {
		adjustedUsed = 0
	}
	projectedUsed := adjustedUsed + newBytes
	if projectedUsed > rule.GetQuotaBytes() {
		return &PathQuotaExceededError{
			Path:      string(entry.FullPath),
			Prefix:    rule.GetLocationPrefix(),
			UsedBytes: adjustedUsed,
			NewBytes:  newBytes,
			Limit:     rule.GetQuotaBytes(),
		}
	}

	return nil
}

func (f *Filer) pathUsageBytes(ctx context.Context, prefix util.FullPath) (uint64, error) {
	var usage uint64

	_, err := f.StreamListDirectoryEntries(ctx, prefix, "", false, math.MaxInt32, "", "", "", func(entry *Entry) (bool, error) {
		if entry.IsDirectory() {
			subUsage, subErr := f.pathUsageBytes(ctx, entry.FullPath)
			if subErr != nil {
				return false, subErr
			}
			usage += subUsage
			return true, nil
		}

		usage += entry.Size()
		return true, nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}

	return usage, nil
}
