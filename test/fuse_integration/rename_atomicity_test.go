package fuse_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRenameAtomicity exercises rename under concurrent readers and writers.
// pjdfstest covers single-threaded POSIX rename semantics; these tests focus
// on races that only appear under load.
func TestRenameAtomicity(t *testing.T) {
	framework := NewFuseTestFramework(t, DefaultTestConfig())
	defer framework.Cleanup()

	require.NoError(t, framework.Setup(DefaultTestConfig()))

	t.Run("SerializedPingPongRename", func(t *testing.T) {
		testSerializedPingPongRename(t, framework)
	})
	t.Run("RenameReplaceExisting", func(t *testing.T) {
		testRenameReplaceExisting(t, framework)
	})
	t.Run("RenameDuringRead", func(t *testing.T) {
		testRenameDuringRead(t, framework)
	})
	t.Run("CrossDirectoryRenameRace", func(t *testing.T) {
		testCrossDirectoryRenameRace(t, framework)
	})
	t.Run("ConcurrentUniqueRenames", func(t *testing.T) {
		testConcurrentUniqueRenames(t, framework)
	})
}

// testSerializedPingPongRename alternates rename between two paths in one goroutine.
func testSerializedPingPongRename(t *testing.T, framework *FuseTestFramework) {
	dir := filepath.Join(framework.GetMountPoint(), "pingpong")
	require.NoError(t, os.Mkdir(dir, 0755))

	pathA := filepath.Join(dir, "a")
	pathB := filepath.Join(dir, "b")
	payload := []byte("ping-pong-rename-payload")
	require.NoError(t, os.WriteFile(pathA, payload, 0644))

	for i := 0; i < 200; i++ {
		if i%2 == 0 {
			require.NoError(t, os.Rename(pathA, pathB))
			require.False(t, fileExists(pathA))
			require.True(t, fileExists(pathB))
		} else {
			require.NoError(t, os.Rename(pathB, pathA))
			require.True(t, fileExists(pathA))
			require.False(t, fileExists(pathB))
		}
	}

	finalPath := pathA
	if !fileExists(pathA) {
		finalPath = pathB
	}
	got, err := os.ReadFile(finalPath)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

// testRenameReplaceExisting verifies rename atomically replaces an existing target.
func testRenameReplaceExisting(t *testing.T, framework *FuseTestFramework) {
	dir := filepath.Join(framework.GetMountPoint(), "replace")
	require.NoError(t, os.Mkdir(dir, 0755))

	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")
	require.NoError(t, os.WriteFile(dst, []byte("old"), 0644))
	require.NoError(t, os.WriteFile(src, []byte("new"), 0644))

	require.NoError(t, os.Rename(src, dst))
	require.False(t, fileExists(src))

	got, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, []byte("new"), got)
}

// testRenameDuringRead keeps a reader open while another goroutine renames.
func testRenameDuringRead(t *testing.T, framework *FuseTestFramework) {
	dir := filepath.Join(framework.GetMountPoint(), "rename-read")
	require.NoError(t, os.Mkdir(dir, 0755))

	src := filepath.Join(dir, "source")
	dst := filepath.Join(dir, "destination")
	payload := bytes.Repeat([]byte("x"), 64*1024)
	require.NoError(t, os.WriteFile(src, payload, 0644))

	f, err := os.Open(src)
	require.NoError(t, err)
	defer f.Close()

	var renameErr atomic.Value
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			if err := os.Rename(src, dst); err != nil {
				if !os.IsNotExist(err) {
					renameErr.Store(err)
					return
				}
			}
			if err := os.Rename(dst, src); err != nil {
				if !os.IsNotExist(err) {
					renameErr.Store(err)
					return
				}
			}
		}
	}()

	buf := make([]byte, len(payload))
	totalRead := 0
	for totalRead < len(payload) {
		n, err := f.Read(buf[totalRead:])
		if err != nil {
			break
		}
		totalRead += n
	}

	wg.Wait()
	if v := renameErr.Load(); v != nil {
		t.Fatalf("rename failed: %v", v)
	}

	require.Equal(t, len(payload), totalRead)
	require.Equal(t, payload, buf)

	switch {
	case fileExists(src):
	case fileExists(dst):
	default:
		t.Fatal("renamed file missing after concurrent read")
	}
}

// testCrossDirectoryRenameRace moves a file between directories concurrently.
func testCrossDirectoryRenameRace(t *testing.T, framework *FuseTestFramework) {
	root := filepath.Join(framework.GetMountPoint(), "cross-dir")
	dirA := filepath.Join(root, "a")
	dirB := filepath.Join(root, "b")
	require.NoError(t, os.MkdirAll(dirA, 0755))
	require.NoError(t, os.MkdirAll(dirB, 0755))

	name := "data.bin"
	pathInA := filepath.Join(dirA, name)
	pathInB := filepath.Join(dirB, name)
	require.NoError(t, os.WriteFile(pathInA, []byte("cross-dir"), 0644))

	const iterations = 100
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(startInA bool) {
			defer wg.Done()
			inA := startInA
			for n := 0; n < iterations; n++ {
				if inA {
					_ = os.Rename(pathInA, pathInB)
				} else {
					_ = os.Rename(pathInB, pathInA)
				}
				inA = !inA
			}
		}(i == 0)
	}
	wg.Wait()

	aExists := fileExists(pathInA)
	bExists := fileExists(pathInB)
	require.True(t, aExists != bExists, "file must exist in exactly one directory")
}

// testConcurrentUniqueRenames assigns each worker a unique file then renames into a shared dir.
func testConcurrentUniqueRenames(t *testing.T, framework *FuseTestFramework) {
	root := filepath.Join(framework.GetMountPoint(), "unique-renames")
	srcDir := filepath.Join(root, "src")
	dstDir := filepath.Join(root, "dst")
	require.NoError(t, os.MkdirAll(srcDir, 0755))
	require.NoError(t, os.MkdirAll(dstDir, 0755))

	const workers = 20
	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			src := filepath.Join(srcDir, fmt.Sprintf("file-%d", id))
			dst := filepath.Join(dstDir, fmt.Sprintf("file-%d", id))
			content := []byte(fmt.Sprintf("worker-%d", id))
			if err := os.WriteFile(src, content, 0644); err != nil {
				errCh <- err
				return
			}
			if err := os.Rename(src, dst); err != nil {
				errCh <- err
				return
			}
			got, err := os.ReadFile(dst)
			if err != nil {
				errCh <- err
				return
			}
			if !bytes.Equal(content, got) {
				errCh <- fmt.Errorf("worker %d: content mismatch", id)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	entries, err := os.ReadDir(dstDir)
	require.NoError(t, err)
	require.Len(t, entries, workers)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
