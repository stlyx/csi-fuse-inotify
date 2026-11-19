package driver

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"csi-fuse-inotify/internal/backend"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/sys/unix"
)

type feed chan *backend.Snapshot

func (f feed) Get(ctx context.Context, id, version string) (*backend.Snapshot, error) {
	select {
	case s := <-f:
		return s, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestPublishedInotify(t *testing.T) {
	if os.Getenv("CSI_MOUNT_TEST") != "1" {
		t.Skip("set CSI_MOUNT_TEST=1 in a privileged mount namespace")
	}
	f := make(feed, 1)
	f <- &backend.Snapshot{Version: "initial", Data: []byte("first")}
	d := New("test", t.TempDir(), f)
	target := filepath.Join(t.TempDir(), "mount")
	req := &csi.NodePublishVolumeRequest{VolumeId: "test", TargetPath: target, Readonly: true, VolumeCapability: &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY}}}
	ctx := context.Background()
	unpub := func() {
		_, err := d.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: "test", TargetPath: target})
		if err != nil {
			t.Error(err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := d.NodePublishVolume(ctx, req); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			defer unpub()
		}
	}

	// A container runtime bind-mounts the kubelet's published target again.
	pod := t.TempDir()
	if err := unix.Mount(target, pod, "", unix.MS_BIND, ""); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := unix.Unmount(pod, 0); err != nil {
			t.Error(err)
		}
	}()
	path := filepath.Join(pod, "value")
	if err := os.WriteFile(path, []byte("forbidden"), 0644); !errors.Is(err, unix.EROFS) {
		t.Fatalf("expected read-only bind, got %v", err)
	}
	old, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	fd, err := unix.InotifyInit1(unix.IN_CLOEXEC | unix.IN_NONBLOCK)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	wdFile, err := unix.InotifyAddWatch(fd, path, unix.IN_MODIFY)
	if err != nil {
		t.Fatal(err)
	}
	wdDir, err := unix.InotifyAddWatch(fd, pod, unix.IN_MODIFY)
	if err != nil {
		t.Fatal(err)
	}
	for i, data := range [][]byte{bytes.Repeat([]byte("abc"), 150000), {}, []byte("last")} {
		f <- &backend.Snapshot{Version: []string{"large", "empty", "small"}[i], Data: data}
		seen := map[int]bool{}
		for len(seen) < 2 {
			n, err := unix.Poll([]unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}, 3000)
			if err == unix.EINTR {
				continue
			}
			if err != nil || n == 0 {
				t.Fatalf("missing inotify events, received %v, poll %v", seen, err)
			}
			var buf [4096]byte
			n, err = unix.Read(fd, buf[:])
			if err != nil {
				t.Fatal(err)
			}
			for b := buf[:n]; len(b) >= unix.SizeofInotifyEvent; {
				wd := int(binary.NativeEndian.Uint32(b))
				mask := binary.NativeEndian.Uint32(b[4:])
				size := int(binary.NativeEndian.Uint32(b[12:]))
				name := string(bytes.TrimRight(b[16:16+size], "\x00"))
				if mask&unix.IN_MODIFY != 0 && (wd == wdFile || wd == wdDir && name == "value") {
					seen[wd] = true
				}
				b = b[16+size:]
			}
			got, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("read after notification: length %d, expected %d, err %v", len(got), len(data), err)
			}
		}
		stat, err := os.Stat(path)
		if err != nil || stat.Size() != int64(len(data)) {
			t.Fatalf("bad size after update: %v %v", stat, err)
		}
	}
	if got, err := io.ReadAll(old); err != nil || string(got) != "first" {
		t.Fatalf("old descriptor lost its snapshot: %v", err)
	}
}

func TestInitialFetchCancellation(t *testing.T) {
	d := New("test", t.TempDir(), make(feed))
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err := d.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{VolumeId: "test", TargetPath: filepath.Join(t.TempDir(), "mount"), Readonly: true, VolumeCapability: &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY}}})
	if err == nil || ctx.Err() == nil {
		t.Fatal("publish should honor context deadline")
	}
}
