package driver

import (
	"context"
	"errors"
	"hash/maphash"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"csi-fuse-inotify/internal/backend"
	"csi-fuse-inotify/internal/volume"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/moby/sys/mountinfo"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const Name = "ramvalue.csi.local"

type Source interface {
	Get(context.Context, string, string) (*backend.Snapshot, error)
}

type session struct {
	id     string
	mount  *volume.Mount
	cancel context.CancelFunc
	done   chan struct{}
}

type Driver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedNodeServer
	nodeID, workDir string
	source          Source
	sessions        sync.Map // keyed by target path
	seed            maphash.Seed
	locks           [64]sync.Mutex
}

func New(nodeID, workDir string, source Source) *Driver {
	return &Driver{nodeID: nodeID, workDir: workDir, source: source, seed: maphash.MakeSeed()}
}

func (d *Driver) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: Name, VendorVersion: "0.1.0"}, nil
}

func (d *Driver) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{}, nil
}

func (d *Driver) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}

func (d *Driver) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{NodeId: d.nodeID}, nil
}

func (d *Driver) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{}, nil
}

func validateTarget(id, target string) error {
	if !backend.ValidID(id) || !filepath.IsAbs(target) || target == "/" || filepath.Clean(target) != target {
		return status.Error(codes.InvalidArgument, "valid volume ID and clean absolute target path required")
	}
	return nil
}

func (d *Driver) lock(target string) func() {
	l := &d.locks[maphash.String(d.seed, target)%uint64(len(d.locks))]
	l.Lock()
	return l.Unlock
}

func inspect(id, target string) (bool, error) {
	mounts, err := mountinfo.GetMounts(func(m *mountinfo.Info) (bool, bool) { return m.Mountpoint != target, false })
	if err != nil {
		return false, status.Error(codes.Internal, "cannot inspect mount table")
	}
	if len(mounts) == 0 {
		return false, nil
	}
	if len(mounts) != 1 || mounts[0].Source != id || mounts[0].FSType != "fuse.ramvalue" || !slices.Contains(strings.Split(mounts[0].Options, ","), "ro") {
		return false, status.Error(codes.FailedPrecondition, "target has a conflicting mount")
	}
	return true, nil
}

func (d *Driver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	id, target := req.GetVolumeId(), req.GetTargetPath()
	if err := validateTarget(id, target); err != nil {
		return nil, err
	}
	cap := req.GetVolumeCapability()
	mode := cap.GetAccessMode().GetMode()
	if !req.GetReadonly() || cap.GetMount() == nil || (mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY && mode != csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY) {
		return nil, status.Error(codes.InvalidArgument, "a read-only filesystem volume is required")
	}
	for _, f := range cap.GetMount().GetMountFlags() {
		if !slices.Contains([]string{"ro", "nosuid", "nodev", "noexec"}, f) {
			return nil, status.Error(codes.InvalidArgument, "unsupported mount option")
		}
	}
	unlock := d.lock(target)
	defer unlock()
	mounted, err := inspect(id, target)
	if err != nil {
		return nil, err
	}
	if item, ok := d.sessions.Load(target); ok {
		s := item.(*session)
		if s.id != id {
			return nil, status.Error(codes.AlreadyExists, "target is in use")
		}
		if mounted {
			return &csi.NodePublishVolumeResponse{}, nil
		}
		if err := d.stop(target, s); err != nil {
			return nil, err
		}
	} else if mounted {
		// A crashed FUSE server leaves disconnected binds. Replace only one
		// positively identified as ours, after checking its connection.
		var st unix.Statfs_t
		if !errors.Is(unix.Statfs(target, &st), unix.ENOTCONN) {
			return nil, status.Error(codes.FailedPrecondition, "mount belongs to another active session")
		}
		if err := unix.Unmount(target, 0); err != nil {
			return nil, status.Error(codes.Internal, "cannot remove disconnected mount")
		}
	}

	initial, err := d.source.Get(ctx, id, "")
	if err != nil || initial == nil {
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return nil, status.Error(codes.Unavailable, "initial backend snapshot unavailable")
	}
	if err := os.MkdirAll(target, 0750); err != nil {
		return nil, status.Error(codes.Internal, "cannot create target")
	}
	resolved, err := filepath.EvalSymlinks(target)
	if err != nil || resolved != target {
		return nil, status.Error(codes.FailedPrecondition, "invalid target directory")
	}
	m, err := volume.New(d.workDir, id, initial.Data)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "FUSE mount: %v", err)
	}
	if err = m.Publish(target); err != nil {
		closeErr := m.Close()
		return nil, status.Errorf(codes.Internal, "publish mount: %v", errors.Join(err, closeErr))
	}
	watch, cancel := context.WithCancel(context.Background())
	s := &session{id: id, mount: m, cancel: cancel, done: make(chan struct{})}
	d.sessions.Store(target, s)
	go d.watch(watch, s, initial.Version)
	return &csi.NodePublishVolumeResponse{}, nil
}

func (d *Driver) watch(ctx context.Context, s *session, version string) {
	defer close(s.done)
	backoff := time.Second
	for ctx.Err() == nil {
		next, err := d.source.Get(ctx, s.id, version)
		delay := time.Duration(0)
		if next != nil && err == nil {
			err = s.mount.Update(next.Data)
			if err == nil {
				version = next.Version
			}
		} else if err == nil {
			delay = 100 * time.Millisecond // bound request rate on immediate 304
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("volume %s update failed (%v), retrying", s.id, err)
			delay = backoff
			backoff = min(2*backoff, 30*time.Second)
		} else {
			backoff = time.Second
		}
		if delay > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
		}
	}
}

func (d *Driver) stop(target string, s *session) error {
	s.cancel()
	<-s.done
	if err := s.mount.Close(); err != nil {
		return status.Errorf(codes.Internal, "release FUSE mount: %v", err)
	}
	d.sessions.Delete(target)
	return nil
}

func (d *Driver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	id, target := req.GetVolumeId(), req.GetTargetPath()
	if err := validateTarget(id, target); err != nil {
		return nil, err
	}
	unlock := d.lock(target)
	defer unlock()
	item, tracked := d.sessions.Load(target)
	if tracked && item.(*session).id != id {
		return nil, status.Error(codes.FailedPrecondition, "volume ID mismatch")
	}
	mounted, err := inspect(id, target)
	if err != nil {
		return nil, err
	}
	if mounted {
		if err := unix.Unmount(target, 0); err != nil {
			return nil, status.Errorf(codes.Internal, "unmount target: %v", err)
		}
	}
	if tracked {
		if err := d.stop(target, item.(*session)); err != nil {
			return nil, err
		}
	}
	if err := unix.Rmdir(target); err != nil && !errors.Is(err, unix.ENOENT) {
		return nil, status.Errorf(codes.Internal, "remove target directory: %v", err)
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}
