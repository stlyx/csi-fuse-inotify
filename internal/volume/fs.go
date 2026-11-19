// Package volume serves backend snapshots through a stable FUSE inode.
package volume

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

type snapshot struct {
	data    []byte
	updated time.Time
}

type valueFile struct {
	fs.Inode
	demo    bool // The notifier and readers share a mount in the local demo.
	current atomic.Pointer[snapshot]
	pending atomic.Pointer[snapshot]
}

type root struct {
	fs.Inode
	file *valueFile
}

func (r *root) OnAdd(ctx context.Context) {
	child := r.NewPersistentInode(ctx, r.file, fs.StableAttr{Mode: syscall.S_IFREG, Ino: 2})
	r.AddChild("value", child, false)
}

func (f *valueFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	s := f.current.Load()
	out.Mode = syscall.S_IFREG | 0444
	out.Size = uint64(len(s.data))
	out.SetTimes(nil, &s.updated, &s.updated)
	return 0
}

// A read handle pins one complete revision across multiple kernel READs.
type reader struct{ snap *snapshot }

func (r *reader) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if off < 0 {
		return nil, syscall.EINVAL
	}
	if off >= int64(len(r.snap.data)) {
		return fuse.ReadResultData(nil), 0
	}
	data := r.snap.data[off:]
	return fuse.ReadResultData(data[:min(len(dest), len(data))]), 0
}

type notifier struct{ file *valueFile }

func (n *notifier) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	if off != 0 || len(data) != 1 {
		return 0, syscall.EINVAL
	}
	next := n.file.pending.Swap(nil)
	if next == nil {
		return 0, syscall.EPERM
	}
	n.file.current.Store(next)
	return 1, 0
}

func (f *valueFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&syscall.O_ACCMODE == syscall.O_RDONLY {
		return &reader{f.current.Load()}, fuse.FOPEN_DIRECT_IO, 0
	}
	// CSI publishes a read-only clone. The local demo uses one mount and
	// restricts the notifier handle to the serving process itself.
	if flags&syscall.O_ACCMODE == syscall.O_WRONLY {
		if f.demo {
			caller, ok := fuse.FromContext(ctx)
			if !ok {
				return nil, 0, syscall.EROFS
			}
			// Linux FUSE may identify the calling thread by TID.
			if _, err := os.Stat("/proc/self/task/" + strconv.FormatUint(uint64(caller.Pid), 10)); err != nil {
				return nil, 0, syscall.EROFS
			}
		}
		return &notifier{f}, fuse.FOPEN_DIRECT_IO, 0
	}
	return nil, 0, syscall.EROFS
}

type Mount struct {
	dir    string
	server *fuse.Server
	file   *valueFile
}

func New(workDir, source string, data []byte) (*Mount, error) {
	dir, err := os.MkdirTemp(workDir, "mount-")
	if err != nil {
		return nil, err
	}
	m, err := mountAt(dir, data, false, fuse.MountOptions{
		Name:              "ramvalue",
		FsName:            source,
		AllowOther:        true,
		DirectMountStrict: true,
		Options:           []string{"default_permissions"},
		DisableXAttrs:     true,
	})
	if err != nil {
		os.Remove(dir)
	}
	return m, err
}

func mountAt(dir string, data []byte, demo bool, options fuse.MountOptions) (*Mount, error) {
	file := &valueFile{demo: demo}
	file.current.Store(&snapshot{data, time.Now()})
	zero := time.Duration(0)
	server, err := fs.Mount(dir, &root{file: file}, &fs.Options{
		AttrTimeout: &zero, EntryTimeout: &zero, MountOptions: options,
	})
	if err != nil {
		return nil, err
	}
	return &Mount{dir, server, file}, nil
}

func (m *Mount) Publish(target string) error {
	tree, err := unix.OpenTree(unix.AT_FDCWD, m.dir, unix.OPEN_TREE_CLONE|unix.OPEN_TREE_CLOEXEC)
	if err != nil {
		return err
	}
	defer unix.Close(tree)
	flags := uint64(unix.MOUNT_ATTR_RDONLY | unix.MOUNT_ATTR_NODEV | unix.MOUNT_ATTR_NOSUID | unix.MOUNT_ATTR_NOEXEC)
	if err := unix.MountSetattr(tree, "", unix.AT_EMPTY_PATH, &unix.MountAttr{Attr_set: flags}); err != nil {
		return err
	}
	return unix.MoveMount(tree, "", unix.AT_FDCWD, target, unix.MOVE_MOUNT_F_EMPTY_PATH)
}

// Update has one caller per mount. A real VFS write generates IN_MODIFY on
// the same inode seen through the bind mount, even for an empty new value.
func (m *Mount) Update(data []byte) error {
	fd, err := os.OpenFile(filepath.Join(m.dir, "value"), os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	return errors.Join(m.writeSnapshot(fd, data), fd.Close())
}

func (m *Mount) writeSnapshot(fd *os.File, data []byte) error {
	m.file.pending.Store(&snapshot{data, time.Now()})
	_, writeErr := fd.WriteAt([]byte{0}, 0)
	m.file.pending.Store(nil)
	return writeErr
}

func (m *Mount) Close() error {
	if err := m.server.Unmount(); err != nil {
		return err
	}
	m.server.Wait()
	return os.Remove(m.dir)
}
