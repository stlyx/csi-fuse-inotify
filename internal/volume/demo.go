package volume

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// RunDemo mounts a synthetic value using the unprivileged fusermount helper.
func RunDemo(ctx context.Context, target string, interval time.Duration) (err error) {
	target, err = filepath.Abs(target)
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(target)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return fmt.Errorf("mount directory must be empty: %s", target)
	}

	m, err := mountAt(target, []byte("revision 0\n"), true, fuse.MountOptions{
		Name: "ramvalue", FsName: "ramvalue-demo", DisableXAttrs: true,
	})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, m.server.Unmount()) }()
	// Pin the writer to this FUSE inode for the lifetime of the demo.
	writer, err := os.OpenFile(filepath.Join(target, "value"), os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, writer.Close()) }()
	log.Printf("demo mounted at %s, updating value every %s", target, interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for revision := uint64(1); ; revision++ {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := m.writeSnapshot(writer, fmt.Appendf(nil, "revision %d\n", revision)); err != nil {
				return err
			}
		}
	}
}
