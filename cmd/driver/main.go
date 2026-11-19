package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"csi-fuse-inotify/internal/backend"
	"csi-fuse-inotify/internal/driver"
	"csi-fuse-inotify/internal/volume"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
)

func protectMemory() error {
	if err := unix.Setrlimit(unix.RLIMIT_CORE, &unix.Rlimit{}); err != nil {
		return err
	}
	if err := unix.Prctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0); err != nil {
		return err
	}
	return unix.Mlockall(unix.MCL_CURRENT | unix.MCL_FUTURE | unix.MCL_ONFAULT)
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "demo" {
		flags := flag.NewFlagSet("demo", flag.ExitOnError)
		target := flags.String("mount", "", "existing empty mount directory")
		interval := flags.Duration("interval", 2*time.Second, "time between synthetic revisions")
		flags.Parse(os.Args[2:])
		if *target == "" || *interval <= 0 || flags.NArg() != 0 {
			log.Fatal("usage: driver demo -mount DIR [-interval 2s]")
		}
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		if err := volume.RunDemo(ctx, *target, *interval); err != nil {
			log.Fatal(err)
		}
		return
	}
	socket := flag.String("socket", "/csi/csi.sock", "CSI Unix socket path")
	node := flag.String("node-id", os.Getenv("NODE_ID"), "Kubernetes node name")
	endpoint := flag.String("backend", os.Getenv("BACKEND_URL"), "HTTPS base URL")
	work := flag.String("work-dir", "/run/ramvalue", "private FUSE mount directory")
	flag.Parse()
	if *node == "" {
		log.Fatal("node-id required")
	}
	if err := protectMemory(); err != nil {
		log.Fatalf("cannot protect process memory: %v", err)
	}
	source, err := backend.New(*endpoint)
	if err != nil {
		log.Fatal(err)
	}
	if err = os.MkdirAll(*work, 0700); err != nil {
		log.Fatal(err)
	}
	if err = os.Chmod(*work, 0700); err != nil {
		log.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Dir(*socket), 0755); err != nil {
		log.Fatal(err)
	}
	if info, err := os.Lstat(*socket); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			log.Fatal("CSI endpoint is not a socket")
		}
		if err = os.Remove(*socket); err != nil {
			log.Fatal(err)
		}
	} else if !os.IsNotExist(err) {
		log.Fatal(err)
	}
	unix.Umask(0077)
	l, err := net.Listen("unix", *socket)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	d := driver.New(*node, *work, source)
	g := grpc.NewServer()
	csi.RegisterIdentityServer(g, d)
	csi.RegisterNodeServer(g, d)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go func() { <-ctx.Done(); g.Stop() }()
	if err = g.Serve(l); err != nil {
		log.Fatal(err)
	}
}
