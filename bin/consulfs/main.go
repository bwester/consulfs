// consulfs is a command for mounting Consul-FS to your filesystem. A basic execution,
//
//     $ consulfs /mnt/kv
//
// will mount the local Consul agent's KV store to the `/mnt/kv` directory.
// Optionally, a two-argument form allows you specify the location of the Consul agent to
// contact:
//
//     $ consulfs http://consul0.mydomain.com:5678 /mnt/kv
//
// For more information about the file system itself, refer to the package documentation
// in the main "github.com/bwester/consulfs" package.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/Sirupsen/logrus"
	consul "github.com/hashicorp/consul/api"
	"golang.org/x/net/context"

	"github.com/bwester/consulfs"
)

// Default timeout for all requests. 60s is the default for OSXFUSE.
const defaultTimeout = "60s"

func init() {
	flag.Usage = func() {
		fmt.Fprintf(
			os.Stderr,
			"usage: %s [flags] [server_addr] mount_point\n\nAvailable flags:\n",
			filepath.Base(os.Args[0]),
		)
		flag.PrintDefaults()
		os.Exit(1)
	}
}

func main() {
	allowOther := flag.Bool("allow-other", false, "allow all users access to the filesystem")
	allowRoot := flag.Bool("allow-root", false, "allow root to access the filesystem")
	debug := flag.Bool("debug", false, "enable debug output")
	gid := flag.Int("gid", os.Getgid(), "set the GID that should own all files")
	perm := flag.Int("perm", 0, "set the file permission flags for all files")
	ro := flag.Bool("ro", false, "mount the filesystem read-only")
	root := flag.String("root", "", "path in Consul to the root of the filesystem")
	timeout := flag.String("timeout", defaultTimeout, "timeout for Consul requests")
	uid := flag.Int("uid", os.Getuid(), "set the UID that should own all files")
	flag.Parse()

	logger := logrus.New()
	if *debug {
		logger.Level = logrus.DebugLevel
	}

	consulConfig := &consul.Config{}
	var mountPoint string
	switch flag.NArg() {
	case 1:
		mountPoint = flag.Arg(0)
	case 2:
		consulConfig.Address = flag.Arg(0)
		mountPoint = flag.Arg(1)
	default:
		flag.Usage()
	}

	// Initialize a Consul client. TODO: connection parameters
	client, err := consul.NewClient(consulConfig)
	if err != nil {
		logrus.NewEntry(logger).WithError(err).Error("could not initialize consul")
		os.Exit(1)
	}

	// Configure some mount options
	timeoutDuration, err := time.ParseDuration(*timeout)
	if err != nil {
		logrus.NewEntry(logger).WithError(err).Fatal("invalid -timeout value")
	}
	mountOptions := []fuse.MountOption{
		fuse.DefaultPermissions(),
		fuse.DaemonTimeout(fmt.Sprint(int64(timeoutDuration.Seconds() + 1))),
		fuse.NoAppleDouble(),
		fuse.NoAppleXattr(),
	}
	if *allowOther {
		mountOptions = append(mountOptions, fuse.AllowOther())
	}
	if *allowRoot {
		mountOptions = append(mountOptions, fuse.AllowRoot())
	}
	if *ro {
		mountOptions = append(mountOptions, fuse.ReadOnly())
	}

	// Mount the file system to start receiving FS events at the mount point.
	logger.WithField("location", mountPoint).Info("mounting kvfs")
	conn, err := fuse.Mount(mountPoint, mountOptions...)
	if err != nil {
		logrus.NewEntry(logger).WithError(err).Fatal("error mounting kvfs")
	}
	defer conn.Close()

	// Try to cleanly unmount the FS if SIGINT or SIGTERM is received
	sigs := make(chan os.Signal, 10)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		for sig := range sigs {
			logger.WithField("signal", sig).Info("attempting to unmount")
			err := fuse.Unmount(mountPoint)
			if err != nil {
				logrus.NewEntry(logger).WithError(err).Error("cannot unmount")
			}
		}
	}()

	// Create a file system object and start handing its requests
	server := fs.New(conn, &fs.Config{
		Debug: func(m interface{}) { logger.Debug(m) },
		WithContext: func(ctx context.Context, req fuse.Request) context.Context {
			// The returned cancel function doesn't matter: the request handler will
			// cancel the parent context at the end of the request.
			newCtx, _ := context.WithTimeout(ctx, timeoutDuration)
			return newCtx
		},
	})
	f := &consulfs.ConsulFS{
		Consul: &consulfs.CancelConsulKV{
			Client: client,
			Logger: logger,
		},
		Logger:   logger,
		UID:      uint32(*uid),
		GID:      uint32(*gid),
		Perms:    os.FileMode(*perm),
		RootPath: *root,
	}
	err = server.Serve(f)
	if err != nil {
		// Not sure what would cause Serve() to exit with an error
		logrus.NewEntry(logger).WithError(err).Error("error serving filesystem")
	}

	// Wait for the FUSE connection to end
	<-conn.Ready
	if conn.MountError != nil {
		logrus.NewEntry(logger).WithError(conn.MountError).Error("unmount error")
		os.Exit(1)
	} else {
		logger.Info("file system exiting normally")
	}
}
