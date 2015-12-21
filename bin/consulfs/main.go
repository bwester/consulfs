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

	"github.com/bwester/consulfs/Godeps/_workspace/src/bazil.org/fuse"
	"github.com/bwester/consulfs/Godeps/_workspace/src/bazil.org/fuse/fs"
	"github.com/bwester/consulfs/Godeps/_workspace/src/github.com/Sirupsen/logrus"
	consul "github.com/bwester/consulfs/Godeps/_workspace/src/github.com/hashicorp/consul/api"

	"github.com/bwester/consulfs"
)

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
	debug := flag.Bool("debug", false, "enable debug output")
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

	// Mount the file system to start receiving FS events at the mount point.
	logger.WithField("location", mountPoint).Info("mounting kvfs")
	conn, err := fuse.Mount(mountPoint)
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
	})
	f := &consulfs.ConsulFs{
		Consul: &consulfs.CancelConsulKv{
			Client: client,
			Logger: logger,
		},
		Logger: logger,
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
