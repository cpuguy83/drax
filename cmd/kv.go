package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net"

	"github.com/Sirupsen/logrus"
	"github.com/cpuguy83/drax"
	"github.com/docker/docker/pkg/signal"
)

var (
	flHome = flag.String("home", "/var/lib/raftkv", "path to db local copy")
	flAddr = flag.String("addr", fmt.Sprintf("127.0.0.1:%s", raftkv.DefaultRaftPort), "address to bind")
	flPeer = flag.String("peer", "", "address of peer")
)

func main() {
	flag.Parse()
	logrus.SetLevel(logrus.DebugLevel)

	var (
		tlsConfig *tls.Config
		l         net.Listener
		err       error
	)

	if tlsConfig == nil {
		l, err = net.Listen("tcp", *flAddr)
	} else {
		l, err = tls.Listen("tcp", *flAddr, tlsConfig)
	}
	if err != nil {
		logrus.Fatalf("error setting up listener: %v", err)
	}

	cluster, err := raftkv.New(l, *flHome, l.Addr().String(), *flPeer, tlsConfig)
	if err != nil {
		logrus.Fatalf("Error setting up cluster: %v", err)
	}

	signal.Trap(func() {
		logrus.Warn("Shutting down")
		cluster.Shutdown()
	})

	for {
		select {
		case err := <-cluster.Errors():
			if err != nil {
				logrus.Error(err)
			}
		case <-cluster.ShutdownCh():
			return
		}
	}
}
