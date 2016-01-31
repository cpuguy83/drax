package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/cpuguy83/drax"
	"github.com/cpuguy83/drax/rpc"
	"github.com/docker/docker/pkg/signal"
)

var (
	flHome = flag.String("home", "/var/lib/drax", "path to db local copy")
	flAddr = flag.String("addr", fmt.Sprintf("127.0.0.1:%s", drax.DefaultRaftPort), "address to bind")
	flPeer = flag.String("peer", "", "address of peer")
)

func main() {
	flag.Parse()
	logrus.SetLevel(logrus.DebugLevel)

	var (
		tlsConfig *tls.Config
		l         net.Listener
		err       error
		dialer    rpc.DialerFn
	)

	if tlsConfig == nil {
		l, err = net.Listen("tcp", *flAddr)
		dialer = func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("tcp", addr, timeout)
		}
	} else {
		l, err = tls.Listen("tcp", *flAddr, tlsConfig)
		d := &net.Dialer{
			Timeout: 30 * time.Second,
		}
		dialer = func(addr string, timeout time.Duration) (net.Conn, error) {
			return tls.DialWithDialer(d, "tcp", *flAddr, tlsConfig)
		}
	}
	if err != nil {
		logrus.Fatalf("error setting up listener: %v", err)
	}

	cluster, err := drax.New(l, dialer, *flHome, l.Addr().String(), *flPeer)
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
