// Package natsembed runs the adapter's in-process NATS server with JetStream,
// optionally clustered with the other regions' adapters.
package natsembed

import (
	"fmt"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

type Config struct {
	Name        string
	StoreDir    string
	ClientPort  int // -1 picks a random port
	ClusterPort int // 0 disables clustering
	Routes      string
	Advertise   string // host:port other regions dial for the cluster route
	Region      string
	Debug       bool
}

func Start(c Config) (*natsserver.Server, error) {
	opts := &natsserver.Options{
		ServerName: c.Name,
		Host:       "127.0.0.1",
		Port:       c.ClientPort,
		JetStream:  true,
		StoreDir:   c.StoreDir,
		NoSigs:     true,
		Debug:      c.Debug,
	}
	if c.Region != "" {
		opts.Tags = []string{"region:" + c.Region}
		opts.JetStreamUniqueTag = "region"
	}
	if c.ClusterPort != 0 {
		// Client port stays on loopback; only the route port is exposed.
		opts.Cluster = natsserver.ClusterOpts{Name: "rl-mesh", Host: "0.0.0.0", Port: c.ClusterPort, Advertise: c.Advertise}
		if c.Routes != "" {
			opts.Routes = natsserver.RoutesFromStr(c.Routes)
		}
	}
	ns, err := natsserver.NewServer(opts)
	if err != nil {
		return nil, err
	}
	ns.ConfigureLogger()
	go ns.Start()
	if !ns.ReadyForConnections(60 * time.Second) {
		return nil, fmt.Errorf("nats %s not ready", c.Name)
	}
	return ns, nil
}
