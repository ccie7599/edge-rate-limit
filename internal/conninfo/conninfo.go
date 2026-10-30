// Package conninfo tags each server connection so handlers can report the
// network round trip to the caller (kernel TCP_INFO) and whether a request is
// the first on its connection. The EdgeWorkers clock doesn't advance during a
// request, so the edge can't time its own sub-requests; the adapter's side of
// the connection is the only place that sees the edge-to-adapter network cost.
package conninfo

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

type key struct{}

type info struct {
	conn net.Conn
	reqs atomic.Int64
}

// ConnContext is an http.Server ConnContext hook.
func ConnContext(ctx context.Context, c net.Conn) context.Context {
	return context.WithValue(ctx, key{}, &info{conn: c})
}

// Next counts a request on r's connection. It returns the kernel's smoothed
// RTT to the peer and the request's sequence on the connection (1 = the
// connection was opened for this request). ok is false when unavailable.
func Next(r *http.Request) (rtt time.Duration, seq int64, ok bool) {
	in, _ := r.Context().Value(key{}).(*info)
	if in == nil {
		return 0, 0, false
	}
	seq = in.reqs.Add(1)
	c := in.conn
	if t, isTLS := c.(*tls.Conn); isTLS {
		c = t.NetConn()
	}
	tc, isTCP := c.(*net.TCPConn)
	if !isTCP {
		return 0, seq, false
	}
	raw, err := tc.SyscallConn()
	if err != nil {
		return 0, seq, false
	}
	var ti *unix.TCPInfo
	raw.Control(func(fd uintptr) { ti, err = unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO) })
	if err != nil || ti == nil {
		return 0, seq, false
	}
	return time.Duration(ti.Rtt) * time.Microsecond, seq, true
}
