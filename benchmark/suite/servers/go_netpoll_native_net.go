// Baseline server: Go net echo, one goroutine per connection.
// Spec: cap Go's cores to int(cpu*0.7) via GOMAXPROCS.
//
// Acceptors: by default the server opens N SO_REUSEPORT listeners (N =
// GOMAXPROCS), one accept loop each, so the kernel load-balances incoming SYNs
// across N accept queues -- the same architecture as runloom's per-hub reuseport
// acceptors. This matters ONLY for connection-churn (accept in the hot loop); the
// persistent req/s / bandwidth paths accept once at establishment and are
// unaffected. Pass -acceptors 1 to reproduce the old single-Accept() baseline.
//
// -work N applies the SAME FNV-1a byte hash as the runloom work curve (identical
// constants) N times over each chunk before echoing, folded into byte 0 so it
// can't be elided. work=0 is the plain echo. This is the compiled/native
// reference for the cross-runtime handler work curve (multi-core via GOMAXPROCS).
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
	"syscall"
)

const (
	fnvOff   uint32 = 2166136261 // 0x811c9dc5
	fnvPrime uint32 = 16777619   // 0x01000193
	// SO_REUSEPORT (Linux); literal so we need no x/sys/unix dependency.
	soReusePort = 0xf
)

// goFnv: native uint32 FNV-1a, wraparound is automatic (no mask needed, unlike
// the Python twin) -- the same work, expressed the natural way in each language.
func goFnv(buf []byte, passes int) uint32 {
	h := fnvOff
	for p := 0; p < passes; p++ {
		for _, b := range buf {
			h = (h ^ uint32(b)) * fnvPrime
		}
	}
	return h
}

func handle(c *net.TCPConn, work int) {
	defer c.Close()
	c.SetNoDelay(true)
	buf := make([]byte, 65536)
	for {
		n, err := c.Read(buf)
		if err != nil {
			return
		}
		if work > 0 {
			h := goFnv(buf[:n], work)
			buf[0] ^= byte(h & 0xff) // fold in -> no elision
		}
		if _, err := c.Write(buf[:n]); err != nil {
			return
		}
	}
}

// listenReuseport binds addr with SO_REUSEPORT set, so multiple listeners can
// share the same host:port and the kernel distributes accepts across them.
func listenReuseport(addr string) (net.Listener, error) {
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var serr error
			if cerr := c.Control(func(fd uintptr) {
				serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, soReusePort, 1)
			}); cerr != nil {
				return cerr
			}
			return serr
		},
	}
	return lc.Listen(context.Background(), "tcp", addr)
}

func acceptLoop(ln net.Listener, work int) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go handle(c.(*net.TCPConn), work)
	}
}

func main() {
	host := flag.String("host", "10.99.0.1", "")
	port := flag.Int("port", 9000, "")
	gomax := flag.Int("gomaxprocs", int(float64(runtime.NumCPU())*0.7), "")
	work := flag.Int("work", 0, "FNV passes per chunk (0 = echo)")
	acceptors := flag.Int("acceptors", 0, "SO_REUSEPORT accept loops (0 = gomaxprocs; 1 = old single-Accept baseline)")
	flag.String("token", "", "")
	flag.Parse()
	runtime.GOMAXPROCS(*gomax)

	nacc := *acceptors
	if nacc <= 0 {
		nacc = *gomax
	}
	if nacc < 1 {
		nacc = 1
	}

	addr := fmt.Sprintf("%s:%d", *host, *port)
	// First listener reports the real port (covers port==0); the rest bind that
	// resolved address with SO_REUSEPORT.
	first, err := listenReuseport(addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "listen:", err)
		os.Exit(1)
	}
	la := first.Addr().(*net.TCPAddr)
	realAddr := fmt.Sprintf("%s:%d", *host, la.Port)
	fmt.Printf("LISTENING %d\n", la.Port)
	os.Stdout.Sync()

	go acceptLoop(first, *work)
	for i := 1; i < nacc; i++ {
		ln, err := listenReuseport(realAddr)
		if err != nil {
			fmt.Fprintln(os.Stderr, "listen acceptor", i, ":", err)
			os.Exit(1)
		}
		go acceptLoop(ln, *work)
	}
	select {} // accept loops run in goroutines; block main forever
}
