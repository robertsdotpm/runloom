// Baseline server: Go net echo, one goroutine per connection.
// Spec: cap Go's cores to int(cpu*0.7) via GOMAXPROCS.
//
// -work N applies the SAME FNV-1a byte hash as the runloom work curve (identical
// constants) N times over each chunk before echoing, folded into byte 0 so it
// can't be elided. work=0 is the plain echo. This is the compiled/native
// reference for the cross-runtime handler work curve (multi-core via GOMAXPROCS).
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
)

const (
	fnvOff   uint32 = 2166136261 // 0x811c9dc5
	fnvPrime uint32 = 16777619   // 0x01000193
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

func main() {
	host := flag.String("host", "10.99.0.1", "")
	port := flag.Int("port", 9000, "")
	gomax := flag.Int("gomaxprocs", int(float64(runtime.NumCPU())*0.7), "")
	work := flag.Int("work", 0, "FNV passes per chunk (0 = echo)")
	flag.String("token", "", "")
	flag.Parse()
	runtime.GOMAXPROCS(*gomax)

	addr := fmt.Sprintf("%s:%d", *host, *port)
	lc := net.ListenConfig{}
	_ = lc
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "listen:", err)
		os.Exit(1)
	}
	la := ln.Addr().(*net.TCPAddr)
	fmt.Printf("LISTENING %d\n", la.Port)
	os.Stdout.Sync()
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go handle(c.(*net.TCPConn), *work)
	}
}
