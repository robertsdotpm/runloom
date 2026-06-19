// Baseline server: Go net echo, one goroutine per connection.
// Spec: cap Go's cores to int(cpu*0.7) via GOMAXPROCS.
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
)

func handle(c *net.TCPConn) {
	defer c.Close()
	c.SetNoDelay(true)
	buf := make([]byte, 65536)
	for {
		n, err := c.Read(buf)
		if err != nil {
			return
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
		go handle(c.(*net.TCPConn))
	}
}
