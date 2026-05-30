package main

import (
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"time"
)

var payload = []byte("hellopyg") // 8 bytes, same as pygo bench

var work = [][2]int{
	{1, 1000}, {8, 1000}, {64, 500}, {256, 200},
	{512, 100}, {1024, 50}, {2048, 25}, {4096, 10},
}

// Same shape as examples/bench_tcpconn_concurrent.py: N concurrent client
// conns, each does M 8-byte echo round-trips against an in-process server
// (one handler goroutine per conn). Aggregate K/s = N*M/wall.
func bench(N, M int) float64 {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	addr := ln.Addr().String()

	go func() {
		for i := 0; i < N; i++ {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				buf := make([]byte, len(payload))
				for j := 0; j < M; j++ {
					if _, err := io.ReadFull(c, buf); err != nil {
						break
					}
					if _, err := c.Write(buf); err != nil {
						break
					}
				}
				c.Close()
			}(c)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(N)
	t0 := time.Now()
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			c, err := net.Dial("tcp", addr) // Go sets TCP_NODELAY by default
			if err != nil {
				return
			}
			buf := make([]byte, len(payload))
			for j := 0; j < M; j++ {
				if _, err := c.Write(payload); err != nil {
					break
				}
				if _, err := io.ReadFull(c, buf); err != nil {
					break
				}
			}
			c.Close()
		}()
	}
	wg.Wait()
	dt := time.Since(t0).Seconds()
	ln.Close()
	return dt
}

func main() {
	fmt.Printf("GOMAXPROCS=%d\n", runtime.GOMAXPROCS(0))
	fmt.Printf("%6s %6s %12s %10s\n", "N", "M", "Go K/s", "us/RT")
	for _, w := range work {
		N, M := w[0], w[1]
		dt := bench(N, M)
		total := float64(N * M)
		fmt.Printf("%6d %6d %12.1f %10.1f\n", N, M, total/dt/1000, dt*1e6/total)
	}
}
