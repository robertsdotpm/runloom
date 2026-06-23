// Scale twin of tests_c/bench_server_py.py, in Go.
//
// Same topology: one accept-loop goroutine, N per-connection echo handler
// goroutines, N client goroutines, M round-trips each, 8-byte payload, RST
// close (SetLinger 0) to skip TIME_WAIT, client source IPs round-robined
// across 127.0.0.2..251 so the ephemeral-port pool isn't exhausted at high N.
//
// Reports the same columns: N, done/N, wall (first connect -> last done),
// K/s, peak RSS (VmHWM from /proc/self/status), GOMAXPROCS.
//
//   go run bench/bench_scale_go.go N [GOMAXPROCS] [M]
package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var payload = []byte("hellopyg") // 8 bytes, same as the runloom bench

const numSrcIPs = 250

func peakRSSkib() int64 {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return -1
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "VmHWM:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				v, _ := strconv.ParseInt(fields[1], 10, 64)
				return v
			}
		}
	}
	return -1
}

func rstClose(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetLinger(0) // RST on close -> no TIME_WAIT
	}
	c.Close()
}

func main() {
	N := 1024
	gomaxprocs := 8
	M := 1
	if len(os.Args) > 1 {
		N, _ = strconv.Atoi(os.Args[1])
	}
	if len(os.Args) > 2 {
		gomaxprocs, _ = strconv.Atoi(os.Args[2])
	}
	if len(os.Args) > 3 {
		M, _ = strconv.Atoi(os.Args[3])
	}
	runtime.GOMAXPROCS(gomaxprocs)

	// Lift fd limit to match the Python bench's intent.
	var rlim syscall.Rlimit
	if syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim) == nil {
		rlim.Cur = rlim.Max
		syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rlim)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	addr := ln.Addr().(*net.TCPAddr)
	port := addr.Port

	// Accept loop: one goroutine, spawns one echo handler per conn.
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

	var done int64
	var wg sync.WaitGroup
	wg.Add(N)

	dialer := func(idx int) {
		// Bind a round-robined source IP so the ephemeral-port pool per
		// source IP isn't exhausted at high N (matches the Python bench).
		srcIP := fmt.Sprintf("127.0.0.%d", 2+idx%numSrcIPs)
		d := net.Dialer{
			LocalAddr: &net.TCPAddr{IP: net.ParseIP(srcIP)},
		}
		c, err := d.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			wg.Done()
			return
		}
		buf := make([]byte, len(payload))
		ok := true
		for j := 0; j < M; j++ {
			if _, err := c.Write(payload); err != nil {
				ok = false
				break
			}
			if _, err := io.ReadFull(c, buf); err != nil {
				ok = false
				break
			}
		}
		rstClose(c)
		if ok {
			atomic.AddInt64(&done, 1)
		}
		wg.Done()
	}

	t0 := time.Now()
	for i := 0; i < N; i++ {
		go dialer(i)
	}
	wg.Wait()
	dt := time.Since(t0).Seconds()

	peak := peakRSSkib()
	thr := float64(N*M) / dt / 1000.0
	fmt.Printf("N=%d GOMAXPROCS=%d M=%d done=%d/%d %.3fs %.1fK/s peak_rss_kib=%d\n",
		N, gomaxprocs, M, atomic.LoadInt64(&done), N, dt, thr, peak)
	if int(atomic.LoadInt64(&done)) != N {
		fmt.Fprintf(os.Stderr, "FAIL: %d/%d completed\n", atomic.LoadInt64(&done), N)
		os.Exit(1)
	}
}
