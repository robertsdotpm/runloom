// bench_throughput_go.go -- STEADY-STATE echo throughput, canonical Go.
//
// The fair twin of tests_c/bench_throughput_py.py: measure the runtime's real
// req/s with N concurrent connections, with connection setup OUTSIDE the
// measured window so the number reflects the scheduler + netpoller, not the
// accept/connect ramp.
//
// Canonical Go max-concurrency idioms used here:
//   * goroutine-per-connection (the Go model): one handler goroutine per conn,
//     plain blocking Read/Write -- the runtime netpoller multiplexes.
//   * SO_REUSEPORT with ACCEPTORS listeners on one port, each with its own
//     accept goroutine, so establishment is kernel-load-balanced, not
//     serialized through a single Accept loop.
//   * GOMAXPROCS pinned, per-goroutine reused buffers (no per-RT allocation),
//     a WaitGroup barrier until all N are connected, then an atomic counter
//     over a fixed window closed by a stop channel.
//
//   go run bench/bench_throughput_go.go N [GOMAXPROCS] [MEASURE_S] [WARMUP_S]
package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

var payload = []byte("hellopyg")

const (
	numSrcIPs = 250
	acceptors = 64
)

func reusePortControl(network, address string, c syscall.RawConn) error {
	var ctrlErr error
	err := c.Control(func(fd uintptr) {
		ctrlErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
	})
	if err != nil {
		return err
	}
	return ctrlErr
}

func curRSSkib() int64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return -1
	}
	for _, line := range splitLines(string(data)) {
		if len(line) >= 6 && line[:6] == "VmRSS:" {
			var v int64
			fmt.Sscanf(line[6:], "%d", &v)
			return v
		}
	}
	return -1
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return out
}

func main() {
	N := 1024
	gomaxprocs := 8
	measureS := 3.0
	warmupS := 1.0
	if len(os.Args) > 1 {
		N, _ = strconv.Atoi(os.Args[1])
	}
	if len(os.Args) > 2 {
		gomaxprocs, _ = strconv.Atoi(os.Args[2])
	}
	if len(os.Args) > 3 {
		measureS, _ = strconv.ParseFloat(os.Args[3], 64)
	}
	if len(os.Args) > 4 {
		warmupS, _ = strconv.ParseFloat(os.Args[4], 64)
	}
	runtime.GOMAXPROCS(gomaxprocs)

	var rlim syscall.Rlimit
	if syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim) == nil {
		rlim.Cur = rlim.Max
		syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rlim)
	}

	lc := net.ListenConfig{Control: reusePortControl}

	// First listener fixes the port; the rest reuse it via SO_REUSEPORT.
	l0, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	port := l0.Addr().(*net.TCPAddr).Port
	listeners := []net.Listener{l0}
	for i := 1; i < acceptors; i++ {
		ln, err := lc.Listen(context.Background(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			panic(err)
		}
		listeners = append(listeners, ln)
	}

	var stop int32              // 0 -> running, 1 -> stop
	var rts int64               // atomic round-trip counter
	var connected int64         // clients that finished connecting

	// Accept goroutines (one per REUSEPORT listener): goroutine-per-conn.
	for _, ln := range listeners {
		go func(ln net.Listener) {
			for atomic.LoadInt32(&stop) == 0 {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				go func(c net.Conn) {
					buf := make([]byte, len(payload))
					for atomic.LoadInt32(&stop) == 0 {
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
		}(ln)
	}

	var wg sync.WaitGroup
	wg.Add(N)
	dial := func(idx int) {
		defer wg.Done()
		srcIP := fmt.Sprintf("127.0.0.%d", 2+idx%numSrcIPs)
		d := net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP(srcIP)}}
		c, err := d.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			return
		}
		atomic.AddInt64(&connected, 1)
		buf := make([]byte, len(payload))
		for atomic.LoadInt32(&stop) == 0 {
			if _, err := c.Write(payload); err != nil {
				break
			}
			if _, err := io.ReadFull(c, buf); err != nil {
				break
			}
			atomic.AddInt64(&rts, 1)
		}
		if tc, ok := c.(*net.TCPConn); ok {
			tc.SetLinger(0)
		}
		c.Close()
	}

	// Ramp: establish all N connections (not measured).
	tRamp0 := time.Now()
	for i := 0; i < N; i++ {
		go dial(i)
	}
	for atomic.LoadInt64(&connected) < int64(N) {
		time.Sleep(10 * time.Millisecond)
		if time.Since(tRamp0).Seconds() > 120 {
			break
		}
	}
	rampS := time.Since(tRamp0).Seconds()
	established := atomic.LoadInt64(&connected)

	// Warmup (not counted), then the measured window.
	time.Sleep(time.Duration(warmupS * float64(time.Second)))
	rssLive := curRSSkib()
	start := atomic.LoadInt64(&rts)
	t0 := time.Now()
	time.Sleep(time.Duration(measureS * float64(time.Second)))
	win := time.Since(t0).Seconds()
	end := atomic.LoadInt64(&rts)
	atomic.StoreInt32(&stop, 1)

	winRTs := end - start
	thr := float64(winRTs) / win / 1000.0
	fmt.Printf("N=%d GOMAXPROCS=%d established=%d/%d ramp=%.2fs window=%.2fs "+
		"rts=%d %.1fK req/s rss_live_kib=%d\n",
		N, gomaxprocs, established, N, rampS, win, winRTs, thr, rssLive)
}
