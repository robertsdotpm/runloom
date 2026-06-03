// loadgen.go -- common neutral load generator for the pygo/Go/asyncio
// I/O-bound concurrency comparison.  Opens N long-lived keepalive TCP
// connections (staggered over a ramp), each looping: write a fixed REQ,
// read a fixed RESP, record the round-trip latency (only inside the
// measure window), optional client think-gap, repeat.  Reports in-window
// throughput + p50/p99/p99.9 from per-connection histograms (zero shared
// contention; merged at the end).  The SERVER decides the simulated I/O
// delay -- the loadgen is identical for every runtime under test.
package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	reqLen  = 10   // "GET /work\n"
	respLen = 1029 // "200 " + 1024*'x' + "\n"
	nBucket = 4096 // histogram buckets
	usPerB  = 100  // 100 us per bucket -> 0..409.6 ms range
)

var req = []byte("GET /work\n")

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "server address")
	n := flag.Int("n", 1000, "concurrent keepalive connections")
	thinkMs := flag.Float64("think", 0, "client think-gap between requests (ms)")
	rampS := flag.Float64("ramp", 3, "connection establishment stagger (s)")
	warmupS := flag.Float64("warmup", 3, "run-but-don't-record window (s)")
	measureS := flag.Float64("measure", 10, "measured steady-state window (s)")
	flag.Parse()

	think := time.Duration(*thinkMs * float64(time.Millisecond))
	t0 := time.Now()
	measureStart := t0.Add(time.Duration(*rampS * float64(time.Second))).
		Add(time.Duration(*warmupS * float64(time.Second)))
	measureEnd := measureStart.Add(time.Duration(*measureS * float64(time.Second)))

	hists := make([][]int32, *n) // per-conn histogram, merged at end
	maxUs := make([]int64, *n)   // per-conn exact max
	var established, completedInWin int64
	var wg sync.WaitGroup

	for i := 0; i < *n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			h := make([]int32, nBucket)
			hists[id] = h
			// stagger establishment across the ramp
			if *rampS > 0 && *n > 1 {
				time.Sleep(time.Duration(float64(id) / float64(*n) * *rampS * float64(time.Second)))
			}
			c, err := net.Dial("tcp", *addr)
			if err != nil {
				return
			}
			defer c.Close()
			if tc, ok := c.(*net.TCPConn); ok {
				tc.SetNoDelay(true)
				tc.SetLinger(0) // RST on close -> no client-side TIME_WAIT across runs
			}
			atomic.AddInt64(&established, 1)
			resp := make([]byte, respLen)
			for time.Now().Before(measureEnd) {
				start := time.Now()
				if _, err := c.Write(req); err != nil {
					return
				}
				if _, err := io.ReadFull(c, resp); err != nil {
					return
				}
				end := time.Now()
				if !end.Before(measureStart) && end.Before(measureEnd) {
					us := end.Sub(start).Microseconds()
					b := us / usPerB
					if b < 0 {
						b = 0
					} else if b >= nBucket {
						b = nBucket - 1
					}
					h[b]++
					if us > maxUs[id] {
						maxUs[id] = us
					}
					atomic.AddInt64(&completedInWin, 1)
				}
				if think > 0 {
					time.Sleep(think)
				}
			}
		}(i)
	}
	wg.Wait()

	// merge histograms
	merged := make([]int64, nBucket)
	var total, gmax int64
	for i := 0; i < *n; i++ {
		if hists[i] != nil {
			for b := 0; b < nBucket; b++ {
				merged[b] += int64(hists[i][b])
				total += int64(hists[i][b])
			}
		}
		if maxUs[i] > gmax {
			gmax = maxUs[i]
		}
	}
	pct := func(q float64) float64 {
		target := int64(q * float64(total))
		var cum int64
		for b := 0; b < nBucket; b++ {
			cum += merged[b]
			if cum >= target {
				return float64(b) * float64(usPerB) / 1000.0 // ms
			}
		}
		return float64(nBucket) * float64(usPerB) / 1000.0
	}
	rps := float64(completedInWin) / *measureS
	overflow := merged[nBucket-1]
	fmt.Printf("conns=%d established=%d in_win_reqs=%d rps=%.0f "+
		"p50=%.2fms p99=%.2fms p99.9=%.2fms max=%.1fms overflow_bucket=%d\n",
		*n, established, completedInWin, rps,
		pct(0.50), pct(0.99), pct(0.999), float64(gmax)/1000.0, overflow)
	if established < int64(*n) {
		fmt.Fprintf(os.Stderr, "WARN: only %d/%d connections established\n", established, *n)
	}
}
