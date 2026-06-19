// loadgen -- the Go closed-loop echo load generator (spec: client language = go).
//
// Method (spec): open `conns` persistent keepalive connections to the server,
// disable Nagle, then on each connection repeatedly send the payload and read it
// back (closed loop).  Count round-trips over a fixed measurement window after a
// ramp/warmup phase; req/s = round-trips / window.  A lock-free log-bucket
// histogram records per-round-trip latency for p50/p99/p99.9 (no sampling bias,
// bounded memory).  Emits one JSON object on stdout.
//
// Decisions baked in: TCP_NODELAY is set once per connection at setup, never in
// the per-request loop (decision #6).  GOMAXPROCS is the client core budget
// (spec: int(cpu*0.25)).  Per-connection buffers are sized to the payload, so
// the small-payload req/s run and the few-connection 1.5 MB bandwidth run both
// stay within memory.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// ---- lock-free latency histogram (log-spaced buckets, ~1.04x each) ----
const (
	nBuckets = 512
	logBase  = 1.04
)

var lnBase = math.Log(logBase)

type hist struct {
	b [nBuckets]int64
}

func bucketOf(ns int64) int {
	if ns < 1 {
		ns = 1
	}
	us := float64(ns) / 1000.0
	if us < 1 {
		return 0
	}
	i := int(math.Log(us) / lnBase)
	if i < 0 {
		i = 0
	}
	if i >= nBuckets {
		i = nBuckets - 1
	}
	return i
}

func bucketLowUS(i int) float64 { return math.Pow(logBase, float64(i)) }

func (h *hist) record(ns int64) { atomic.AddInt64(&h.b[bucketOf(ns)], 1) }

func (h *hist) merge(o *hist) {
	for i := range h.b {
		h.b[i] += o.b[i]
	}
}

func (h *hist) total() int64 {
	var t int64
	for _, c := range h.b {
		t += c
	}
	return t
}

// percentile returns the bucket-low latency in microseconds at quantile q.
func (h *hist) pct(q float64) float64 {
	total := h.total()
	if total == 0 {
		return 0
	}
	target := int64(q * float64(total))
	var cum int64
	for i, c := range h.b {
		cum += c
		if cum >= target {
			return bucketLowUS(i)
		}
	}
	return bucketLowUS(nBuckets - 1)
}

func main() {
	addr := flag.String("addr", "10.99.0.1:9000", "server address")
	conns := flag.Int("conns", 64, "concurrent persistent connections")
	payload := flag.Int("payload", 1024, "bytes sent+echoed per round trip")
	ramp := flag.Float64("ramp", 2.0, "ramp/warmup seconds (not counted)")
	measure := flag.Float64("measure", 5.0, "measurement window seconds")
	gomax := flag.Int("gomaxprocs", runtime.NumCPU()/4, "GOMAXPROCS (client cores)")
	flag.Parse()
	runtime.GOMAXPROCS(*gomax)

	var measuring atomic.Bool
	var totalReqs atomic.Int64
	var connErrs atomic.Int64
	var establishErrs atomic.Int64
	var stop atomic.Bool

	sendTemplate := make([]byte, *payload)
	for i := range sendTemplate {
		sendTemplate[i] = byte(i)
	}

	connsList := make([]net.Conn, 0, *conns)
	var listMu sync.Mutex
	perHist := make([]hist, *conns)
	var wg sync.WaitGroup

	worker := func(idx int, c *net.TCPConn) {
		defer wg.Done()
		send := make([]byte, *payload)
		copy(send, sendTemplate)
		recvbuf := make([]byte, *payload)
		h := &perHist[idx]
		for !stop.Load() {
			t0 := time.Now()
			if _, err := c.Write(send); err != nil {
				if !stop.Load() {
					connErrs.Add(1)
				}
				return
			}
			if _, err := io.ReadFull(c, recvbuf); err != nil {
				if !stop.Load() {
					connErrs.Add(1)
				}
				return
			}
			if measuring.Load() {
				totalReqs.Add(1)
				h.record(time.Since(t0).Nanoseconds())
			}
		}
	}

	// Establish all connections first (spec: connect num conns, then measure).
	// Dial in PARALLEL (capped) so high connection counts don't serialise into
	// a multi-second ramp -- sequential dials made the 8k+ rungs time out.
	d := net.Dialer{Timeout: 5 * time.Second}
	var estWg sync.WaitGroup
	sem := make(chan struct{}, 512)
	for i := 0; i < *conns; i++ {
		estWg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer estWg.Done()
			defer func() { <-sem }()
			c, err := d.Dial("tcp", *addr)
			if err != nil {
				establishErrs.Add(1)
				return
			}
			tcp := c.(*net.TCPConn)
			tcp.SetNoDelay(true)
			listMu.Lock()
			connsList = append(connsList, c)
			listMu.Unlock()
			wg.Add(1)
			go worker(idx, tcp)
		}(i)
	}
	estWg.Wait()

	live := len(connsList)
	if live == 0 {
		out := map[string]any{"error": "no connections established",
			"establish_errors": establishErrs.Load(), "requested_conns": *conns}
		b, _ := json.Marshal(out)
		fmt.Println(string(b))
		os.Exit(1)
	}

	time.Sleep(time.Duration(*ramp * float64(time.Second)))
	measuring.Store(true)
	t0 := time.Now()
	time.Sleep(time.Duration(*measure * float64(time.Second)))
	measuring.Store(false)
	elapsed := time.Since(t0).Seconds()

	stop.Store(true)
	listMu.Lock()
	for _, c := range connsList {
		c.SetDeadline(time.Now())
	}
	listMu.Unlock()
	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
	}

	var agg hist
	for i := range perHist {
		agg.merge(&perHist[i])
	}
	reqs := totalReqs.Load()
	rps := float64(reqs) / elapsed
	out := map[string]any{
		"requested_conns":  *conns,
		"live_conns":       live,
		"payload":          *payload,
		"gomaxprocs":       *gomax,
		"measure_s":        elapsed,
		"reqs":             reqs,
		"rps":              rps,
		"bytes_per_s":      rps * float64(*payload) * 2, // sent + echoed
		"p50_us":           agg.pct(0.50),
		"p99_us":           agg.pct(0.99),
		"p999_us":          agg.pct(0.999),
		"max_us":           agg.pct(1.0),
		"conn_errors":      connErrs.Load(),
		"establish_errors": establishErrs.Load(),
	}
	b, _ := json.Marshal(out)
	fmt.Println(string(b))
}
