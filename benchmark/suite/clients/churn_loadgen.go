// churn_loadgen -- connection-CHURN load generator: the metric the persistent
// loadgen deliberately avoids.  Each worker loops: dial -> send payload -> read
// it back -> CLOSE -> repeat, as hard as it can.  So the server pays
// accept + spawn-a-handler + serve + teardown for EVERY counted unit, in the hot
// loop -- this is conn/s (a.k.a. "new connection per request"), where the
// per-connection spawn cost actually lands.  Counterpart to loadgen.go (req/s on
// persistent keepalive connections).  Emits one JSON object on stdout.
//
// 1 request per connection, so conn/s == req/s for this workload, but every
// request is a fresh connection -> a fresh server-side handler spawn.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	nBuckets = 512
	logBase  = 1.04
)

var lnBase = math.Log(logBase)

type hist struct{ b [nBuckets]int64 }

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
func (h *hist) record(ns int64)  { atomic.AddInt64(&h.b[bucketOf(ns)], 1) }
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
	workers := flag.Int("conns", 64, "concurrent dialers (in-flight connections)")
	payload := flag.Int("payload", 64, "bytes sent+echoed per connection")
	ramp := flag.Float64("ramp", 2.0, "ramp/warmup seconds (not counted)")
	measure := flag.Float64("measure", 5.0, "measurement window seconds")
	gomax := flag.Int("gomaxprocs", runtime.NumCPU()/4, "GOMAXPROCS (client cores)")
	flag.Parse()
	runtime.GOMAXPROCS(*gomax)

	var measuring atomic.Bool
	var totalConns atomic.Int64
	var dialErrs, ioErrs atomic.Int64
	var stop atomic.Bool

	send := make([]byte, *payload)
	for i := range send {
		send[i] = byte(i)
	}
	perHist := make([]hist, *workers)
	var wg sync.WaitGroup
	d := net.Dialer{Timeout: 5 * time.Second}

	worker := func(idx int) {
		defer wg.Done()
		recvbuf := make([]byte, *payload)
		mine := make([]byte, *payload)
		copy(mine, send)
		h := &perHist[idx]
		for !stop.Load() {
			t0 := time.Now()
			c, err := d.Dial("tcp", *addr)        // a NEW connection -> server spawns a handler
			if err != nil {
				if !stop.Load() {
					dialErrs.Add(1)
				}
				continue
			}
			tcp := c.(*net.TCPConn)
			tcp.SetNoDelay(true)
			ok := true
			if _, err := tcp.Write(mine); err != nil {
				ok = false
			}
			if ok {
				if _, err := io.ReadFull(tcp, recvbuf); err != nil {
					ok = false
				}
			}
			tcp.Close()
			if !ok {
				if !stop.Load() {
					ioErrs.Add(1)
				}
				continue
			}
			if measuring.Load() {
				totalConns.Add(1)
				h.record(time.Since(t0).Nanoseconds())
			}
		}
	}

	wg.Add(*workers)
	for i := 0; i < *workers; i++ {
		go worker(i)
	}

	time.Sleep(time.Duration(*ramp * float64(time.Second)))
	measuring.Store(true)
	t0 := time.Now()
	time.Sleep(time.Duration(*measure * float64(time.Second)))
	measuring.Store(false)
	elapsed := time.Since(t0).Seconds()
	stop.Store(true)

	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-time.After(8 * time.Second):
	}

	var agg hist
	for i := range perHist {
		agg.merge(&perHist[i])
	}
	conns := totalConns.Load()
	cps := float64(conns) / elapsed
	out := map[string]any{
		"workers":      *workers,
		"payload":      *payload,
		"gomaxprocs":   *gomax,
		"measure_s":    elapsed,
		"conns":        conns,
		"conns_per_s":  cps,
		"p50_us":       agg.pct(0.50),
		"p99_us":       agg.pct(0.99),
		"p999_us":      agg.pct(0.999),
		"dial_errors":  dialErrs.Load(),
		"io_errors":    ioErrs.Load(),
	}
	b, _ := json.Marshal(out)
	fmt.Println(string(b))
}
