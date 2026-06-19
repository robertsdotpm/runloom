// speed_go -- the Go side of the speed benchmark (the [go] column) plus the two
// fixed Go targets the other runtimes are measured against.
//
// Subcommands (-metric):
//   spawn       : spawn N goroutines (each wg.Done), drain -> seconds
//   ctxswitch   : 2-goroutine unbuffered-channel ping-pong, N round-trips -> ns/switch
//   rtt         : TCP client; N sequential round-trips to -addr echo server -> ns/RTT
//   httpd       : HTTP server target for the http-req/s metric (prints LISTENING)
//   httpclient  : concurrent HTTP GET load vs -addr for -measure s -> req/s
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

func emit(m map[string]any) {
	b, _ := json.Marshal(m)
	fmt.Println(string(b))
}

func spawn(n, gomax int) {
	runtime.GOMAXPROCS(gomax)
	var wg sync.WaitGroup
	wg.Add(n)
	t0 := time.Now()
	for i := 0; i < n; i++ {
		go func() { wg.Done() }()
	}
	wg.Wait()
	dt := time.Since(t0).Seconds()
	emit(map[string]any{"runtime": "go", "metric": "spawn", "n": n,
		"cores": gomax, "seconds": dt, "rate_per_s": float64(n) / dt})
}

func ctxswitch(n, gomax int) {
	// Loaded-yield (matches the other runtimes): G goroutines each Gosched K
	// times so the run queues stay full and switches are real re-dispatch, not a
	// 2-party ping-pong that idles all but two threads.
	runtime.GOMAXPROCS(gomax)
	G := gomax * 16
	if G < 2 {
		G = 2
	}
	K := n / G
	if K < 1 {
		K = 1
	}
	var wg sync.WaitGroup
	wg.Add(G)
	t0 := time.Now()
	for i := 0; i < G; i++ {
		go func() {
			for j := 0; j < K; j++ {
				runtime.Gosched()
			}
			wg.Done()
		}()
	}
	wg.Wait()
	dt := time.Since(t0)
	switches := G * K
	emit(map[string]any{"runtime": "go", "metric": "ctxswitch", "n": n,
		"cores": gomax, "switches": switches, "fibers": G,
		"seconds": dt.Seconds(),
		"ns_per_switch": float64(dt.Nanoseconds()) / float64(switches)})
}

func rtt(addr string, n, payload int) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		emit(map[string]any{"runtime": "go", "metric": "rtt", "error": err.Error()})
		os.Exit(1)
	}
	c.(*net.TCPConn).SetNoDelay(true)
	send := make([]byte, payload)
	recv := make([]byte, payload)
	// warmup
	for i := 0; i < 1000; i++ {
		c.Write(send)
		io.ReadFull(c, recv)
	}
	t0 := time.Now()
	for i := 0; i < n; i++ {
		if _, err := c.Write(send); err != nil {
			break
		}
		if _, err := io.ReadFull(c, recv); err != nil {
			break
		}
	}
	dt := time.Since(t0)
	emit(map[string]any{"runtime": "go", "metric": "rtt", "n": n, "payload": payload,
		"ns_per_rtt": float64(dt.Nanoseconds()) / float64(n)})
}

func httpd(host string, port, gomax int) {
	runtime.GOMAXPROCS(gomax)
	body := []byte("OK\n")
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write(body)
	})
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("LISTENING %d\n", ln.Addr().(*net.TCPAddr).Port)
	os.Stdout.Sync()
	srv := &http.Server{Handler: mux}
	srv.Serve(ln)
}

func httpclient(addr string, conns, gomax int, ramp, measure float64) {
	runtime.GOMAXPROCS(gomax)
	url := "http://" + addr + "/"
	tr := &http.Transport{MaxIdleConns: conns * 2, MaxIdleConnsPerHost: conns * 2,
		MaxConnsPerHost: conns * 2, DisableCompression: true}
	client := &http.Client{Transport: tr}
	var measuring atomic.Bool
	var reqs atomic.Int64
	var errs atomic.Int64
	var stop atomic.Bool
	var wg sync.WaitGroup
	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				resp, err := client.Get(url)
				if err != nil {
					errs.Add(1)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if measuring.Load() {
					reqs.Add(1)
				}
			}
		}()
	}
	time.Sleep(time.Duration(ramp * float64(time.Second)))
	measuring.Store(true)
	t0 := time.Now()
	time.Sleep(time.Duration(measure * float64(time.Second)))
	measuring.Store(false)
	elapsed := time.Since(t0).Seconds()
	stop.Store(true)
	go func() { wg.Wait() }()
	time.Sleep(200 * time.Millisecond)
	emit(map[string]any{"runtime": "go", "metric": "http", "conns": conns,
		"cores": gomax, "measure_s": elapsed, "reqs": reqs.Load(),
		"rps": float64(reqs.Load()) / elapsed, "errors": errs.Load()})
}

func main() {
	metric := flag.String("metric", "spawn", "spawn|ctxswitch|rtt|httpd|httpclient")
	n := flag.Int("n", 1000000, "")
	gomax := flag.Int("gomaxprocs", runtime.NumCPU(), "")
	addr := flag.String("addr", "127.0.0.1:9100", "")
	host := flag.String("host", "127.0.0.1", "")
	port := flag.Int("port", 9100, "")
	payload := flag.Int("payload", 64, "")
	conns := flag.Int("conns", 64, "")
	ramp := flag.Float64("ramp", 1.0, "")
	measure := flag.Float64("measure", 3.0, "")
	flag.String("token", "", "")
	flag.Parse()
	switch *metric {
	case "spawn":
		spawn(*n, *gomax)
	case "ctxswitch":
		ctxswitch(*n, *gomax)
	case "rtt":
		rtt(*addr, *n, *payload)
	case "httpd":
		httpd(*host, *port, *gomax)
	case "httpclient":
		httpclient(*addr, *conns, *gomax, *ramp, *measure)
	default:
		fmt.Fprintln(os.Stderr, "unknown metric", *metric)
		os.Exit(2)
	}
}
