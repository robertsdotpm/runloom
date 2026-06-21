// Memory probe for the [go] column: hold N blocked goroutines in a given state
// and report USED memory (VmRSS from /proc/self/status -- not virtual size).
//
//	-state empty   : N goroutines blocked on a channel receive (bare goroutine)
//	-state socket  : N goroutines each holding a socketpair end, blocked on Read
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

func vmRSSBytes() int64 {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return -1
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			kb, _ := strconv.ParseInt(fields[1], 10, 64)
			return kb * 1024
		}
	}
	return -1
}

func main() {
	state := flag.String("state", "empty", "empty|socket")
	n := flag.Int("n", 100000, "")
	settle := flag.Float64("settle", 0, "")
	flag.Parse()

	s := *settle
	if s == 0 {
		s = 3.0
		if v := float64(*n) / 150000.0; v > s {
			s = v
		}
	}

	done := make(chan struct{}) // never closed -> receivers block forever
	var peers []*os.File
	var conns []net.Conn
	var mu sync.Mutex

	for i := 0; i < *n; i++ {
		if *state == "socket" {
			fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
			if err != nil {
				continue
			}
			// Hand the read end to the runtime netpoller (net.Conn) instead of a
			// blocking os.File.Read -- otherwise each blocked Read pins an OS
			// thread and 10k+ of them hit Go's thread-exhaustion limit (and
			// inflate RSS with thread stacks). This matches how a real Go server
			// holds idle connections: epoll-managed, one goroutine, no thread.
			syscall.SetNonblock(fds[0], true)
			f := os.NewFile(uintptr(fds[0]), "a")
			c, err := net.FileConn(f)
			f.Close()
			if err != nil {
				syscall.Close(fds[1])
				continue
			}
			peer := os.NewFile(uintptr(fds[1]), "b")
			mu.Lock()
			conns = append(conns, c)
			peers = append(peers, peer)
			mu.Unlock()
			go func(c net.Conn) {
				// match the runloom handler buffer (mem_runloom.py bytearray(65536))
				// and the perf-path Go server (srv_go.go make([]byte,65536)) so the
				// w/socket comparison holds an equal-size handler buffer per task.
				buf := make([]byte, 65536)
				c.Read(buf) // parks via the netpoller: peer never writes
			}(c)
		} else {
			go func() { <-done }()
		}
	}

	time.Sleep(time.Duration(s * float64(time.Second)))
	out := map[string]any{"runtime": "go", "state": *state, "n": *n,
		"rss_bytes": vmRSSBytes(), "pss_bytes": nil}
	b, _ := json.Marshal(out)
	fmt.Println(string(b))
	_ = peers
	_ = conns
}
