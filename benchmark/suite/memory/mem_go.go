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
	var mu sync.Mutex

	for i := 0; i < *n; i++ {
		if *state == "socket" {
			fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
			if err != nil {
				continue
			}
			f := os.NewFile(uintptr(fds[0]), "a")
			peer := os.NewFile(uintptr(fds[1]), "b")
			mu.Lock()
			peers = append(peers, peer)
			mu.Unlock()
			go func(f *os.File) {
				buf := make([]byte, 1)
				f.Read(buf) // blocks: peer never writes
				_ = done
			}(f)
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
}
