// Linearizability checker for runloom channels, built on Porcupine.
//
// Reads a concurrent operation history (JSON from record_history.py) and asks
// whether some linearization consistent with the real-time call/return
// intervals satisfies the sequential FIFO-channel spec: sends enqueue, recvs
// dequeue the front (so values arrive in send order), and recv on an empty
// closed channel returns "closed". If yes the channel behaved linearizably on
// that run; if no, Porcupine reports the smallest non-linearizable prefix.
//
// Usage: go run . <history.json>   (exit 0 = linearizable, 1 = not, 2 = error)
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/anishathalye/porcupine"
)

type event struct {
	Proc   int    `json:"proc"`
	Op     string `json:"op"`
	Value  int    `json:"value"`
	Result string `json:"result"`
	Call   int64  `json:"call"`
	Ret    int64  `json:"ret"`
}

type history struct {
	Events []event `json:"events"`
}

type input struct {
	Op    string
	Value int
}
type output struct {
	Result string
	Value  int
}

// State is encoded as a comparable string "v0,v1,...|T|F" = queue | closed.
func encode(q []int, closed bool) string {
	parts := make([]string, len(q))
	for i, v := range q {
		parts[i] = strconv.Itoa(v)
	}
	c := "F"
	if closed {
		c = "T"
	}
	return strings.Join(parts, ",") + "|" + c
}

func decode(s string) ([]int, bool) {
	i := strings.LastIndex(s, "|")
	closed := s[i+1:] == "T"
	qs := s[:i]
	if qs == "" {
		return nil, closed
	}
	parts := strings.Split(qs, ",")
	q := make([]int, len(parts))
	for j, p := range parts {
		q[j], _ = strconv.Atoi(p)
	}
	return q, closed
}

var chanModel = porcupine.Model{
	Init: func() interface{} { return encode(nil, false) },
	Step: func(st, in, out interface{}) (bool, interface{}) {
		q, closed := decode(st.(string))
		i := in.(input)
		o := out.(output)
		switch i.Op {
		case "send":
			if closed {
				return false, st // sending on a closed channel is not modeled
			}
			return o.Result == "ok", encode(append(append([]int{}, q...), i.Value), closed)
		case "close":
			return o.Result == "ok", encode(q, true)
		case "recv":
			if len(q) > 0 {
				if o.Result == "ok" && o.Value == q[0] {
					return true, encode(q[1:], closed)
				}
				return false, st // out-of-FIFO-order or wrong value
			}
			// empty queue: the only valid recv result is "closed", and only if closed
			if o.Result == "closed" && closed {
				return true, st
			}
			return false, st
		}
		return false, st
	},
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: lincheck <history.json>")
		os.Exit(2)
	}
	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "read:", err)
		os.Exit(2)
	}
	var h history
	if err := json.Unmarshal(data, &h); err != nil {
		fmt.Fprintln(os.Stderr, "json:", err)
		os.Exit(2)
	}

	ops := make([]porcupine.Operation, 0, len(h.Events))
	for _, e := range h.Events {
		ops = append(ops, porcupine.Operation{
			ClientId: e.Proc,
			Input:    input{Op: e.Op, Value: e.Value},
			Call:     e.Call,
			Output:   output{Result: e.Result, Value: e.Value},
			Return:   e.Ret,
		})
	}

	res := porcupine.CheckOperations(chanModel, ops)
	if res {
		fmt.Printf("LINEARIZABLE: %d operations satisfy the FIFO-channel spec\n", len(ops))
		os.Exit(0)
	}
	fmt.Printf("NOT LINEARIZABLE: %d operations have no valid FIFO linearization\n", len(ops))
	os.Exit(1)
}
