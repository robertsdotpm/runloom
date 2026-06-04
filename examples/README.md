# runloom examples

Small, self-contained programs — each one demonstrates a single aspect of
runloom. They run goroutines across all your cores via the M:N scheduler
(`run(HUBS, ...)`), so they need **free-threaded CPython 3.13t with the GIL
off** — runloom is a free-threaded runtime, and `run(n>1)` deliberately raises
on a GIL build rather than pretend.

Install runloom first — `pip install runloom`, or `pip install -e .` from a
clone — then run any example with the GIL off:

```bash
PYTHON_GIL=0 ~/.pyenv/versions/3.13.13t/bin/python3 examples/hello_goroutines.py
```

(On a stock GIL build the M:N examples raise a clear error telling you to use
`run(1, ...)`; only `asyncio_bridge.py` is single-loop by nature.)

For raw performance numbers and the measurement harness, see [`../bench/`](../bench/).

## Goroutines & channels

| Example | Shows |
| --- | --- |
| [hello_goroutines.py](hello_goroutines.py) | `go` / `run` / `yield_` / `sleep` — the basics |
| [channels.py](channels.py) | buffered vs unbuffered channels, `close`, `for v in ch` |
| [select_demo.py](select_demo.py) | `select` over recv/send cases, plus non-blocking `default` |
| [ping_pong.py](ping_pong.py) | two goroutines synchronised purely by channels |

## Concurrency patterns

| Example | Shows |
| --- | --- |
| [worker_pool.py](worker_pool.py) | a fixed pool of workers draining a job channel |
| [pipeline.py](pipeline.py) | staged processing connected by channels |
| [fan_in.py](fan_in.py) | many producers merged into one channel |
| [fan_out.py](fan_out.py) | one producer spread across many consumers |
| [semaphore.py](semaphore.py) | bounding concurrency with a buffered channel |
| [waitgroup.py](waitgroup.py) | a `sync.WaitGroup` in ~10 lines |
| [prime_sieve.py](prime_sieve.py) | the classic concurrent prime sieve |

## Time, timeouts & cancellation

| Example | Shows |
| --- | --- |
| [timeout.py](timeout.py) | race work against `runloom.time.After` with `select` |
| [ticker.py](ticker.py) | periodic ticks with `runloom.time.NewTicker` |
| [context_cancel.py](context_cancel.py) | `runloom.context` cancellation fanned out to many goroutines |

## Networking (blocking-style sockets)

| Example | Shows |
| --- | --- |
| [echo_server.py](echo_server.py) / [echo_client.py](echo_client.py) | a TCP echo server + load-generating client |
| [http_server.py](http_server.py) | hand-rolled HTTP server + `urllib.urlopen` clients, all cooperative |
| [tcp_proxy.py](tcp_proxy.py) | a port forwarder with two pump goroutines per connection |
| [port_scanner.py](port_scanner.py) | thousands of concurrent `connect()`s via fan-out |
| [udp_echo.py](udp_echo.py) | cooperative UDP datagrams with the `runloom.sync` front-end |

## Runtime features

| Example | Shows |
| --- | --- |
| [offload_blocking.py](offload_blocking.py) | `runloom.blocking` keeps a hub alive across a non-cooperative call |
| [mn_parallel.py](mn_parallel.py) | the M:N scheduler scaling across cores (free-threaded 3.13t) |
| [segfault_dump.py](segfault_dump.py) | `install_crash_handler()` turns a goroutine stack overflow into a classified dump |
| [asyncio_bridge.py](asyncio_bridge.py) | run existing `async`/`await` code on runloom via `runloom.aio.run` |

### Free-threaded run (for `mn_parallel.py`)

```bash
PYTHON_GIL=0 ~/.pyenv/versions/3.13.13t/bin/python3 examples/mn_parallel.py
```
