# Monkey-patching the stdlib

`runloom.monkey.patch()` replaces blocking stdlib calls with cooperative
equivalents that park the current goroutine instead of blocking the OS
thread.  After the patch, ordinary `socket.recv`, `time.sleep`,
`select.select`, `ssl.read`, file I/O, `subprocess` waits, DNS lookups,
and several `threading` primitives all yield instead of stalling the
whole interpreter.

This means **you can use any synchronous library** -- `requests`,
`pymysql`, stdlib `urllib`, `psycopg2`, plain `socket` code -- and it
becomes cooperative.

## The basic call

```python
import runloom

runloom.monkey.patch()                    # patch everything
```

After this:

```python
import socket, time

def worker():
    s = socket.socket()
    s.connect(("example.com", 80))     # cooperative: parks during the TCP handshake
    s.sendall(b"GET / HTTP/1.0\r\n\r\n")
    time.sleep(0.5)                    # cooperative: only this goroutine sleeps
    data = s.recv(8192)                # cooperative: parks until data arrives
    s.close()
    return data

runloom.go(worker)
runloom.run_single()
```

## What gets patched

The patch is divided into **categories** you can selectively
enable/disable:

| Category | What changes |
| --- | --- |
| `socket` | `socket.socket`'s `connect`, `recv`, `send`, `sendall`, `accept`, `recv_into`, `recvfrom`, `sendto` park on `wait_fd` instead of blocking. |
| `time` | `time.sleep` becomes cooperative (uses scheduler's sleep heap). |
| `select` | `select.select`, `select.poll`, `selectors.*` rerouted through runloom's netpoll. |
| `os` | `os.read`, `os.write` on regular files dispatch to a worker thread (`run_in_executor`-style) so the goroutine doesn't block. |
| `ssl` | `ssl.SSLSocket` reads/writes park on `wait_fd`; handshake is cooperative. |
| `subprocess` | `Popen.wait` / `communicate` poll the child's exit cooperatively. |
| `threading` | `threading.Event`, `threading.Lock` (when used inside a goroutine) park instead of locking. |
| `queue` | `queue.Queue.get/put` cooperate when called from a goroutine. |
| `stdio` | `sys.stdin.readline()` and friends park on the underlying fd. |
| `dns` | `socket.getaddrinfo` runs in parallel for A/AAAA records. |

All default to enabled.  Opt out with kwargs:

```python
runloom.monkey.patch(threading=False, queue=False)
```

## Unpatch

```python
runloom.monkey.unpatch()              # reverse everything
runloom.monkey.unpatch(socket=False)  # keep socket patched, reverse the rest
```

Patching is **idempotent** -- calling `patch()` twice does nothing the
second time.  Unpatch is the inverse.

## Recipe: a fully synchronous-looking HTTP fetcher

```python
import runloom
import urllib.request

runloom.monkey.patch()

def fetch(url):
    with urllib.request.urlopen(url, timeout=5) as resp:
        return resp.read()

def main():
    urls = [
        "http://example.com",
        "http://example.org",
        "http://example.net",
    ]
    results = runloom.Chan(len(urls))
    for u in urls:
        runloom.go(lambda url=u: results.send((url, len(fetch(url)))))
    for _ in urls:
        print(results.recv()[0])

runloom.go(main)
runloom.run_single()
```

Three HTTP requests, fully concurrent, written in completely linear
synchronous style -- `urllib.request` doesn't know it's been
monkey-patched.

## Recipe: a database pool with `pymysql`

```python
import runloom
import pymysql                                # plain blocking driver

runloom.monkey.patch()

def query(sql):
    conn = pymysql.connect(host="db", user="x", password="y", db="z")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
    finally:
        conn.close()

def worker(i):
    rows = query("SELECT id FROM jobs WHERE bucket = %s" % i)
    print("bucket", i, "->", len(rows), "rows")

for i in range(32):
    runloom.go(lambda i=i: worker(i))
runloom.run_single()
```

32 concurrent MySQL queries on one OS thread, no thread pool, no
`async` rewrite.

## Caveats

### Patch early

```python
import runloom
runloom.monkey.patch()        # <-- before importing modules that capture sockets

import some_library        # this sees patched socket from the start
```

If a library does `from socket import socket` and caches the class
at import time, *and* you patch after that import, the library's
cached reference still points at the original.  Some patches rebind
class attributes (so the cached class becomes cooperative), but the
safe ordering is patch-then-import.

### Reentrancy on legacy `selectors`

`selectors.DefaultSelector` is replaced wholesale.  Code that imports
`DefaultSelector` *before* `patch()` and then constructs new instances
still gets the patched version because we rebind the class attribute.

### `threading.Thread` is not replaced

Patching doesn't turn `threading.Thread` into a goroutine -- it would
break too many assumptions.  If you spawn an OS thread, it runs
independently of the runloom scheduler.

For "I want runloom, not threads," use `runloom.go(fn)` or
`runloom.sync.go(fn)`.

### `os.read` on a regular file dispatches to a thread

The Linux io_uring backend (when available) lets us do truly async
file I/O, but the default path is a small executor that runs the
read/write off the scheduler's OS thread.  This means file I/O won't
*block* your goroutines, but it does pay a thread-hop on each call.
See [io_uring](https://github.com/robertsdotpm/runloom/blob/main/src/runloom_c/io_uring.c)
for direct ring access.

### Windows

The Windows patch is selective: socket / time / select / queue /
threading work the same way (via `WSAPoll`).  File I/O dispatches to a
worker.  `subprocess` works.  `os.read`/`os.write` on regular files
goes through the same executor path.

## Listing applied patches

```python
import runloom
runloom.monkey.patch()
print(runloom.monkey._applied)
# {'socket', 'time', 'select', 'os', 'ssl', 'subprocess',
#  'threading', 'queue', 'stdio', 'dns'}
```

(`_applied` is a private set but stable across versions.)

## When NOT to monkey-patch

If your entire program is written in `async def` and uses
`runloom.aio.run` for I/O, you don't need monkey-patching -- the asyncio
bridge already drives I/O through runloom's netpoll.  Monkey-patching is
for **mixing** sync code with the scheduler.

If you're embedding runloom inside another process that also uses
threads + blocking I/O for unrelated work, don't patch -- confining
runloom to its own region keeps the rest unaffected.
