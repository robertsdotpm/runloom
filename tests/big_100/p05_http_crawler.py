"""big_100 / 05 -- HTTP/1.1 crawler against a local server.

A local HTTP server serves a fixed graph of pages; page N links to a
deterministic set of other pages.  Tens of thousands of crawler goroutines
start at a random page, fetch it, extract the links, and follow a few -- each
on its own short-lived connection.

Stresses: sockets, request/response parsing, connection churn, fan-out.
"""
import re
import socket

import harness
import httputil
import netutil

NPAGES = 5000
LINK_RE = re.compile(rb'href="/page/(\d+)"')


def page_links(n, seed):
    """Deterministic out-links for page n."""
    rng_state = (n * 2654435761 + seed) & 0xFFFFFFFF
    out = []
    for i in range(4):
        rng_state = (rng_state * 1103515245 + 12345) & 0x7FFFFFFF
        out.append(rng_state % NPAGES)
    return out


def render(n, seed):
    links = "".join(
        '<a href="/page/{0}">{0}</a>'.format(m) for m in page_links(n, seed))
    return "<html><body>page {0} {1}</body></html>".format(n, links)


def setup(H):
    def handler(conn):
        try:
            while True:
                method, path, headers, keep_alive = httputil.read_request(conn)
                m = re.match(r"/page/(\d+)", path)
                if m and int(m.group(1)) < NPAGES:
                    httputil.send_response(
                        conn, render(int(m.group(1)), H.seed),
                        keep_alive=keep_alive)
                else:
                    httputil.send_response(conn, "not found",
                                           status="404 Not Found",
                                           keep_alive=keep_alive)
                if not keep_alive:
                    break
        except OSError:
            pass
        finally:
            netutil.close_quiet(conn)

    servers = netutil.listen_all(
        H, lambda conn, addr: H.fiber(handler, conn))
    H.state = {"servers": servers, "seed": H.seed}


def fetch(H, host, port, page, keep_alive=False):
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        status, body = httputil.get(sock, "/page/{0}".format(page),
                                    keep_alive=keep_alive)
        return status, body
    finally:
        netutil.close_quiet(sock)


def crawler(H, wid, rng, state):
    servers = state["servers"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        try:
            frontier = [rng.randrange(NPAGES)]
            for _ in range(rng.randint(3, 12)):
                if not H.running() or not frontier:
                    break
                page = frontier.pop()
                host, port = netutil.pick_server(servers, rng)
                status, body = fetch(H, host, port, page)
                if not H.check(status == 200,
                               "status {0} for page {1}".format(status, page)):
                    return
                links = [int(x) for x in LINK_RE.findall(body)]
                H.check(len(links) == 4,
                        "expected 4 links on page {0}, got {1}".format(
                            page, len(links)))
                frontier.extend(links[:2])
                H.op(wid)
            H.task_done(wid)
        except OSError:
            if not H.running():
                break
            H.sleep(0.005)


def body(H):
    H.run_pool(H.funcs, crawler, H.state)


if __name__ == "__main__":
    harness.main("p05_http_crawler", body, setup=setup, default_funcs=8000,
                 describe="crawl a local HTTP page graph, parse + follow links")
