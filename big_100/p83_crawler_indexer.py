"""big_100 / 83 -- mini crawler plus indexer.

A local HTTP server serves a fixed graph of pages, each containing a
deterministic set of word tokens (including one token unique to the page).
Crawler goroutines fetch pages, tokenize them, and build a shared inverted
index (token -> set of pages).  We verify the unique per-page token maps to
exactly that page.

Stresses: network + CPU tokenizing + a shared locked index + queues.
"""
import re
import socket
import threading

import harness
import httputil
import netutil

NPAGES = 2000
WORD_RE = re.compile(rb"[a-z0-9]+")


def page_tokens(n):
    # A deterministic bag of words plus a token unique to this page.
    common = ["the", "quick", "brown", "fox", "lazy", "dog", "data", "node"]
    words = [common[(n + i) % len(common)] for i in range(6)]
    words.append("uniq{0}".format(n))
    return words


def render(n):
    return "page {0} {1}".format(n, " ".join(page_tokens(n)))


def setup(H):
    srv = netutil.listen_tcp()
    H.state = {"port": srv.getsockname()[1],
               "host": srv.getsockname()[0],
               "index": {}, "lock": threading.Lock()}

    def handle(conn):
        try:
            while True:
                method, path, headers, keep_alive = httputil.read_request(conn)
                m = re.match(r"/page/(\d+)", path)
                if m and int(m.group(1)) < NPAGES:
                    httputil.send_response(conn, render(int(m.group(1))),
                                           keep_alive=keep_alive)
                else:
                    httputil.send_response(conn, "nf", status="404 Not Found",
                                           keep_alive=keep_alive)
                if not keep_alive:
                    break
        except OSError:
            pass
        finally:
            netutil.close_quiet(conn)

    H.go(netutil.serve_forever, H, srv,
         lambda conn, addr: H.go(handle, conn))


def crawler(H, wid, rng, state):
    port = state["port"]
    host = state["host"]
    index = state["index"]
    lock = state["lock"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        page = rng.randrange(NPAGES)
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            status, body = httputil.get(sock, "/page/{0}".format(page),
                                        keep_alive=False)
            if not H.check(status == 200, "status {0}".format(status)):
                return
            tokens = WORD_RE.findall(body)
            with lock:
                for tok in tokens:
                    index.setdefault(tok, set()).add(page)
            # Verify the unique token maps to this page (and only this page).
            uniq = "uniq{0}".format(page).encode()
            with lock:
                pages = index.get(uniq, set())
            if not H.check(pages == {page},
                           "inverted index wrong for {0}: {1}".format(
                               uniq, pages)):
                return
            H.op(wid, len(tokens))
            H.task_done(wid)
        except (OSError, ValueError):
            if not H.running():
                break
            H.sleep(0.005)
        finally:
            netutil.close_quiet(sock)


def body(H):
    H.run_pool(H.funcs, crawler, H.state)


def post(H):
    H.log("index_terms={0}".format(len(H.state["index"])))


if __name__ == "__main__":
    harness.main("p83_crawler_indexer", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="crawl local pages, tokenize, build inverted index")
