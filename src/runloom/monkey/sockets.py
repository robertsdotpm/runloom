"""Cooperative socket.socket I/O patches."""
from ._base import *  # noqa: F401,F403  (shared foundation)

# ============================================================
# socket
# ============================================================
_orig_recv = None
_orig_recv_into = None
_orig_send = None
_orig_sendall = None
_orig_accept = None
_orig_connect = None
_orig_recvfrom = None
_orig_sendto = None
_orig_recvmsg = None
_orig_recvmsg_into = None
_orig_sendmsg = None
_orig_recvfrom_into = None
_orig_sendfile = None

# recvmsg / recvmsg_into / sendmsg are POSIX-only (fd passing, ancillary
# data via SCM_RIGHTS).  Windows sockets have no equivalent, so socket.socket
# simply lacks the attributes there; the patch is skipped.
_HAVE_RECVMSG = hasattr(socket.socket, "recvmsg")
_HAVE_SENDMSG = hasattr(socket.socket, "sendmsg")


_tcp_recv_alloc = getattr(runloom_c, "tcp_recv_alloc", None)
_tcp_recv       = getattr(runloom_c, "tcp_recv", None)
_tcp_send_once  = getattr(runloom_c, "tcp_send_once", None)
_tcp_send_all   = getattr(runloom_c, "tcp_send", None)


def _coop_timeout(sock):
    """The cooperative deadline for an I/O op, or None for "block forever".

    Under monkey.patch() every socket is forced to the OS-level non-blocking
    mode (`_make_nonblocking` -> setblocking(False)), so `gettimeout()` reads
    back as 0.0 even on a plain blocking socket the caller never touched.  A
    0.0 here is therefore NOT "the caller asked for a non-blocking, deadline-of-
    zero socket" -- it is the internal non-blocking flag that makes _orig_recv
    raise BlockingIOError so we can park on the netpoll.  Treating it as a real
    timeout meant `max(1, int(0.0*1000)) == 1` -- a 1 MILLISECOND deadline on
    every recv/send/connect, so any round trip slower than 1 ms (i.e. anything
    under real concurrency) died with socket.timeout, shredding connections.

    So: a falsy timeout (None or 0.0) means "no deadline, block cooperatively";
    only a POSITIVE timeout imposes a real cooperative deadline.  This matches
    gevent/eventlet, where blocking sockets are the norm and the cooperative
    layer supplies the blocking.  (A caller wanting genuinely-non-blocking,
    raise-immediately semantics is not distinguishable here because the flag is
    always forced on; that was already true before this change.)"""
    t = sock.gettimeout()
    return t if t else None


def _patched_recv(self, bufsize, flags=0):
    """Cooperative recv.  Routes to the C primitive when available
    (saves the BlockingIOError raise/catch on every EAGAIN plus the
    Python frame around _orig_recv), falls back to the old loop
    otherwise.  Outside a fiber, falls through to the raw
    blocking recv so non-fiber threads (e.g. helper threads in
    tests / fixtures) still work after monkey.patch()."""
    if not _in_fiber():
        return _orig_recv(self, bufsize, flags)
    _make_nonblocking(self)
    t = _coop_timeout(self)
    if _tcp_recv_alloc is not None and t is None:
        return _tcp_recv_alloc(self.fileno(), bufsize, flags)
    if t is not None:
        timeout_ms = max(1, int(t * 1000))
        while True:
            try:
                return _orig_recv(self, bufsize, flags)
            except (BlockingIOError, InterruptedError):
                r = runloom_c.wait_fd(self.fileno(), READ, timeout_ms)
                if r == 0:
                    raise socket.timeout("timed out")
    while True:
        try:
            return _orig_recv(self, bufsize, flags)
        except (BlockingIOError, InterruptedError):
            runloom_c.wait_fd(self.fileno(), READ)


def _patched_recv_into(self, buffer, nbytes=0, flags=0):
    """recv_into avoids the bytes-object allocation that recv() does
    every call.  Callers that already own a buffer (high-throughput
    proxies, line readers, framing layers) save one heap allocation
    and one memcpy per recv -- typically 10-20 us / call at 4 KB."""
    if not _in_fiber():
        return _orig_recv_into(self, buffer, nbytes, flags)
    _make_nonblocking(self)
    t = _coop_timeout(self)
    if _tcp_recv is not None and t is None:
        n = nbytes if nbytes else len(buffer)
        return _tcp_recv(self.fileno(), buffer, n, flags)
    if t is not None:
        timeout_ms = max(1, int(t * 1000))
        while True:
            try:
                return _orig_recv_into(self, buffer, nbytes, flags)
            except (BlockingIOError, InterruptedError):
                r = runloom_c.wait_fd(self.fileno(), READ, timeout_ms)
                if r == 0:
                    raise socket.timeout("timed out")
    while True:
        try:
            return _orig_recv_into(self, buffer, nbytes, flags)
        except (BlockingIOError, InterruptedError):
            runloom_c.wait_fd(self.fileno(), READ)


def _patched_send(self, data, flags=0):
    if not _in_fiber():
        return _orig_send(self, data, flags)
    _make_nonblocking(self)
    if _tcp_send_once is not None:
        return _tcp_send_once(self.fileno(), data, flags)
    while True:
        try:
            return _orig_send(self, data, flags)
        except (BlockingIOError, InterruptedError):
            runloom_c.wait_fd(self.fileno(), WRITE)


def _patched_sendall(self, data, flags=0):
    if not _in_fiber():
        return _orig_sendall(self, data, flags)
    _make_nonblocking(self)
    if _tcp_send_all is not None:
        _tcp_send_all(self.fileno(), data, flags)
        return None
    view = data if isinstance(data, memoryview) else memoryview(data)
    sent = 0
    while sent < len(view):
        try:
            n = _orig_send(self, view[sent:], flags)
            if n:
                sent += n
        except (BlockingIOError, InterruptedError):
            runloom_c.wait_fd(self.fileno(), WRITE)


def _patched_accept(self):
    if not _in_fiber():
        return _orig_accept(self)
    _make_nonblocking(self)
    while True:
        try:
            return _orig_accept(self)
        except (BlockingIOError, InterruptedError):
            runloom_c.wait_fd(self.fileno(), READ)


def _patched_connect(self, address):
    if not _in_fiber():
        return _orig_connect(self, address)
    _make_nonblocking(self)
    # connect_ex returns the errno instead of raising, so a synchronous
    # completion (loopback frequently connects at once) and the in-flight
    # case share one path.
    err = self.connect_ex(address)
    if err == 0 or err == errno.EISCONN:
        return
    if err not in (errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY):
        raise OSError(err, os.strerror(err))
    # In flight: wait for writability, then read the outcome via SO_ERROR.
    # The POSIX idiom of re-calling connect() to learn the result does NOT
    # work on Windows -- a refused connect re-reports WSAEALREADY/
    # WSAEWOULDBLOCK forever instead of the actual error, so the old loop hung
    # there.  SO_ERROR is the portable way both stacks agree on (it is exactly
    # what asyncio's selector loop uses), and OSError(err, ...) maps to the
    # right subclass -- ConnectionRefusedError etc. -- on Linux AND Windows
    # (where errno.ECONNREFUSED is the WSA code).
    t = _coop_timeout(self)
    timeout_ms = max(1, int(t * 1000)) if t is not None else None
    while True:
        if timeout_ms is not None:
            r = runloom_c.wait_fd(self.fileno(), WRITE, timeout_ms)
            if r == 0:
                raise socket.timeout("connect timed out")
        else:
            runloom_c.wait_fd(self.fileno(), WRITE)
        err = self.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if err == 0:
            return
        if err not in (errno.EINPROGRESS, errno.EALREADY):
            raise OSError(err, os.strerror(err))


def _patched_recvfrom(self, bufsize, flags=0):
    if not _in_fiber():
        return _orig_recvfrom(self, bufsize, flags)
    _make_nonblocking(self)
    while True:
        try:
            return _orig_recvfrom(self, bufsize, flags)
        except (BlockingIOError, InterruptedError):
            runloom_c.wait_fd(self.fileno(), READ)


def _patched_sendto(self, data, *args):
    if not _in_fiber():
        return _orig_sendto(self, data, *args)
    _make_nonblocking(self)
    while True:
        try:
            return _orig_sendto(self, data, *args)
        except (BlockingIOError, InterruptedError):
            runloom_c.wait_fd(self.fileno(), WRITE)


def _patched_recvmsg(self, bufsize, ancbufsize=0, flags=0):
    """Cooperative recvmsg.  Same EAGAIN -> wait_fd loop as recv, but
    carries the ancillary-data tuple (data, ancdata, msg_flags, address)
    that SCM_RIGHTS fd-passing and IP_PKTINFO callers rely on."""
    if not _in_fiber():
        return _orig_recvmsg(self, bufsize, ancbufsize, flags)
    _make_nonblocking(self)
    while True:
        try:
            return _orig_recvmsg(self, bufsize, ancbufsize, flags)
        except (BlockingIOError, InterruptedError):
            runloom_c.wait_fd(self.fileno(), READ)


def _patched_recvmsg_into(self, buffers, ancbufsize=0, flags=0):
    if not _in_fiber():
        return _orig_recvmsg_into(self, buffers, ancbufsize, flags)
    _make_nonblocking(self)
    while True:
        try:
            return _orig_recvmsg_into(self, buffers, ancbufsize, flags)
        except (BlockingIOError, InterruptedError):
            runloom_c.wait_fd(self.fileno(), READ)


def _patched_sendmsg(self, buffers, ancdata=(), flags=0, address=None):
    if not _in_fiber():
        return _orig_sendmsg(self, buffers, ancdata, flags, address)
    _make_nonblocking(self)
    while True:
        try:
            return _orig_sendmsg(self, buffers, ancdata, flags, address)
        except (BlockingIOError, InterruptedError):
            runloom_c.wait_fd(self.fileno(), WRITE)


def _patched_recvfrom_into(self, buffer, nbytes=0, flags=0):
    """Zero-alloc datagram receive -- the recvfrom analogue of recv_into.
    UDP servers that own a reusable buffer save the bytes allocation per
    packet that plain recvfrom() pays."""
    if not _in_fiber():
        return _orig_recvfrom_into(self, buffer, nbytes, flags)
    _make_nonblocking(self)
    while True:
        try:
            return _orig_recvfrom_into(self, buffer, nbytes, flags)
        except (BlockingIOError, InterruptedError):
            runloom_c.wait_fd(self.fileno(), READ)


# ---------- cooperative sendfile ----------
#
# Stock socket.sendfile refuses non-blocking sockets (raises ValueError) and
# drives the os.sendfile loop with its own selectors.PollSelector.  Our
# fiber sockets are non-blocking by construction, so we reimplement both
# halves of the stdlib's two-strategy sendfile -- the zero-copy os.sendfile
# fast path and the read()+send() fallback -- parking on wait_fd instead of a
# selector.  Faithful to Lib/socket.py: same _check_sendfile_params validation,
# same _GiveupOnSendfile fallback trigger, same offset/seek bookkeeping.

def _co_sendfile_use_sendfile(self, file, offset, count):
    self._check_sendfile_params(file, offset, count)
    sockno = self.fileno()
    try:
        fileno = file.fileno()
    except (AttributeError, io.UnsupportedOperation) as err:
        raise socket._GiveupOnSendfile(err)        # not a regular file
    try:
        fsize = os.fstat(fileno).st_size
    except OSError as err:
        raise socket._GiveupOnSendfile(err)        # not a regular file
    if not fsize:
        return 0                                    # empty file
    # Truncate to 1 GiB to avoid OverflowError, mirroring bpo-38319.
    blocksize = min(count or fsize, 2 ** 30)
    total_sent = 0
    try:
        while True:
            if count:
                blocksize = min(count - total_sent, blocksize)
                if blocksize <= 0:
                    break
            try:
                sent = _raw_os_sendfile(sockno, fileno, offset, blocksize)
            except (BlockingIOError, InterruptedError):
                runloom_c.wait_fd(sockno, WRITE)
                continue
            except OSError as err:
                if total_sent == 0:
                    # 'file' is likely not a regular mmap-like file; fall
                    # back to plain send().
                    raise socket._GiveupOnSendfile(err)
                raise err from None
            else:
                if sent == 0:
                    break                           # EOF
                offset += sent
                total_sent += sent
        return total_sent
    finally:
        if total_sent > 0 and hasattr(file, "seek"):
            file.seek(offset)


def _co_sendfile_use_send(self, file, offset, count):
    self._check_sendfile_params(file, offset, count)
    if offset:
        file.seek(offset)
    blocksize = min(count, 8192) if count else 8192
    total_sent = 0
    file_read = file.read
    sock_send = self.send                           # cooperative patched send
    try:
        while True:
            if count:
                blocksize = min(count - total_sent, blocksize)
                if blocksize <= 0:
                    break
            data = memoryview(file_read(blocksize))
            if not data:
                break                               # EOF
            while data:
                # The patched send parks rather than raising BlockingIOError,
                # so it always returns a positive count here.
                sent = sock_send(data)
                total_sent += sent
                data = data[sent:]
        return total_sent
    finally:
        if total_sent > 0 and hasattr(file, "seek"):
            file.seek(offset + total_sent)


def _patched_sendfile(self, file, offset=0, count=None):
    """Cooperative socket.sendfile.  Zero-copy os.sendfile fast path, with the
    stdlib's read()+send() fallback for non-regular files -- both parking on
    wait_fd so the whole transfer doesn't pin a scheduler thread."""
    if not _in_fiber():
        return _orig_sendfile(self, file, offset, count)
    _make_nonblocking(self)
    if _raw_os_sendfile is not None:
        try:
            return _co_sendfile_use_sendfile(self, file, offset, count)
        except socket._GiveupOnSendfile:
            pass
    return _co_sendfile_use_send(self, file, offset, count)


_orig_close   = None
_orig_detach  = None
_netpoll_unregister = getattr(runloom_c, "netpoll_unregister", None)
_netpoll_cancel_fd = getattr(runloom_c, "netpoll_cancel_fd", None)


def _patched_close(self):
    """Clear the netpoll registration bit before closing (so an fd reuse
    re-registers cleanly under the LEVEL-triggered per-direction scheme), then -- AFTER the
    close -- wake any fiber parked in accept()/recv()/connect() on this fd.
    The woken op retries on the now-closed (fileno == -1) socket, gets EBADF and
    unwinds, instead of being stranded forever when another fiber closed the
    socket out from under it (BUG #5)."""
    fd = -1
    try:
        fd = self.fileno()
    except (OSError, ValueError):
        fd = -1
    if _netpoll_unregister is not None and fd >= 0:
        try:
            _netpoll_unregister(fd)
        except (OSError, ValueError):
            pass
    result = _orig_close(self)
    if _netpoll_cancel_fd is not None and fd >= 0:
        try:
            _netpoll_cancel_fd(fd)
        except (OSError, ValueError):
            pass
    return result


def _patched_detach(self):
    """Same bitmap clear as close: the fd is leaving our control."""
    if _netpoll_unregister is not None:
        try:
            fd = self.fileno()
            if fd >= 0:
                _netpoll_unregister(fd)
        except (OSError, ValueError):
            pass
    return _orig_detach(self)


def _patch_socket():
    global _orig_recv, _orig_recv_into, _orig_send, _orig_sendall, _orig_accept
    global _orig_connect, _orig_recvfrom, _orig_sendto, _orig_close, _orig_detach
    global _orig_recvmsg, _orig_recvmsg_into, _orig_sendmsg
    global _orig_recvfrom_into, _orig_sendfile
    s = socket.socket
    _orig_recv      = s.recv
    _orig_recv_into = s.recv_into
    _orig_send      = s.send
    _orig_sendall   = s.sendall
    _orig_accept    = s.accept
    _orig_connect   = s.connect
    _orig_recvfrom  = s.recvfrom
    _orig_recvfrom_into = s.recvfrom_into
    _orig_sendto    = s.sendto
    _orig_sendfile  = s.sendfile
    _orig_close     = s.close
    _orig_detach    = s.detach
    s.recv      = _patched_recv
    s.recv_into = _patched_recv_into
    s.send      = _patched_send
    s.sendall   = _patched_sendall
    s.accept    = _patched_accept
    s.connect   = _patched_connect
    s.recvfrom  = _patched_recvfrom
    s.recvfrom_into = _patched_recvfrom_into
    s.sendto    = _patched_sendto
    s.sendfile  = _patched_sendfile
    s.close     = _patched_close
    s.detach    = _patched_detach
    if _HAVE_RECVMSG:
        _orig_recvmsg      = s.recvmsg
        _orig_recvmsg_into = s.recvmsg_into
        s.recvmsg      = _patched_recvmsg
        s.recvmsg_into = _patched_recvmsg_into
    if _HAVE_SENDMSG:
        _orig_sendmsg = s.sendmsg
        s.sendmsg     = _patched_sendmsg


def _unpatch_socket():
    s = socket.socket
    s.recv      = _orig_recv
    s.recv_into = _orig_recv_into
    s.send      = _orig_send
    s.sendall   = _orig_sendall
    s.accept    = _orig_accept
    s.connect   = _orig_connect
    s.recvfrom  = _orig_recvfrom
    s.recvfrom_into = _orig_recvfrom_into
    s.sendto    = _orig_sendto
    s.sendfile  = _orig_sendfile
    s.close     = _orig_close
    s.detach    = _orig_detach
    if _HAVE_RECVMSG:
        s.recvmsg      = _orig_recvmsg
        s.recvmsg_into = _orig_recvmsg_into
    if _HAVE_SENDMSG:
        s.sendmsg = _orig_sendmsg
