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

_raw_wait_fd = runloom_c.wait_fd
_WAIT_FD_CANCELLED = getattr(runloom_c, "WAIT_FD_CANCELLED", 0x40000000)

# MSG_WAITALL / MSG_PEEK are absent on some platforms (e.g. Windows recv has no
# MSG_WAITALL).  Fall back to 0 so `flags & _MSG_WAITALL` is always False there.
_MSG_WAITALL = getattr(socket, "MSG_WAITALL", 0)
_MSG_PEEK    = getattr(socket, "MSG_PEEK", 0)

# _coop_timeout sentinels.  _NONBLOCK marks a socket the USER made non-blocking
# (setblocking(False) / settimeout(0) before any cooperative I/O): the I/O paths
# must raise BlockingIOError on EAGAIN rather than park cooperatively.  _MISSING
# is the "no side-table entry" probe -- a single atomic dict lookup, race-free
# against a concurrent close() that pops the entry.
_MISSING  = object()
_NONBLOCK = object()


def _wait_fd_coop(fd, ev, timeout_ms=-1):
    """wait_fd for the cooperative socket loops below, honouring cancellation.

    A cancellation -- a cross-fiber close (runloom_netpoll_cancel_fd) or the
    teardown backstop (runloom_c.cancel_all_parked, audit finding B3) -- makes
    the raw wait_fd return the POSITIVE RUNLOOM_NETPOLL_CANCELLED sentinel.  The
    loops below ignore wait_fd's return and just retry the op, so on a still-OPEN
    socket a cancelled waiter would re-park forever (the close path only unwinds
    because the fd is then closed and the retry hits EBADF).  Raise
    OSError(ECANCELED) instead so the parked op unwinds; the harness swallows
    OSError during teardown.  Returns the wait_fd result otherwise (0 == timeout
    on the timed paths)."""
    r = _raw_wait_fd(fd, ev, timeout_ms) if timeout_ms >= 0 else _raw_wait_fd(fd, ev)
    if r == _WAIT_FD_CANCELLED:
        raise OSError(errno.ECANCELED, "wait_fd cancelled")
    return r


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

    So: a recorded falsy timeout (None) means "no deadline, block cooperatively";
    only a POSITIVE recorded timeout imposes a real cooperative deadline.  This
    matches gevent/eventlet, where blocking sockets are the norm and the
    cooperative layer supplies the blocking.  A caller wanting genuinely-non-
    blocking, raise-immediately semantics IS distinguished -- see the _NONBLOCK
    note below (it hinges on there being no side-table entry, since a socket the
    user forced non-blocking never records one).

    Reads the per-fd side table (populated by _make_nonblocking before
    setblocking(False) zeroed the live gettimeout()), NOT gettimeout() directly
    -- which always reads back 0.0 once the socket is forced non-blocking, so
    the caller's settimeout() would otherwise be invisible here.

    Returns the _NONBLOCK sentinel when the fd has NO side-table entry.
    _make_nonblocking only records an entry when it first sees the socket with a
    non-zero gettimeout() (a blocking or timed socket); a socket the user made
    non-blocking (setblocking(False) / settimeout(0)) BEFORE any cooperative I/O
    reads gettimeout()==0.0 on that first sighting, so nothing is recorded.  That
    absence is the one reliable signal of explicit raise-immediately intent, so
    the I/O paths let the BlockingIOError propagate instead of parking forever
    (a hand-rolled non-blocking / drain-until-EWOULDBLOCK loop must not deadlock
    under monkey.patch())."""
    try:
        fd = sock.fileno()
    except OSError:
        return None
    # Single atomic get -- a concurrent close() may pop the entry between a
    # membership test and a lookup, so probe with a sentinel default instead.
    t = _SOCK_TIMEOUTS.get(fd, _MISSING)
    if t is _MISSING:
        return _NONBLOCK
    return t if t else None


def _wait_io(sock, fd, direction):
    """Park for readiness on `direction`, honoring the socket's cooperative
    timeout: raise socket.timeout if the deadline elapses before the fd becomes
    ready.  The datagram/msg ops (recvfrom/sendto/recvmsg/...) use this --
    unlike recv/send they have no C fast path and previously parked on a bare
    wait_fd with NO deadline, so a timed datagram socket that never receives
    hung the fiber forever instead of raising socket.timeout.  Matches the
    fixed-per-wait timeout the recv/connect paths already use.

    Routes through _wait_fd_coop (NOT a bare runloom_c.wait_fd) so a cross-fiber
    close / teardown cancel raises OSError(ECANCELED) here too (audit finding B3);
    otherwise a cancelled datagram/msg waiter on a still-open socket re-parks
    forever."""
    t = _coop_timeout(sock)
    if t is _NONBLOCK:
        # User made this socket non-blocking: don't park -- surface the
        # BlockingIOError the caller just caught (stock non-blocking semantics).
        raise BlockingIOError(errno.EWOULDBLOCK,
                              os.strerror(errno.EWOULDBLOCK))
    if t is None:
        _wait_fd_coop(fd, direction)
    elif _wait_fd_coop(fd, direction, max(1, int(t * 1000))) == 0:
        raise socket.timeout("timed out")


def _recv_waitall(sock, buffer, nbytes, flags, t):
    """Fill buffer[:nbytes] completely (MSG_WAITALL semantics), returning the
    number of bytes read; stops short only on EOF (peer closed), exactly like a
    blocking recv(..., MSG_WAITALL).

    _make_nonblocking forces every socket to O_NONBLOCK, and the kernel IGNORES
    MSG_WAITALL on a non-blocking socket -- it returns whatever bytes are already
    available.  So neither the C fast path nor a single _orig_recv accumulates to
    n bytes; framed protocols that read fixed-length headers/payloads with
    MSG_WAITALL silently short-read.  Re-implement the wait-for-all loop here,
    parking on readiness between partial reads and honoring the socket's
    cooperative timeout (raise socket.timeout if it elapses before n bytes).

    `flags` arrives with MSG_WAITALL already stripped -- it is a no-op on a
    non-blocking fd and only muddies intent.  `t` is the resolved cooperative
    timeout (None == block, or a positive deadline); never _NONBLOCK here."""
    fd = sock.fileno()
    view = memoryview(buffer)
    if nbytes > len(view):
        nbytes = len(view)
    got = 0
    deadline = (time.monotonic() + t) if t is not None else None
    while got < nbytes:
        try:
            n = _orig_recv_into(sock, view[got:nbytes], 0, flags)
        except (BlockingIOError, InterruptedError):
            if deadline is None:
                _wait_fd_coop(fd, READ)
            else:
                rem = deadline - time.monotonic()
                if rem <= 0 or _wait_fd_coop(fd, READ, max(1, int(rem * 1000))) == 0:
                    raise socket.timeout("timed out")
            continue
        if n == 0:
            break                     # EOF before n bytes -- short, like blocking recv
        got += n
    return got


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
    if t is _NONBLOCK:
        # User made this socket non-blocking: one attempt, let BlockingIOError
        # propagate (and MSG_WAITALL is a kernel no-op here, as it is for stock
        # non-blocking recv).
        return _orig_recv(self, bufsize, flags)
    if _MSG_WAITALL and (flags & _MSG_WAITALL) and not (flags & _MSG_PEEK) and bufsize > 0:
        buf = bytearray(bufsize)
        got = _recv_waitall(self, buf, bufsize, flags & ~_MSG_WAITALL, t)
        return bytes(buf[:got])
    if _tcp_recv_alloc is not None and t is None:
        return _tcp_recv_alloc(self.fileno(), bufsize, flags)
    if t is not None:
        timeout_ms = max(1, int(t * 1000))
        while True:
            try:
                return _orig_recv(self, bufsize, flags)
            except (BlockingIOError, InterruptedError):
                r = _wait_fd_coop(self.fileno(), READ, timeout_ms)
                if r == 0:
                    raise socket.timeout("timed out")
    while True:
        try:
            return _orig_recv(self, bufsize, flags)
        except (BlockingIOError, InterruptedError):
            _wait_fd_coop(self.fileno(), READ)


def _patched_recv_into(self, buffer, nbytes=0, flags=0):
    """recv_into avoids the bytes-object allocation that recv() does
    every call.  Callers that already own a buffer (high-throughput
    proxies, line readers, framing layers) save one heap allocation
    and one memcpy per recv -- typically 10-20 us / call at 4 KB."""
    if not _in_fiber():
        return _orig_recv_into(self, buffer, nbytes, flags)
    _make_nonblocking(self)
    t = _coop_timeout(self)
    if t is _NONBLOCK:
        # User non-blocking socket: one attempt, let BlockingIOError propagate.
        return _orig_recv_into(self, buffer, nbytes, flags)
    if _MSG_WAITALL and (flags & _MSG_WAITALL) and not (flags & _MSG_PEEK):
        n = nbytes if nbytes else len(buffer)
        if n > 0:
            return _recv_waitall(self, buffer, n, flags & ~_MSG_WAITALL, t)
    if _tcp_recv is not None and t is None:
        n = nbytes if nbytes else len(buffer)
        return _tcp_recv(self.fileno(), buffer, n, flags)
    if t is not None:
        timeout_ms = max(1, int(t * 1000))
        while True:
            try:
                return _orig_recv_into(self, buffer, nbytes, flags)
            except (BlockingIOError, InterruptedError):
                r = _wait_fd_coop(self.fileno(), READ, timeout_ms)
                if r == 0:
                    raise socket.timeout("timed out")
    while True:
        try:
            return _orig_recv_into(self, buffer, nbytes, flags)
        except (BlockingIOError, InterruptedError):
            _wait_fd_coop(self.fileno(), READ)


def _patched_send(self, data, flags=0):
    if not _in_fiber():
        return _orig_send(self, data, flags)
    _make_nonblocking(self)
    t = _coop_timeout(self)
    if t is _NONBLOCK:
        # User non-blocking socket: one attempt, let BlockingIOError propagate.
        return _orig_send(self, data, flags)
    # The C fast path parks with an infinite deadline, so it can only be used
    # when there is no timeout; a timed socket must honor settimeout() (like
    # recv/connect) and raise socket.timeout instead of hanging on a stalled
    # peer.
    if _tcp_send_once is not None and t is None:
        return _tcp_send_once(self.fileno(), data, flags)
    if t is not None:
        deadline = time.monotonic() + t
        while True:
            try:
                return _orig_send(self, data, flags)
            except (BlockingIOError, InterruptedError):
                rem = deadline - time.monotonic()
                if rem <= 0 or _wait_fd_coop(self.fileno(), WRITE,
                                             max(1, int(rem * 1000))) == 0:
                    raise socket.timeout("timed out")
    while True:
        try:
            return _orig_send(self, data, flags)
        except (BlockingIOError, InterruptedError):
            _wait_fd_coop(self.fileno(), WRITE)


def _patched_sendall(self, data, flags=0):
    if not _in_fiber():
        return _orig_sendall(self, data, flags)
    _make_nonblocking(self)
    t = _coop_timeout(self)
    if t is _NONBLOCK:
        # User non-blocking socket: let stock non-blocking sendall raise
        # BlockingIOError when it can't drain the buffer immediately.
        return _orig_sendall(self, data, flags)
    # C fast path parks with an infinite deadline -- only usable with no timeout;
    # a timed socket must raise socket.timeout rather than hang forever.
    if _tcp_send_all is not None and t is None:
        _tcp_send_all(self.fileno(), data, flags)
        return None
    view = data if isinstance(data, memoryview) else memoryview(data)
    sent = 0
    deadline = (time.monotonic() + t) if t is not None else None
    while sent < len(view):
        try:
            n = _orig_send(self, view[sent:], flags)
            if n:
                sent += n
        except (BlockingIOError, InterruptedError):
            if deadline is None:
                _wait_fd_coop(self.fileno(), WRITE)
            else:
                rem = deadline - time.monotonic()
                if rem <= 0 or _wait_fd_coop(self.fileno(), WRITE,
                                             max(1, int(rem * 1000))) == 0:
                    raise socket.timeout("timed out")
    return None


def _patched_accept(self):
    if not _in_fiber():
        return _orig_accept(self)
    _make_nonblocking(self)
    while True:
        try:
            return _orig_accept(self)
        except (BlockingIOError, InterruptedError):
            _wait_io(self, self.fileno(), READ)


def _connect_needs_resolve(host):
    """True when `host` is a name that must be resolved before connect().

    IP literals -- including zone'd IPv6 like 'fe80::1%eth0' -- return False so
    connect_ex handles them verbatim (preserving exact sockaddr/scope semantics);
    only names return True and get routed through the cooperative resolver."""
    if not isinstance(host, str) or not host:
        return False
    probe = host.split("%", 1)[0]           # strip an IPv6 zone id for the check
    for af in (socket.AF_INET6, socket.AF_INET):
        try:
            socket.inet_pton(af, probe)
            return False                    # parses as a literal -> no DNS
        except (OSError, ValueError):
            continue
    return True


def _connect_resolve(sock, address):
    """Resolve a (host, port[, ...]) connect target through the cooperatively
    patched socket.getaddrinfo when `host` is a name.

    connect_ex on a raw (hostname, port) tuple resolves the name INLINE via
    libc getaddrinfo on the hub's OS thread, freezing every fiber on that hub
    for the full (possibly multi-second) resolution -- the exact stall the
    monkey layer exists to prevent.  Resolve here instead so DNS parks on the
    cooperative resolver.  AF_UNIX paths / already-numeric addresses pass
    through unchanged."""
    if not isinstance(address, tuple) or len(address) < 2:
        return address                      # AF_UNIX path or non-(host,port) form
    if not _connect_needs_resolve(address[0]):
        return address
    infos = socket.getaddrinfo(address[0], address[1],
                               sock.family, sock.type, sock.proto)
    if not infos:
        return address
    return infos[0][4]                      # sockaddr of the first result (as connect does)


def _connect_wait_result(sock, timeout_ms):
    """In-flight connect: wait for writability, then read the outcome via
    SO_ERROR.  The POSIX idiom of re-calling connect() to learn the result does
    NOT work on Windows -- a refused connect re-reports WSAEALREADY/
    WSAEWOULDBLOCK forever instead of the actual error, so the old loop hung
    there.  SO_ERROR is the portable way both stacks agree on (it is exactly
    what asyncio's selector loop uses), and OSError(err, ...) maps to the
    right subclass -- ConnectionRefusedError etc. -- on Linux AND Windows
    (where errno.ECONNREFUSED is the WSA code)."""
    while True:
        if timeout_ms is not None:
            if _wait_fd_coop(sock.fileno(), WRITE, timeout_ms) == 0:
                raise socket.timeout("connect timed out")
        else:
            _wait_fd_coop(sock.fileno(), WRITE)
        err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if err == 0:
            return
        if err not in (errno.EINPROGRESS, errno.EALREADY):
            raise OSError(err, os.strerror(err))


def _connect_retry_backlog(sock, address):
    """POSIX AF_UNIX connect() returned EAGAIN: the listener's backlog is full
    and NO connection was started (unlike a TCP EINPROGRESS -- the unconnected
    socket would poll writable with SO_ERROR==0, so the SO_ERROR path would
    falsely report success).  A stock BLOCKING connect waits in the kernel for
    backlog space; emulate that cooperatively with a capped backoff until the
    connect completes or fails for real.  Only reached for a cooperative-
    blocking socket (timed / user-non-blocking sockets raise before we get
    here, matching stock, which never waits on EAGAIN)."""
    backoff = 0.0005
    while True:
        _co_sleep(backoff)
        if backoff < 0.05:
            backoff *= 2
        err = sock.connect_ex(address)
        if err == 0 or err == errno.EISCONN:
            return
        if err in (errno.EWOULDBLOCK, errno.EAGAIN):
            continue
        if err in (errno.EINPROGRESS, errno.EALREADY):
            _connect_wait_result(sock, None)   # backlog opened; now in flight
            return
        raise OSError(err, os.strerror(err))


def _patched_connect(self, address):
    if not _in_fiber():
        return _orig_connect(self, address)
    _make_nonblocking(self)
    # Resolve a hostname target via the cooperative resolver BEFORE connect_ex,
    # which would otherwise do a blocking inline libc getaddrinfo on the hub.
    address = _connect_resolve(self, address)
    # connect_ex returns the errno instead of raising, so a synchronous
    # completion (loopback frequently connects at once) and the in-flight
    # case share one path.
    err = self.connect_ex(address)
    if err == 0 or err == errno.EISCONN:
        return
    in_progress = (errno.EINPROGRESS, errno.EALREADY)
    if _IS_WINDOWS:
        # A non-blocking TCP connect on Windows reports WSAEWOULDBLOCK (not
        # EINPROGRESS) while the handshake is in flight -- treat it as such.
        in_progress = in_progress + (errno.EWOULDBLOCK,)
    if err not in in_progress:
        if err in (errno.EWOULDBLOCK, errno.EAGAIN):
            # POSIX only reaches here (Windows EWOULDBLOCK handled above): an
            # AF_UNIX connect with a full listen backlog.  NO connection was
            # started -- do NOT treat it as connect-in-progress (that reads
            # SO_ERROR==0 on an unconnected socket and reports FALSE SUCCESS).
            t = _coop_timeout(self)
            if t is None:
                return _connect_retry_backlog(self, address)
            # Timed or user-non-blocking socket: stock never waits on EAGAIN
            # from connect(), so surface it immediately like stock does.
            raise BlockingIOError(err, os.strerror(err))
        raise OSError(err, os.strerror(err))
    # In flight: wait for writability, then read the outcome via SO_ERROR.
    t = _coop_timeout(self)
    if t is _NONBLOCK:
        # User non-blocking connect: surface the in-progress state (BlockingIOError
        # EINPROGRESS) rather than parking, exactly like a stock non-blocking connect.
        raise BlockingIOError(err, os.strerror(err))
    timeout_ms = max(1, int(t * 1000)) if t is not None else None
    _connect_wait_result(self, timeout_ms)


def _patched_recvfrom(self, bufsize, flags=0):
    if not _in_fiber():
        return _orig_recvfrom(self, bufsize, flags)
    _make_nonblocking(self)
    while True:
        try:
            return _orig_recvfrom(self, bufsize, flags)
        except (BlockingIOError, InterruptedError):
            _wait_io(self, self.fileno(), READ)


def _patched_sendto(self, data, *args):
    if not _in_fiber():
        return _orig_sendto(self, data, *args)
    _make_nonblocking(self)
    while True:
        try:
            return _orig_sendto(self, data, *args)
        except (BlockingIOError, InterruptedError):
            _wait_io(self, self.fileno(), WRITE)


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
            _wait_io(self, self.fileno(), READ)


def _patched_recvmsg_into(self, buffers, ancbufsize=0, flags=0):
    if not _in_fiber():
        return _orig_recvmsg_into(self, buffers, ancbufsize, flags)
    _make_nonblocking(self)
    while True:
        try:
            return _orig_recvmsg_into(self, buffers, ancbufsize, flags)
        except (BlockingIOError, InterruptedError):
            _wait_io(self, self.fileno(), READ)


def _patched_sendmsg(self, buffers, ancdata=(), flags=0, address=None):
    if not _in_fiber():
        return _orig_sendmsg(self, buffers, ancdata, flags, address)
    _make_nonblocking(self)
    while True:
        try:
            return _orig_sendmsg(self, buffers, ancdata, flags, address)
        except (BlockingIOError, InterruptedError):
            _wait_io(self, self.fileno(), WRITE)


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
            _wait_io(self, self.fileno(), READ)


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
            # Re-read the OUT socket fd EVERY iteration -- NEVER a value cached
            # before the wait_fd(WRITE) park below.  A sibling on another hub can
            # close THIS socket while we are parked on its WRITE arm; _patched_close
            # wakes the park (netpoll_cancel_fd), but if we then drove os.sendfile /
            # re-parked on a fd NUMBER captured before the park, a fresh socketpair
            # that recycled that number would receive our file bytes / our wake --
            # foreign-byte crossover onto a live unrelated socket (the same fd-reuse
            # window _patched_close closes for accept/recv/connect, which all re-read
            # self.fileno() per retry).  A closed socket reports fileno() == -1, so
            # os.sendfile(-1, ...) raises EBADF and the loop unwinds cleanly below --
            # exactly the recv/connect close behaviour -- never touching a reused fd.
            sockno = self.fileno()
            try:
                sent = _raw_os_sendfile(sockno, fileno, offset, blocksize)
            except (BlockingIOError, InterruptedError):
                # Park on the SAME freshly-read fd we issued the op on -- a close
                # between the read and here makes wait_fd(-1) return immediately
                # (EBADF, not the CANCELLED sentinel) and the next iteration's
                # os.sendfile(-1, ...) unwinds, so we never park on a recycled fd.
                _wait_fd_coop(sockno, WRITE)
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
    """Clear the netpoll registration bit, then wake any fiber parked in
    accept()/recv()/connect() on this fd, then close.

    The wake (runloom_netpoll_cancel_fd) happens BEFORE _orig_close, while this
    socket still OWNS fd N (audit finding B6).  Cancelling first closes the
    cross-hub fd-REUSE window: with the old cancel-AFTER-close order, between
    _orig_close (which frees N) and cancel_fd(N) another hub could socket()/
    accept() reusing fd number N and link a fresh parker, which cancel_fd(N) would
    then spuriously cancel.  Cancelling first is now correct because a cancelled
    op honours the CANCELLED sentinel and raises OSError(ECANCELED) even on the
    still-open socket (the wait_fd_coop / _wait_fd_coop change, finding B3) --
    it no longer relies on the post-close retry hitting EBADF to unwind.  So a
    cross-fiber close still wakes the parked accept()/recv()/connect() (BUG #5),
    without the reuse window."""
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
    if fd >= 0:
        _SOCK_TIMEOUTS.pop(fd, None)
    if _netpoll_cancel_fd is not None and fd >= 0:
        try:
            _netpoll_cancel_fd(fd)        # wake parked waiters BEFORE freeing fd N
        except (OSError, ValueError):
            pass
    return _orig_close(self)


def _patched_detach(self):
    """Same bitmap clear as close: the fd is leaving our control."""
    try:
        fd = self.fileno()
    except (OSError, ValueError):
        fd = -1
    if fd >= 0:
        _SOCK_TIMEOUTS.pop(fd, None)
        if _netpoll_unregister is not None:
            try:
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
    # Cooperative settimeout/setblocking (defined in _base) keep the
    # _SOCK_TIMEOUTS side table honest across a user's LATER change to
    # non-blocking/timed: without them a settimeout(0)/setblocking(False) AFTER
    # prior I/O leaves the stale recorded timeout authoritative, so _coop_timeout
    # returns that deadline (or block-forever) instead of _NONBLOCK and the op the
    # user made non-blocking parks/hangs instead of raising BlockingIOError.  The
    # raw methods are captured in _base before this runs.
    s.settimeout  = _patched_settimeout
    s.setblocking = _patched_setblocking
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
    s.settimeout  = _raw_sock_settimeout
    s.setblocking = _raw_sock_setblocking
    if _HAVE_RECVMSG:
        s.recvmsg      = _orig_recvmsg
        s.recvmsg_into = _orig_recvmsg_into
    if _HAVE_SENDMSG:
        s.sendmsg = _orig_sendmsg
