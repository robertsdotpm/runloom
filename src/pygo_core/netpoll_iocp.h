/* netpoll_iocp.h -- Windows IOCP+AFD backend for pygo netpoll.
 *
 * Why this exists: WSAPoll is fine up to a few hundred sockets per
 * pump call, but it walks the entire fd-set linearly per call.  At
 * 10k+ concurrent connections per hub the linear walk dominates.
 * IOCP-with-AFD avoids that -- the kernel only signals sockets that
 * actually became ready, and the wait is O(1) on socket count.
 *
 * How it works: we use the same trick libuv and wepoll use --
 * NtDeviceIoControlFile against the undocumented \Device\Afd handle
 * with IOCTL_AFD_POLL.  The IOCTL is "asynchronous select on this
 * SOCKET; complete when ready or timeout".  When it completes, the
 * IRP packet flows back via the IOCP we associated with \Device\Afd.
 *
 * The AFD interface isn't documented by Microsoft, but it's been
 * stable since Windows NT 4.0 (it's what Winsock's select() uses
 * internally).  libuv depends on it; wepoll exposes it as an
 * epoll-like API.  Our usage matches libuv's: per-socket request,
 * one outstanding poll IRP at a time, completion delivers (revents,
 * status).
 *
 * On hosts where AFD isn't available (very old NT, restricted
 * sandboxes), netpoll falls through to WSAPoll, then select.  All
 * three backends share the same pygo_netpoll_wait_fd / _pump
 * contract.
 */
#ifndef PYGO_NETPOLL_IOCP_H
#define PYGO_NETPOLL_IOCP_H

#include "plat.h"

#if defined(PYGO_OS_WINDOWS)

#include "plat_compat.h"

/* Returns 0 if the AFD-based IOCP backend is usable on this host
 * (process has loaded ntdll, the IOCTL works, etc.), -1 otherwise.
 * Idempotent.  Loads NtDeviceIoControlFile + NtCreateFile from
 * ntdll.dll the first time. */
int pygo_iocp_init(void);

/* Tear down the IOCP + AFD handles.  Called from pygo_netpoll_fini. */
void pygo_iocp_fini(void);

/* Submit a poll request for `fd` (a SOCKET, cast to int per pygo's
 * fd convention).  `events` is the same PYGO_NETPOLL_READ/WRITE
 * bitmask the rest of pygo uses.  `timeout_ns` is when to give up
 * (-1 = forever).  Returns 0 on success; the caller waits via
 * pygo_iocp_pump.  -1 on error. */
int pygo_iocp_submit(int fd, int events, long long timeout_ns);

/* Wait for at most timeout_ns for the next completion (-1 = forever).
 * Writes the completed fd into *out_fd and the readiness mask into
 * *out_events.  Returns 1 if a completion arrived, 0 on timeout,
 * -1 on error.  Caller loops to drain. */
int pygo_iocp_wait(long long timeout_ns,
                   int *out_fd, int *out_events);

/* Pump-wake: break an idle pump out of GetQueuedCompletionStatus from
 * another thread (the IOCP analogue of the epoll eventfd write).
 * pygo_iocp_wake_armed() reports whether the IOCP exists so a wake can
 * be posted; pygo_iocp_wake() posts it.  Both are no-ops/-1 when IOCP
 * was never created. */
int pygo_iocp_wake_armed(void);
int pygo_iocp_wake(void);

#endif /* PYGO_OS_WINDOWS */
#endif /* PYGO_NETPOLL_IOCP_H */
