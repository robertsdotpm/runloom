/* netpoll_iocp.c -- IOCP+AFD backend.
 *
 * Implementation notes:
 *
 * 1. We open \Device\Afd via NtCreateFile.  This handle is what
 *    Winsock itself uses under the hood for select()/recv() etc.
 *    Once associated with an IOCP, every async operation on it
 *    delivers completions to that IOCP.
 *
 * 2. To "poll" a socket we build an AFD_POLL_INFO struct describing
 *    {SOCKET, events-of-interest, timeout} and submit it via
 *    NtDeviceIoControlFile with IOCTL_AFD_POLL.  The call is
 *    asynchronous -- it returns STATUS_PENDING immediately, and
 *    the IRP completes via the IOCP when one of:
 *      a. the socket becomes ready for one of the requested events
 *      b. the timeout expires
 *      c. the socket errors / closes
 *
 * 3. On completion, the IO_STATUS_BLOCK and the AFD_POLL_INFO
 *    (we keep the same struct alive across the call) tell us
 *    which socket and which events.
 *
 * 4. We pass a per-request "context" pointer (the original `fd` as
 *    a uintptr) as the CompletionKey of the IOCP queue submission
 *    so GetQueuedCompletionStatus tells us which fd fired without
 *    walking a list.
 *
 * Memory: each in-flight poll holds an AFD_POLL_INFO + an
 * IO_STATUS_BLOCK + an OVERLAPPED slot.  We allocate these per
 * request and free on completion.  At steady state the count
 * equals the number of currently-parked fibers.
 *
 * AFD struct definitions: cribbed verbatim from wepoll and libuv.
 * Both projects have been doing this since ~2015 with no Windows
 * release breaking them.  These structures live in ntoskrnl's
 * AFD driver and are an effective public ABI even though
 * Microsoft never documented them.
 */
#include "netpoll_iocp.h"

#if defined(RUNLOOM_OS_WINDOWS)

#include "netpoll.h"

#include <stdlib.h>
#include <string.h>
#include <mswsock.h>            /* SIO_BASE_HANDLE */

/* ============================================================ */
/* NT / AFD declarations                                        */
/* ============================================================ */

typedef LONG NTSTATUS;
#define NT_SUCCESS(s)  (((NTSTATUS)(s)) >= 0)
#ifndef STATUS_SUCCESS
#  define STATUS_SUCCESS            ((NTSTATUS)0x00000000L)
#endif
#ifndef STATUS_PENDING
#  define STATUS_PENDING            ((NTSTATUS)0x00000103L)
#endif
#ifndef STATUS_INVALID_HANDLE
#  define STATUS_INVALID_HANDLE     ((NTSTATUS)0xC0000008L)
#endif
#ifndef STATUS_CANCELLED
#  define STATUS_CANCELLED          ((NTSTATUS)0xC0000120L)
#endif

/* NtCreateFile disposition value -- normally lives in <ntifs.h> in the
 * WDK, not in the public windows.h.  Hand-define it here. */
#ifndef FILE_OPEN
#  define FILE_OPEN                 0x00000001
#endif
#ifndef FILE_OPEN_IF
#  define FILE_OPEN_IF              0x00000003
#endif
#ifndef OBJ_CASE_INSENSITIVE
#  define OBJ_CASE_INSENSITIVE      0x00000040L
#endif
#ifndef OBJ_INHERIT
#  define OBJ_INHERIT               0x00000002L
#endif

typedef struct _IO_STATUS_BLOCK {
    union {
        NTSTATUS Status;
        PVOID Pointer;
    };
    ULONG_PTR Information;
} IO_STATUS_BLOCK, *PIO_STATUS_BLOCK;

typedef struct _UNICODE_STRING {
    USHORT Length;
    USHORT MaximumLength;
    PWSTR  Buffer;
} UNICODE_STRING, *PUNICODE_STRING;

typedef struct _OBJECT_ATTRIBUTES {
    ULONG Length;
    HANDLE RootDirectory;
    PUNICODE_STRING ObjectName;
    ULONG Attributes;
    PVOID SecurityDescriptor;
    PVOID SecurityQualityOfService;
} OBJECT_ATTRIBUTES, *POBJECT_ATTRIBUTES;

/* AFD_POLL_INFO and friends, transcribed from wepoll. */
typedef struct _AFD_POLL_HANDLE_INFO {
    HANDLE Handle;
    ULONG Events;
    NTSTATUS Status;
} AFD_POLL_HANDLE_INFO, *PAFD_POLL_HANDLE_INFO;

typedef struct _AFD_POLL_INFO {
    LARGE_INTEGER Timeout;
    ULONG NumberOfHandles;
    ULONG Exclusive;
    AFD_POLL_HANDLE_INFO Handles[1];
} AFD_POLL_INFO, *PAFD_POLL_INFO;

#define AFD_POLL_RECEIVE             0x0001
#define AFD_POLL_RECEIVE_EXPEDITED   0x0002
#define AFD_POLL_SEND                0x0004
#define AFD_POLL_DISCONNECT          0x0008
#define AFD_POLL_ABORT               0x0010
#define AFD_POLL_LOCAL_CLOSE         0x0020
#define AFD_POLL_ACCEPT              0x0080
#define AFD_POLL_CONNECT_FAIL        0x0100

#define IOCTL_AFD_POLL  0x00012024

/* ntdll exports we need.  Loaded via GetProcAddress so the
 * extension doesn't refuse to load on hosts missing them. */
typedef NTSTATUS (NTAPI *NtCreateFile_t)(
    PHANDLE FileHandle,
    ACCESS_MASK DesiredAccess,
    POBJECT_ATTRIBUTES ObjectAttributes,
    PIO_STATUS_BLOCK IoStatusBlock,
    PLARGE_INTEGER AllocationSize,
    ULONG FileAttributes,
    ULONG ShareAccess,
    ULONG CreateDisposition,
    ULONG CreateOptions,
    PVOID EaBuffer,
    ULONG EaLength);

typedef NTSTATUS (NTAPI *NtDeviceIoControlFile_t)(
    HANDLE FileHandle,
    HANDLE Event,
    PVOID  ApcRoutine,
    PVOID  ApcContext,
    PIO_STATUS_BLOCK IoStatusBlock,
    ULONG  IoControlCode,
    PVOID  InputBuffer,
    ULONG  InputBufferLength,
    PVOID  OutputBuffer,
    ULONG  OutputBufferLength);

typedef NTSTATUS (NTAPI *NtCancelIoFileEx_t)(
    HANDLE FileHandle,
    PIO_STATUS_BLOCK IoRequestToCancel,
    PIO_STATUS_BLOCK IoStatusBlock);

static NtCreateFile_t           pf_NtCreateFile = NULL;
static NtDeviceIoControlFile_t  pf_NtDeviceIoControlFile = NULL;
static NtCancelIoFileEx_t       pf_NtCancelIoFileEx = NULL;
static HANDLE runloom_afd_handle = INVALID_HANDLE_VALUE;
static HANDLE runloom_iocp_handle = NULL;
static volatile LONG runloom_iocp_inited = 0;

/* Per-poll context block.  Held alive by the IOCP itself until
 * GetQueuedCompletionStatus returns it; freed by the caller after
 * runloom_iocp_wait consumes the completion. */
typedef struct runloom_poll_ctx {
    OVERLAPPED      overlapped;     /* must be first */
    IO_STATUS_BLOCK iosb;
    /* AFD_POLL uses the same buffer as input and output -- the kernel
     * overwrites our request with the actual readiness mask on
     * completion.  wepoll / libuv do it this way too. */
    AFD_POLL_INFO   poll_info;
    int             fd;             /* the socket */
    int             requested;      /* RUNLOOM_NETPOLL_* mask */
    /* Snapshot of the submitting parker's acquire generation
     * (runloom_parked_t.gen), carried purely for diagnostics: a stale
     * completion's gen no longer matching the live parker on by_fd[fd]
     * is the signature of the released-then-reused-fd race this fix
     * closes.  Not consulted by the dispatch fast path (orphaned/Status
     * gate that). */
    unsigned int    gen;
    /* Set to 1 by runloom_iocp_cancel when the owning parker is released
     * early (deadline-heap timeout / fd-ready dispatch on a sibling /
     * cancel_all teardown), BEFORE NtCancelIoFileEx forces this IRP to
     * complete STATUS_CANCELLED.  runloom_iocp_wait reads it on every
     * completion: an orphaned completion is dropped (not dispatched), so
     * a stale IRP whose fd was reused by another fiber can never wake /
     * UAF the new owner's parker.  Distinct from the refs free-arbiter
     * below -- orphaned is the *dispatch* gate, refs is the *lifetime*
     * gate. */
    volatile LONG   orphaned;
    /* Two-party free arbiter (the UAF fix).  A ctx is referenced by
     * exactly TWO parties: (A) the in-flight AFD IRP, released when its
     * completion is drained by runloom_iocp_wait; and (B) the submitting
     * parker, released when runloom_parker_unlink runs (which may also
     * runloom_iocp_cancel, dereferencing the ctx).  Each party drops its
     * ref exactly once via runloom_iocp_ctx_unref; the party that drops
     * the LAST ref (refs 1->0) frees.  This decouples WHO frees from WHO
     * holds the pointer, so a concurrent wait-drain (party A) and
     * unlink-cancel (party B) on the SAME ctx can never free-under-
     * dereference: the cancel-deref happens while B still holds its ref
     * (refs >= 1), so A's drain cannot have freed it.  Initialised to 2
     * at submit.  volatile LONG for InterlockedDecrement. */
    volatile LONG   refs;
} runloom_poll_ctx_t;

/* Drop one reference to ctx; free on the 1->0 transition.  Both the
 * wait-drain (IRP party) and the unlink/cancel (parker party) call this
 * exactly once, so whichever runs LAST frees -- no double free, no
 * free-under-dereference.  Internal to this file. */
static void runloom_iocp_ctx_unref(runloom_poll_ctx_t *ctx)
{
    if (ctx == NULL) return;
    if (InterlockedDecrement(&ctx->refs) == 0) {
        free(ctx);
    }
}

/* Translate RUNLOOM_NETPOLL_READ/WRITE to AFD event mask. */
static ULONG runloom_to_afd_events(int events)
{
    ULONG mask = 0;
    if (events & RUNLOOM_NETPOLL_READ)
        mask |= AFD_POLL_RECEIVE | AFD_POLL_DISCONNECT |
                AFD_POLL_ABORT   | AFD_POLL_ACCEPT |
                AFD_POLL_RECEIVE_EXPEDITED;
    if (events & RUNLOOM_NETPOLL_WRITE)
        mask |= AFD_POLL_SEND | AFD_POLL_CONNECT_FAIL;
    /* LOCAL_CLOSE is always interesting -- the parked fiber
     * needs to be woken if the socket gets closed by another thread. */
    mask |= AFD_POLL_LOCAL_CLOSE;
    return mask;
}

static int runloom_from_afd_events(ULONG afd)
{
    int events = 0;
    /* Pure readiness bits stay direction-specific so healthy traffic wakes
     * only the correct-direction waiter (no spurious cross-direction wakes). */
    if (afd & (AFD_POLL_RECEIVE | AFD_POLL_RECEIVE_EXPEDITED | AFD_POLL_ACCEPT))
        events |= RUNLOOM_NETPOLL_READ;
    if (afd & AFD_POLL_SEND)
        events |= RUNLOOM_NETPOLL_WRITE;
    /* Teardown / error bits carry no IN/OUT readiness (a bare close, RST, or
     * failed connect) yet must release waiters in BOTH directions -- a
     * WRITE-direction (send/connect) parker on a closed fd would otherwise
     * never be reached by any waker: cancel_fd is a deliberate no-op on IOCP
     * (netpoll_wake_iouring.c.inc, to avoid a double-wake UAF vs the AFD
     * auto-completion), so AFD_POLL_LOCAL_CLOSE is the SOLE close-waker here.
     * Mirror the epoll EPOLLERR/HUP fold and the WSAPoll POLLHUP/POLLERR
     * branch, which both wake read+write so every waiter observes the close.
     * Each parker still receives only its OWN direction bit (dispatch masks
     * with p->events), so this widens who is woken, not what they observe.
     * Windows bug #2. */
    if (afd & (AFD_POLL_LOCAL_CLOSE | AFD_POLL_DISCONNECT |
               AFD_POLL_ABORT | AFD_POLL_CONNECT_FAIL))
        events |= RUNLOOM_NETPOLL_READ | RUNLOOM_NETPOLL_WRITE;
    return events;
}

/* ============================================================ */
/* init / fini                                                  */
/* ============================================================ */

int runloom_iocp_init(void)
{
    HMODULE ntdll;
    NTSTATUS st;
    UNICODE_STRING device_name;
    OBJECT_ATTRIBUTES oa;
    IO_STATUS_BLOCK iosb;
    /* Per-consumer AFD instance name -- AFD requires a sub-path under
     * \Device\Afd; the bare device path is reserved for the kernel
     * internal callers.  wepoll uses "Wepoll", libuv uses "Mswsock". */
    static const WCHAR afd_path[] = L"\\Device\\Afd\\Runloom";

    if (InterlockedCompareExchange(&runloom_iocp_inited, 1, 0) != 0) {
        /* Someone else won the init race; spin until they finish. */
        while (runloom_iocp_inited != 2) { /* spin */ }
        return (runloom_afd_handle != INVALID_HANDLE_VALUE) ? 0 : -1;
    }

    /* Resolve ntdll functions. */
    ntdll = GetModuleHandleA("ntdll.dll");
    if (ntdll == NULL) ntdll = LoadLibraryA("ntdll.dll");
    if (ntdll == NULL) goto fail;
    pf_NtCreateFile = (NtCreateFile_t)(void *)
        GetProcAddress(ntdll, "NtCreateFile");
    pf_NtDeviceIoControlFile = (NtDeviceIoControlFile_t)(void *)
        GetProcAddress(ntdll, "NtDeviceIoControlFile");
    pf_NtCancelIoFileEx = (NtCancelIoFileEx_t)(void *)
        GetProcAddress(ntdll, "NtCancelIoFileEx");
    if (pf_NtCreateFile == NULL || pf_NtDeviceIoControlFile == NULL) {
        goto fail;
    }

    /* Open \Device\Afd. */
    device_name.Buffer = (PWSTR)afd_path;
    device_name.Length = (USHORT)((sizeof(afd_path) - sizeof(WCHAR)));
    device_name.MaximumLength = (USHORT)sizeof(afd_path);

    memset(&oa, 0, sizeof(oa));
    oa.Length = sizeof(oa);
    oa.ObjectName = &device_name;
    /* OBJ_CASE_INSENSITIVE | OBJ_INHERIT not strictly needed. */

    /* wepoll / libuv attribute set: case-insensitive lookup of the
     * device object, all share modes, sync access only (we drive I/O
     * with our own IO_STATUS_BLOCK + IOCP). */
    oa.Attributes = OBJ_CASE_INSENSITIVE;

    memset(&iosb, 0, sizeof(iosb));
    st = pf_NtCreateFile(
        &runloom_afd_handle,
        SYNCHRONIZE,
        &oa,
        &iosb,
        NULL,                       /* AllocationSize */
        0,                          /* FileAttributes */
        FILE_SHARE_READ | FILE_SHARE_WRITE,
        FILE_OPEN,                  /* CreateDisposition */
        0,                          /* CreateOptions: ASYNC */
        NULL, 0);
    if (!NT_SUCCESS(st)) {
        runloom_afd_handle = INVALID_HANDLE_VALUE;
        goto fail;
    }

    /* Create the IOCP and associate AFD with it. */
    runloom_iocp_handle = CreateIoCompletionPort(
        INVALID_HANDLE_VALUE, NULL, 0, 0);
    if (runloom_iocp_handle == NULL) goto fail;
    if (CreateIoCompletionPort(runloom_afd_handle, runloom_iocp_handle,
                               (ULONG_PTR)0, 0) == NULL) {
        goto fail;
    }
    /* SKIP_SET_EVENT_ON_HANDLE: we never wait on the device handle
     * itself, so don't bother setting the event.  We intentionally do
     * NOT pass SKIP_COMPLETION_PORT_ON_SUCCESS -- our pump assumes
     * every poll, sync-completed or not, surfaces a completion. */
    SetFileCompletionNotificationModes(runloom_afd_handle,
        FILE_SKIP_SET_EVENT_ON_HANDLE);

    runloom_iocp_inited = 2;
    return 0;

fail:
    if (runloom_iocp_handle != NULL) {
        CloseHandle(runloom_iocp_handle);
        runloom_iocp_handle = NULL;
    }
    if (runloom_afd_handle != INVALID_HANDLE_VALUE) {
        CloseHandle(runloom_afd_handle);
        runloom_afd_handle = INVALID_HANDLE_VALUE;
    }
    runloom_iocp_inited = 2;       /* mark "init attempted, failed" */
    return -1;
}

void runloom_iocp_fini(void)
{
    /* Teardown ordering matters -- it must reclaim EVERY still-in-flight AFD
     * IRP's ctx before the IOCP is destroyed, else those calloc'd ctxs leak and
     * (worse) a late completion could post to a freed IOCP.
     *
     * Concurrency assumption: mn_fini quiesces every hub (no pump thread is
     * running) and runloom_netpoll_cancel_all_parked has run before
     * runloom_netpoll_fini -> runloom_iocp_fini, so nothing else touches the
     * IOCP / AFD handles or these ctxs while we drain.  This single thread is the
     * sole owner here, so the bounded drain below is race-free.
     *
     * This is the TERMINAL reclaim: it raw-frees each completion's ctx,
     * deliberately BYPASSING the two-party refcount (runloom_iocp_ctx_unref).
     * Post-quiescence the only conceivable remaining holder of a parker-party
     * ref is a parker on a fiber that will never run again (its hub is stopped),
     * so that ref would never be dropped -- a refcount-unref here would LEAK such
     * a ctx.  Raw-free is correct because no live thread can still dereference
     * these ctxs after the AFD handle is closed and the hubs are stopped.
     *
     *   1. CloseHandle(AFD) FIRST: closing the \Device\Afd handle forces all of
     *      its outstanding poll IRPs to complete STATUS_CANCELLED INTO the IOCP
     *      (any parker that somehow skipped its runloom_iocp_cancel is covered).
     *   2. Drain the IOCP with a bounded non-blocking loop, freeing every ctx
     *      surfaced (the OVERLAPPED is the first field, so CONTAINING_RECORD
     *      recovers the ctx).  Stop on the first empty poll with a NULL overlapped
     *      (no more queued completions and none pending).
     *   3. CloseHandle(IOCP) LAST, now that no ctx and no completion remains. */
    if (runloom_afd_handle != INVALID_HANDLE_VALUE) {
        CloseHandle(runloom_afd_handle);
        runloom_afd_handle = INVALID_HANDLE_VALUE;
    }
    if (runloom_iocp_handle != NULL) {
        DWORD bytes;
        ULONG_PTR key;
        LPOVERLAPPED ov;
        /* Bounded: GetQueuedCompletionStatus with 0 ms returns FALSE+ov==NULL
         * once the queue is empty.  `ok || ov` reaches a completion whether the
         * IRP succeeded or completed with a failure status (FALSE + non-NULL ov
         * == a failed/cancelled completion, which we still must free). */
        for (;;) {
            BOOL ok = GetQueuedCompletionStatus(runloom_iocp_handle,
                                                &bytes, &key, &ov, 0);
            if (ov != NULL) {
                /* A pump-wake (NULL-overlapped post) cannot occur here -- hubs
                 * are quiesced -- so any non-NULL ov is a poll-ctx completion. */
                free(CONTAINING_RECORD(ov, runloom_poll_ctx_t, overlapped));
                continue;
            }
            if (!ok) break;   /* empty queue: FALSE + NULL overlapped */
            /* ok && ov==NULL would be a stray NULL-overlapped post; ignore. */
            break;
        }
        CloseHandle(runloom_iocp_handle);
        runloom_iocp_handle = NULL;
    }
    runloom_iocp_inited = 0;
}

/* ============================================================ */
/* submit / wait                                                */
/* ============================================================ */

/* Winsock layers wrap the underlying AFD socket; AFD_POLL only works
 * against the base handle.  See wepoll / libuv for the same trick.
 *
 * SIO_BASE_HANDLE has a Windows 11 regression where it can fail with
 * WSAEFAULT against LSP-wrapped sockets; we fall back to the broader
 * BSP poll/select handles, finally accepting the socket itself if all
 * else fails.  Matches wepoll's ws_get_base_socket lookup chain. */
#ifndef SIO_BSP_HANDLE_POLL
#  define SIO_BSP_HANDLE_POLL    0x4800001D
#endif
#ifndef SIO_BSP_HANDLE_SELECT
#  define SIO_BSP_HANDLE_SELECT  0x4800001C
#endif
#ifndef SIO_BSP_HANDLE
#  define SIO_BSP_HANDLE         0x4800001B
#endif

static SOCKET runloom_iocp_base_socket(SOCKET s)
{
    static const DWORD ioctls[] = {
        SIO_BASE_HANDLE, SIO_BSP_HANDLE_POLL,
        SIO_BSP_HANDLE_SELECT, SIO_BSP_HANDLE,
    };
    SOCKET base = INVALID_SOCKET;
    DWORD bytes = 0;
    size_t i;
    for (i = 0; i < sizeof(ioctls)/sizeof(ioctls[0]); i++) {
        if (WSAIoctl(s, ioctls[i], NULL, 0,
                     &base, sizeof(base), &bytes,
                     NULL, NULL) != SOCKET_ERROR) {
            return base;
        }
    }
    /* No LSP base handle reachable; the socket likely IS the base
     * (or the Microsoft TCP/IP provider has been hijacked but is
     * forwarding poll IRPs correctly).  Try the original socket. */
    return s;
}

int runloom_iocp_submit(int fd, int events, long long timeout_ns,
                        unsigned int gen, void **out_ctx)
{
    runloom_poll_ctx_t *ctx;
    NTSTATUS st;
    SOCKET base;

    /* Always define the out param on EVERY path so the caller can store it
     * unconditionally; NULL means "no ctx to track / cancel". */
    if (out_ctx != NULL) *out_ctx = NULL;

    if (runloom_iocp_inited != 2 || runloom_afd_handle == INVALID_HANDLE_VALUE) {
        return -1;
    }

    base = runloom_iocp_base_socket((SOCKET)(uintptr_t)fd);
    if (base == INVALID_SOCKET) return -1;

    ctx = (runloom_poll_ctx_t *)calloc(1, sizeof(*ctx));
    if (ctx == NULL) return -1;
    ctx->fd = fd;
    ctx->requested = events;
    ctx->gen = gen;
    /* orphaned starts 0 from calloc.  Two references: the in-flight IRP and the
     * submitting parker (see runloom_iocp_ctx_unref).  Set BEFORE the IOCTL so a
     * completion drained on another thread the instant the IRP is queued already
     * sees refs==2.  On the error path below (IRP never queued) we free directly
     * -- no completion will arrive and the parker never receives the ctx. */
    ctx->refs = 2;

    /* AFD timeout is a relative time in 100-ns ticks (negative).
     * INT64_MAX = "wait forever". */
    if (timeout_ns < 0) {
        ctx->poll_info.Timeout.QuadPart = INT64_MAX;
    } else {
        ctx->poll_info.Timeout.QuadPart = -(timeout_ns / 100);
    }
    ctx->poll_info.NumberOfHandles = 1;
    ctx->poll_info.Exclusive = 0;
    ctx->poll_info.Handles[0].Handle = (HANDLE)base;
    ctx->poll_info.Handles[0].Events = runloom_to_afd_events(events);
    ctx->poll_info.Handles[0].Status = 0;

    st = pf_NtDeviceIoControlFile(
        runloom_afd_handle,
        NULL,                       /* no event handle */
        NULL,                       /* no APC */
        ctx,                        /* ApcContext -> CompletionKey for IOCP */
        &ctx->iosb,
        IOCTL_AFD_POLL,
        &ctx->poll_info,
        sizeof(ctx->poll_info),
        &ctx->poll_info,
        sizeof(ctx->poll_info));

    if (st == STATUS_SUCCESS || st == STATUS_PENDING) {
        /* Either case, the IOCP will deliver one completion -- the
         * kernel auto-posts sync completions because we didn't pass
         * FILE_SKIP_COMPLETION_PORT_ON_SUCCESS at init time.  Hand the
         * caller the ctx (a WEAK ref: still owned/freed by runloom_iocp_wait
         * when the completion drains) so the parker can runloom_iocp_cancel
         * this exact IRP if it is released before that completion arrives. */
        if (out_ctx != NULL) *out_ctx = ctx;
        return 0;
    }
    /* Hard error.  Drop the ctx -- caller's wait_fd will see no
     * completion and fall back to whatever its error path is.  Classify the
     * failure so module_run.c's PyErr_SetFromErrno surfaces a meaningful
     * OSError instead of OSError(0): AFD_POLL only works on Winsock sockets,
     * so the overwhelmingly common cause is a non-socket fd (e.g. an
     * os.pipe() read end) -- detect that via SO_TYPE and report ENOTSOCK. */
    free(ctx);
    {
        int so_type; int so_len = (int)sizeof(so_type);
        if (getsockopt((SOCKET)(uintptr_t)fd, SOL_SOCKET, SO_TYPE,
                       (char *)&so_type, &so_len) == SOCKET_ERROR
            && WSAGetLastError() == WSAENOTSOCK) {
            errno = ENOTSOCK;   /* non-socket fd: AFD poll is sockets-only */
        } else {
            errno = EIO;
        }
    }
    return -1;
}

/* Cancel the in-flight AFD_POLL IRP a released parker submitted (ctxp is the
 * runloom_poll_ctx_t* the parker tracked via runloom_iocp_submit's out_ctx).
 *
 * Called from the single parker-unlink choke point (runloom_parker_unlink) on
 * EVERY early-release path -- deadline-heap timeout, fd-ready dispatch on a
 * sibling completion, cancel_all teardown -- so a parker never leaves its IRP
 * in flight after its stack-allocated record is recycled.  This is the parker
 * party's single ref-drop (the unlink choke point extracts park->iocp_ctx
 * exactly once, so this runs at most once per ctx).
 *
 * ORDER MATTERS:
 *   1. InterlockedExchange(&ctx->orphaned, 1) FIRST so the pump thread that
 *      later drains this completion in runloom_iocp_wait observes orphaned!=0
 *      and DROPS it (no dispatch) -- a stale IRP whose fd was reused can never
 *      wake the new owner.
 *   2. NtCancelIoFileEx on the SHARED runloom_afd_handle, keyed by &ctx->iosb
 *      (the UserIosb the IRP was issued with), to FORCE that specific IRP to
 *      complete now as STATUS_CANCELLED instead of lingering until the socket
 *      actually closes.  The forced completion still posts to the IOCP, where
 *      runloom_iocp_wait drops the IRP party's ref.
 *   3. runloom_iocp_ctx_unref drops THIS (the parker) party's ref.
 *
 * Lifetime safety: steps 1-2 dereference ctx while the parker party STILL holds
 * its ref (we drop it only in step 3), so a concurrent wait-drain (the IRP
 * party) that frees on its decrement cannot have taken refs to 0 underneath us
 * -- the deref is free-safe.  The LAST party to unref frees; no double free. */
void runloom_iocp_cancel(void *ctxp)
{
    runloom_poll_ctx_t *ctx = (runloom_poll_ctx_t *)ctxp;
    if (ctx == NULL) return;
    /* Publish orphaned before the cancel so the drain side's load cannot miss
     * it (InterlockedExchange is a full barrier). */
    InterlockedExchange(&ctx->orphaned, 1);
    if (pf_NtCancelIoFileEx != NULL &&
        runloom_afd_handle != INVALID_HANDLE_VALUE) {
        IO_STATUS_BLOCK local_iosb;
        memset(&local_iosb, 0, sizeof(local_iosb));
        /* IoRequestToCancel == the IRP's UserIosb (&ctx->iosb).  Best-effort:
         * STATUS_NOT_FOUND (already completed) is fine -- the queued completion
         * carries the orphaned flag and the drain drops the IRP ref. */
        (void)pf_NtCancelIoFileEx(runloom_afd_handle, &ctx->iosb, &local_iosb);
    }
    /* Drop the parker party's ref last (frees iff the IRP party already drained). */
    runloom_iocp_ctx_unref(ctx);
}

int runloom_iocp_wait(long long timeout_ns,
                   int *out_fd, int *out_events)
{
    DWORD ms;
    DWORD bytes;
    ULONG_PTR key;
    LPOVERLAPPED ov;
    BOOL ok;
    runloom_poll_ctx_t *ctx;

    if (runloom_iocp_handle == NULL) return -1;

    /* Convert ns -> ms; the IOCP API uses ms with INFINITE = wait
     * forever.  Cap at 1 second per call so a hub can re-check
     * runloom_mn_fini's stopping flag. */
    if (timeout_ns < 0) {
        ms = INFINITE;
    } else {
        long long ms_ll = (timeout_ns + 999999LL) / 1000000LL;
        if (ms_ll > 1000) ms_ll = 1000;
        ms = (DWORD)ms_ll;
    }

    /* Drain loop.  A single GetQueuedCompletionStatus can return a completion
     * that belongs to a RELEASED parker -- one whose AFD IRP we cancelled (see
     * runloom_iocp_cancel): it completes STATUS_CANCELLED and/or carries the
     * orphaned flag, and its fd may already have been closed and REUSED by a
     * different fiber.  Dispatching it (return 1) would wake / UAF the new owner;
     * returning 0 would make the pump's drain loop BREAK, stranding real
     * readiness completions still queued behind it.  So we free the orphaned ctx
     * and RE-POLL with a 0 ms timeout to keep draining, looping until we hit a
     * genuine readiness completion (return 1) or the queue is empty (return 0).
     * The first iteration honours the caller's timeout; every retry uses 0 ms so
     * we never block past a real readiness while skipping orphans. */
    for (;;) {
        ok = GetQueuedCompletionStatus(runloom_iocp_handle, &bytes, &key, &ov, ms);
        ms = 0;   /* subsequent skips re-poll non-blocking */
        if (!ok && ov == NULL) {
            /* timeout (or the queue drained to empty across skips) -- nothing
             * (more) ready.  Matches the original 0-return timeout contract. */
            return 0;
        }
        /* The completion routes via the OVERLAPPED pointer.  For
         * STATUS_PENDING -> async completion the kernel delivers our
         * ApcContext (== ctx, since OVERLAPPED is the first field of
         * runloom_poll_ctx_t).  Reading `key` would only work for the
         * PostQueuedCompletionStatus path we use on STATUS_SUCCESS;
         * the async kernel-driven path has key == 0 (our AFD
         * association key). */
        if (ov == NULL) {
            /* A completion with a NULL OVERLAPPED is a pump-wake posted by
             * runloom_iocp_wake() (PostQueuedCompletionStatus, NULL overlapped) --
             * the Windows analogue of the epoll backend's eventfd write.  No
             * fd to dispatch; report "woke, nothing ready" so the pump loop
             * returns and the scheduler drains its wake_list.  Preserved
             * verbatim: a pump-wake must surface as a 0-return, not be skipped. */
            return 0;
        }
        ctx = CONTAINING_RECORD(ov, runloom_poll_ctx_t, overlapped);
        (void)key;
        (void)bytes;

        /* ORPHANED / CANCELLED gate (checked BEFORE touching *out_fd):
         * this completion belongs to a parker that was released early -- its
         * IRP was cancelled (runloom_iocp_cancel set orphaned + forced
         * STATUS_CANCELLED) -- so the fd may now be owned by a different fiber.
         * Drop the IRP party's ref and re-poll for the next queued completion
         * WITHOUT dispatching: skipping it here is what stops the
         * stale-completion -> reused-fd use-after-free.  NumberOfHandles==0 is
         * folded in: with the deadline heap as the sole timeout (AFD Timeout is
         * INFINITE; see netpoll_wait_fd.c.inc), a 0-handle completion is a
         * cancelled/stale IRP, not a per-op timeout -- skip it the same way.
         * The unref frees iff the parker party already dropped its ref. */
        if (InterlockedCompareExchange(&ctx->orphaned, 0, 0) != 0 ||
            ctx->iosb.Status == STATUS_CANCELLED ||
            ctx->poll_info.NumberOfHandles == 0) {
            runloom_iocp_ctx_unref(ctx);
            continue;
        }

        /* Real readiness.  Snapshot fd+events out of the ctx, THEN drop the IRP
         * party's ref.  The parker party's ref keeps the parker's
         * runloom_iocp_cancel(park->iocp_ctx) at the unlink choke point free-safe:
         * if it has not yet unlinked, its ref is still live (this unref leaves
         * refs>=1); if it already unlinked+unref'd, this unref frees.  Either way
         * the subsequent runloom_pump_dispatch_event finds the parker by fd and
         * its unlink-cancel never touches freed memory (the refcount, not the
         * pointer, arbitrates the free). */
        *out_fd = ctx->fd;
        *out_events = runloom_from_afd_events(ctx->poll_info.Handles[0].Events);
        runloom_iocp_ctx_unref(ctx);
        return 1;
    }
}

/* ============================================================ */
/* pump wake (cross-thread interrupt of an idle pump)           */
/* ============================================================ */

/* 1 if runloom_iocp_wake() can post (the IOCP exists), 0 otherwise. */
int runloom_iocp_wake_armed(void)
{
    return runloom_iocp_handle != NULL;
}

/* Break an idle pump out of GetQueuedCompletionStatus from any thread.
 * Posts a completion with a NULL OVERLAPPED, which runloom_iocp_wait()
 * recognises as a wake (returns 0 there).  This is the IOCP analogue of
 * writing the epoll backend's pump-interrupt eventfd: the single-thread
 * scheduler blocks here with no timeout when only blocking-offload /
 * iouring waiters are outstanding, and a worker thread pokes it on
 * completion so the scheduler wakes to drain its wake_list.  A no-op
 * (returns -1) if the IOCP was never created. */
int runloom_iocp_wake(void)
{
    if (runloom_iocp_handle == NULL) return -1;
    return PostQueuedCompletionStatus(runloom_iocp_handle, 0, 0, NULL) ? 0 : -1;
}

#endif /* RUNLOOM_OS_WINDOWS */
