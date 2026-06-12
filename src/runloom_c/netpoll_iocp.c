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
} runloom_poll_ctx_t;

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
    if (afd & (AFD_POLL_RECEIVE | AFD_POLL_RECEIVE_EXPEDITED |
               AFD_POLL_DISCONNECT | AFD_POLL_ABORT |
               AFD_POLL_ACCEPT | AFD_POLL_LOCAL_CLOSE))
        events |= RUNLOOM_NETPOLL_READ;
    if (afd & (AFD_POLL_SEND | AFD_POLL_CONNECT_FAIL))
        events |= RUNLOOM_NETPOLL_WRITE;
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
    if (runloom_iocp_handle != NULL) {
        CloseHandle(runloom_iocp_handle);
        runloom_iocp_handle = NULL;
    }
    if (runloom_afd_handle != INVALID_HANDLE_VALUE) {
        CloseHandle(runloom_afd_handle);
        runloom_afd_handle = INVALID_HANDLE_VALUE;
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

int runloom_iocp_submit(int fd, int events, long long timeout_ns)
{
    runloom_poll_ctx_t *ctx;
    NTSTATUS st;
    SOCKET base;

    if (runloom_iocp_inited != 2 || runloom_afd_handle == INVALID_HANDLE_VALUE) {
        return -1;
    }

    base = runloom_iocp_base_socket((SOCKET)(uintptr_t)fd);
    if (base == INVALID_SOCKET) return -1;

    ctx = (runloom_poll_ctx_t *)calloc(1, sizeof(*ctx));
    if (ctx == NULL) return -1;
    ctx->fd = fd;
    ctx->requested = events;

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
         * FILE_SKIP_COMPLETION_PORT_ON_SUCCESS at init time. */
        return 0;
    }
    /* Hard error.  Drop the ctx -- caller's wait_fd will see no
     * completion and fall back to whatever its error path is. */
    free(ctx);
    return -1;
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

    ok = GetQueuedCompletionStatus(runloom_iocp_handle, &bytes, &key, &ov, ms);
    if (!ok && ov == NULL) {
        /* timeout */
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
         * returns and the scheduler drains its wake_list. */
        return 0;
    }
    ctx = CONTAINING_RECORD(ov, runloom_poll_ctx_t, overlapped);
    (void)key;
    (void)bytes;

    *out_fd = ctx->fd;

    /* AFD poll timeout: the driver writes NumberOfHandles = (# handles that
     * signaled) on completion -- 0 means the per-op Timeout elapsed with
     * nothing ready.  The Handles[0].Events field still holds the INPUT mask we
     * submitted (the buffer is shared in/out), so reading it would report a
     * phantom readiness (e.g. a wait_fd(READ, timeout) returning READ instead of
     * 0 on its deadline -- caught by test_netpoll_conformance on iocp-afd).
     * Report "nothing ready" (return 0) and let the parker wake via its deadline
     * heap entry, matching the wsapoll/select and epoll/kqueue backends. */
    if (ctx->poll_info.NumberOfHandles == 0) {
        *out_events = 0;
        free(ctx);
        return 0;
    }

    *out_events = runloom_from_afd_events(ctx->poll_info.Handles[0].Events);

    free(ctx);
    return 1;
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
