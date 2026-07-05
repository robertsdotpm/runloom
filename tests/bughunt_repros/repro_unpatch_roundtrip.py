"""patch() -> unpatch() reversibility audit: every patched binding should be
restored to the pre-patch original."""
import builtins, os, sys, time, socket, select, ssl, subprocess, signal, queue
import threading, _thread
import concurrent.futures as cf
import fcntl, hashlib, zlib, io
import multiprocessing  # so the mp patch applies
import getpass

orig = {
    "time.sleep": time.sleep,
    "os.read": os.read, "os.write": os.write, "os.close": os.close,
    "os.readv": getattr(os, "readv", None), "os.writev": getattr(os, "writev", None),
    "os.waitpid": os.waitpid, "os.wait": os.wait, "os.waitid": os.waitid,
    "os.wait3": os.wait3, "os.wait4": os.wait4, "os.system": os.system,
    "os.stat": os.stat, "os.open": os.open, "os.fsync": os.fsync,
    "builtins.open": builtins.open, "io.open": io.open,
    "builtins.input": builtins.input, "builtins.compile": builtins.compile,
    "threading.Lock": threading.Lock, "threading.RLock": threading.RLock,
    "threading.Event": threading.Event, "threading.Condition": threading.Condition,
    "threading.Semaphore": threading.Semaphore,
    "threading.BoundedSemaphore": threading.BoundedSemaphore,
    "threading.Thread.join": threading.Thread.join,
    "_thread.allocate_lock": _thread.allocate_lock,
    "_thread.RLock": _thread.RLock,
    "queue.SimpleQueue": queue.SimpleQueue,
    "cf.ThreadPoolExecutor": cf.ThreadPoolExecutor,
    "subprocess.Popen.wait": subprocess.Popen.wait,
    "subprocess.Popen.__init__": subprocess.Popen.__init__,
    "signal.sigwait": signal.sigwait,
    "signal.pause": signal.pause,
    "select.select": select.select,
    "fcntl.flock": fcntl.flock, "fcntl.lockf": fcntl.lockf,
    "hashlib.sha256": hashlib.sha256, "zlib.compress": zlib.compress,
    "socket.getaddrinfo": socket.getaddrinfo,
    "getpass.getpass": getpass.getpass,
    "sys._current_frames": sys._current_frames,
    "runloom_c.fiber": None,
    "tempfile._once_lock": None,
}
import tempfile
orig["tempfile._once_lock"] = tempfile._once_lock
import runloom_c
orig["runloom_c.fiber"] = runloom_c.fiber

import runloom.monkey as monkey
monkey.patch()
monkey.unpatch()

cur = {
    "time.sleep": time.sleep,
    "os.read": os.read, "os.write": os.write, "os.close": os.close,
    "os.readv": getattr(os, "readv", None), "os.writev": getattr(os, "writev", None),
    "os.waitpid": os.waitpid, "os.wait": os.wait, "os.waitid": os.waitid,
    "os.wait3": os.wait3, "os.wait4": os.wait4, "os.system": os.system,
    "os.stat": os.stat, "os.open": os.open, "os.fsync": os.fsync,
    "builtins.open": builtins.open, "io.open": io.open,
    "builtins.input": builtins.input, "builtins.compile": builtins.compile,
    "threading.Lock": threading.Lock, "threading.RLock": threading.RLock,
    "threading.Event": threading.Event, "threading.Condition": threading.Condition,
    "threading.Semaphore": threading.Semaphore,
    "threading.BoundedSemaphore": threading.BoundedSemaphore,
    "threading.Thread.join": threading.Thread.join,
    "_thread.allocate_lock": _thread.allocate_lock,
    "_thread.RLock": _thread.RLock,
    "queue.SimpleQueue": queue.SimpleQueue,
    "cf.ThreadPoolExecutor": cf.ThreadPoolExecutor,
    "subprocess.Popen.wait": subprocess.Popen.wait,
    "subprocess.Popen.__init__": subprocess.Popen.__init__,
    "signal.sigwait": signal.sigwait,
    "signal.pause": signal.pause,
    "select.select": select.select,
    "fcntl.flock": fcntl.flock, "fcntl.lockf": fcntl.lockf,
    "hashlib.sha256": hashlib.sha256, "zlib.compress": zlib.compress,
    "socket.getaddrinfo": socket.getaddrinfo,
    "getpass.getpass": getpass.getpass,
    "sys._current_frames": sys._current_frames,
    "runloom_c.fiber": runloom_c.fiber,
    "tempfile._once_lock": tempfile._once_lock,
}

bad = [k for k in orig if orig[k] is not cur[k]]
if bad:
    print("NOT RESTORED after unpatch():")
    for k in bad:
        print("  %-28s orig=%r now=%r" % (k, orig[k], cur[k]))
else:
    print("all restored")

# also check patch(); unpatch(); patch() works again
monkey.patch()
print("re-patch after unpatch: threading.Lock is CoLock:",
      threading.Lock is monkey.CoLock)
