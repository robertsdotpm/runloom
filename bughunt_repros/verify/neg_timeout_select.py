"""Verify: select.select with negative timeout — stock vs runloom-patched (in fiber)."""
import socket as _bare_socket
import sys

# --- stock behavior first, before patching ---
import select as sel_mod
a, b = _bare_socket.socketpair()
try:
    r = sel_mod.select([a], [], [], -1)
    print("stock: returned", r)
except ValueError as e:
    print("stock: ValueError:", e)
except Exception as e:
    print("stock:", type(e).__name__, e)

# empty-fd-list case, stock
try:
    r = sel_mod.select([], [], [], -1)
    print("stock-empty: returned", r)
except ValueError as e:
    print("stock-empty: ValueError:", e)

import runloom.monkey as monkey
monkey.patch()
import select
import runloom_c as rc

out = {}
def main():
    try:
        out["patched"] = ("returned", select.select([a], [], [], -1))
    except ValueError as e:
        out["patched"] = ("ValueError", str(e))
    except Exception as e:
        out["patched"] = (type(e).__name__, str(e))
    try:
        out["patched-empty"] = ("returned", select.select([], [], [], -1))
    except ValueError as e:
        out["patched-empty"] = ("ValueError", str(e))
    except Exception as e:
        out["patched-empty"] = (type(e).__name__, str(e))

rc.fiber(main)
rc.run()
print("patched:", out.get("patched"))
print("patched-empty:", out.get("patched-empty"))
a.close(); b.close()
