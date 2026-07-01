import socket, select, sys
def scenario(tag):
    a, b = socket.socketpair()
    try:
        r = select.select([b], [], [], -1)
        print(tag, "returned", r, flush=True)
    except ValueError as e:
        print(tag, "ValueError:", e, flush=True)
    except Exception as e:
        print(tag, type(e).__name__, e, flush=True)
    a.close(); b.close()
if sys.argv[1] == "stock":
    scenario("stock:")
else:
    import runloom
    def main(): runloom.fiber(lambda: scenario("patched:"))
    runloom.monkey.patch(); runloom.run(2, main)
