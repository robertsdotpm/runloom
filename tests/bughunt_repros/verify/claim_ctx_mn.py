import time as wall, runloom, runloom.context as ctx
def main():
    c, cancel = ctx.WithTimeout(ctx.Background(), 3.0)
    runloom.sleep(0.2)   # let the waker park first
    cancel()
    print("deadline_g =", c._deadline_g)
t0 = wall.monotonic()
runloom.run(2, main)
print("run(2) took %.2fs" % (wall.monotonic() - t0))
