import time as wall, runloom, runloom.context as ctx
def main():
    c, cancel = ctx.WithTimeout(ctx.Background(), 3.0)
    runloom.sleep(0.2)
    cancel()
    print("deadline_g =", c._deadline_g)
t0 = wall.monotonic()
runloom.run(1, main)
print("run(1) took %.2fs" % (wall.monotonic() - t0))
