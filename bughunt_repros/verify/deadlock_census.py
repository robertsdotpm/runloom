import runloom, time
def cpu():
    t0 = time.monotonic(); x = 0
    while time.monotonic() - t0 < 1.0: x += 1
    print("cpu fiber finished fine, x =", x)
runloom.mn_init(2)
runloom.mn_fiber(cpu)
print("mn_run returned", runloom.mn_run())
runloom.mn_fini()
