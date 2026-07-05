import runloom, runloom_c as rc
ch, ch2 = rc.Chan(0), rc.Chan(0)
def main():
    rc.mn_fiber(lambda: ch2.recv())          # blocked forever
    rc.mn_fiber(lambda: rc.select([('recv', ch)]))  # blocked forever
    # main returns; both fibers deadlocked
runloom.run(2, main)
