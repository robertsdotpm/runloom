import runloom, runloom_c as rc
ch = rc.Chan(0)
def main():
    rc.mn_fiber(lambda: rc.select([('recv', ch)]))
runloom.run(2, main)
