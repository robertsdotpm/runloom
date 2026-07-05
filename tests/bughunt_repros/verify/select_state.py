import runloom, runloom_c as rc
ch, ch2 = rc.Chan(0), rc.Chan(0)
def main():
    rc.mn_fiber(lambda: ch2.recv())
    rc.mn_fiber(lambda: rc.select([('recv', ch)]))
    runloom.sleep(0.2)
    for f in runloom.fibers():
        print(f['id'], f.get('state'), f.get('wait_reason'))
    ch.send(1); ch2.send(1)
runloom.run(2, main)
