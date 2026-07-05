import runloom, runloom_c
# 1) Chan recv outside run: does it raise a diagnostic?
try:
    runloom.Chan().recv()
    print("chan recv: returned??")
except BaseException as e:
    print("chan recv raised:", type(e).__name__, e)
# 2) does runloom_c.fiber outside run raise or silently queue?
g = runloom_c.fiber(lambda: print("fiber ran"))
print("fiber() returned:", g)
print("mn_hub_count:", runloom_c.mn_hub_count())
print("current_g:", runloom_c.current_g())
