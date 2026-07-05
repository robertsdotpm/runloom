import copy, runloom_c as rc
ch = rc.Chan(4)
try:
    c2 = copy.copy(ch)
    print('copy succeeded:', c2)
    print(len(c2))
except Exception as e:
    print('copy raised:', type(e).__name__, e)
