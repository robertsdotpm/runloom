import runloom
res = runloom.gather(lambda: 1, lambda: 2)   # never returns?
print(res)
