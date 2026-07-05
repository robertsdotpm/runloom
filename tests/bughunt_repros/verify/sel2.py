import sys, runloom_c
ch = runloom_c.Chan.__new__(runloom_c.Chan)
sys.stdout.write('chan created\n'); sys.stdout.flush()
runloom_c.select([('recv', ch)])
print('survived select')
