import runloom_c
ch = runloom_c.Chan.__new__(runloom_c.Chan)
print('created', ch)
ch.send(1)   # NULL runloom_chan_t* -> SIGSEGV
print('survived')
