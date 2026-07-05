import runloom_c as rc
ch = rc.Chan.__new__(rc.Chan)   # bypass __init__
print('about to call try_send on uninitialized Chan...', flush=True)
ch.try_send(1)                  # NULL deref
print('survived')
