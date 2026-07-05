import runloom, sys
print("has attrs:", [a for a in dir(runloom) if 'steal' in a.lower() or 'migrat' in a.lower() or 'tstate' in a.lower()])
