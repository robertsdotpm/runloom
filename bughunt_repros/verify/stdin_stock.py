import os, sys
r, w = os.pipe()
os.write(w, b"line-one\nline-two\n")
os.dup2(r, 0)
sys.stdin = os.fdopen(0, "r")
results = [input(), input()]
print("DONE:", results)
