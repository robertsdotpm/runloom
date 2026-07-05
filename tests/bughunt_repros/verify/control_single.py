import runloom.sync as gsync
ran = {"v": False}
def w(): ran["v"] = True
def main():
    gsync.fiber(w)
    gsync.sleep(0.2)
gsync.run(main)
print("control sync.fiber under single-thread sync.run:", ran["v"], "(expect True)")
