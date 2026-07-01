import runloom_c as rc
m = rc.Mutex()
m.lock()
print('locked before reinit:', m.locked())
m.__init__()
print('locked after reinit:', m.locked())

# subclass path (a user forgetting super().__init__ would crash too)
class MyChan(rc.Chan):
    def __init__(self):
        pass  # forgot super().__init__()
c = MyChan()
print('subclass made; calling len() -> segfault expected', flush=True)
print(len(c))
