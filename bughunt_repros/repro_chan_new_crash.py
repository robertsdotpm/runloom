"""Chan/Mutex objects created without __init__ (e.g. a subclass overriding
__init__, or copy/pickle protocols) have self->ch == NULL; methods deref it."""
import runloom_c
ch = runloom_c.Chan.__new__(runloom_c.Chan)
print("created", ch)
ch.send(1)   # NULL runloom_chan_t* -> expected SIGSEGV
print("survived send")
