"""Chan.__new__(Chan) (tp_new without tp_init) leaves self->ch == NULL;
every method then dereferences NULL -> hard crash of the interpreter."""
import runloom_c as rc

ch = rc.Chan.__new__(rc.Chan)     # bypass __init__ (e.g. pickle / copy protocols)
print("created", ch, flush=True)
print("about to call try_send on uninitialized Chan...", flush=True)
ch.try_send(1)                    # NULL deref expected
print("survived (no crash)")
