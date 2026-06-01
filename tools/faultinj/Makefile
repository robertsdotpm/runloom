# Build the LD_PRELOAD fault-injection shim.  See faultinj.c for env knobs.
faultinj.so: faultinj.c
	$(CC) -shared -fPIC -O2 -o $@ $< -ldl

clean:
	rm -f faultinj.so

.PHONY: clean
