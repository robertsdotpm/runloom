"""runloom_c.MachineCode -- execute runtime-provided native machine code.

A goroutine runs on a real C stack, so a JIT'd blob can be called straight from
one: MachineCode maps the bytes W^X and a call jumps the CPU directly into them
(no interpreter).  These tests assemble tiny x86-64 SysV blobs by hand and check
the results from the main thread and from goroutines (M:1 and M:N).

Execution is x86-64-only (the blobs are architecture-specific machine code); on
other arches the type is still checked but execution is skipped.  The blobs are
trusted/self-generated -- never do this with untrusted bytes.
"""
import platform
import unittest

import runloom
import runloom_c

_IS_X86_64 = platform.machine() in ("x86_64", "AMD64", "x86-64")

# x86-64 SysV: 1st arg rdi, 2nd rsi, return rax.
INC = bytes([0x48, 0x89, 0xf8, 0x48, 0xff, 0xc0, 0xc3])        # f(x)=x+1
SQ  = bytes([0x48, 0x89, 0xf8, 0x48, 0x0f, 0xaf, 0xc7, 0xc3])  # f(x)=x*x
ADD = bytes([0x48, 0x89, 0xf8, 0x48, 0x01, 0xf0, 0xc3])        # f(a,b)=a+b
RET0 = bytes([0x48, 0x31, 0xc0, 0xc3])                         # xor eax,eax; ret


class TestMachineCodeType(unittest.TestCase):
    def test_type_exists(self):
        self.assertTrue(hasattr(runloom_c, "MachineCode"))

    def test_empty_rejected(self):
        with self.assertRaises(ValueError):
            runloom_c.MachineCode(b"")


@unittest.skipUnless(_IS_X86_64, "blobs are x86-64 machine code")
class TestMachineCodeExec(unittest.TestCase):
    def test_inc(self):
        fn = runloom_c.MachineCode(INC)
        try:
            self.assertEqual(fn(41), 42)
            self.assertEqual(fn(-1), 0)
            self.assertEqual(fn(999), 1000)
        finally:
            fn.close()

    def test_square(self):
        with runloom_c.MachineCode(SQ) as fn:
            self.assertEqual(fn(9), 81)
            self.assertEqual(fn(0), 0)
            self.assertEqual(fn(7), 49)

    def test_two_args(self):
        with runloom_c.MachineCode(ADD) as fn:
            self.assertEqual(fn(20, 22), 42)
            self.assertEqual(fn(0, 0), 0)

    def test_zero_args(self):
        with runloom_c.MachineCode(RET0) as fn:
            self.assertEqual(fn(), 0)

    def test_address_and_size(self):
        with runloom_c.MachineCode(INC) as fn:
            self.assertEqual(fn.size, len(INC))
            self.assertIsInstance(fn.address, int)
            self.assertNotEqual(fn.address, 0)

    def test_close_is_idempotent_and_guards(self):
        fn = runloom_c.MachineCode(INC)
        self.assertEqual(fn(1), 2)
        fn.close()
        fn.close()                       # idempotent
        with self.assertRaises(ValueError):
            fn(1)                        # call-after-close guarded

    def test_too_many_args(self):
        with runloom_c.MachineCode(RET0) as fn:
            with self.assertRaises(TypeError):
                fn(1, 2, 3, 4, 5, 6, 7)

    def test_runs_in_single_goroutine(self):
        box = []

        def g():
            fn = runloom_c.MachineCode(SQ)
            box.append(fn(12))
            fn.close()

        runloom_c.go(g)
        runloom_c.run()
        self.assertEqual(box, [144])

    def test_runs_across_mn_goroutines(self):
        # Each goroutine JITs + calls native code on its own swapped C stack,
        # in genuine parallel under M:N.  Results come back over a Chan.
        N = 8
        ch = runloom_c.Chan()
        box = {}

        def worker(n):
            with runloom_c.MachineCode(SQ) as fn:
                ch.send((n, fn(n)))

        def main():
            for n in range(N):
                runloom_c.mn_go(lambda n=n: worker(n))
            got = {}
            for _ in range(N):
                (n, sq), ok = ch.recv()    # Chan.recv() -> (value, ok)
                got[n] = sq
            box["r"] = got

        runloom_c.mn_init(2)
        runloom_c.mn_go(main)
        runloom_c.mn_run()
        runloom_c.mn_fini()
        self.assertEqual(box["r"], {n: n * n for n in range(N)})


if __name__ == "__main__":
    unittest.main()
