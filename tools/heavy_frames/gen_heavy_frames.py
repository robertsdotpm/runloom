#!/usr/bin/env python3
"""Generate src/runloom_c/runloom_heavy_frames.h from the stdlib C-frame profile.

The profile (stdlib_fat_frames.json, trimmed from a full DWARF .eh_frame scan of
the 3.13t stdlib C extensions) lists every C function whose SINGLE stack frame is
larger than 8 KiB.  A goroutine runs on a small C stack, so one of these fat
frames can overflow it in a single call -- the standout is _decimal's
`squaretrans_pow2` at 256 KiB (big-integer multiply/pow).

C function names are not Python-visible, so we map the fat-frame *modules* to the
Python API symbols that reach them (hand-maintained -- you can't derive
`Decimal` from `squaretrans_pow2`).  Only modules whose work can actually run on
a goroutine are included; startup/path helpers, the already-cooperative
`select`, the already-neutralised module-shadowing hint, and test-only functions
are deliberately left out.  The emitted size per symbol is the largest fat frame
in its module.

The runtime cold-start optimizer (runloom_stackadvice.c) scans an unseen
goroutine kind's bytecode names for these symbols and, if present, starts it big
enough to hold the fat frame -- so a Decimal-heavy goroutine does not overflow on
its very first run, before the auto-sizer has measured it.

Regenerate:  python3 tools/heavy_frames/gen_heavy_frames.py
"""
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
PROFILE = os.path.join(HERE, "stdlib_fat_frames.json")
OUT = os.path.join(HERE, "..", "..", "src", "runloom_c", "runloom_heavy_frames.h")

# module -> Python-visible symbols that reach that module's fat C frames.
# Hand-maintained (C symbol -> Python API is not mechanically derivable).
MODULE_SYMBOLS = {
    "_decimal": ["Decimal", "getcontext", "localcontext", "setcontext", "Context"],
    "_socket":  ["gethostbyname", "gethostbyname_ex", "gethostbyaddr"],
}

# ---------------------------------------------------------------------------
# HEURISTIC class-detectors -- NOT measured stdlib frames.
#
# Some operations run deep, CUMULATIVE native call chains in third-party C
# libraries we neither ship nor scan.  The standout class is CRYPTOGRAPHY:
# signing / verification / AEAD encryption / KDFs route through OpenSSL
# (`cryptography`/`pyOpenSSL`), libsodium (`PyNaCl`), or `pycryptodome`, whose
# bignum / elliptic-curve / ASN.1 math is deep enough to overflow a small
# goroutine stack on the FIRST call -- before the auto-sizer has measured it.
#
# We deliberately do NOT try to DWARF-measure these the way the stdlib table is
# built: the binaries vary by version / platform / build flags, manylinux wheels
# are usually stripped, the cost is cumulative DEPTH rather than one fat frame,
# and cffi/Cython indirection hides the Python entry point from the native frame.
# Instead we give the COLD START a deliberately roomy "effective frame" and let
# the runtime auto-sizer LEARN THE REAL SIZE DOWN from there, on the actual
# installed binary, per deployment (which is strictly better than a static guess).
# 512 KiB here -> a 1 MiB first-run stack: cold_start adds ~50% headroom and
# rounds up to a power of two (pow2(512K * 1.5) == 1 MiB).
#
# This is a NAME list, not a size table, so it needs no installs and covers
# libraries we have never seen.  Grow it from a survey of common deps -- keep the
# names crypto-SPECIFIC (a false match just over-provisions virtual stack the
# auto-sizer reclaims, but generic names like `update`/`new`/`encode` would
# over-provision far too broadly).
HEURISTIC_FRAME_BYTES = 512 * 1024     # -> 1 MiB cold start (pow2(*1.5))
HEURISTIC_SYMBOLS = [
    # the core operations the heuristic targets: signing / verification /
    # encryption -- the verbs nearly every crypto library exposes.
    "encrypt", "decrypt", "sign", "verify",
    # `cryptography` (pyca/OpenSSL): AEAD ciphers, asymmetric keys, KDFs.
    "Cipher", "Fernet", "AESGCM", "AESCCM", "ChaCha20Poly1305",
    "Ed25519PrivateKey", "Ed448PrivateKey", "X25519PrivateKey",
    "RSAPrivateKey", "EllipticCurvePrivateKey", "DSAPrivateKey",
    "load_pem_private_key", "load_der_private_key", "load_ssh_private_key",
    "generate_private_key", "HKDF", "PBKDF2HMAC", "Scrypt",
    # PyNaCl / libsodium.
    "SigningKey", "VerifyKey", "SecretBox", "SealedBox",
    # PyCryptodome.
    "PKCS1_OAEP", "pkcs1_15", "pss", "eddsa",
    # hashlib / password KDFs -- genuinely deep native work.
    "pbkdf2_hmac", "scrypt", "hashpw", "gensalt",
]


def main():
    prof = json.load(open(PROFILE))
    # largest fat frame per module
    by_module = {}
    for f in prof["fat_frames_over_8k"]:
        m, b = f["module"], f["stack_bytes"]
        if b > by_module.get(m, 0):
            by_module[m] = b

    rows = []
    seen = set()
    for module, syms in MODULE_SYMBOLS.items():
        bytes_ = by_module.get(module)
        if bytes_ is None:
            continue
        for sym in syms:
            rows.append((sym, bytes_, module))
            seen.add(sym)
    # Heuristic class-detectors (crypto): a fixed roomy cold-start frame, not a
    # measured one.  Skip any name a measured stdlib entry already claims.
    for sym in HEURISTIC_SYMBOLS:
        if sym in seen:
            continue
        rows.append((sym, HEURISTIC_FRAME_BYTES, "heuristic:crypto"))
        seen.add(sym)
    rows.sort(key=lambda r: (-r[1], r[0]))

    lines = []
    lines.append("/* runloom_heavy_frames.h -- GENERATED by "
                 "tools/heavy_frames/gen_heavy_frames.py.  Do not edit by hand.")
    lines.append(" *")
    lines.append(" * Python-visible symbols whose C implementation has an unusually fat SINGLE")
    lines.append(" * stack frame (from the stdlib .eh_frame profile, %s)."
                 % prof["meta"]["python"])
    lines.append(" * The cold-start optimizer bumps a goroutine's first-incarnation stack to")
    lines.append(" * hold the fat frame when its code references one of these.  Stdlib sizes are")
    lines.append(" * the largest fat frame in the symbol's module; `heuristic:crypto` entries are")
    lines.append(" * NOT measured -- they give signing/verification/encryption a roomy cold start")
    lines.append(" * (512 KiB -> 1 MiB) that the runtime auto-sizer then learns down. */")
    lines.append("#ifndef RUNLOOM_HEAVY_FRAMES_H")
    lines.append("#define RUNLOOM_HEAVY_FRAMES_H")
    lines.append("#include <stddef.h>")
    lines.append("")
    lines.append("typedef struct { const char *sym; size_t frame_bytes; } runloom_heavy_frame_t;")
    lines.append("")
    lines.append("static const runloom_heavy_frame_t runloom_heavy_frames[] = {")
    for sym, b, module in rows:
        lines.append('    {"%s", %d},   /* %s */' % (sym, b, module))
    lines.append("};")
    lines.append("#define RUNLOOM_HEAVY_FRAME_COUNT "
                 "((int)(sizeof runloom_heavy_frames / sizeof runloom_heavy_frames[0]))")
    lines.append("")
    lines.append("#endif /* RUNLOOM_HEAVY_FRAMES_H */")
    text = "\n".join(lines) + "\n"
    with open(os.path.normpath(OUT), "w") as f:
        f.write(text)
    print("wrote", os.path.normpath(OUT), "with", len(rows), "symbols")


if __name__ == "__main__":
    main()
