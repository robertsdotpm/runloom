# heavy_frames -- the stdlib fat-frame profile + header generator

A goroutine runs on a small C stack, so a single stdlib C function with a very
large stack frame can overflow it in one call. `gen_heavy_frames.py` turns the
profile into the header the scheduler uses to size goroutine stacks safely.

- **`stdlib_fat_frames.json`** -- every 3.13t stdlib C-extension function whose
  SINGLE stack frame is larger than 8 KiB (trimmed from a full DWARF `.eh_frame`
  scan). The standout is `_decimal`'s `squaretrans_pow2` at 256 KiB.
- **`gen_heavy_frames.py`** -- maps those fat-frame *modules* to the Python API
  symbols that reach them (hand-maintained -- you can't derive `Decimal` from
  `squaretrans_pow2`) and emits `src/runloom_c/runloom_heavy_frames.h`. Only
  modules whose work can actually run on a goroutine are included.

```sh
python tools/heavy_frames/gen_heavy_frames.py    # regenerate the header
```

Regenerate when the profile changes (a new fat-frame module is found, or the
stdlib is re-scanned on a new CPython). Feeds the goroutine stack cold-start
sizing in the scheduler.
