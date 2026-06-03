# Releasing runloom to PyPI

The PyPI distribution name is **`runloom`** — same as the import name (`import runloom`). Version lives in
`pyproject.toml` (`[project] version`).

runloom is a C-extension package, so for users to `pip install runloom`
without a compiler we publish **prebuilt wheels**. There is **no hosted CI**
in this project (deliberately — see `CLAUDE.md`), so wheels are built by hand,
once per platform, per release.

## What a user gets

- **Wheel present for their platform + Python** → `pip` downloads it; no
  compiler, no build. (Linux x86_64/aarch64, macOS arm64/x86_64, Windows
  AMD64, CPython 3.11–3.14.)
- **No matching wheel** → `pip` downloads the **sdist** and compiles locally
  (needs a C compiler). The sdist is complete and builds on every supported
  arch (both `.S` asm files ship — see `MANIFEST.in`).

So: the more platforms you build wheels on at release time, the more users get
the zero-compile experience. The sdist guarantees it still installs everywhere
else.

## One-time setup

Install the maintainer tooling:

```bash
pip install "runloom[dev]"     # build + twine + cibuildwheel
```

- **Linux wheels** need **Docker** (cibuildwheel builds inside manylinux
  containers).
- **macOS wheels** must be built on a Mac; **Windows wheels** on Windows.

## Release steps

1. **Bump the version** in `pyproject.toml`, commit.

2. **Build the sdist + this platform's wheels:**

   ```bash
   ./scripts/build_wheels.sh
   ```

   Repeat on each platform you have access to (Linux+Docker, a Mac, a Windows
   box). Collect every `wheelhouse/*.whl` into one folder. The `dist/*.tar.gz`
   sdist is identical everywhere — keep just one.

3. **Check the artifacts:**

   ```bash
   twine check dist/*.tar.gz wheelhouse/*.whl
   ```

4. **Smoke-test a wheel with no compiler available** (proves it needs none):

   ```bash
   python -m venv /tmp/t && CC=/bin/false /tmp/t/bin/pip install wheelhouse/<your>.whl
   /tmp/t/bin/python -c "import runloom; print(runloom.backend(), runloom.netpoll_backend())"
   ```

5. **Upload** (do a TestPyPI dry run first if you like:
   `twine upload -r testpypi ...`):

   ```bash
   twine upload dist/*.tar.gz wheelhouse/*.whl
   ```

That's it — `pip install runloom` now serves wheels to everyone on a
covered platform, and the sdist to everyone else.

## Quick local build without cibuildwheel

To produce just an sdist + a wheel for the Python you're on (no matrix, no
Docker):

```bash
python -m build          # -> dist/*.tar.gz and dist/*.whl
```
