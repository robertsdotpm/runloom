#!/bin/sh
# Exp D: build a free-threaded CPython 3.14.4t with mimalloc purge_delay defaulted
# to -1 (keep-resident) into a SEPARATE prefix, so the working 3.14.4t is untouched.
set -e
V=3.13.13
SRC=/home/x/Python-$V
PREFIX=/home/x/cpython-keepres
cd /home/x
[ -f Python-$V.tar.xz ] || curl -sS -o Python-$V.tar.xz https://www.python.org/ftp/python/$V/Python-$V.tar.xz
rm -rf "$SRC"
tar xf Python-$V.tar.xz -C /home/x
cd "$SRC"
# keep-resident: purge_delay default 10ms -> -1 (never purge -> no MADV_DONTNEED)
sed -i 's/{ 10,  UNINIT, MI_OPTION_LEGACY(purge_delay,reset_delay) }/{ -1,  UNINIT, MI_OPTION_LEGACY(purge_delay,reset_delay) }/' Objects/mimalloc/options.c
echo "PATCH CHECK:"; grep -n 'purge_delay,reset_delay' Objects/mimalloc/options.c
./configure --disable-gil --without-pydebug --prefix="$PREFIX" >/home/x/keepres_configure.log 2>&1
make -j"$(nproc)" >/home/x/keepres_make.log 2>&1
make install >/home/x/keepres_install.log 2>&1
"$PREFIX"/bin/python3 -VV
echo "KEEPRES_BUILD_DONE prefix=$PREFIX"
