#!/bin/sh
# Optional benchmark dependencies, extracted locally; no system installation.
set -eu
if [ ! -f build/vendor/ips/root/usr/include/tbb/concurrent_queue.h ]; then
    mkdir -p build/vendor/ips
    (
        cd build/vendor/ips
        apt-get download libtbb-dev libtbb12 libtbbmalloc2 libtbbbind-2-5 libnuma-dev
        for archive in ./*.deb; do dpkg-deb -x "$archive" root; done
        rm ./*.deb
    )
fi
