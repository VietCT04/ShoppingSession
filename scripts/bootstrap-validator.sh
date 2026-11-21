#!/bin/sh
# DuckDB is an independent Parquet reader used by the C++ test/benchmark tool.
# It is never loaded by, linked into, or copied into the submitted pipeline.
set -eu
mkdir -p build/vendor
if [ ! -f build/vendor/libduckdb.so ] || [ ! -f build/vendor/duckdb.h ]; then
    wget -q https://github.com/duckdb/duckdb/releases/download/v1.4.3/libduckdb-linux-amd64.zip -O build/vendor/duckdb.zip
    echo '3c7bb8d586d39ccce56442c3f6bafa97f53d7b6bb5405dac827442fcb31494aa  build/vendor/duckdb.zip' | sha256sum -c -
    unzip -oq build/vendor/duckdb.zip libduckdb.so duckdb.h -d build/vendor
    rm build/vendor/duckdb.zip
fi
