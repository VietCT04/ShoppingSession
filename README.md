# Shopping Sessions — C++20

Exact native implementation of the [Databricks Shopping Sessions track](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions).
The pipeline, CSV parser, union-find, Parquet writer, validator driver, unit tests,
and benchmark driver are C++. The container needs no external engine or network
at run time.
The production sorter directly reuses DuckDB's vendored Ska Sort, PDQSort, and
Vergesort C++ components; see `third_party/duckdb_sort/README.md` for attribution.

## Build and run

Requires Linux, a C++20 compiler, pthreads, and Make.

```sh
make -j8
build/shopping /path/to/data /path/to/output --threads 8
# Or honor DATA_DIR, OUTPUT_DIR, and NCPU as supplied by the scorer:
build/shopping
```

Outputs are actual Parquet files, with DATE days, UTF8 strings, 64-bit totals,
and nullable top categories. Reruns replace existing Parquet files in the two
output table directories. Phase timings go to stderr.

## Correctness checks

```sh
make test
```

This compiles the C++ unit tests and runs all 36 official fixtures with the
baseline, sorting, hash deduplication, flat-table deduplication, and compact-ID
variants. A test-only DuckDB C library independently reads the produced Parquet
and performs exact, order-independent comparisons, including duplicate rows and
key uniqueness. The bootstrap script downloads a checksum-pinned Linux amd64
release into ignored `build/vendor/`; it needs `wget` and `unzip`.

Original fixture inputs, handwritten answers, and rules are preserved in
`fixtures/` and `docs/rules.md`.

## Public dev dataset

The organizers' original generator source is in `tools/generator/`. It requires
Rust 1.85+ solely to generate test data; it is excluded from the runtime image.

```sh
make dev                    # seed 1, 1M events, 250K clicks
sh scripts/fetch-dev-answer.sh
make run
make check-dev
make baseline               # independent attribution scan, single thread
build/check compare out-baseline data/dev-answer
```

## Optimization loop

```sh
make benchmark              # run matrix, three repeats, verify every result
CONFIG=flat-compact-8-64 make benchmark
CONFIG=duckdb-compact-8-256 make benchmark
# Restrict the driver and its children to eight logical CPUs on Linux:
taskset -c 0-7 build/check bench data/dev data/dev-answer 5
```

The driver measures from immediately before launching the child through process
exit, including parsing, output, and cleanup. Correctness checks happen outside
the timed interval. It records each successful run's duration and peak RSS in
`benchmarks/runs.jsonl`, and writes phase profiles to `benchmarks/*.log`.
Benchmark configurations are listed in `tools/check.cpp`.

GCC can build a profile-guided binary using a separate local training input.
The build uses compiler execution counters and writes `build/shopping-pgo`.
Keep the benchmark input separate from the profile training input.

```sh
make pgo PGO_DATA=data/dev
BINARY_SHORT=build/shopping-pgo CONFIG=row-csv,short-ids ROUND_ROBIN=1 \
  taskset -c 0-7 build/check bench data/practice20 out-practice-baseline 5
```

For a larger, fresh-seed differential test:

```sh
make dev SEED=7 EVENTS=20000000 CLICKS=5000000 DATA=data/practice20
build/shopping data/practice20 out-practice-baseline --baseline
SKIP_BASELINE=1 build/check bench data/practice20 out-practice-baseline 3
```

The large reference output comes from the simple C++ baseline. Only seed 1's
1M-event answers and the handwritten fixtures have an official independent
oracle. Use new seeds when checking an optimization. Do not infer final-score
performance from the 1M-event dataset alone.

## Algorithms

* Memory-map every CSV and split files into newline-aligned work chunks, including
  skewed files and header-only partitions. Preserve CR, spaces, case, and UTF8.
* Resolve all links with union-by-size and path compression, keeping each
  component's byte-wise minimum ID, including IDs without events.
* Partition events by the full event ID's hash. Keep malformed records out of
  deduplication, but retain invalid parsed records until the earliest ingest wins.
* Partition surviving events and clicks by resolved person. Hashes select
  partitions; byte comparisons determine identity.
* Sort each person's events by milliseconds and split only on gaps strictly
  greater than 30 minutes. Count the entire session on its start day.
* Sort clicks by person, time, and descending click ID. Maintain an inclusive
  seven-day sliding window; its final click wins attribution, and each active
  campaign assists once. Identical click duplicates are semantically idempotent.
* Aggregate categories by purchase revenue with byte-wise tie breaking. Reduce
  campaign totals across workers, including assist-only rows and organic.
* Write Parquet directly using PLAIN, uncompressed data pages, RLE definition
  levels, and Thrift compact metadata. Each partition's column pages are limited
  to signed 32-bit size.

`--baseline` forces one partition/thread and rescans the person's eligible clicks
for each session. It shares parsing and event validation, but uses a simpler
attribution algorithm as a differential oracle. `--dedup sort|hash|flat`,
`--shards N`, `--compact|--no-compact`, and `--sorter std|duckdb` expose the
optimizations for measurement. Defaults: 4096 dedup partitions and 256 person/
output partitions, flat dedup, inline IDs,
per-person histogram grouping and local sorting, and dense campaign totals. IDs longer than 15
bytes use an exact general path. `--compact` enables intermediate integer IDs
for comparison. `--no-dense-totals` restores
map aggregation for comparison.
`--no-group-person` restores sorting by person/time within each hash partition.
`--no-fused-csv` restores separate newline/field scanning; the default scanner
fuses both and immediately processes validated field views.
`--dedup-shards N` tunes dedup cache locality independently of `--shards N`.
Campaign-window membership and category revenue use arrays with sparse reset.
`--no-array-active` and `--no-array-categories` restore the map implementations.

## Submission container

Optional large-group sort experiments:

```sh
make build/shopping-ips
ROUND_ROBIN=1 BINARY=build/shopping-ips \
  CONFIG=person-8-256,person-ips4o-8-256,person-ips2ra-8-256 \
  build/check bench data/practice20 out-practice-baseline 3
```

This uses sequential IPS entry points on the pipeline's eight workers, without
nested worker pools. Optional TBB headers/libraries are extracted into ignored
`build/vendor/ips/` using the host's APT packages. They are not needed by the
default binary. A comma-separated CONFIG list plus ROUND_ROBIN=1 alternates
candidate/control order between repetitions.

```sh
make docker
sh scripts/docker_run.sh data/dev out-container
build/check compare out-container data/dev-answer
```

The multistage Dockerfile builds a portable binary, statically links the C++
runtime, and uses uid 1000. The pipeline writes only to OUTPUT_DIR and needs no
temporary files. `docker_run.sh` reproduces the published read-only filesystem,
no-network, memory, process, and CPU constraints. Generated data, outputs,
binaries, and benchmark logs are ignored by Git and excluded from the image.

See [benchmark results](docs/benchmarks.md) for measured performance and limits.
