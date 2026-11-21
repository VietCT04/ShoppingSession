# Local benchmark record

Hardware: Intel Core i5-12500H, Linux amd64, 16 logical CPUs. Pipeline runs use
8 worker threads unless marked baseline. These measurements are local results,
not official leaderboard scores. The host is shared and its frequency varies;
small timing differences need confirmation on the scorer.

## Initial implementation

Official seed-1 dev set: 1M requested events and 250K clicks, exact matches to
the published `dev-answer.zip`. Three process-start-to-exit measurements:

| Configuration | Median seconds | Peak RSS, approx. |
| --- | ---: | ---: |
| C++ baseline, single thread, full click rescans | 1.722 | 219 MiB |
| std::sort dedup, 64 partitions, 8 threads | 0.346 | 204 MiB |
| std::unordered_map dedup, 64 partitions, 8 threads | 0.325 | 206 MiB |
| std::unordered_map dedup, 256 partitions, 8 threads | 0.406 | 205 MiB |
| std::unordered_map dedup, 64 partitions, 4 threads | 0.569 | 212 MiB |

Fresh seed-7 scale test: 20M requested events and 5M clicks, about 1.8 GiB of CSV.
Every timed output exactly matches the single-thread reference output.

| Configuration | Median seconds (3 repeats) |
| --- | ---: |
| std::sort dedup, 64 partitions, 8 threads | 15.343 |
| std::unordered_map dedup, 64 partitions, 8 threads | 15.497 |
| std::unordered_map dedup, 256 partitions, 8 threads | **12.789** |
| std::unordered_map dedup, 64 partitions, 4 threads | 16.766 |
| Flat-table dedup + compact IDs, 64 partitions, 8 threads | 16.868 |
| DuckDB Ska Sort + compact IDs + hash dedup, 256 partitions | **12.462** |
| Above + ported DuckDB HashBytes | **12.429** |
| Corrected flat-table bucket hash + dense totals | **8.736** |
| Above + SSE2 CSV special-character scanning | **8.534** |
| Inline 128-bit IDs, direct sorting, no ID interning | **5.498** |
| Per-person histograms + timestamp-only local sorting | **4.992** |

The per-person experiment lives on `perf/person-partition`. Its three exact
20M runs took 4.992, 5.272, and 4.859 seconds. It uses ID encoding, a histogram,
prefix sums, and scatter to make each person's events/clicks contiguous. Groups
up to 24 records use insertion sort, medium groups use PDQSort, and large groups
use radix sorting. Sorting work fell from roughly 5.1–5.3 worker-seconds to 1.2–1.3;
the histogram/scatter and encoding passes add memory traffic. It is selected as
the default; `--no-group-person` retains the preceding algorithm. All 396
fixture/configuration checks and unit tests pass.

Optional `ankerl::unordered_dense` 5.3.3 deduplication was tested separately on
`perf/dense-dedup`. All 432 fixture/configuration checks and official dev answers
pass. Its 20M runs took 6.612, 5.869, and 5.293 seconds (median 5.869), so it has
not replaced the custom flat table. Dedup itself fell to 0.78 seconds in one run,
but unrelated phases also varied substantially; close comparisons need
alternating control/candidate runs on the same host.

The packed-ID record's three runs were 4.915, 5.498, and 5.801 seconds, all
matching the original baseline. IDs up to 15 bytes live inline; longer IDs keep
an exact mapped view. Both paths preserve arbitrary bytes and byte-wise order.
The new default removes the intermediate user-ID interning pass. The old
string-view implementation remains available as `make build/shopping-view`.

The SIMD candidate ran in 8.398, 8.534, and 8.630 seconds, all exact matches.
It also passes 200K randomized scalar-versus-SIMD CSV comparisons and the full
fixture suite. This is still far from the 0.3-second leaderboard target; the next
experiment changes ID representation to remove pointer-heavy processing.

The 8.736-second record uses flat deduplication, 256 partitions, compact IDs,
ported DuckDB sorting/hashing, and dense campaign totals. Its three runs were
8.736, 8.865, and 8.685 seconds, all exact matches. Peak RSS was about 3.8 GiB.
This is the current production default. Fixing secondary hash selection avoids
probe clusters caused by the fixed partition hash bits. Campaign totals use
arrays indexed by day and campaign instead of repeated tree-map lookups.

The sorter-only record's three runs were 13.295, 12.462, and 12.055 seconds, all exact
matches. The improvement over
12.789 seconds is small relative to local timing variation and needs confirmation
on the official scorer; it is a verified local record, not a leaderboard result.

The HashBytes candidate ran in 12.429, 12.104, and 13.742 seconds. Its median
improvement is within measurement noise. Exact outputs and all fixtures passed.

The 20M baseline took 60.98 seconds in one exploratory run, partly overlapping
another process; it is not a controlled speedup denominator. The parallel
variants used approximately 3.7–3.9 GiB peak RSS. At this scale the flat-table
dedup experiment regressed and is not selected as the default.

## Correctness gate

All 36 official fixtures pass with baseline, sort, hash, flat, compact-ID,
and ported-DuckDB-sort configurations. Public 1M dev outputs match the official
answers. Larger datasets are differential tests against the C++ baseline, which
shares parsing and validation but independently rescans click attribution.

The C++ checker uses DuckDB as an independent Parquet decoder and compares row
multiplicities with `EXCEPT ALL` in both directions. Unit tests compare the ported
sort to `std::sort` on 100K random records, including duplicate keys, and test both
32-bit-fitting timestamp offsets and wider-window fallback.

## Reproducing the loop

```sh
make test
make dev
sh scripts/fetch-dev-answer.sh
make benchmark
# Each candidate must pass exact output comparison on every timed repeat:
CONFIG=duckdb-compact-8-256 build/check bench data/practice20 out-practice-baseline 3
```

The complete local per-run records and phase profiles are in the ignored
`benchmarks/` directory. Use the same CPU affinity, dataset, build flags, and
repeat count when comparing candidates. Docker is unavailable on the current
host, so the scorer-style container flags have not yet been executed locally.
