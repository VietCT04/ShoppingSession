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

The optional IPS libraries were compared in alternating order, three repeats
each, using the same binary and dataset. All nine outputs match:

| Large-group sorter | Median seconds |
| --- | ---: |
| Existing DuckDB Ska/PDQ components (control) | 4.772 |
| IPS4o | 4.808 |
| IPS2Ra, including click-ID tie correction | 4.822 |

The existing backend remains selected. The 4.772-second control is a lower
observed local median for the same algorithm; it does not establish an algorithmic
gain over the earlier 4.992-second run. The verified `submit-v1` tag stays frozen
on main while experiments continue in a separate worktree.

Fused line/field CSV scanning was compared in alternating order against the
same build's original scanner. All six outputs match:

| CSV scanner | Runs, seconds | Median |
| --- | --- | ---: |
| Separate line/field scanning | 5.389, 4.730, 4.877 | 4.877 |
| Fused scanning | 5.092, 4.707, 4.769 | **4.769** |

Event parsing's median fell from 2.103 to 1.969 seconds. The callback processes
mapped fields immediately, without a preceding newline scan. The total gain is
small (about 2.2%) and host-dependent. All 468 fixture/configuration checks,
official dev answers, and randomized fused-versus-scalar CSV tests pass. Fused
scanning is now the default; `--no-fused-csv` retains the old path. This is
original C++ code inspired by the fused model in the csveee paper, not a claim
to reproduce its server benchmark throughput.

An independent ingestion-order-key experiment was written before inspecting
the external competition reference. It preserves earliest-F1 ordering and the
millisecond-exact late cutoff, and passes all 504 fixture/configuration checks
plus the dev answers and randomized boundary checks. Alternating 20M medians
were 4.548 seconds for the control and 4.694 for the experiment, so it remains
disabled and has not replaced the validated production path.

Deduplication partition counts were then decoupled from person/output
partitioning, as a cache-locality experiment on our existing pipeline. All twelve
alternating 20M runs match the baseline:

| Dedup partitions (person/output partitions remain 256) | Median seconds |
| --- | ---: |
| 256, control | 4.600 |
| 1024 | 4.350 |
| 4096 | **4.287** |
| 16384 | 4.617 |

4096 is selected. Its dedup phase took approximately 0.40–0.47 seconds versus
about 1 second with 256 partitions. Peak RSS was roughly 4 GiB. `--dedup-shards 0`
uses the person partition count for historical comparisons. All 540 fixture/
configuration checks and official dev answers pass. This is independent work;
no external competition implementation was incorporated.

Array-backed campaign-window counters and category revenue were tested against
the map-based state. The first mixed matrix was noisy; a focused five-repeat
alternating comparison produced exact matches for every output:

| State | Runs, seconds | Median |
| --- | --- | ---: |
| Map control | 3.808, 6.482, 6.334, 4.233, 4.387 | 4.387 |
| Arrays with sparse membership/reset | 3.428, 3.378, 4.539, 4.167, 4.247 | **4.167** |

Aggregation work fell from about 1.9–2.0 worker-seconds to 1.3–1.4. Both array
states are selected; `--no-array-active` and `--no-array-categories` retain the
original algorithms. All 648 fixture/configuration checks, official dev answers,
and randomized multiplicity/reset/tie tests pass. Large host timing variation
remains visible; these are local comparisons, not leaderboard scores.

The strict timestamp parser caches calendar dates around the manifest window
and processes eight validated decimal digits at once for epoch timestamps.
Five alternating 20M trials gave medians of **4.104 s** for the new parser and
4.728 s for the control. The previous best control median on this host was
4.167 s. Both parsers matched the reference on every trial. The production
entrypoint also matched the official 1M answers and the 20M reference.
Use `--no-fast-time` to reproduce the generic parser path.

Storing grouped events in 16 bytes with a timestamp relative to the manifest
start gave a **4.097 s** median versus **4.347 s** with 24-byte events in five
alternating 20M trials. All outputs matched. The code selects the 64-bit layout
automatically when the window cannot fit in a 32-bit millisecond offset. Click
timestamps remain absolute so clicks before the event window still qualify.
Use `--wide-events` to force the 24-byte path.

Encoding directly from per-worker buckets removes the gather copy before
compaction. Across three alternating 20M runs, the median was **3.854 s** versus
**3.906 s** for the gather path, and peak RSS fell by about 30 MiB. A separate
experiment skipping the raw gather during deduplication took 3.933 s alone
and 3.908 s with direct encoding, so that path remains opt-in.

On eight pinned cores, event parsing and deduplication initially averaged 7.64
and 7.93 active workers, while link setup used one core for about 0.475 s.
Scheduling link setup alongside CSV parsing and resolving canonical names
only for dedup winners improved the five-run alternating median to **3.240 s**
from **3.712 s**. Every trial matched the reference. Enable `SHOP_PROFILE_CPU=1`
for phase CPU time and average active workers, and use `--no-overlap-links`
to reproduce serial link setup.

Two-tier radix partitioning uses 256 ingest buckets per worker and subdivides
them into 4,096 partitions for deduplication. Three alternating pinned runs
gave a **3.082 s** median versus **3.266 s** when parsing directly into 4,096
buckets. Peak RSS fell from about 4,006 MiB to 3,723 MiB. All outputs matched.
Use `--ingest-shards 0` to parse directly into the final dedup buckets.

The read-only linked-user table now checks 16 metadata bytes at a time before
loading candidate keys. Full key equality still decides every match. The layout
is inspired by [Swiss tables](https://abseil.io/about/design/swisstables).
The pinned alternating median was **2.973 s** versus **3.157 s** with scalar
probes, with all outputs matching. Largest-first session scheduling measured
3.253 s in the same experiment and remains opt-in. Use `--no-tag-links` for
scalar linked-user probes.

The runtime-dispatched AVX2 CSV scanner enumerates delimiters and uses a
separate quote mask. Event parse wall time fell in all five paired trials,
with a median of **0.865 s** versus **0.942 s**. The complete benchmark
medians were **2.868 s** and **2.889 s** respectively, with all outputs equal.
Use `--no-avx2-csv` to select the SSE2 scanner on an AVX2-capable CPU.

For dedup buckets containing at least 1,024 records, two small bitmaps prove
which 16-bit hash fingerprints occur exactly once. Those rows can bypass the
dedup table. Shared fingerprints always go through exact ID comparison and
earliest-ingest selection. Five alternating pinned trials gave **2.678 s**
versus **2.837 s** with all rows entering the table. Every run matched, including
the official dev output and a duplicate-heavy test with an invalid winner.
Use `--no-sparse-duplicates` for the ordinary dedup table.

The raw event now fits in 48 bytes by storing window-relative event time
and combining the category number and purchase price into one word. Ingest
time keeps its original width. Long windows and dictionaries with more than
256 categories use the 56-byte layout automatically. Five alternating pinned
20M runs measured **2.597 s** versus **2.669 s**, all exact. Peak RSS was about
3,520 MiB versus 3,745 MiB. Use `--wide-raw` to force the wider layout.

The AMD64 build packs IDs of up to nine nonzero ASCII bytes into 63 bits.
Longer IDs and other byte values retain exact mapped views. Key comparisons,
click tie-breaking, canonical names and output text all preserve the original
byte ordering. This reduces the compact raw event from 48 to 32 bytes and
narrows the linked-person table. It makes no assumptions about which ID
patterns the input contains. Five alternating pinned runs measured **2.216 s**
versus **2.655 s**, with all outputs matching and median peak RSS about
3,013 MiB versus 3,496 MiB. Build with `make build/shopping-packed` for the
previous 16-byte key representation.

With eight-byte keys, a canonical key fits in the space already occupied by
the root index and slot padding. The linker replaces root indices in place
and releases the graph arrays, so a hit no longer loads a separate root name.
The five-run pinned median was **2.084 s** versus **2.236 s**. Every trial
matched the reference. Use `--no-inline-short-links` to retain the root-name
lookup when benchmarking.

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

GCC profile-guided optimization trained on the public seed-1 dev input
improved all ten paired timings on the separate seed-7 20M dataset across
two batches. The trained and ordinary builds took **1.498 vs. 1.570 seconds**
in the first batch and **1.898 vs. 2.004 seconds** in the second. Both builds
produced exact results on every run. The trained executable also passed the
36 official fixtures and the dev answers. Build it with `make pgo PGO_DATA=data/dev`.

Row-wise AVX2 parsing uses BMI2 bit extraction to match opening and closing
quotes in short event and click records. Long rows and CPUs without fast BMI2
use the existing scanners. The new and previous parsers took **1.900 vs. 2.031
seconds** in one five-pair batch and **1.547 vs. 1.588 seconds** in a second.
All 20 timed outputs, the default 20M run, and the official dev results
matched exactly. All 684 fixture/configuration checks and the randomized CSV
checks passed. The row scanner is selected automatically when supported.
`--no-row-csv` selects the previous scanner for comparison.

Building the row scanner with profile-guided optimization yielded the fastest
local median so far, **1.403 seconds** for the full 20M input, compared with
**1.534 seconds** for the standard build in the same five-pair batch. The
profile used the separate 1M dev input. The faster build matched every large
reference output, all 36 official fixtures, and the official dev answers.

Packing the records routed to sessionization reduces events from 24 to 16
bytes and clicks from 32 to 24 bytes on amd64. The 20M median improved from
**1.910 to 1.838 seconds** across five alternating pairs after enabling the row
scanner, with a median peak RSS reduction of 131 MiB. Every timed result and a
separate run using production defaults matched the reference. The fixture
matrix, dev answers, and fallbacks for long windows and large category tables
passed. Use `--wide-routing` to compare the previous layout.

The link graph now uses the fused CSV scanner and preallocates its tables from
a bounded count of input lines. The full 20M medians were **1.779 vs. 1.846
seconds** in the three-pair trial and **1.825 vs. 1.862 seconds** across five
pairs in the repeat. Each output matched exactly. The earlier link path is
available with `--legacy-link-input`.

Deduplication now prefetches canonical-user lookups in groups of 16 surviving
events, then resolves and routes those events. The 20M median fell from
**1.805 to 1.741 seconds** over five alternating pairs, and every output matched
the reference. Use `--unbatched-link-probes` to reproduce the previous path.
The batching experiment follows the general technique described in
[Improving Hash Join Performance through Prefetching](https://pdl.cmu.edu/PDL-FTP/Database/icde04_abs.shtml).

With the batched probes and profile-guided compilation, the five-run exact
20M median was **1.342 seconds**, compared with **1.438 seconds** for the
standard build in that batch. The profile-guided executable also passed all
36 official fixtures, the official dev answers, and a separate 20M run with
production defaults.

A separate event-parser measurement took 0.153 seconds to extract fields,
0.236 with timestamp parsing, 0.307 with ID encoding and hashing, and 0.470
with partitioned record storage. These timings cover event ingest work only.

The writer uses 32-bit Parquet count columns when the values fit, with a
64-bit fallback. The same 20M output shrank from about 99 MB to 81 MB.
Across five alternating pairs the medians were **1.766 seconds** with narrower
counts and **1.801 seconds** with 64-bit counts. All outputs matched the
reference. Use `--wide-count-columns` for the 64-bit layout.

The product and campaign dimensions use contiguous read-only hash tables with
SIMD probe metadata and exact string comparisons. Two five-pair 20M runs
measured medians of **1.739 vs 1.780 seconds** and **1.722 vs 1.772 seconds**
against the node-based maps. Both tables matched the reference for every run.

With linked users and compact routing, deduplication and click parsing now
route by the original user ID. Grouping produces contiguous event and click
ranges, resolves each alias once, and merges only the ranges that belong to
the same canonical person. The five-pair median improved from **1.723 to
1.638 seconds** on 20M, with exact matches in both tables for every run. Use
`--no-redirect-linked-groups` to benchmark per-record alias resolution.

A separate pinned five-pair run on the same production source measured a
**1.334-second** median for the ordinary build and **1.544 seconds** for the
profile-guided build. Both builds matched the full 20M reference on every
run. Wall times within the batch varied from 1.287 to 1.690 seconds.

The grouped ranges drop the redundant person code after partitioning and keep
timestamps relative to the manifest window. On amd64, events now use 8 bytes
and clicks use 16, down from 16 and 24. Across seven alternating pairs under
sustained load, the median improved from **1.659 to 1.599 seconds**. The new
layout was faster in all seven pairs, and every output matched the reference.

The compact-range build set a new local 20M median of **1.204 seconds** with
GCC profile-guided optimization. The ordinary build measured **1.276 seconds**
in the same five-pair batch on eight pinned workers. Outputs matched exactly
in all ten runs. Profile training used the separate official 1M dev input.

Linked-user aggregation uses a specialized array path when campaign and
category arrays are available. It computes the base of a day's campaign
totals once per person-day and indexes sessions and assists directly into
that row. The five-pair median improved from **1.591 to 1.562 seconds** on
the compact layout. Each run matched the full 20M reference. Use
`--generic-aggregation` to compare the other aggregation path.

With dense-array aggregation enabled, the GCC profile-guided build improved
the five-run local median to **1.191 seconds**. The ordinary build measured
**1.298 seconds** in the same alternating run. Every full 20M output matched
the reference, and profile training used the separate official dev input.

Raw event partitions reserve address space based on total input bytes and
touch pages as workers append rows. A full partition can grow into a vector.
The five-pair repeat improved from **1.554 to 1.506 seconds** at 20M, and peak
RSS was about 2,842 MiB in every candidate run. All outputs matched exactly.
Use `--vector-raw` to compare ordinary partition vectors.

With reserved raw partitions, the profile-guided production build measured
an exact **1.108-second** median over five pinned 20M runs. The ordinary build
measured **1.176 seconds** in the same alternating batch. The separate official
dev workload supplied the profile counters.

For compact linked-user inputs, parsing puts valid payloads in their user
partitions and carries an event ID, ingest time and location through dedup.
Dedup marks losing payloads in place, including valid copies whose earliest
copy was invalid. The five-pair median improved from **1.520 to 1.478 seconds**
with about 180 MiB less peak RSS. Both tables matched the reference every run.
Use `--copy-dedup-winners` to compare routing the surviving records afterward.

With in-place marking, the ordinary and profile-guided builds reached exact
five-run 20M medians of **1.109** and **1.106 seconds**, respectively. In a
separate five-pair comparison of profile-guided builds, in-place marking
improved the median from **1.395 to 1.344 seconds**. All outputs matched.

Dedup records for compact linked-user inputs use 16 bytes on amd64. They
store ingest time relative to the window and pack the worker, shard, and row
position. A side array preserves full values when either field exceeds the
compact range. Two five-pair runs measured **1.422 vs 1.478 seconds** and
**1.078 vs 1.100 seconds**, with roughly 120 MiB lower peak RSS. Every 20M
output matched the reference. Use `--wide-duplicate-locations` to compare.

With 16-byte dedup records, the GCC profile-guided build reached a
**0.979-second** median over five pinned 20M runs. The ordinary build took
**1.064 seconds** in the same alternating batch. A second batch compared
profile-guided builds under sustained host load and measured **1.297 seconds**
with 16-byte records and **1.381 seconds** with the previous layout. Both
tables matched the full reference in every run. The new profile-guided binary
also passed all 36 fixtures, the official dev comparison, and a separate 20M
run with default settings.

Event validation compares the exact four-byte `view` and `cart` values and
combines the independent validity and lateness checks. The five-pair repeat
on the 16-byte dedup layout measured **1.067 vs 1.073 seconds**. The new path
was faster in four pairs and all ten full 20M outputs matched the reference.
Use `--conditional-event-validation` to compare the former validation path.

Worker-local event and click vectors reserve space using the actual byte
count of each input. The vectors still grow for skewed partitions. Two
five-pair runs measured **1.387 vs 1.428 seconds** and **1.022 vs 1.042
seconds**, with approximately 170 MiB higher peak RSS. All 20M outputs
matched the reference. Use `--grow-routes` to compare growth on demand.

The timestamp parser caches exact UTC dates around the input window and
validates all eight time-of-day bytes together. Uncached dates and other
timestamp formats use the existing parser. Two five-pair runs measured
**1.361 vs 1.403 seconds** and **1.016 vs 1.021 seconds**. Event parsing used
about 6% less CPU in the first batch, and every full 20M output matched.
Use `--no-cache-utc-days` to compare the previous parser.

With the UTC date cache enabled, the GCC profile-guided build measured a
**0.978-second** median over five pinned 20M runs, with a fastest run of
**0.948 seconds**. The ordinary build measured **1.051 seconds** in the
same alternating batch. Every output matched the reference, and the
profile-guided binary passed all 36 fixtures, the official dev comparison,
and a separate default-settings 20M run. This observed median is within
normal run-to-run variation of the earlier 0.979-second result.

A separate five-pair comparison of the current and previous profile-guided
builds measured **0.953 vs 1.040 seconds** on the same pinned CPUs. All ten
20M outputs matched. The current build includes the smaller validation path,
reserved routing vectors, and the UTC date cache.

Linked-user groups store four-byte event timestamps and a separate stream
of purchases. After sorting both by time, the last event in each session
delimits its purchases. On the UTC cache build, the five-pair 20M median
was **1.372 vs 1.378 seconds** and peak RSS was about 60 MiB lower. An
earlier paired batch measured **1.376 vs 1.397 seconds**. Every comparison
matched the reference. Use `--inline-group-purchases` for the original layout.

Short event IDs use a reversible bit permutation at ingest. The mixed word
provides routing and fingerprint bits while keeping exact equality. Unusual
IDs retain their original mapped bytes. Two five-pair 20M sets measured
**1.343 vs 1.350 seconds** and **1.343 vs 1.359 seconds**, with exact reference
matches in every run. Define `SHOP_PLAIN_EVENT_HASH` to build the control.

The profile-guided build with reversible event IDs measured a **0.948-second**
median over five pinned 20M runs. The ordinary build measured **1.034 seconds**
in the same alternating batch. The fastest complete run took **0.930 seconds**.
Both tables matched the full reference every time. The same profile-guided
binary passed all 36 fixtures, the official dev comparison, and a separate
default-settings 20M run.

One worker pool now handles the successive parsing, deduplication, and
session tasks. It joins each stage at a barrier and passes worker failures
back to the caller. The five-pair 20M medians improved from **1.427 to 1.415
seconds** and from **1.423 to 1.405 seconds**, with exact outputs in both
batches. Use `--transient-workers` to compare per-stage thread creation.

Campaign assists use a contiguous array of 32-bit counters when the input
size proves that they fit. Final reduction still uses 64-bit counters. The
five-pair 20M medians were **1.350 vs 1.352 seconds** with persistent workers
and **1.359 vs 1.380 seconds** in an earlier batch. All outputs matched the
reference. Use `--wide-assist-counters` to compare the wider layout.

Users absent from the identity-link graph can be sessionized immediately
after their events and clicks are grouped. Their rows are written from the
source partition while it is still in cache. Linked aliases are collected
for canonical-person processing. The five-pair 20M median improved from
**1.337 to 1.321 seconds** with compact assist counters and **1.352 to 1.325
seconds** in an earlier batch. All outputs matched the reference. Use
`--defer-isolated-groups` to compare the single aggregation pass.

Groups with at least 256 events can recover their session boundaries from
30-minute bins. Each bin retains its first timestamp, last timestamp, and
event count. Adjacent occupied bins merge if the gap between them is at most
30 minutes. Purchases and clicks retain their time ordering for attribution.
The five-pair 20M medians improved from **1.356 to 1.329 seconds** and
**1.373 to 1.316 seconds**, with exact outputs in both batches. Use
`--gap-bin-min-events 0` to time the full timestamp sort.

The profile-guided build of `eef75ea` measured a **0.904-second** median
over five pinned 20M runs. The previous record binary measured **0.947
seconds** in the same alternating batch. The fastest new run took **0.898
seconds**, and all ten outputs matched the full reference. The new binary
also passed the 36 default fixtures, the official dev comparison, and an
independent default-settings 20M run.

Groups of 16 to 1,023 events keep a bitmap of occupied time bins. Visiting
only those bins removes the full bin scan for medium-sized groups. Two
five-pair 20M batches measured **1.331 vs 1.336 seconds** and **1.300 vs
1.322 seconds** against the dense-bin build. Every output matched the
reference. Use `--sparse-bin-min-events 0` to time the dense-bin policy.

The worker pool reserves route buffers in parallel. On the 20M benchmark,
this lowered peak RSS by about 140 MiB and improved the five-pair medians
from **1.329 to 1.293 seconds** and from **1.311 to 1.294 seconds**. Every
output matched. Use `--serial-route-reserve` to compare the original allocation.

## Refreshed generator with mixed-length IDs

The preceding 20M timings used the earlier public generator. The refreshed
generator and its matching dev answers introduced mixed-length IDs. The
timings below use a fresh seed-7 set with 20M requested events, 5M clicks,
and about 2 GiB of CSV. Each timed output is compared with the single-thread
C++ reference for that dataset. [Official track files](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions)

Deduplication keeps the computed hash alongside event IDs held as mapped
views. It still compares their original bytes to decide equality. Inline
event IDs use the compact 16-byte location record. The first five-pair
run improved from **1.240 to 1.105 seconds**. The production-default
build repeated the gain from **1.236 to 1.139 seconds**. Deduplication
took **0.067 seconds** in the profiled candidate run and **0.209 seconds**
in its control. All outputs matched. Use `--uncached-indirect-events` to
time the original deduplication path on mixed-length inputs.

The profile-guided build of `4be76bd` measured a **1.090-second** median
over five pinned runs on the refreshed 20M set. The standard build of the
same source measured **1.121 seconds** in the alternating batch. The fastest
profile-guided run took **1.056 seconds**. Every output matched the full
reference. The binary also passed all 36 fixtures, the refreshed official
dev comparison, and a separate default-settings 20M run.
