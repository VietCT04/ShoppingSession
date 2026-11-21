# DuckDB sort components

Unmodified headers from **DuckDB v1.4.3**:

* `third_party/ska_sort/ska_sort.hpp`: Ska Sort, Malte Skarupke, Boost 1.0.
* `third_party/pdqsort/pdqsort.h`: PDQSort, Orson Peters, zlib license in header.
* `third_party/vergesort/vergesort.h` and `detail/`: Vergesort, Morwenn, MIT license in headers.

Upstream: https://github.com/duckdb/duckdb/tree/v1.4.3/third_party

DuckDB's source license and the complete Boost license are included here.
Individual headers retain their copyright and license notices. Minimal standalone
adapters for DuckDB's cast/fallthrough helpers, key extraction, and interrupt
interface are in `native/sort.hpp`. That adapter is project code.

The pipeline reuses these components directly, without embedding the database
engine. DuckDB describes their composition here:
https://duckdb.org/2025/09/24/sorting-again

`native/hash.hpp` additionally adapts `HashBytes` and `MurmurHash64` from the same
DuckDB version's `src/common/types/hash.cpp` and
`src/include/duckdb/common/types/hash.hpp`, using standard C++ types and safe
unaligned loads. The adapted file is marked as such. This uses the DuckDB MIT
license; upstream credits the hash seed to Martin Ankerl (MIT, 2018-2021).
