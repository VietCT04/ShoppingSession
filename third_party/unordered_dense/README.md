# ankerl::unordered_dense

Unmodified headers from https://github.com/martinus/unordered_dense, pinned to
commit `bcc2bb5801fb920a1df06a2ee205b93b2514964e` (header version 5.3.3).
Copyright Martin Leitner-Ankerl. MIT license in LICENSE and in the header.

Used as an optional deduplication backend. The adapter marks the project's
already-mixed hashes as avalanching, and folds high bits into low bits so that
partition-selection bits cannot turn into constant bucket fingerprints.
