# Submission candidate v1

Submit the public repository `https://github.com/VietCT04/ShoppingSession` and
the exact commit marked by the immutable `submit-v1` Git tag. This freezes the
verified per-person histogram implementation while later experiments continue
on separate branches.

The repository has a root Dockerfile. Its default entrypoint reads DATA_DIR,
OUTPUT_DIR, and NCPU. No extra command or runtime dependency is needed.
Generated data and local binaries are excluded from Git and the image.

Validation before tagging:

* All 36 official fixtures, with 11 native configurations.
* Native unit tests, randomized CSV and ID comparisons.
* Exact match to the official 1M-event seed-1 answers.
* Three exact 20M-event seed-7 comparisons with the original baseline:
  4.992, 5.272, and 4.859 seconds on the local host.
* Linux amd64 Docker image built in CI; all 36 fixtures and public dev answers
  checked with a read-only root/input, uid 1000, no network, 64 MiB tmpfs,
  bounded stdout/processes, and eight worker threads.

The CI host has four CPUs, so the container check uses a four-CPU quota and a
6 GiB memory limit with eight worker threads. The scoring environment supplies
eight vCPUs and 24 GiB. Local measurements are not official leaderboard scores.

Later changes to main do not change the tagged commit. Use its full SHA in the
submission form, not just the branch name.
