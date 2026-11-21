# IPS2Ra

Unmodified headers from https://github.com/ips4o/ips2ra, commit
`83f6b76ca308b277b2acc3a36426a3baa3c9cb73`.
BSD 2-Clause license in LICENSE and each header.

Optional timestamp radix sorting backend for large person groups. Click IDs are
sorted in descending byte order within equal-timestamp runs after radix sorting,
preserving the attribution tie rule. Independent groups run on the pipeline's
eight workers.
