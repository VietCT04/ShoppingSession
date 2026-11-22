ARG BUILD_MODE=pgo

FROM rust:1.85-bookworm AS profile-data
WORKDIR /generator
COPY tools/generator/Cargo.toml tools/generator/Cargo.lock ./
COPY tools/generator/src/ src/
RUN cargo build --release --locked --offline && \
    target/release/generate --seed 66 --events 4000000 --clicks 1000000 --threads 8 --output /training

FROM gcc:14-bookworm AS compiler
WORKDIR /src
COPY native/ native/
COPY third_party/ third_party/
COPY Makefile ./
ENV CXXFLAGS="-O3 -DNDEBUG -std=c++20 -pthread -Ithird_party/duckdb_sort -static-libstdc++ -static-libgcc"

FROM compiler AS plain-build
RUN make all && mv build/shopping /shopping

FROM compiler AS pgo-build
COPY --from=profile-data /training /training
RUN make pgo PGO_DATA=/training && mv build/shopping-pgo /shopping

FROM ${BUILD_MODE}-build AS build

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /shopping /app/shopping
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch HOME=/scratch NCPU=8 MEM_GB=24
USER 1000:1000
ENTRYPOINT ["/app/shopping"]
