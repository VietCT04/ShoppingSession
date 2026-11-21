FROM gcc:14-bookworm AS build
WORKDIR /src
COPY native/ native/
COPY third_party/ third_party/
RUN g++ -O3 -DNDEBUG -std=c++20 -pthread -Ithird_party/duckdb_sort -static-libstdc++ -static-libgcc native/main.cpp -o /shopping

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /shopping /app/shopping
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch HOME=/scratch NCPU=8 MEM_GB=24
USER 1000:1000
ENTRYPOINT ["/app/shopping"]
