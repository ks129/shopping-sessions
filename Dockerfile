# Shopping Sessions: Rust pipeline. The scorer builds this natively on linux/amd64.
FROM rust:1.100-bookworm AS build
ARG TARGETARCH
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Scorers are Intel Xeon Cascade Lake or newer, so AVX2/BMI2 (x86-64-v3) is safe there.
RUN if [ "$TARGETARCH" = "amd64" ] || [ "$(uname -m)" = "x86_64" ]; then export RUSTFLAGS="-C target-cpu=x86-64-v3"; fi \
    && cargo build --release --locked && strip target/release/pipeline

FROM debian:bookworm-slim
COPY --from=build /src/target/release/pipeline /app/pipeline
# Runs as uid 1000 with a read-only root and no network; reads $DATA_DIR
# (/data), writes $OUTPUT_DIR (/output), uses $NCPU threads. Logs go to stderr.
WORKDIR /app
CMD ["/app/pipeline"]
