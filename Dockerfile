# Build a native binary tuned for the scorer's Intel Xeon CPUs (AVX2).
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo .cargo
COPY src src
RUN cargo build --release --locked

# The runtime image only holds the binary and the C runtime it links against.
FROM debian:bookworm-slim
COPY --from=build /src/target/release/clickstream-sessionizer /app/clickstream-sessionizer
USER 1000:1000
ENV DATA_DIR=/data OUTPUT_DIR=/output
CMD ["/app/clickstream-sessionizer"]
