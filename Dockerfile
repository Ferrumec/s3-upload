FROM rust:1-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.toml
COPY src src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
RUN useradd --system --uid 10001 app
WORKDIR /app
COPY --from=builder /app/target/release/rust-s3-upload /usr/local/bin/rust-s3-upload
USER app
EXPOSE 3000
CMD ["rust-s3-upload"]
