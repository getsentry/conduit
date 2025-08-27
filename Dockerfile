# Build image
FROM rust:1-bookworm AS build
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src/

COPY . .
RUN cargo build --release --bin conduit

# Runtime image
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \ 
    openssl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1000 conduit && useradd --uid 1000 --gid 1000 conduit

COPY --from=build /app/target/release/conduit /usr/local/bin/conduit

EXPOSE 8080

WORKDIR /app
USER conduit:conduit

CMD ["/usr/local/bin/conduit"]
