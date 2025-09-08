# Build image
FROM rust:1-bookworm AS build
WORKDIR /app

ARG SERVICE_NAME

COPY Cargo.toml Cargo.lock ./
COPY services services/

RUN cargo build --release --bin ${SERVICE_NAME}

# Runtime image
FROM debian:bookworm-slim

ARG SERVICE_NAME

RUN apt-get update && apt-get install -y --no-install-recommends \ 
    openssl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1000 conduit && useradd --uid 1000 --gid 1000 conduit

COPY --from=build /app/target/release/${SERVICE_NAME} /usr/local/bin/service

EXPOSE 8080

WORKDIR /app
USER conduit:conduit

CMD ["/usr/local/bin/service"]
