# Build image
FROM rust:1 AS build

WORKDIR /app

ARG SERVICE_NAME

COPY Cargo.toml Cargo.lock ./
COPY services services/
COPY shared shared/

RUN cargo build --release --bin ${SERVICE_NAME}

# Development image
FROM rust:1 AS dev

WORKDIR /app

ARG SERVICE_NAME

RUN groupadd --gid 1000 conduit && useradd --uid 1000 --gid 1000 conduit

COPY --from=build /app/target/release/${SERVICE_NAME} /app/service

RUN chown conduit:conduit /app/service

USER conduit:conduit

EXPOSE 8080

CMD ["/app/service"]

# Runtime image
FROM gcr.io/distroless/cc-debian12:nonroot AS prod 

ARG SERVICE_NAME

COPY --from=build /app/target/release/${SERVICE_NAME} /usr/local/bin/service

EXPOSE 8080

CMD ["/usr/local/bin/service"]
