# Build image
FROM rust:1 AS build

WORKDIR /app

ARG SERVICE_NAME

COPY Cargo.toml Cargo.lock ./
COPY services services/
COPY shared shared/

RUN cargo build --release --bin ${SERVICE_NAME}

# Runtime image
FROM gcr.io/distroless/cc-debian12:nonroot as runtime

ARG SERVICE_NAME

COPY --from=build /app/target/release/${SERVICE_NAME} /usr/local/bin/service

EXPOSE 8080

CMD ["/usr/local/bin/service"]
