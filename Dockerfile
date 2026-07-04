# syntax=docker/dockerfile:1

FROM rust:1-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked && \
    cp target/release/oppo-multiplexer /oppo-multiplexer

FROM scratch
USER 65532:65532
COPY --from=build /oppo-multiplexer /oppo-multiplexer
ENTRYPOINT ["/oppo-multiplexer"]
