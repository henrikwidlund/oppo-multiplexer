# syntax=docker/dockerfile:1

FROM rust:1.98.1-alpine3.24@sha256:7cc1c22d77d9432f7fe012a70e6d3e555af54c2a6832700ed7d553f1769ae89f AS build
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
