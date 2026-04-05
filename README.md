# oppo-multiplexer

Multiplexes client connections onto a single TCP connection to the Oppo Blu-ray Player, broadcasting unsolicited updates to all clients.

## Build

Requires [Rust](https://rustup.rs).

```
cargo build --release
```

The binary will be at `target/release/oppo-multiplexer`.

## Run

```
oppo-multiplexer <listen_port> <backend_host:backend_port> <timeout_seconds>
```

- `listen_port` — port to accept incoming client connections on
- `backend_host:backend_port` — address of the Oppo player
- `timeout_seconds` — how long to wait for a response from the player before giving up

Example:

```
oppo-multiplexer 23 192.168.1.50:23 10
```

### Logging

Log level is controlled via the `RUST_LOG` environment variable. Defaults to `info`. On Linux, logs are sent to journald.

```
RUST_LOG=debug oppo-multiplexer 23 192.168.1.50:23 10
```

