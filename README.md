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
oppo-multiplexer <listen_port> <backend_host:backend_port> <timeout_seconds> [max_consecutive_timeouts]
```

- `listen_port` — port to accept incoming client connections on
- `backend_host:backend_port` — address of the Oppo player
- `timeout_seconds` — how long to wait for a response from the player before giving up
- `max_consecutive_timeouts` — optional; reconnect backend only after this many consecutive timed-out requests (default: `3`, must be in the range `1-100`)

Example:

```shell
oppo-multiplexer 23 192.168.1.50:23 10 5
```

### Installing on Linux

Download `install_linux.sh` and follow the instructions. If you want to run on ports below 1024,
you will need to run as root.
You can use the `CAP_NET_BIND_SERVICE` capability to avoid root on ports below 1024,
but the script does not take this into consideration.

### Logging

Log level is controlled via the `RUST_LOG` environment variable. Defaults to `info`. On Linux, logs are sent to journald.

```
RUST_LOG=debug oppo-multiplexer 23 192.168.1.50:23 10
```

