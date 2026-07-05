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
oppo-multiplexer <listen_port> <backend_host:backend_port> <timeout_seconds> [max_consecutive_timeouts] [--protocol udp20x|magnetar]
```

- `listen_port` — port to accept incoming client connections on
- `backend_host:backend_port` — address of the Oppo player
- `timeout_seconds` — how long to wait for a response from the player before giving up
- `max_consecutive_timeouts` — optional; reconnect backend only after this many consecutive timed-out requests (default: `3`, must be in the range `1-100`)
- `--protocol` — optional; the player's control protocol (default: `udp20x`). One instance serves one player, so this is fixed per instance:
  - `udp20x` — Oppo UDP-203/205 IP protocol: `#CODE\r` commands, `\r`-terminated `@…` responses, and `@U??` unsolicited updates broadcast to all clients.
  - `magnetar` — Magnetar network protocol: `#CODE\r\n` commands. The player is fire-and-forget (sends no responses and no unsolicited updates), so the proxy acks each client immediately. It still multiplexes because the Magnetar, like the Oppo players, accepts only one control connection.

    > **Liveness note:** because Magnetar sends no responses, there is nothing to time out on, so the `max_consecutive_timeouts` reconnect does not apply. A backend that dies cleanly (FIN/RST) is detected and reconnected, but a *silently black-holed* player cannot be detected at the application layer — commands are ack'd even if the player never received them. This is inherent to a fire-and-forget protocol.

Example:

```shell
oppo-multiplexer 23 192.168.1.50:23 10 5
```

Magnetar (listens on port 8102):

```shell
oppo-multiplexer 8102 192.168.1.50:8102 10 --protocol magnetar
```

### Docker

A prebuilt multi-arch image (`linux/amd64`, `linux/arm64`) is published to Docker Hub as
[`henrikwidlund/oppo-multiplexer`](https://hub.docker.com/r/henrikwidlund/oppo-multiplexer)
on each `v*.*.*` tag. It is a static `scratch` image that runs as a non-root user.

> **Ports below 1024:** the container runs as a non-root user (UID `65532`), which
> cannot bind privileged ports *inside* the container. Have the app listen on a high
> port (e.g. `1024`) and use Docker's port mapping to expose it on a low host port if needed —
> e.g. `-p 23:1024` with `command` listening on `1024`. The `listen_port` argument is
> the **container** port, and the left side of `-p host:container` is what clients connect to.

Arguments are passed the same way as the binary:

```shell
docker run --rm -p 23:1024 henrikwidlund/oppo-multiplexer \
  1024 192.168.1.50:23 10 5
```

Run detached:

```shell
docker run -d --name oppo -p 23:1024 --restart unless-stopped \
  henrikwidlund/oppo-multiplexer 1024 192.168.1.50:23 10 5
```

Or with Docker Compose:

```yaml
services:
  oppo-multiplexer:
    image: henrikwidlund/oppo-multiplexer
    ports: ["23:1024"]
    command: ["1024", "192.168.1.50:23", "10", "5"]
    restart: unless-stopped
    environment:
      RUST_LOG: info
```

Logs go to stderr (journald is not available in the container).

#### Building the image locally

```shell
docker build -t oppo-multiplexer:local .
docker run --rm -p 23:1024 oppo-multiplexer:local 1024 192.168.1.50:23 10 5
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

