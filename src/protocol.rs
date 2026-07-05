use std::sync::{Arc, LazyLock};

/// Which player-family control protocol a proxy instance speaks. One instance
/// serves one player, so this is fixed for the process lifetime (chosen via
/// `--protocol`). It governs the client-command line terminator and whether the
/// player answers commands at all.
///
/// - `Udp20x` (default): the UDP-203/205 IP protocol — `#CODE\r` commands,
///   `\r`-terminated `@...` responses, and `@U??` unsolicited updates.
/// - `Magnetar`: the Magnetar network protocol — `#CODE\r\n` commands, and the
///   player sends **no** response and **no** updates (fire-and-forget). The
///   proxy still multiplexes it because Magnetar allows only one control
///   connection, same as the Oppo players.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    Udp20x,
    Magnetar,
}

impl Protocol {
    /// Byte that terminates one command line arriving from a client. Oppo
    /// UDP-20X frames end in `\r`; Magnetar frames end in `\r\n`, so we delimit
    /// on the trailing `\n` and forward the whole frame verbatim.
    pub fn client_delim(self) -> u8 {
        match self {
            Protocol::Udp20x => b'\r',
            Protocol::Magnetar => b'\n',
        }
    }

    /// True when the player never answers a command on the wire, so the proxy
    /// must ack the client immediately instead of waiting for (and timing out
    /// on) a response that will never come.
    pub fn is_fire_and_forget(self) -> bool {
        matches!(self, Protocol::Magnetar)
    }

    /// Bytes that terminate a line the proxy writes back to a client (currently
    /// only the `ERROR: …` notice). Matches the family's framing so the client's
    /// own line delimiter (see `client_delim`) sees a complete line: `\r` for
    /// UDP-20X, `\r\n` for Magnetar.
    pub fn response_terminator(self) -> &'static [u8] {
        match self {
            Protocol::Udp20x => b"\r",
            Protocol::Magnetar => b"\r\n",
        }
    }

    /// Parses the `--protocol` value. Returns `None` for unknown modes so the
    /// caller can print a usage error.
    pub fn parse(raw: &str) -> Option<Protocol> {
        match raw {
            "udp20x" => Some(Protocol::Udp20x),
            "magnetar" => Some(Protocol::Magnetar),
            _ => None,
        }
    }
}

const UPDATE_PREFIXES: [&[u8]; 11] = [
    b"@UPW ",
    b"@UPL ",
    b"@UVL ",
    b"@UDT ",
    b"@UAT ",
    b"@UST ",
    b"@UIS ",
    b"@U3D ",
    b"@UAR ",
    b"@UTC ",
    b"@UVO ",
];

/// Pre-built shared payloads for synthesized `@UPW` broadcasts. Avoids a heap
/// allocation per power-state transition that would otherwise happen inside
/// `Arc::from(&'static [u8])`.
pub static SYNTHETIC_UPW_OFF: LazyLock<Arc<[u8]>> =
    LazyLock::new(|| Arc::from(b"@UPW 0\r".as_slice()));
pub static SYNTHETIC_UPW_ON: LazyLock<Arc<[u8]>> =
    LazyLock::new(|| Arc::from(b"@UPW 1\r".as_slice()));

/// True if `line` is one of the player's unsolicited status updates (any of
/// the `@U??` prefixes), as opposed to a response to an issued command.
pub fn is_backend_update(line: &[u8]) -> bool {
    UPDATE_PREFIXES.iter().any(|prefix| line.starts_with(prefix))
}

pub fn parse_upw_state(line: &[u8]) -> Option<u8> {
    let body = line.strip_suffix(b"\r").unwrap_or(line);
    match body {
        b"@UPW 0" => Some(0),
        b"@UPW 1" => Some(1),
        _ => None,
    }
}

pub fn synthetic_power_state_from_exchange(request: &[u8], response: &[u8]) -> Option<u8> {
    let req = request.strip_suffix(b"\r").unwrap_or(request);
    let resp = response.strip_suffix(b"\r").unwrap_or(response);

    match (req, resp) {
        (b"#POF", b"@POF OK OFF") | (b"#QPW", b"@QPW OK OFF") => Some(0),
        (b"#PON", b"@PON OK ON") | (b"#QPW", b"@QPW OK ON") => Some(1),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_parses_known_modes_and_rejects_others() {
        assert_eq!(Protocol::parse("udp20x"), Some(Protocol::Udp20x));
        assert_eq!(Protocol::parse("magnetar"), Some(Protocol::Magnetar));
        assert_eq!(Protocol::parse("UDP20X"), None);
        assert_eq!(Protocol::parse("pre20x"), None);
        assert_eq!(Protocol::parse(""), None);
    }

    #[test]
    fn protocol_delim_and_fire_and_forget_match_family() {
        assert_eq!(Protocol::Udp20x.client_delim(), b'\r');
        assert_eq!(Protocol::Magnetar.client_delim(), b'\n');
        assert!(!Protocol::Udp20x.is_fire_and_forget());
        assert!(Protocol::Magnetar.is_fire_and_forget());
    }

    #[test]
    fn is_backend_update_matches_all_prefixes() {
        for prefix in UPDATE_PREFIXES {
            let mut line = prefix.to_vec();
            line.extend_from_slice(b"data\r");
            assert!(
                is_backend_update(&line),
                "{:?} should be an update",
                String::from_utf8_lossy(prefix),
            );
        }
    }

    #[test]
    fn is_backend_update_rejects_non_updates() {
        let cases: &[&[u8]] = &[
            b"@OK\r",
            b"@ERR INVALID\r",
            b"\r",
            b"",
            b"@U",
            b"@UPW",
            b"prefix @UTC mid\r",
        ];
        for line in cases {
            assert!(
                !is_backend_update(line),
                "{:?} should NOT be an update",
                String::from_utf8_lossy(line),
            );
        }
    }

    #[test]
    fn synthetic_power_update_maps_expected_ack_responses() {
        assert_eq!(
            synthetic_power_state_from_exchange(b"#POF\r", b"@POF OK OFF\r"),
            Some(0)
        );
        assert_eq!(
            synthetic_power_state_from_exchange(b"#QPW\r", b"@QPW OK OFF\r"),
            Some(0)
        );
        assert_eq!(
            synthetic_power_state_from_exchange(b"#PON\r", b"@PON OK ON\r"),
            Some(1)
        );
        assert_eq!(
            synthetic_power_state_from_exchange(b"#QPW\r", b"@QPW OK ON\r"),
            Some(1)
        );
    }

    #[test]
    fn synthetic_power_update_ignores_other_responses() {
        let cases: &[(&[u8], &[u8])] = &[
            (b"#QPW\r", b"@QPW OK STANDBY\r"),
            (b"#POF\r", b"@POF ERR BUSY\r"),
            (b"#QPW\r", b"@UPW 0\r"),
            (b"#PON\r", b"@PON OK OFF\r"),
            (b"#QPW\r", b"@QPW OK OFFLINE\r"),
            (b"#QVL\r", b"@QPW OK OFF\r"),
            (b"#QVL\r", b"@QPW OK ON\r"),
            (b"#QVL\r", b"@POF OK OFF\r"),
            (b"#QVL\r", b"@PON OK ON\r"),
            (b"", b""),
        ];

        for &(req, line) in cases {
            assert_eq!(
                synthetic_power_state_from_exchange(req, line),
                None,
                "req={:?}, line={:?} should not map to a synthetic @UPW update",
                String::from_utf8_lossy(req),
                String::from_utf8_lossy(line)
            );
        }
    }

    #[test]
    fn parse_upw_state_recognizes_power_updates() {
        assert_eq!(parse_upw_state(b"@UPW 0\r"), Some(0));
        assert_eq!(parse_upw_state(b"@UPW 1\r"), Some(1));
        assert_eq!(parse_upw_state(b"@UPW 0"), Some(0));
        assert_eq!(parse_upw_state(b"@UPW 1"), Some(1));
        assert_eq!(parse_upw_state(b"@UPW OFF\r"), None);
        assert_eq!(parse_upw_state(b"@QPW OK ON\r"), None);
    }
}
