use std::sync::{Arc, LazyLock};

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
