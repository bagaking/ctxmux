//! Native child terminal identity applied at spawn.

use std::collections::BTreeMap;

const NATIVE_TERM: &str = "xterm-256color";
const NATIVE_COLORTERM: &str = "truecolor";

/// Returns the environment overlay for one native child.
///
/// `TERM` and `COLORTERM` default to a stable xterm identity so a Run does
/// not inherit the daemon host terminal. Explicit `RunSpec.env` entries win.
pub(crate) fn with_native_terminal_identity(
    spec_env: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut env = spec_env.clone();
    env.entry("TERM".to_owned())
        .or_insert_with(|| NATIVE_TERM.to_owned());
    env.entry("COLORTERM".to_owned())
        .or_insert_with(|| NATIVE_COLORTERM.to_owned());
    env
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_identity_fills_term_and_colorterm() {
        let env = with_native_terminal_identity(&BTreeMap::new());
        assert_eq!(env.get("TERM").map(String::as_str), Some(NATIVE_TERM));
        assert_eq!(
            env.get("COLORTERM").map(String::as_str),
            Some(NATIVE_COLORTERM)
        );
    }

    #[test]
    fn spec_env_wins_over_default_identity() {
        let spec = BTreeMap::from([
            ("TERM".to_owned(), "vt100".to_owned()),
            ("COLORTERM".to_owned(), "24bit".to_owned()),
            ("FOO".to_owned(), "bar".to_owned()),
        ]);
        let env = with_native_terminal_identity(&spec);
        assert_eq!(env.get("TERM").map(String::as_str), Some("vt100"));
        assert_eq!(env.get("COLORTERM").map(String::as_str), Some("24bit"));
        assert_eq!(env.get("FOO").map(String::as_str), Some("bar"));
    }
}
