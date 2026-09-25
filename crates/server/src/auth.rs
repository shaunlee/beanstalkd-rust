//! Token authentication (`auth = "token"` listeners; docs/PLAN.md §5.3
//! decision 3): the configured tokens and their constant-time comparison.
//!
//! Every token (at most `MAX_TOKEN_LEN` bytes, enforced by the
//! configuration) is stored zero-padded to `MAX_TOKEN_LEN` bytes along
//! with its length. A candidate is padded the same way and compared with
//! every configured token in full, with `subtle`'s constant-time equality,
//! and the results are combined without short-circuiting. The time taken
//! therefore depends only on the number of configured tokens, never on
//! their contents, their lengths, or on how much of a candidate matches.
//!
//! Tokens are secrets: `Debug` shows only how many there are, and nothing
//! in this module logs.

use std::fmt;

use subtle::{Choice, ConstantTimeEq};

use crate::config::{MAX_TOKEN_LEN, Tokens};

/// A token padded to a fixed width, plus its real length.
struct Padded {
    bytes: [u8; MAX_TOKEN_LEN],
    len: u64,
}

impl Padded {
    /// Pads `token` with zeros. A longer token (which cannot match any
    /// configured one) is truncated and marked with an impossible length.
    fn new(token: &[u8]) -> Padded {
        let mut bytes = [0u8; MAX_TOKEN_LEN];
        let n = token.len().min(MAX_TOKEN_LEN);
        bytes[..n].copy_from_slice(&token[..n]);
        let len = if token.len() > MAX_TOKEN_LEN {
            u64::MAX
        } else {
            n as u64
        };
        Padded { bytes, len }
    }

    fn ct_eq(&self, other: &Padded) -> Choice {
        self.bytes.ct_eq(&other.bytes) & self.len.ct_eq(&other.len)
    }
}

/// The tokens accepted by `auth = "token"` listeners.
pub struct TokenSet {
    tokens: Vec<Padded>,
}

impl fmt::Debug for TokenSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TokenSet({} token(s))", self.tokens.len())
    }
}

impl TokenSet {
    pub fn new(tokens: &Tokens) -> TokenSet {
        TokenSet {
            tokens: tokens.iter().map(Padded::new).collect(),
        }
    }

    /// Whether `candidate` equals one of the tokens, in constant time (see
    /// the module docs). An empty set accepts nothing.
    pub fn verify(&self, candidate: &[u8]) -> bool {
        let candidate = Padded::new(candidate);
        let mut ok = Choice::from(0);
        for token in &self.tokens {
            ok |= token.ct_eq(&candidate);
        }
        bool::from(ok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(tokens: &[&str]) -> TokenSet {
        let toml = format!(
            "[auth]\ntokens = [{}]\n",
            tokens
                .iter()
                .map(|t| format!("{t:?}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        #[derive(serde::Deserialize)]
        struct Auth {
            tokens: Tokens,
        }
        #[derive(serde::Deserialize)]
        struct File {
            auth: Auth,
        }
        let file: File = toml::from_str(&toml).expect("valid toml");
        TokenSet::new(&file.auth.tokens)
    }

    #[test]
    fn accepts_exactly_the_configured_tokens() {
        let s = set(&["alpha", "beta-token"]);
        assert!(s.verify(b"alpha"));
        assert!(s.verify(b"beta-token"));
        assert!(!s.verify(b"alph"));
        assert!(!s.verify(b"alphaa"));
        assert!(!s.verify(b"alpha\0"));
        assert!(!s.verify(b""));
        assert!(!s.verify(b"beta"));
    }

    #[test]
    fn padding_does_not_make_a_prefix_match() {
        let s = set(&["abc"]);
        // Same padded bytes, different length.
        assert!(!s.verify(b"abc\0\0"));
    }

    #[test]
    fn overlong_candidates_never_match() {
        let long = "x".repeat(MAX_TOKEN_LEN);
        let s = set(&[&long]);
        assert!(s.verify(long.as_bytes()));
        let longer = "x".repeat(MAX_TOKEN_LEN + 1);
        assert!(!s.verify(longer.as_bytes()));
    }

    #[test]
    fn empty_set_accepts_nothing() {
        let s = set(&[]);
        assert!(!s.verify(b""));
        assert!(!s.verify(b"x"));
    }

    #[test]
    fn debug_hides_tokens() {
        let s = set(&["secret-value"]);
        let shown = format!("{s:?}");
        assert!(!shown.contains("secret"), "{shown}");
    }
}
