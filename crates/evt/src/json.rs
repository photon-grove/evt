//! JSON encoding that matches Go's `encoding/json` byte for byte.
//!
//! The Go `evt` framework writes event payloads, metadata, snapshots, and stream envelopes with
//! `json.Marshal`. For the two implementations to write identical bytes, this module reproduces
//! the parts of Go's encoder that differ from `serde_json`'s defaults:
//!
//! - `<`, `>`, and `&` are escaped as `\u003c`, `\u003e`, and `\u0026` (Go's HTML-safe escaping).
//! - U+2028 and U+2029 are escaped as `\u2028` and `\u2029`.
//! - `[]byte` fields encode as standard, padded base64 (see [`base64_bytes`]).
//!
//! Control characters, quotes, and backslashes already escape the same way in both encoders.
//! Struct fields serialize in declaration order, so declare fields in the same order as the Go
//! struct. Maps must be ordered by key (`BTreeMap`) because Go sorts map keys. Go sorts them by
//! their encoded string, so a map with integer keys needs [`go_key_order`]: Go writes `10` before
//! `2`, while `BTreeMap<i64, _>` iterates `2` first.
//!
//! Floating-point numbers are not normalized: Go and Rust format some floats differently. Prefer
//! integers (for example, cents) in event payloads that must be byte-identical across languages.
//! A stored rejection's details are the exception: their numbers are rewritten into a canonical
//! text by [`canonical_numbers`](crate::canonical_numbers).

use std::io;

use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use serde::{Deserialize, Deserializer};
use serde_json::ser::{Formatter, Serializer};

/// A `serde_json` formatter that escapes strings the way Go's `encoding/json` does.
#[derive(Clone, Copy, Debug, Default)]
pub struct GoFormatter;

impl Formatter for GoFormatter {
    fn write_string_fragment<W>(&mut self, writer: &mut W, fragment: &str) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        let bytes = fragment.as_bytes();
        let mut start = 0;

        for (index, ch) in fragment.char_indices() {
            let escaped: &[u8] = match ch {
                '<' => b"\\u003c",
                '>' => b"\\u003e",
                '&' => b"\\u0026",
                '\u{2028}' => b"\\u2028",
                '\u{2029}' => b"\\u2029",
                _ => continue,
            };

            writer.write_all(&bytes[start..index])?;
            writer.write_all(escaped)?;
            start = index + ch.len_utf8();
        }

        writer.write_all(&bytes[start..])
    }
}

/// Serializes a value to JSON bytes exactly as Go's `json.Marshal` would for the same shape.
pub fn to_vec<T: ?Sized + Serialize>(value: &T) -> serde_json::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(128);
    let mut serializer = Serializer::with_formatter(&mut out, GoFormatter);
    value.serialize(&mut serializer)?;

    Ok(out)
}

/// Serializes a value to a JSON string exactly as Go's `json.Marshal` would for the same shape.
pub fn to_string<T: ?Sized + Serialize>(value: &T) -> serde_json::Result<String> {
    let bytes = to_vec(value)?;

    // The serializer only ever writes valid UTF-8.
    Ok(String::from_utf8(bytes).expect("serde_json writes UTF-8"))
}

/// Deserializes JSON bytes. Provided for symmetry with [`to_vec`].
pub fn from_slice<T: DeserializeOwned>(bytes: &[u8]) -> serde_json::Result<T> {
    serde_json::from_slice(bytes)
}

/// Serializes a map with its keys in Go's order: sorted by their encoded string, so `10` comes
/// before `2`. Use it on maps whose keys are not strings, such as integers.
///
/// ```
/// use std::collections::BTreeMap;
///
/// #[derive(serde::Serialize)]
/// struct Scores {
///     #[serde(serialize_with = "photon_evt::json::go_key_order")]
///     by_round: BTreeMap<i64, u32>,
/// }
///
/// let scores = Scores { by_round: BTreeMap::from([(2, 5), (10, 7)]) };
/// assert_eq!(photon_evt::json::to_vec(&scores).unwrap(), br#"{"by_round":{"10":7,"2":5}}"#);
/// ```
pub fn go_key_order<'a, M, K, V, S>(map: &'a M, serializer: S) -> Result<S::Ok, S::Error>
where
    &'a M: IntoIterator<Item = (&'a K, &'a V)>,
    K: std::fmt::Display + 'a,
    V: Serialize + 'a,
    S: serde::Serializer,
{
    let mut entries: Vec<(String, &V)> = map
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    serializer.collect_map(entries)
}

/// Deserializes a field that Go would leave at its zero value when the JSON holds `null`.
pub fn null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

/// Serde adapter for Go `[]byte` fields: standard base64 with padding, `null` decodes to empty.
pub mod base64_bytes {
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serializer};

    /// Encodes bytes as a padded, standard-alphabet base64 string.
    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&encode(bytes))
    }

    /// Decodes a base64 string (or `null`) into bytes.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        match Option::<String>::deserialize(deserializer)? {
            Some(text) => decode(&text).map_err(D::Error::custom),
            None => Ok(Vec::new()),
        }
    }

    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    /// Encodes bytes as padded, standard-alphabet base64 (Go's `base64.StdEncoding`).
    pub fn encode(bytes: &[u8]) -> String {
        let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);

        for chunk in bytes.chunks(3) {
            let b0 = chunk[0] as u32;
            let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
            let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
            let triple = (b0 << 16) | (b1 << 8) | b2;

            out.push(ALPHABET[(triple >> 18) as usize & 63] as char);
            out.push(ALPHABET[(triple >> 12) as usize & 63] as char);
            out.push(if chunk.len() > 1 {
                ALPHABET[(triple >> 6) as usize & 63] as char
            } else {
                '='
            });
            out.push(if chunk.len() > 2 {
                ALPHABET[triple as usize & 63] as char
            } else {
                '='
            });
        }

        out
    }

    /// Decodes padded, standard-alphabet base64. Like Go, it ignores `\r` and `\n`.
    pub fn decode(text: &str) -> Result<Vec<u8>, String> {
        let symbols: Vec<u8> = text
            .bytes()
            .filter(|b| *b != b'\r' && *b != b'\n')
            .collect();
        if !symbols.len().is_multiple_of(4) {
            return Err(format!("illegal base64 data: length {}", symbols.len()));
        }

        let mut out = Vec::with_capacity(symbols.len() / 4 * 3);

        for (index, quad) in symbols.chunks(4).enumerate() {
            let last = index == symbols.len() / 4 - 1;
            let padding = quad.iter().rev().take_while(|b| **b == b'=').count();
            if padding > 2 || (padding > 0 && !last) {
                return Err(format!("illegal base64 data at input byte {}", index * 4));
            }

            let mut triple = 0u32;
            for (offset, symbol) in quad.iter().enumerate() {
                let value = if offset >= 4 - padding {
                    0
                } else {
                    decode_symbol(*symbol).ok_or_else(|| {
                        format!("illegal base64 data at input byte {}", index * 4 + offset)
                    })?
                };
                triple = (triple << 6) | value;
            }

            out.push((triple >> 16) as u8);
            if padding < 2 {
                out.push((triple >> 8) as u8);
            }
            if padding < 1 {
                out.push(triple as u8);
            }
        }

        Ok(out)
    }

    fn decode_symbol(symbol: u8) -> Option<u32> {
        match symbol {
            b'A'..=b'Z' => Some((symbol - b'A') as u32),
            b'a'..=b'z' => Some((symbol - b'a' + 26) as u32),
            b'0'..=b'9' => Some((symbol - b'0' + 52) as u32),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::base64_bytes::{decode, encode};

    #[test]
    fn base64_matches_rfc_4648() {
        let vectors = [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ];

        for (plain, encoded) in vectors {
            assert_eq!(encode(plain.as_bytes()), encoded, "encode {plain:?}");
            assert_eq!(
                decode(encoded).unwrap(),
                plain.as_bytes(),
                "decode {encoded:?}"
            );
        }
    }

    #[test]
    fn base64_round_trips_every_byte_value() {
        for len in 0..=258 {
            let bytes: Vec<u8> = (0..len).map(|i| (i * 7 + len) as u8).collect();

            assert_eq!(decode(&encode(&bytes)).unwrap(), bytes, "length {len}");
        }
    }

    #[test]
    fn base64_rejects_what_go_rejects() {
        for invalid in ["Zm8", "Zm=8", "Zg==Zg==", "Z===", "Zm9v!A=="] {
            assert!(decode(invalid).is_err(), "{invalid:?}");
        }

        assert_eq!(decode("Zm9v\r\nYmFy").unwrap(), b"foobar");
    }
}
