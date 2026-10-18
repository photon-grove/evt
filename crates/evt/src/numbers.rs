//! The canonical text of the numbers in a stored rejection (storage spec, Rejections).
//!
//! PostgreSQL's `jsonb` prints a number in plain decimal digits, whatever literal it was given, so
//! every encoder writes that text itself: then every backend stores, and reads back, the same
//! bytes, and a first execution equals a replay.

use std::borrow::Cow;

use crate::error::Error;
use crate::outcome::MAX_OUTCOME_BYTES;

/// The most digits after the decimal point in the canonical text.
const MAX_SCALE: i64 = 16383;

/// The most significant digits before the decimal point.
const MAX_INTEGER_DIGITS: i64 = 131_072;

/// Saturates a written exponent so the digit arithmetic cannot overflow. It is far past both
/// limits: a non-zero number with a larger exponent is refused by one of them, and a zero is `0`
/// whatever its exponent.
const EXPONENT_CAP: i64 = 1 << 30;

/// Rewrites every number in an encoded JSON document into its canonical text, the one
/// PostgreSQL's `jsonb` prints: plain decimal digits with no exponent, the fraction digits the
/// literal implies (its own, minus the exponent, never fewer than zero), and no sign on zero. So
/// `1e+21` becomes `1000000000000000000000`, `1.50e1` becomes `15.0`, and `-0` becomes `0`.
/// Everything outside the numbers is kept byte for byte.
///
/// The input must be valid JSON, as an encoder writes it; other input never panics, but its output
/// is unspecified. A number whose canonical text PostgreSQL's numeric type cannot hold (more than
/// 16383 digits after the decimal point, or more than 131072 before it) is
/// [`Error::InvalidArgument`]; a document whose canonical form is over
/// [`MAX_OUTCOME_BYTES`] is [`Error::OutcomeTooLarge`], refused before it is built.
pub fn canonical_numbers(json: &str) -> Result<Cow<'_, str>, Error> {
    let mut size: i64 = 0;
    let mut changed = false;

    for token in tokens(json) {
        match token {
            Token::Other(text) => size += text.len() as i64,
            Token::Number(text, number) => {
                number.check(text)?;
                size += number.canonical_length();

                // A literal without an exponent is already canonical, except for a negative zero:
                // a JSON encoder writes no leading zeros and no other sign.
                changed |= text.contains(['e', 'E']) || (number.negative && number.is_zero());
            }
        }
    }

    let size = usize::try_from(size).unwrap_or(usize::MAX);
    if size > MAX_OUTCOME_BYTES {
        return Err(Error::OutcomeTooLarge {
            what: "rejection",
            size,
        });
    }

    if !changed {
        return Ok(Cow::Borrowed(json));
    }

    let mut out = String::with_capacity(size);
    for token in tokens(json) {
        match token {
            Token::Other(text) => out.push_str(text),
            Token::Number(_, number) => number.write_canonical(&mut out),
        }
    }

    Ok(Cow::Owned(out))
}

enum Token<'a> {
    /// A run of bytes outside any number: strings, keys, punctuation.
    Other(&'a str),
    /// A number literal and its parts.
    Number(&'a str, Number<'a>),
}

/// Splits a valid, compact JSON document into number literals and the runs between them.
fn tokens(json: &str) -> impl Iterator<Item = Token<'_>> {
    let bytes = json.as_bytes();
    let mut start = 0;
    let mut index = 0;
    let mut in_string = false;
    let mut pending: Option<Token<'_>> = None;

    std::iter::from_fn(move || {
        if let Some(token) = pending.take() {
            return Some(token);
        }

        while index < bytes.len() {
            let c = bytes[index];

            if in_string && c == b'\\' {
                index += 2;
                continue;
            }

            if c == b'"' {
                in_string = !in_string;
            } else if !in_string && (c == b'-' || c.is_ascii_digit()) {
                let mut end = index + 1;
                while end < bytes.len() && is_number_byte(bytes[end]) {
                    end += 1;
                }

                let text = &json[index..end];
                let number = Token::Number(text, Number::parse(text));
                let before = &json[start..index];
                start = end;
                index = end;

                if before.is_empty() {
                    return Some(number);
                }

                pending = Some(number);

                return Some(Token::Other(before));
            }

            index += 1;
        }

        if start < bytes.len() {
            let rest = &json[start..];
            start = bytes.len();

            return Some(Token::Other(rest));
        }

        None
    })
}

fn is_number_byte(c: u8) -> bool {
    c.is_ascii_digit() || matches!(c, b'-' | b'+' | b'.' | b'e' | b'E')
}

/// One JSON number literal, split into the parts its canonical text is built from.
struct Number<'a> {
    negative: bool,
    /// The integer digits and the fraction digits, without the decimal point.
    integer: &'a [u8],
    fraction: &'a [u8],
    /// The decimal point's position in the digits once the exponent is applied. It may lie
    /// before the first digit (negative) or past the last.
    point: i64,
    /// The number of digits after the decimal point in the canonical text.
    scale: i64,
}

impl<'a> Number<'a> {
    /// Splits a literal from a JSON encoder, so a well-formed one.
    fn parse(text: &'a str) -> Self {
        let (negative, text) = match text.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, text),
        };

        let (mantissa, exponent) = match text.find(['e', 'E']) {
            Some(at) => (&text[..at], parse_exponent(&text[at + 1..])),
            None => (text, 0),
        };

        let (integer, fraction) = match mantissa.find('.') {
            Some(at) => (&mantissa[..at], &mantissa[at + 1..]),
            None => (mantissa, ""),
        };

        Self {
            negative,
            integer: integer.as_bytes(),
            fraction: fraction.as_bytes(),
            point: integer.len() as i64 + exponent,
            scale: (fraction.len() as i64 - exponent).max(0),
        }
    }

    fn digit(&self, index: i64) -> u8 {
        let Ok(index) = usize::try_from(index) else {
            return b'0';
        };

        if index < self.integer.len() {
            self.integer[index]
        } else {
            *self
                .fraction
                .get(index - self.integer.len())
                .unwrap_or(&b'0')
        }
    }

    fn digit_count(&self) -> i64 {
        (self.integer.len() + self.fraction.len()) as i64
    }

    /// The index of the first non-zero digit, or the digit count for zero.
    fn first_significant(&self) -> i64 {
        self.integer
            .iter()
            .chain(self.fraction)
            .position(|&c| c != b'0')
            .map_or(self.digit_count(), |at| at as i64)
    }

    fn is_zero(&self) -> bool {
        self.first_significant() == self.digit_count()
    }

    /// The number of significant digits before the decimal point.
    fn integer_digits(&self) -> i64 {
        if self.is_zero() {
            return 0;
        }

        (self.point - self.first_significant()).max(0)
    }

    /// Refuses a number past PostgreSQL's numeric limits.
    fn check(&self, text: &str) -> Result<(), Error> {
        if self.scale > MAX_SCALE || self.integer_digits() > MAX_INTEGER_DIGITS {
            let shown = if text.len() <= 64 {
                text.to_owned()
            } else {
                format!("{}... ({} bytes)", &text[..64], text.len())
            };

            return Err(Error::InvalidArgument(format!(
                "rejection details hold a number out of the storable range: {shown}"
            )));
        }

        Ok(())
    }

    fn canonical_length(&self) -> i64 {
        let mut length = self.integer_digits().max(1);

        if self.negative && !self.is_zero() {
            length += 1;
        }

        if self.scale > 0 {
            length += 1 + self.scale;
        }

        length
    }

    /// Writes the sign unless zero, the integer digits without leading zeros (at least one), then
    /// the decimal point and exactly `scale` fraction digits.
    fn write_canonical(&self, out: &mut String) {
        if self.negative && !self.is_zero() {
            out.push('-');
        }

        if self.integer_digits() == 0 {
            out.push('0');
        } else {
            for index in self.first_significant()..self.point {
                out.push(char::from(self.digit(index)));
            }
        }

        if self.scale > 0 {
            out.push('.');
            for index in self.point..self.point + self.scale {
                out.push(char::from(self.digit(index)));
            }
        }
    }
}

/// Reads an optionally signed exponent, saturating at [`EXPONENT_CAP`]. A byte that is not a digit
/// counts as zero, so malformed input cannot panic.
fn parse_exponent(text: &str) -> i64 {
    let (negative, digits) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };

    let magnitude = digits.bytes().fold(0i64, |exponent, c| {
        let digit = if c.is_ascii_digit() { c - b'0' } else { 0 };

        (exponent * 10 + i64::from(digit)).min(EXPONENT_CAP)
    });

    if negative { -magnitude } else { magnitude }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn canonical(json: &str) -> String {
        canonical_numbers(json).unwrap().into_owned()
    }

    #[test]
    fn numbers_take_the_text_jsonb_prints() {
        for (input, expected) in [
            ("1e+21", "1000000000000000000000"),
            ("1e21", "1000000000000000000000"),
            ("-0", "0"),
            ("-0.0", "0.0"),
            ("1.5e-7", "0.00000015"),
            ("1.50e1", "15.0"),
            ("0.0010e2", "0.10"),
            ("12.5e-1", "1.25"),
            ("100.00", "100.00"),
            ("1E2", "100"),
            ("0e5", "0"),
            ("-1.5e3", "-1500"),
            ("123", "123"),
        ] {
            assert_eq!(canonical(input), expected, "{input}");
        }
    }

    #[test]
    fn strings_and_keys_are_untouched() {
        let json = r#"{"1e5":"2e3 \"-0\" \\","n":[1e2,-0]}"#;

        assert_eq!(canonical(json), r#"{"1e5":"2e3 \"-0\" \\","n":[100,0]}"#);
        assert!(matches!(
            canonical_numbers(r#"{"a":1}"#).unwrap(),
            Cow::Borrowed(_)
        ));
    }

    #[test]
    fn numbers_past_the_numeric_limits_are_refused() {
        for input in [
            "1e-16384",
            "1e131072",
            "0.0e-16383",
            "1e99999999999",
            "1e-99999999999",
        ] {
            assert!(
                matches!(canonical_numbers(input), Err(Error::InvalidArgument(_))),
                "{input}"
            );
        }

        // A zero is 0 whatever its exponent, and malformed input does not panic.
        assert_eq!(canonical("0e99999999999"), "0");
        assert!(canonical_numbers("1e+-5").is_ok());
        assert!(matches!(
            canonical_numbers("1e65536"),
            Err(Error::OutcomeTooLarge {
                what: "rejection",
                size: 65537
            })
        ));
    }
}
