package evt

import (
	"bytes"
	"fmt"
	"strconv"
)

// Limits PostgreSQL's numeric type puts on a JSON number (storage spec, Rejections). A number past
// any of them cannot be stored in a jsonb column, so it is refused on every backend alike.
const (
	// maxNumberExponent bounds the magnitude of a number's written exponent.
	maxNumberExponent = 1<<30 - 1

	// maxNumberScale bounds the digits after the decimal point in the canonical text.
	maxNumberScale = 16383

	// maxNumberIntegerDigits bounds the significant digits before the decimal point.
	maxNumberIntegerDigits = 131072
)

// jsonNumber is one JSON number literal, split into the parts its canonical text is built from.
type jsonNumber struct {
	negative bool

	// exponent is the written exponent, zero when there is none, saturated past
	// maxNumberExponent.
	exponent int64

	// digits are the integer and fraction digits, in order, without the decimal point.
	digits []byte

	// point is the decimal point's position in digits once the exponent is applied. It may lie
	// before the first digit (negative) or past the last.
	point int64

	// scale is the number of digits after the decimal point in the canonical text.
	scale int64
}

// canonicalNumbers rewrites every number in an encoded JSON document into its canonical text, the
// one PostgreSQL's jsonb prints: plain decimal digits with no exponent, the fraction digits the
// literal implies (its own, minus the exponent, never fewer than zero), and no sign on zero. So
// 1e+21 becomes 1000000000000000000000, 1.50e1 becomes 15.0, and -0 becomes 0. Everything outside
// the numbers is kept byte for byte. The rewritten document is bounded by MaxOutcomeBytes before it
// is built, since an exponent can expand a few bytes into many.
func canonicalNumbers(encoded []byte) ([]byte, error) {
	size, changed, err := canonicalNumbersSize(encoded)
	if err != nil {
		return nil, err
	}

	if size > MaxOutcomeBytes {
		return nil, &OutcomeTooLargeError{What: "rejection", Size: size}
	}

	if !changed {
		return encoded, nil
	}

	out := make([]byte, 0, size)

	scanJSONNumbers(encoded, func(text []byte, number *jsonNumber) {
		if number == nil {
			out = append(out, text...)

			return
		}

		out = number.appendCanonical(out)
	})

	return out, nil
}

// canonicalNumbersSize returns the size of the rewritten document and whether any number changes,
// without building it. A number PostgreSQL cannot store is an invalid-argument error.
func canonicalNumbersSize(encoded []byte) (int, bool, error) {
	var (
		size    int64
		changed bool
		err     error
	)

	scanJSONNumbers(encoded, func(text []byte, number *jsonNumber) {
		if err != nil {
			return
		}

		if number == nil {
			size += int64(len(text))

			return
		}

		if err = number.check(text); err != nil {
			return
		}

		size += number.canonicalLength()

		// A literal without an exponent is already canonical, except for a negative zero: a JSON
		// encoder writes no leading zeros and no other sign.
		changed = changed || bytes.ContainsAny(text, "eE") || (number.negative && number.zero())
	})

	if err != nil {
		return 0, false, err
	}

	return int(min(size, maxInt)), changed, nil
}

// scanJSONNumbers walks a valid, compact JSON document, calling visit with each number literal
// parsed and with each run of other bytes (strings, punctuation) as is, with a nil number.
func scanJSONNumbers(encoded []byte, visit func(text []byte, number *jsonNumber)) {
	start, inString := 0, false

	for index := 0; index < len(encoded); index++ {
		c := encoded[index]

		switch {
		case inString && c == '\\':
			index++

		case c == '"':
			inString = !inString

		case !inString && (c == '-' || isDigit(c)):
			if start < index {
				visit(encoded[start:index], nil)
			}

			end := index + 1
			for end < len(encoded) && isNumberByte(encoded[end]) {
				end++
			}

			number := parseJSONNumber(encoded[index:end])
			visit(encoded[index:end], &number)
			start, index = end, end-1
		}
	}

	if start < len(encoded) {
		visit(encoded[start:], nil)
	}
}

const maxInt = int64(^uint(0) >> 1)

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isNumberByte(c byte) bool {
	return isDigit(c) || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E'
}

// parseJSONNumber splits a JSON number literal. The literal comes from a JSON encoder, so it is
// well formed; an exponent too long to hold is saturated, and refused by check.
func parseJSONNumber(text []byte) jsonNumber {
	var number jsonNumber

	if len(text) > 0 && text[0] == '-' {
		number.negative = true
		text = text[1:]
	}

	mantissa, exponentText := text, []byte(nil)
	if at := bytes.IndexAny(text, "eE"); at >= 0 {
		mantissa, exponentText = text[:at], text[at+1:]
	}

	integer, fraction := mantissa, []byte(nil)
	if at := bytes.IndexByte(mantissa, '.'); at >= 0 {
		integer, fraction = mantissa[:at], mantissa[at+1:]
	}

	exponent := parseExponent(exponentText)

	number.exponent = exponent
	number.digits = append(append([]byte{}, integer...), fraction...)
	number.point = int64(len(integer)) + exponent
	number.scale = max(0, int64(len(fraction))-exponent)

	return number
}

// parseExponent reads an optionally signed exponent, saturating past maxNumberExponent.
func parseExponent(text []byte) int64 {
	negative := false

	if len(text) > 0 && (text[0] == '+' || text[0] == '-') {
		negative = text[0] == '-'
		text = text[1:]
	}

	var exponent int64

	for _, c := range text {
		exponent = min(exponent*10+int64(c-'0'), maxNumberExponent+1)
	}

	if negative {
		return -exponent
	}

	return exponent
}

// firstSignificant returns the index of the first non-zero digit, or len(digits) for zero.
func (n *jsonNumber) firstSignificant() int64 {
	for index, c := range n.digits {
		if c != '0' {
			return int64(index)
		}
	}

	return int64(len(n.digits))
}

func (n *jsonNumber) zero() bool { return n.firstSignificant() == int64(len(n.digits)) }

// integerDigits is the number of significant digits before the decimal point.
func (n *jsonNumber) integerDigits() int64 {
	if n.zero() {
		return 0
	}

	return max(0, n.point-n.firstSignificant())
}

// check refuses a number past PostgreSQL's numeric limits.
func (n *jsonNumber) check(text []byte) error {
	if n.exponent > maxNumberExponent || n.exponent < -maxNumberExponent ||
		n.scale > maxNumberScale || n.integerDigits() > maxNumberIntegerDigits {
		return &InvalidArgumentError{
			Argument: "rejection",
			Message:  fmt.Sprintf("rejection details hold a number out of the storable range: %s", truncateNumber(text)),
		}
	}

	return nil
}

// truncateNumber shortens a long literal for an error message.
func truncateNumber(text []byte) string {
	const limit = 64

	if len(text) <= limit {
		return string(text)
	}

	return string(text[:limit]) + "... (" + strconv.Itoa(len(text)) + " bytes)"
}

// canonicalLength is the length of the canonical text.
func (n *jsonNumber) canonicalLength() int64 {
	length := max(1, n.integerDigits())

	if n.negative && !n.zero() {
		length++
	}

	if n.scale > 0 {
		length += 1 + n.scale
	}

	return length
}

// appendCanonical appends the canonical text: the sign unless zero, the integer digits without
// leading zeros (at least one), then the decimal point and exactly scale fraction digits.
func (n *jsonNumber) appendCanonical(out []byte) []byte {
	if n.negative && !n.zero() {
		out = append(out, '-')
	}

	if integerDigits := n.integerDigits(); integerDigits == 0 {
		out = append(out, '0')
	} else {
		first := n.firstSignificant()
		for index := first; index < n.point; index++ {
			out = append(out, n.digit(index))
		}
	}

	if n.scale > 0 {
		out = append(out, '.')
		for index := n.point; index < n.point+n.scale; index++ {
			out = append(out, n.digit(index))
		}
	}

	return out
}

// digit returns the digit at a position in the shifted number: zero outside the written digits.
func (n *jsonNumber) digit(index int64) byte {
	if index < 0 || index >= int64(len(n.digits)) {
		return '0'
	}

	return n.digits[index]
}
