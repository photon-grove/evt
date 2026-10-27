// Package result provides a container for values or errors with
// ergonomic helpers inspired by languages like Rust and TypeScript.
package result

// Result holds either a value of type T or an error.
// If err is nil, the Result is successful; otherwise it is a failure.
type Result[T any] struct {
	err   error
	value T
}

// Ok builds an Ok Result with the provided value.
func Ok[T any](value T) Result[T] {
	return Result[T]{value: value}
}

// Err constructs a failed Result with the provided error.
func Err[T any](err error) Result[T] {
	return Result[T]{err: err}
}

// IsOk reports whether the Result is successful.
func (r Result[T]) IsOk() bool { return r.err == nil }

// IsErr reports whether the Result is a failure.
func (r Result[T]) IsErr() bool { return r.err != nil }

// Unwrap returns the value and error.
func (r Result[T]) Unwrap() (T, error) {
	return r.value, r.err
}

// From constructs an Result from a (value, err) pair.
func From[T any](value T, err error) Result[T] {
	if err == nil {
		return Ok(value)
	}

	return Err[T](err)
}

// Value returns the new value and whether it represents a failure.
func (r Result[T]) Value() (T, error) { return r.value, r.err }

// WithDefault returns the value if Ok, otherwise fallback.
func (r Result[T]) WithDefault(defaultValue T) T {
	if r.err != nil {
		return defaultValue
	}

	return r.value
}

// Map transforms the new value using mapper if changed; otherwise returns Keep.
func (r Result[T]) Map(mapper func(value T) T) Result[T] {
	if r.err == nil {
		return Ok(mapper(r.value))
	}
	return Err[T](r.err)
}

// Zip maps the new value using a mapper that returns the pair (value, changed).
func (r Result[T]) Zip(mapper func(value T) (T, error)) Result[T] {
	if r.err == nil {
		return From(mapper(r.value))
	}
	return Err[T](r.err)
}

// Map maps Ok[A] to Ok[B] using mapper, or passes through Err.
func Map[A any, B any](r Result[A], mapper func(value A) B) Result[B] {
	if r.err == nil {
		return Ok(mapper(r.value))
	}

	return Err[B](r.err)
}

// Bind maps Ok[A] to Result[B] using mapper, or passes through Err.
func Bind[A any, B any](r Result[A], mapper func(value A) Result[B]) Result[B] {
	if r.err == nil {
		return mapper(r.value)
	}

	return Err[B](r.err)
}
