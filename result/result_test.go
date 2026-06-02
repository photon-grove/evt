package result

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// CustomError implements error interface for testing
type CustomError struct {
	msg string
}

func (e *CustomError) Error() string {
	return e.msg
}

func Test_Result_Ok(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
	}{
		{"string value", "hello world"},
		{"integer value", 42},
		{"boolean value", true},
		{"nil pointer", (*string)(nil)},
		{"struct value", struct{ Name string }{Name: "test"}},
		{"slice value", []int{1, 2, 3}},
		{"map value", map[string]int{"a": 1, "b": 2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch v := tt.value.(type) {
			case string:
				result := Ok(v)
				require.True(t, result.IsOk())
				require.False(t, result.IsErr())
				value, err := result.Value()
				require.Equal(t, v, value)
				require.NoError(t, err)
			case int:
				result := Ok(v)
				require.True(t, result.IsOk())
				require.False(t, result.IsErr())
				value, err := result.Value()
				require.Equal(t, v, value)
				require.NoError(t, err)
			case bool:
				result := Ok(v)
				require.True(t, result.IsOk())
				require.False(t, result.IsErr())
				value, err := result.Value()
				require.Equal(t, v, value)
				require.NoError(t, err)
			case *string:
				result := Ok(v)
				require.True(t, result.IsOk())
				require.False(t, result.IsErr())
				value, err := result.Value()
				require.Equal(t, v, value)
				require.NoError(t, err)
			default:
				// Handle other types generically
				t.Skip("Type not specifically handled in this test")
			}
		})
	}
}

func Test_Result_Err(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"simple error", errors.New("simple error")},
		{"formatted error", fmt.Errorf("formatted error: %s", "details")},
		{"wrapped error", fmt.Errorf("wrapped: %w", errors.New("original"))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Err[string](tt.err)
			require.False(t, result.IsOk())
			require.True(t, result.IsErr())
			value, err := result.Value()
			require.Equal(t, "", value) // zero value for string
			require.Equal(t, tt.err, err)
		})
	}
}

func Test_Result_IsOk(t *testing.T) {
	okResult := Ok("test")
	errResult := Err[string](errors.New("error"))

	require.True(t, okResult.IsOk())
	require.False(t, errResult.IsOk())
}

func Test_Result_IsErr(t *testing.T) {
	okResult := Ok("test")
	errResult := Err[string](errors.New("error"))

	require.False(t, okResult.IsErr())
	require.True(t, errResult.IsErr())
}

func Test_Result_Get(t *testing.T) {
	t.Run("Ok result", func(t *testing.T) {
		result := Ok(42)
		value, err := result.Value()
		require.Equal(t, 42, value)
		require.NoError(t, err)
	})

	t.Run("Err result", func(t *testing.T) {
		expectedErr := errors.New("test error")
		result := Err[int](expectedErr)
		value, err := result.Value()
		require.Equal(t, 0, value) // zero value for int
		require.Equal(t, expectedErr, err)
	})
}

func Test_Result_WithDefault(t *testing.T) {
	t.Run("Ok result returns original value", func(t *testing.T) {
		result := Ok("original")
		value := result.WithDefault("fallback")
		require.Equal(t, "original", value)
	})

	t.Run("Err result returns fallback value", func(t *testing.T) {
		result := Err[string](errors.New("error"))
		value := result.WithDefault("fallback")
		require.Equal(t, "fallback", value)
	})
}

func Test_Map(t *testing.T) {
	mapper := func(value string) int {
		return len(value)
	}

	t.Run("Ok result maps value", func(t *testing.T) {
		result := Ok("hello")
		mapped := Map(result, mapper)
		require.True(t, mapped.IsOk())
		value, err := mapped.Value()
		require.Equal(t, 5, value)
		require.NoError(t, err)
	})

	t.Run("Err result passes through error", func(t *testing.T) {
		expectedErr := errors.New("test error")
		result := Err[string](expectedErr)
		mapped := Map(result, mapper)
		require.True(t, mapped.IsErr())
		value, err := mapped.Value()
		require.Equal(t, 0, value) // zero value for int
		require.Equal(t, expectedErr, err)
	})
}

func Test_Bind(t *testing.T) {
	mapper := func(value string) Result[int] {
		if value == "error" {
			return Err[int](errors.New("mapped error"))
		}
		return Ok(len(value))
	}

	t.Run("Ok result with Ok mapper", func(t *testing.T) {
		result := Ok("hello")
		mapped := Bind(result, mapper)
		require.True(t, mapped.IsOk())
		value, err := mapped.Value()
		require.Equal(t, 5, value)
		require.NoError(t, err)
	})

	t.Run("Ok result with Err mapper", func(t *testing.T) {
		result := Ok("error")
		mapped := Bind(result, mapper)
		require.True(t, mapped.IsErr())
		value, err := mapped.Value()
		require.Equal(t, 0, value)
		require.EqualError(t, err, "mapped error")
	})

	t.Run("Err result passes through error", func(t *testing.T) {
		expectedErr := errors.New("original error")
		result := Err[string](expectedErr)
		mapped := Bind(result, mapper)
		require.True(t, mapped.IsErr())
		value, err := mapped.Value()
		require.Equal(t, 0, value)
		require.Equal(t, expectedErr, err)
	})
}

func Test_Result_ZeroValues(t *testing.T) {
	t.Run("zero value string", func(t *testing.T) {
		result := Ok("")
		require.True(t, result.IsOk())
		value, err := result.Value()
		require.Equal(t, "", value)
		require.NoError(t, err)
	})

	t.Run("zero value int", func(t *testing.T) {
		result := Ok(0)
		require.True(t, result.IsOk())
		value, err := result.Value()
		require.Equal(t, 0, value)
		require.NoError(t, err)
	})

	t.Run("zero value bool", func(t *testing.T) {
		result := Ok(false)
		require.True(t, result.IsOk())
		value, err := result.Value()
		require.Equal(t, false, value)
		require.NoError(t, err)
	})
}

func Test_Result_PointerTypes(t *testing.T) {
	t.Run("nil pointer", func(t *testing.T) {
		var nilPtr *string
		result := Ok(nilPtr)
		require.True(t, result.IsOk())
		value, err := result.Value()
		require.Nil(t, value)
		require.NoError(t, err)
	})

	t.Run("valid pointer", func(t *testing.T) {
		str := "test"
		result := Ok(&str)
		require.True(t, result.IsOk())
		value, err := result.Value()
		require.NotNil(t, value)
		require.Equal(t, "test", *value)
		require.NoError(t, err)
	})
}

func Test_Result_ComplexTypes(t *testing.T) {
	type TestStruct struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}

	t.Run("struct value", func(t *testing.T) {
		original := TestStruct{Name: "test", Value: 42}
		result := Ok(original)
		require.True(t, result.IsOk())
		value, err := result.Value()
		require.Equal(t, original, value)
		require.NoError(t, err)
	})

	t.Run("slice value", func(t *testing.T) {
		original := []string{"a", "b", "c"}
		result := Ok(original)
		require.True(t, result.IsOk())
		value, err := result.Value()
		require.Equal(t, original, value)
		require.NoError(t, err)
	})

	t.Run("map value", func(t *testing.T) {
		original := map[string]int{"a": 1, "b": 2}
		result := Ok(original)
		require.True(t, result.IsOk())
		value, err := result.Value()
		require.Equal(t, original, value)
		require.NoError(t, err)
	})
}

func Test_Result_ChainedOperations(t *testing.T) {
	t.Run("chain multiple Ok results", func(t *testing.T) {
		result := Ok("hello")

		// Chain map operations
		final := Bind(result, func(s string) Result[int] {
			return Ok(len(s))
		})

		mapped := Map(final, func(i int) string {
			return fmt.Sprintf("length: %d", i)
		})

		require.True(t, mapped.IsOk())
		value, err := mapped.Value()
		require.Equal(t, "length: 5", value)
		require.NoError(t, err)
	})

	t.Run("chain with error propagation", func(t *testing.T) {
		result := Err[string](errors.New("initial error"))

		// Chain operations - error should propagate
		final := Bind(result, func(s string) Result[int] {
			return Ok(len(s))
		})

		mapped := Map(final, func(i int) string {
			return fmt.Sprintf("length: %d", i)
		})

		require.True(t, mapped.IsErr())
		value, err := mapped.Value()
		require.Equal(t, "", value)
		require.EqualError(t, err, "initial error")
	})
}

func Test_Result_ErrorTypes(t *testing.T) {
	t.Run("different error types", func(t *testing.T) {
		// Standard error
		result1 := Err[string](errors.New("standard error"))
		require.True(t, result1.IsErr())

		// Formatted error
		result2 := Err[string](fmt.Errorf("formatted error: %d", 42))
		require.True(t, result2.IsErr())

		// Custom error type
		customErr := &CustomError{msg: "custom error"}
		result3 := Err[string](customErr)
		require.True(t, result3.IsErr())
	})
}

func Test_Result_Concurrency(t *testing.T) {
	// Test that Result is safe to read concurrently
	result := Ok("concurrent test")

	done := make(chan bool, 10)

	// Start 10 goroutines reading the result
	for i := 0; i < 10; i++ {
		go func() {
			defer func() { done <- true }()
			require.True(t, result.IsOk())
			value, err := result.Value()
			require.Equal(t, "concurrent test", value)
			require.NoError(t, err)
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}
