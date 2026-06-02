package evt

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

// NewJSONViewValueFunc creates a destination value for JSON view decoding.
//
// For pointer-backed domain entities that need constructor-injected
// dependencies, pass the app-specific constructor:
//
//	entity, found, err := evt.GetJSONView(ctx, repo, pk, func() *Entity {
//		return NewEntity(projectors...)
//	})
//
// For plain value types, callers may pass nil and the zero value of T will be
// used as the decode destination.
type NewJSONViewValueFunc[T any] func() T

// JSONViewDecodeError describes a payload decode failure for a serialized view.
type JSONViewDecodeError struct {
	PK  string
	SK  string
	Err error
}

// Error returns a human-readable decode error message.
func (e *JSONViewDecodeError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("decode JSON view pk=%q sk=%q: %v", e.PK, e.SK, e.Err)
}

// Unwrap returns the underlying JSON decode error.
func (e *JSONViewDecodeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// GetJSONView loads a serialized view by partition key and decodes its JSON
// payload into T.
//
// The boolean return value is false when no view exists for pk. Repository and
// JSON decode failures are returned as errors.
func GetJSONView[T any](
	ctx context.Context,
	repo ViewRepository,
	pk string,
	newValue NewJSONViewValueFunc[T],
) (T, bool, error) {
	var zero T

	if repo == nil {
		return zero, false, fmt.Errorf("view repository is not configured")
	}

	view, err := repo.GetView(ctx, pk)
	if err != nil {
		return zero, false, err
	}
	if view == nil {
		return zero, false, nil
	}

	value := newJSONViewValue(newValue)
	if err := decodeJSONViewInto(view, decodeJSONDestination(&value)); err != nil {
		return zero, false, err
	}

	return value, true, nil
}

// PutJSONView marshals value as JSON and writes it as the Payload of view.
//
// The provided SerializedView is copied before mutation so callers can safely
// reuse metadata templates across writes.
func PutJSONView(ctx context.Context, repo ViewRepository, view *SerializedView, value any) error {
	if repo == nil {
		return fmt.Errorf("view repository is not configured")
	}
	if view == nil {
		return fmt.Errorf("serialized view is required")
	}

	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode JSON view pk=%q sk=%q: %w", view.PK, view.SK, err)
	}

	serialized := *view
	serialized.Payload = payload

	return repo.PutView(ctx, &serialized)
}

// ListJSONViewsByEntityType lists views by entity type and strictly decodes
// every JSON payload into T.
//
// Use ListValidJSONViewsByEntityType when invalid payloads should be skipped
// instead of failing the whole list.
func ListJSONViewsByEntityType[T any](
	ctx context.Context,
	repo ViewRepository,
	entityType EntityType,
	newValue NewJSONViewValueFunc[T],
) ([]T, error) {
	if repo == nil {
		return nil, fmt.Errorf("view repository is not configured")
	}

	views, err := repo.ListViewsByEntityType(ctx, entityType)
	if err != nil {
		return nil, err
	}

	return decodeJSONViews(views, newValue, false, nil)
}

// ListValidJSONViewsByEntityType lists views by entity type and decodes valid
// JSON payloads into T, skipping invalid payloads.
//
// When onDecodeError is non-nil, it is called for each skipped payload.
func ListValidJSONViewsByEntityType[T any](
	ctx context.Context,
	repo ViewRepository,
	entityType EntityType,
	newValue NewJSONViewValueFunc[T],
	onDecodeError func(*SerializedView, error),
) ([]T, error) {
	if repo == nil {
		return nil, fmt.Errorf("view repository is not configured")
	}

	views, err := repo.ListViewsByEntityType(ctx, entityType)
	if err != nil {
		return nil, err
	}

	return decodeJSONViews(views, newValue, true, onDecodeError)
}

// ListJSONViewsByPK lists views by partition key and strictly decodes every
// JSON payload into T.
//
// Use ListValidJSONViewsByPK when invalid payloads should be skipped instead
// of failing the whole list.
func ListJSONViewsByPK[T any](
	ctx context.Context,
	repo ViewRepository,
	pk string,
	newValue NewJSONViewValueFunc[T],
) ([]T, error) {
	if repo == nil {
		return nil, fmt.Errorf("view repository is not configured")
	}

	views, err := repo.ListViewsByPK(ctx, pk)
	if err != nil {
		return nil, err
	}

	return decodeJSONViews(views, newValue, false, nil)
}

// ListValidJSONViewsByPK lists views by partition key and decodes valid JSON
// payloads into T, skipping invalid payloads.
//
// When onDecodeError is non-nil, it is called for each skipped payload.
func ListValidJSONViewsByPK[T any](
	ctx context.Context,
	repo ViewRepository,
	pk string,
	newValue NewJSONViewValueFunc[T],
	onDecodeError func(*SerializedView, error),
) ([]T, error) {
	if repo == nil {
		return nil, fmt.Errorf("view repository is not configured")
	}

	views, err := repo.ListViewsByPK(ctx, pk)
	if err != nil {
		return nil, err
	}

	return decodeJSONViews(views, newValue, true, onDecodeError)
}

// DecodeJSONView decodes a SerializedView payload into target.
//
// target must be a non-nil pointer. Passing a struct value, a nil pointer,
// or a nil slice/map returns an error rather than silently no-op'ing into
// a discarded local copy. Use a pointer to preserve constructor-injected
// fields on domain entities and to receive the decoded JSON payload.
func DecodeJSONView[T any](view *SerializedView, target T) error {
	dest := any(target)
	reflectValue := reflect.ValueOf(dest)
	if !reflectValue.IsValid() || reflectValue.Kind() != reflect.Pointer || reflectValue.IsNil() {
		return fmt.Errorf("decode JSON view: target must be a non-nil pointer, got %T", target)
	}

	return decodeJSONViewInto(view, dest)
}

func decodeJSONViews[T any](
	views []*SerializedView,
	newValue NewJSONViewValueFunc[T],
	skipDecodeErrors bool,
	onDecodeError func(*SerializedView, error),
) ([]T, error) {
	result := make([]T, 0, len(views))

	for _, view := range views {
		if view == nil {
			err := fmt.Errorf("serialized view is required")
			if skipDecodeErrors {
				if onDecodeError != nil {
					onDecodeError(view, err)
				}
				continue
			}
			return nil, err
		}

		value := newJSONViewValue(newValue)
		if err := decodeJSONViewInto(view, decodeJSONDestination(&value)); err != nil {
			if skipDecodeErrors {
				if onDecodeError != nil {
					onDecodeError(view, err)
				}
				continue
			}
			return nil, err
		}

		result = append(result, value)
	}

	return result, nil
}

func newJSONViewValue[T any](newValue NewJSONViewValueFunc[T]) T {
	if newValue != nil {
		return newValue()
	}

	var zero T
	return zero
}

func decodeJSONViewInto(view *SerializedView, dest any) error {
	if view == nil {
		return fmt.Errorf("serialized view is required")
	}

	if err := json.Unmarshal(view.Payload, dest); err != nil {
		return &JSONViewDecodeError{
			PK:  view.PK,
			SK:  view.SK,
			Err: err,
		}
	}

	return nil
}

func decodeJSONDestination[T any](value *T) any {
	if value == nil {
		return value
	}

	target := any(*value)
	reflectValue := reflect.ValueOf(target)
	if reflectValue.IsValid() && reflectValue.Kind() == reflect.Pointer && !reflectValue.IsNil() {
		return target
	}

	return value
}
