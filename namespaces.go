package evt

import (
	"errors"
	"fmt"
	"strings"
)

// Reserved namespace prefixes for engine-managed rows (storage spec, Reserved Namespaces; ADR-0004
// C8). Matching is a case-sensitive byte prefix on the entity ID. Rows in these namespaces belong
// to the engine — the feature marker, the rebuild fence, and command outcomes — so enumeration,
// entity heads, and rebuild scans skip them, and every entity-addressed write path refuses them.
const (
	// SystemNamespacePrefix is reserved for engine control rows: the feature marker
	// (SYSTEM#FEATURES) and the rebuild fence (SYSTEM#REBUILD, SYSTEM#REBUILD#GENERATION).
	SystemNamespacePrefix = "SYSTEM#"

	// CommandOutcomeNamespacePrefix is reserved for command-outcome rows
	// (command-outcome#<commandId>).
	CommandOutcomeNamespacePrefix = "command-outcome#"
)

// ReservedNamespacePrefixes lists every reserved partition prefix.
var ReservedNamespacePrefixes = []string{
	SystemNamespacePrefix,
	CommandOutcomeNamespacePrefix,
}

// IsReservedEntityID reports whether an entity ID falls in a reserved namespace. Matching is a
// case-sensitive byte prefix, exactly as the storage spec pins it.
func IsReservedEntityID(id EntityID) bool {
	for _, prefix := range ReservedNamespacePrefixes {
		if strings.HasPrefix(string(id), prefix) {
			return true
		}
	}

	return false
}

// ReservedEntityIdError is returned when a write path is handed an entity ID that falls in a
// reserved namespace. The refusal happens before any request, so nothing is written.
type ReservedEntityIdError struct {
	EntityID  EntityID
	Namespace string
}

// NewReservedEntityIdError creates a ReservedEntityIdError for the given entity ID.
func NewReservedEntityIdError(id EntityID) *ReservedEntityIdError {
	namespace := SystemNamespacePrefix
	if !strings.HasPrefix(string(id), namespace) {
		namespace = CommandOutcomeNamespacePrefix
	}

	return &ReservedEntityIdError{EntityID: id, Namespace: namespace}
}

// IsReservedEntityIdErr returns true when err is a ReservedEntityIdError.
func IsReservedEntityIdErr(err error) bool {
	var reserved *ReservedEntityIdError
	return errors.As(err, &reserved)
}

// Error returns the error message string.
func (e *ReservedEntityIdError) Error() string {
	return fmt.Sprintf(
		"entity ID %q is in the reserved namespace %q; rename (migrate) the stream to a non-reserved ID",
		e.EntityID, e.Namespace,
	)
}

// CheckReservedEntityID refuses an entity ID that falls in a reserved namespace, or returns nil.
func CheckReservedEntityID(id EntityID) error {
	if IsReservedEntityID(id) {
		return NewReservedEntityIdError(id)
	}

	return nil
}

// CheckReservedEntityIDs refuses as soon as one event targets a reserved namespace. Multi-stream
// commits are refused whole: one reserved stream fails the entire commit.
func CheckReservedEntityIDs(events []SerializedEvent) error {
	for _, event := range events {
		if err := CheckReservedEntityID(event.EntityID); err != nil {
			return err
		}
	}

	return nil
}
