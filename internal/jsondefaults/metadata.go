// Package jsondefaults applies shared encode-time metadata defaults without depending on evt.
package jsondefaults

// Metadata returns effective causation and a non-nil extensions map without mutating its inputs.
func Metadata[C any](commandID, causationID *C, extensions map[string]string) (*C, map[string]string) {
	if causationID == nil {
		causationID = commandID
	}
	if extensions == nil {
		extensions = map[string]string{}
	}

	return causationID, extensions
}
