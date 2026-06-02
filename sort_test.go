package evt

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestByCommandID(t *testing.T) {
	cmdID1 := CommandID("cmd-1")
	cmdID2 := CommandID("cmd-2")
	cmdID3 := CommandID("cmd-3")

	events := []SerializedEvent{
		{
			ID: "entity-1:3",
			Metadata: Metadata{
				CommandID: &cmdID3,
			},
		},
		{
			ID: "entity-1:1",
			Metadata: Metadata{
				CommandID: &cmdID1,
			},
		},
		{
			ID: "entity-1:2",
			Metadata: Metadata{
				CommandID: &cmdID2,
			},
		},
	}

	// Sort
	sort.Sort(ByCommandID(events))

	// Verify order
	assert.Equal(t, cmdID1, *events[0].Metadata.CommandID)
	assert.Equal(t, cmdID2, *events[1].Metadata.CommandID)
	assert.Equal(t, cmdID3, *events[2].Metadata.CommandID)
}

func TestByCommandID_Mixed(t *testing.T) {
	// Some have CommandID, some don't.
	// If one or both don't have CommandID, it falls back to ID.

	cmdID1 := CommandID("cmd-1")

	events := []SerializedEvent{
		{
			ID: "entity-1:3", // No CommandID
		},
		{
			ID: "entity-1:1",
			Metadata: Metadata{
				CommandID: &cmdID1,
			},
		},
		{
			ID: "entity-1:2", // No CommandID
		},
	}

	// Sort
	// Logic:
	// If both have CommandID -> sort by CommandID
	// Else -> sort by ID

	// Expected:
	// entity-1:1 (has cmd-1, wait. How does it compare to others?)
	// If i has cmd and j doesn't -> fallback to ID comparison?
	// Source code:
	// if cmd := events[i].Metadata.CommandID; cmd != nil {
	// 	if otherCmd := events[j].Metadata.CommandID; otherCmd != nil {
	// 		return *cmd < *otherCmd
	// 	}
	// }
	// return events[i].ID < events[j].ID

	// So if only one has CommandID, it falls through to ID comparison.
	// 1. "entity-1:3" vs "entity-1:1"
	//    i="entity-1:3" (nil cmd), j="entity-1:1" (has cmd).
	//    Condition `cmd != nil` is false. Fallback to ID.
	//    "entity-1:3" < "entity-1:1" is FALSE.
	//    So "entity-1:3" >= "entity-1:1".

	// 2. "entity-1:1" vs "entity-1:2"
	//    i="entity-1:1" (has cmd), j="entity-1:2" (nil cmd).
	//    Condition `cmd != nil` is true.
	//    Condition `otherCmd != nil` is false.
	//    Fallback to ID.
	//    "entity-1:1" < "entity-1:2" is TRUE.

	// So effectively, if CommandIDs are missing, it sorts by ID.
	// Even if one has CommandID and other doesn't, it sorts by ID.
	// Only if BOTH have CommandID, it sorts by CommandID.

	// Result should be sorted by ID:
	// entity-1:1
	// entity-1:2
	// entity-1:3

	sort.Sort(ByCommandID(events))

	assert.Equal(t, EventID("entity-1:1"), events[0].ID)
	assert.Equal(t, EventID("entity-1:2"), events[1].ID)
	assert.Equal(t, EventID("entity-1:3"), events[2].ID)
}

func TestByCommandID_NoCommandIDs(t *testing.T) {
	events := []SerializedEvent{
		{ID: "entity-1:2"},
		{ID: "entity-1:1"},
	}

	sort.Sort(ByCommandID(events))

	assert.Equal(t, EventID("entity-1:1"), events[0].ID)
	assert.Equal(t, EventID("entity-1:2"), events[1].ID)
}
