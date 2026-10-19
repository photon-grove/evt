package streamheads

import "testing"

func TestSizes(t *testing.T) {
	// A create of one event: key (2+2+1+2), ":last"+"1", update text, condition text.
	want := 7 + 6 + len(UpdateExpression) + len(CreateCondition)
	if got := UpdateSize("a", 1, 1, 0); got != want {
		t.Fatalf("UpdateSize(create) = %d, want %d", got, want)
	}

	// An append adds :prev.
	want = 7 + 6 + 6 + len(UpdateExpression) + len(AppendCondition)
	if got := UpdateSize("a", 2, 2, 0); got != want {
		t.Fatalf("UpdateSize(append) = %d, want %d", got, want)
	}

	// Retention adds the longer update expression, the name placeholder and name, and :t.
	want = 7 + 6 + 6 + len(RetainedUpdateExpression) + len(RetainedAppendCondition) + len("#ttl") + len("ttl") + len(":t") + len("1800000000")
	if got := UpdateSize("a", 2, 2, 1_800_000_000); got != want {
		t.Fatalf("UpdateSize(retained append) = %d, want %d", got, want)
	}

	if got, want := FenceSize("a", 0), 7+len(FenceAbsentCondition); got != want {
		t.Fatalf("FenceSize(0) = %d, want %d", got, want)
	}

	if got, want := FenceSize("a", 12), 7+len(FenceCondition)+len(":head")+2; got != want {
		t.Fatalf("FenceSize(12) = %d, want %d", got, want)
	}
}
