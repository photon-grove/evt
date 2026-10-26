package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func initStore(t *testing.T, args ...string) (stdout, stderr string, status int) {
	t.Helper()

	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_CONFIG_FILE", t.TempDir()+"/config")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", t.TempDir()+"/credentials")

	var out, errOut bytes.Buffer

	status = runInit(context.Background(), args, &out, &errOut)

	return out.String(), errOut.String(), status
}

func TestInitDeclaresStreamHeadsOnAFreshTable(t *testing.T) {
	// A fresh table: no marker row, and a Scan that finds only an engine row.
	fake := &fakeDynamo{scan: `{"Items":[{"pk":{"S":"command-outcome#c1"}}]}`}
	server := fake.serve(t)

	stdout, stderr, status := initStore(t, "-endpoint", server.URL, "-region", "us-west-2")
	if status != exitOK || !strings.Contains(stdout, "declared stream-heads") {
		t.Fatalf("status=%d stdout=%q stderr=%q", status, stdout, stderr)
	}

	if fake.puts != 1 || fake.updates != 0 {
		t.Fatalf("wrote %d puts and %d updates, want only the marker", fake.puts, fake.updates)
	}
}

func TestInitLeavesAnInitializedTableAlone(t *testing.T) {
	marker := `{"Item":{"pk":{"S":"SYSTEM#FEATURES"},"sk":{"N":"0"},"version":{"N":"1"},"features":{"S":"{\"stream-heads\":1}"}}}`
	// A scan would find data and refuse the table: an initialized table is never scanned.
	fake := &fakeDynamo{marker: marker, scan: v08Scan}
	server := fake.serve(t)

	stdout, stderr, status := initStore(t, "-endpoint", server.URL, "-region", "us-west-2")
	if status != exitOK || !strings.Contains(stdout, "nothing to do") {
		t.Fatalf("status=%d stdout=%q stderr=%q", status, stdout, stderr)
	}

	if fake.puts != 0 || fake.updates != 0 {
		t.Fatalf("wrote %d puts and %d updates to an initialized table", fake.puts, fake.updates)
	}
}

func TestInitRefusesAPopulatedTable(t *testing.T) {
	fake := &fakeDynamo{scan: v08Scan}
	server := fake.serve(t)

	_, stderr, status := initStore(t, "-endpoint", server.URL, "-region", "us-west-2")
	if status != exitFailed || !strings.Contains(stderr, "migrate-heads") {
		t.Fatalf("status=%d stderr=%q, want a refusal pointing at migrate-heads", status, stderr)
	}

	if fake.puts != 0 || fake.updates != 0 {
		t.Fatalf("wrote %d puts and %d updates to a populated table", fake.puts, fake.updates)
	}
}

func TestInitRejectsBadInvocations(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"unknown backend", []string{"-backend", "sqlite"}, "unknown backend"},
		{"stray argument", []string{"now"}, "takes flags only"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, stderr, status := initStore(t, tc.args...)
			if status != exitUsage || !strings.Contains(stderr, tc.want) {
				t.Fatalf("status=%d stderr=%q, want usage status with %q", status, stderr, tc.want)
			}
		})
	}

	_, stderr, status := initStore(t, "-h")
	if status != exitOK || !strings.Contains(stderr, "dynamodb:Scan") {
		t.Fatalf("status=%d, help should explain the permissions: %s", status, stderr)
	}
}
