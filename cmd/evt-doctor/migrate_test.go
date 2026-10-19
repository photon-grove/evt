package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/photon-grove/evt"
)

// fakeDynamo serves the DynamoDB calls migrate-heads makes: a marker read, scans, and head writes.
type fakeDynamo struct {
	mu      sync.Mutex
	marker  string
	scan    string
	updates int
	puts    int
}

func (f *fakeDynamo) serve(t *testing.T) *httptest.Server {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")

		f.mu.Lock()
		defer f.mu.Unlock()

		switch strings.TrimPrefix(r.Header.Get("X-Amz-Target"), "DynamoDB_20120810.") {
		case "GetItem":
			marker := f.marker
			if marker == "" {
				marker = `{}`
			}

			writeDynamoJSON(t, w, http.StatusOK, marker)
		case "Scan":
			writeDynamoJSON(t, w, http.StatusOK, f.scan)
		case "UpdateItem":
			f.updates++

			writeDynamoJSON(t, w, http.StatusOK, `{}`)
		case "PutItem":
			f.puts++

			writeDynamoJSON(t, w, http.StatusOK, `{}`)
		default:
			t.Errorf("unexpected SDK operation %q", r.Header.Get("X-Amz-Target"))
			w.WriteHeader(http.StatusBadRequest)
		}
	}))

	t.Cleanup(server.Close)

	return server
}

func migrateHeads(t *testing.T, args ...string) (stdout, stderr string, status int) {
	t.Helper()

	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_CONFIG_FILE", t.TempDir()+"/config")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", t.TempDir()+"/credentials")

	var out, errOut bytes.Buffer

	status = runMigrateHeads(context.Background(), args, &out, &errOut)

	return out.String(), errOut.String(), status
}

const (
	v08Scan       = `{"Items":[{"pk":{"S":"a"},"sk":{"N":"1"}},{"pk":{"S":"a"},"sk":{"N":"2"}},{"pk":{"S":"b"},"sk":{"N":"1"}}]}`
	migratedScan  = `{"Items":[{"pk":{"S":"a"},"sk":{"N":"1"}},{"pk":{"S":"a"},"sk":{"N":"2"}},{"pk":{"S":"a"},"sk":{"N":"-1"},"head":{"N":"2"}}]}`
	journalMarker = `{"Item":{"pk":{"S":"SYSTEM#FEATURES"},"sk":{"N":"0"},"version":{"N":"1"},"features":{"S":"{\"journal\":1}"}}}`
)

func TestMigrateHeadsRefusesUnsafeInvocations(t *testing.T) {
	fake := &fakeDynamo{scan: v08Scan}
	server := fake.serve(t)

	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"writes need the confirmation", []string{}, "-confirm-writers-stopped"},
		{"repair needs the confirmation", []string{"-repair"}, "-confirm-writers-stopped"},
		{"allow-lower needs repair", []string{"-allow-lower", "-confirm-writers-stopped"}, "-allow-lower needs -repair"},
		{"verify takes no other mode", []string{"-verify", "-repair"}, "-verify is read-only"},
		{"unknown backend", []string{"-backend", "sqlite", "-verify"}, "unknown backend"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, stderr, status := migrateHeads(t, append([]string{"-endpoint", server.URL, "-region", "us-west-2"}, tc.args...)...)
			if status != exitUsage || !strings.Contains(stderr, tc.want) {
				t.Fatalf("status=%d stderr=%q, want usage status with %q", status, stderr, tc.want)
			}
		})
	}

	if fake.updates != 0 || fake.puts != 0 {
		t.Fatalf("a refused invocation wrote %d updates and %d puts", fake.updates, fake.puts)
	}
}

func TestMigrateHeadsHelpPrintsTheProcedure(t *testing.T) {
	_, stderr, status := migrateHeads(t, "-h")
	if status != exitOK {
		t.Fatalf("status=%d", status)
	}

	for _, want := range []string{"stop-the-world", "IAM deny", "-confirm-writers-stopped", "-allow-lower", "restore"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("usage is missing %q: %s", want, stderr)
		}
	}
}

func TestMigrateHeadsVerify(t *testing.T) {
	for _, tc := range []struct {
		name       string
		marker     string
		scan       string
		wantStatus int
		wantOut    string
		wantErr    string
	}{
		{"a clean store", "", migratedScan, exitOK, "every stream's head matches its log", ""},
		{"headless streams", "", v08Scan, exitFailed, "a: missing head item, log derives head 2", ""},
		{"a journal store", journalMarker, migratedScan, exitFailed, "", "journal@1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeDynamo{marker: tc.marker, scan: tc.scan}
			server := fake.serve(t)

			stdout, stderr, status := migrateHeads(t, "-endpoint", server.URL, "-region", "us-west-2", "-verify")
			if status != tc.wantStatus || !strings.Contains(stdout, tc.wantOut) || !strings.Contains(stderr, tc.wantErr) {
				t.Fatalf("status=%d stdout=%q stderr=%q", status, stdout, stderr)
			}

			if fake.updates != 0 || fake.puts != 0 {
				t.Fatalf("verify wrote %d updates and %d puts", fake.updates, fake.puts)
			}
		})
	}
}

func TestMigrateHeadsDryRunCountsAndWritesNothing(t *testing.T) {
	fake := &fakeDynamo{scan: v08Scan}
	server := fake.serve(t)

	stdout, stderr, status := migrateHeads(t, "-endpoint", server.URL, "-region", "us-west-2", "-dry-run")
	if status != exitOK {
		t.Fatalf("status=%d stdout=%q stderr=%q", status, stdout, stderr)
	}

	if !strings.Contains(stdout, "2 stream(s): would write 2 head item(s)") || !strings.Contains(stdout, "nothing was written") {
		t.Fatalf("stdout=%q", stdout)
	}

	if fake.updates != 0 || fake.puts != 0 {
		t.Fatalf("a dry run wrote %d updates and %d puts", fake.updates, fake.puts)
	}
}

func TestMigrateHeadsStopsWhenAHeadStillDoesNotMatch(t *testing.T) {
	// The fake never persists the writes, so verification after the backfill still finds the
	// streams headless: the migration must stop before declaring.
	fake := &fakeDynamo{scan: v08Scan}
	server := fake.serve(t)

	stdout, stderr, status := migrateHeads(t, "-endpoint", server.URL, "-region", "us-west-2", "-confirm-writers-stopped")
	if status != exitFailed {
		t.Fatalf("status=%d stdout=%q stderr=%q", status, stdout, stderr)
	}

	for _, want := range []string{"streams whose head does not match the log", "stream-heads was NOT declared"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr is missing %q: %s", want, stderr)
		}
	}

	if !strings.Contains(stdout, "operator steps this tool cannot check") {
		t.Errorf("stdout is missing the checklist: %s", stdout)
	}

	if fake.updates != 2 || fake.puts != 0 {
		t.Fatalf("wrote %d updates and %d puts, want the two head items and no marker", fake.updates, fake.puts)
	}
}

func TestMigrateHeadsDeclaresAfterTwoCleanVerifications(t *testing.T) {
	fake := &fakeDynamo{scan: migratedScan}
	server := fake.serve(t)

	stdout, stderr, status := migrateHeads(t, "-endpoint", server.URL, "-region", "us-west-2", "-confirm-writers-stopped")
	if status != exitOK {
		t.Fatalf("status=%d stdout=%q stderr=%q", status, stdout, stderr)
	}

	order := []string{
		"migrate-heads: backfill", "migrate-heads: verify\n", "migrate-heads: verify-before-declare",
		"migrate-heads: declare", "migrate-heads: verify-after-declare", "declared stream-heads",
	}

	last := -1

	for _, want := range order {
		index := strings.Index(stdout, want)
		if index <= last {
			t.Fatalf("%q is missing or out of order in: %s", want, stdout)
		}

		last = index
	}

	if fake.updates != 0 || fake.puts != 1 {
		t.Fatalf("wrote %d updates and %d puts, want only the marker", fake.updates, fake.puts)
	}
}

func TestMigrateHeadsOnAMigratedStoreOnlyVerifies(t *testing.T) {
	marker := `{"Item":{"pk":{"S":"SYSTEM#FEATURES"},"sk":{"N":"0"},"version":{"N":"1"},"features":{"S":"{\"stream-heads\":1}"}}}`

	for _, tc := range []struct {
		name       string
		scan       string
		wantStatus int
		wantOut    string
	}{
		{"clean", migratedScan, exitOK, "every stream's head matches its log"},
		{"drifted", v08Scan, exitFailed, "a: missing head item"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeDynamo{marker: marker, scan: tc.scan}
			server := fake.serve(t)

			stdout, stderr, status := migrateHeads(t, "-endpoint", server.URL, "-region", "us-west-2", "-confirm-writers-stopped")
			if status != tc.wantStatus || !strings.Contains(stdout, "already declares stream-heads") ||
				!strings.Contains(stdout, tc.wantOut) {
				t.Fatalf("status=%d stdout=%q stderr=%q", status, stdout, stderr)
			}

			if fake.updates != 0 || fake.puts != 0 {
				t.Fatalf("wrote %d updates and %d puts", fake.updates, fake.puts)
			}
		})
	}
}

func TestDriftCheckFailureIsOnlySkippedForAnUnmodeledFeature(t *testing.T) {
	unsupported := &evt.UnsupportedFeatureError{Unsupported: []evt.UnsupportedPair{{Name: "journal", Format: 1}}}

	var out bytes.Buffer

	if !driftCheckFailed(&out, unsupported) || !strings.Contains(out.String(), "skipped") {
		t.Fatalf("an unmodeled feature should skip the check: %q", out.String())
	}

	out.Reset()

	if driftCheckFailed(&out, errors.New("AccessDeniedException")) || !strings.Contains(out.String(), "could not run") {
		t.Fatalf("any other failure should fail the run: %q", out.String())
	}
}

func TestMigrateHeadsRejectsStrayArguments(t *testing.T) {
	fake := &fakeDynamo{scan: v08Scan}
	server := fake.serve(t)

	_, stderr, status := migrateHeads(t, "-endpoint", server.URL, "-verify", "oops")
	if status != exitUsage || !strings.Contains(stderr, "takes flags only") {
		t.Fatalf("status=%d stderr=%q", status, stderr)
	}
}
