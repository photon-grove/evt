package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/photon-grove/evt"
)

// Run the real entry point in a child so exitf's os.Exit cannot terminate the suite.
func TestDoctorHelperProcess(_ *testing.T) {
	switch os.Getenv("EVT_DOCTOR_TEST_HELPER") {
	case "cli":
		for i, arg := range os.Args {
			if arg == "--" {
				os.Args = append([]string{os.Args[0]}, os.Args[i+1:]...)
				break
			}
		}
		flag.CommandLine = flag.NewFlagSet("evt-doctor", flag.ExitOnError)
		main()
		os.Exit(0)
	case "report":
		var ids []evt.EntityID
		for i, arg := range os.Args {
			if arg == "--" {
				for _, id := range os.Args[i+1:] {
					ids = append(ids, evt.EntityID(id))
				}
				break
			}
		}
		reportReservedStreams(ids)
		os.Exit(0)
	}
}

func runDoctor(t *testing.T, mode string, args ...string) (string, int) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, executable, append([]string{"-test.run=^TestDoctorHelperProcess$", "--"}, args...)...) //nolint:gosec // Only re-executes this test binary.
	cmd.Env = append(os.Environ(), "EVT_DOCTOR_TEST_HELPER="+mode)
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("doctor timed out: %s", out)
	}
	if err == nil {
		return string(out), 0
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatal(err)
	}

	return string(out), exitErr.ExitCode()
}

func TestReportReservedStreams(t *testing.T) {
	for _, tc := range []struct {
		name string
		ids  []string
		want string
	}{
		{"clean", nil, "reserved-namespace check: no pre-existing streams in reserved namespaces\n"},
		{"migration sorted", []string{"command-outcome#old", "SYSTEM#z", "SYSTEM#a"}, "reserved-namespace check: 3 stream(s) must be renamed (migrated) before enabling the v0.9 features:\n  SYSTEM#a\n  SYSTEM#z\n  command-outcome#old\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, status := runDoctor(t, "report", tc.ids...)
			if status != 0 || out != tc.want {
				t.Fatalf("status=%d output=%q, want status=0 output=%q", status, out, tc.want)
			}
		})
	}
}

func writeDynamoJSON(t *testing.T, w http.ResponseWriter, status int, body string) {
	t.Helper()
	w.Header().Set("X-Amz-Crc32", strconv.FormatUint(uint64(crc32.ChecksumIEEE([]byte(body))), 10))
	w.WriteHeader(status)
	if _, err := fmt.Fprint(w, body); err != nil {
		t.Errorf("write DynamoDB response: %v", err)
	}
}

const (
	undeclaredHeads = "head-drift check: stream-heads is not declared; run \"evt-doctor migrate-heads\" to migrate this store\n"
	declaredMarker  = `{"Item":{"pk":{"S":"SYSTEM#FEATURES"},"sk":{"N":"0"},"version":{"N":"1"},"features":{"S":"{\"stream-heads\":1}"}}}`
)

func TestDoctorCLI(t *testing.T) {
	// Force the local SDK configuration regardless of the developer's AWS setup.
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_CONFIG_FILE", t.TempDir()+"/config")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", t.TempDir()+"/credentials")

	for _, tc := range []struct {
		name       string
		scan       string
		marker     string
		contains   []string
		failure    string
		wantStatus int
		wantOutput string
	}{
		{
			name: "clean", scan: `{"Items":[{"pk":{"S":"account-1"},"sk":{"N":"1"}}]}`,
			wantOutput: "events: ACTIVE\nviews: ACTIVE\nreserved-namespace check: no pre-existing streams in reserved namespaces\n" + undeclaredHeads,
		},
		{
			name: "declared heads that match", marker: declaredMarker,
			scan:       `{"Items":[{"pk":{"S":"account-1"},"sk":{"N":"1"},"head":{"N":"1"}},{"pk":{"S":"account-1"},"sk":{"N":"-1"},"head":{"N":"1"}}]}`,
			wantOutput: "events: ACTIVE\nviews: ACTIVE\nreserved-namespace check: no pre-existing streams in reserved namespaces\nhead-drift check: 1 stream(s), every head matches its log\n",
		},
		{
			name: "declared heads that drifted", marker: declaredMarker, wantStatus: 1,
			scan: `{"Items":[{"pk":{"S":"account-1"},"sk":{"N":"1"}},{"pk":{"S":"account-1"},"sk":{"N":"2"}},{"pk":{"S":"account-1"},"sk":{"N":"-1"},"head":{"N":"1"}},{"pk":{"S":"account-2"},"sk":{"N":"1"}}]}`,
			contains: []string{
				"head-drift check: 2 stream(s), 2 with drift",
				"account-1: head 1 is lower than the derived head 2",
				"account-2: missing head item, log derives head 1",
			},
		},
		{
			name: "a journal store skips the head check", marker: `{"Item":{"pk":{"S":"SYSTEM#FEATURES"},"sk":{"N":"0"},"version":{"N":"1"},"features":{"S":"{\"journal\":1}"}}}`,
			scan:       `{"Items":[{"pk":{"S":"account-1"},"sk":{"N":"1"}}]}`,
			wantOutput: "events: ACTIVE\nviews: ACTIVE\nreserved-namespace check: no pre-existing streams in reserved namespaces\nhead-drift check: skipped: store declares unsupported features (journal@1); refusing to write\n",
		},
		{
			name: "migration needed still succeeds", scan: `{"Items":[{"pk":{"S":"SYSTEM#legacy"},"sk":{"N":"1"}}]}`,
			wantOutput: "events: ACTIVE\nviews: ACTIVE\nreserved-namespace check: 1 stream(s) must be renamed (migrated) before enabling the v0.9 features:\n  SYSTEM#legacy\n" + undeclaredHeads,
		},
		{
			name: "scan failure", failure: "Scan", wantStatus: 1,
			wantOutput: "reserved-namespace check: scan events:",
		},
		{
			name: "describe failure", failure: "DescribeTable", wantStatus: 1,
			wantOutput: "describe events:",
		},
		{
			// A check that could not run must not look like a clean bill of health.
			name: "marker read failure", scan: `{"Items":[]}`, failure: "GetItem", wantStatus: 1,
			wantOutput: "head-drift check: could not run:",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				op := strings.TrimPrefix(r.Header.Get("X-Amz-Target"), "DynamoDB_20120810.")
				w.Header().Set("Content-Type", "application/x-amz-json-1.0")
				if op == tc.failure {
					writeDynamoJSON(t, w, http.StatusBadRequest, `{"__type":"ResourceNotFoundException","message":"diagnostic failure"}`)
					return
				}
				switch op {
				case "DescribeTable":
					writeDynamoJSON(t, w, http.StatusOK, `{"Table":{"TableStatus":"ACTIVE"}}`)
				case "Scan":
					writeDynamoJSON(t, w, http.StatusOK, tc.scan)
				case "GetItem":
					marker := tc.marker
					if marker == "" {
						marker = `{}`
					}

					writeDynamoJSON(t, w, http.StatusOK, marker)
				default:
					t.Errorf("unexpected SDK operation %q", op)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()

			out, status := runDoctor(t, "cli", "-endpoint", server.URL, "-region", "us-west-2", "-events-table", "events", "-views-table", "views")
			if status != tc.wantStatus {
				t.Fatalf("status=%d want=%d output=%s", status, tc.wantStatus, out)
			}
			if len(tc.contains) > 0 {
				for _, want := range tc.contains {
					if !strings.Contains(out, want) {
						t.Fatalf("output is missing %q: %s", want, out)
					}
				}
			} else if tc.wantStatus == 0 {
				if out != tc.wantOutput {
					t.Fatalf("output=%q, want %q", out, tc.wantOutput)
				}
			} else if !strings.Contains(out, tc.wantOutput) || !strings.Contains(out, "diagnostic failure") || strings.Contains(out, "must be renamed") {
				t.Fatalf("missing error diagnostic or misleading success: %s", out)
			}
		})
	}
}
