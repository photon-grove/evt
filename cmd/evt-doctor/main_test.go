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

func TestDoctorCLI(t *testing.T) {
	// Force the local SDK configuration regardless of the developer's AWS setup.
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_CONFIG_FILE", t.TempDir()+"/config")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", t.TempDir()+"/credentials")

	for _, tc := range []struct {
		name       string
		scan       string
		failure    string
		wantStatus int
		wantOutput string
	}{
		{
			name: "clean", scan: `{"Items":[{"pk":{"S":"account-1"},"sk":{"N":"1"}}]}`,
			wantOutput: "events: ACTIVE\nviews: ACTIVE\nreserved-namespace check: no pre-existing streams in reserved namespaces\n",
		},
		{
			name: "migration needed still succeeds", scan: `{"Items":[{"pk":{"S":"SYSTEM#legacy"},"sk":{"N":"1"}}]}`,
			wantOutput: "events: ACTIVE\nviews: ACTIVE\nreserved-namespace check: 1 stream(s) must be renamed (migrated) before enabling the v0.9 features:\n  SYSTEM#legacy\n",
		},
		{
			name: "scan failure", failure: "Scan", wantStatus: 1,
			wantOutput: "reserved-namespace check: scan events:",
		},
		{
			name: "describe failure", failure: "DescribeTable", wantStatus: 1,
			wantOutput: "describe events:",
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
			if tc.wantStatus == 0 {
				if out != tc.wantOutput {
					t.Fatalf("output=%q, want %q", out, tc.wantOutput)
				}
			} else if !strings.Contains(out, tc.wantOutput) || !strings.Contains(out, "diagnostic failure") || strings.Contains(out, "must be renamed") {
				t.Fatalf("missing error diagnostic or misleading success: %s", out)
			}
		})
	}
}
