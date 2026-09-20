package store

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
)

const (
	crashExitCode     = 77
	appendBeforeCrash = "append-before"
	appendAfterCrash  = "append-after"
	commitBeforeCrash = "commit-before"
	commitAfterCrash  = "commit-after"
	deadBeforeCrash   = "dead-letter-before"
	deadAfterCrash    = "dead-letter-after"
)

func TestStoreCrashHelper(_ *testing.T) {
	if os.Getenv("TEAK_STORE_CRASH_HELPER") != "1" {
		return
	}
	dir := os.Getenv("TEAK_STORE_CRASH_DIR")
	operation := os.Getenv("TEAK_STORE_CRASH_OPERATION")
	root, err := Open(dir)
	if err != nil {
		crashHelperFail("open", err)
	}
	stream, err := root.Stream("jobs")
	if err != nil {
		crashHelperFail("stream", err)
	}
	if operation == appendBeforeCrash || operation == commitBeforeCrash || operation == deadBeforeCrash {
		stream.beforeCommit = func(current string) error {
			if current == operation[:len(operation)-len("-before")] {
				os.Exit(crashExitCode)
			}
			return nil
		}
	}
	switch operation {
	case appendBeforeCrash, appendAfterCrash:
		_, err = stream.Append([]byte("payload"), time.Unix(100, 0))
	case commitBeforeCrash, commitAfterCrash:
		err = stream.Commit([]uint64{1})
	case deadBeforeCrash, deadAfterCrash:
		err = stream.DeadLetter(1, "failed", time.Unix(101, 0))
	default:
		crashHelperFail("operation", errors.New("unknown operation"))
	}
	if err != nil {
		crashHelperFail(operation, err)
	}
	os.Exit(crashExitCode)
}

func TestCrashTransactionBoundaries(t *testing.T) {
	tests := []struct {
		name        string
		operation   string
		prepare     bool
		wantPending uint64
		wantDead    uint64
		wantTail    uint64
	}{
		{name: "append before commit", operation: appendBeforeCrash, wantPending: 0, wantTail: 0},
		{name: "append after commit", operation: appendAfterCrash, wantPending: 1, wantTail: 1},
		{name: "commit before transaction commit", operation: commitBeforeCrash, prepare: true, wantPending: 1, wantTail: 1},
		{name: "commit after transaction commit", operation: commitAfterCrash, prepare: true, wantPending: 0, wantTail: 1},
		{name: "dead letter before commit", operation: deadBeforeCrash, prepare: true, wantPending: 1, wantDead: 0, wantTail: 1},
		{name: "dead letter after commit", operation: deadAfterCrash, prepare: true, wantPending: 0, wantDead: 1, wantTail: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			if test.prepare {
				prepareCrashRecord(t, dir)
			}
			runCrashHelper(t, dir, test.operation)
			root, err := Open(dir)
			if err != nil {
				t.Fatalf("reopen after crash: %v", err)
			}
			defer func() {
				if err := root.Close(); err != nil {
					t.Errorf("close after crash: %v", err)
				}
			}()
			stream, err := root.Stream("jobs")
			if err != nil {
				t.Fatal(err)
			}
			counts, err := stream.Counts()
			if err != nil {
				t.Fatal(err)
			}
			if counts.Pending != test.wantPending || counts.DeadLetter != test.wantDead || counts.Tail != test.wantTail {
				t.Fatalf("recovered counts = %+v, want pending=%d dead=%d tail=%d", counts, test.wantPending, test.wantDead, test.wantTail)
			}
		})
	}
}

func prepareCrashRecord(t *testing.T, dir string) {
	t.Helper()
	root, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := root.Stream("jobs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Append([]byte("payload"), time.Unix(100, 0)); err != nil {
		t.Fatal(err)
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}
}

func runCrashHelper(t *testing.T, dir, operation string) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestStoreCrashHelper$")
	command.Env = append(os.Environ(),
		"TEAK_STORE_CRASH_HELPER=1",
		"TEAK_STORE_CRASH_DIR="+dir,
		"TEAK_STORE_CRASH_OPERATION="+operation,
	)
	output, err := command.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != crashExitCode {
		t.Fatalf("crash helper %q: err=%v output=%s", operation, err, output)
	}
}

func crashHelperFail(operation string, err error) {
	_, _ = fmt.Fprintf(os.Stderr, "%s: %v\n", operation, err)
	os.Exit(2)
}
