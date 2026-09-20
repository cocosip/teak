package teak

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestStructuredStorageLogExcludesPayload(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	factoryAPI, err := New(DefaultOptions(t.TempDir()).WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = factoryAPI.Close(context.Background()) }()
	logAPI, err := factoryAPI.Open(t.Context(), "jobs")
	if err != nil {
		t.Fatal(err)
	}
	stream := logAPI.(*logStream)
	secret := "secret-payload"
	if _, err := stream.Write(t.Context(), []byte(secret)); err != nil {
		t.Fatal(err)
	}
	_ = stream.storageError("write", 42, errors.New("disk failure"))

	line := output.String()
	for _, expected := range []string{"\"operation\":\"write\"", "\"stream\":\"jobs\"", "\"sequence\":42", "disk failure"} {
		if !strings.Contains(line, expected) {
			t.Errorf("log missing %q: %s", expected, line)
		}
	}
	if strings.Contains(line, secret) {
		t.Fatalf("log contains payload: %s", line)
	}
}
