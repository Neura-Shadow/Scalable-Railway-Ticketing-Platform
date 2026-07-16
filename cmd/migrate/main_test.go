package main

import (
	"bytes"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRunRedactsMalformedDatabaseConnectionErrors(t *testing.T) {
	t.Parallel()

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test path")
	}
	migrationPath := filepath.Join(filepath.Dir(currentFile), "..", "..", "migrations")
	const secret = "sentinel-m11-migration-password"
	malformed := "postgres://railway:" + secret + "@@db.example:5432/railway?token=query-secret"
	var stdout, stderr bytes.Buffer

	exitCode := run([]string{"-path", migrationPath, "-database", malformed, "version"}, &stdout, &stderr)
	if exitCode != 1 {
		t.Fatalf("run() exit code = %d, want 1", exitCode)
	}
	if !strings.Contains(stderr.String(), "migration database connection failed") {
		t.Fatalf("run() stderr = %q, want bounded connection failure", stderr.String())
	}
	for _, forbidden := range []string{secret, "query-secret", malformed, "postgres://"} {
		if strings.Contains(stderr.String(), forbidden) || strings.Contains(stdout.String(), forbidden) {
			t.Fatalf("run() output exposed %q: stdout=%q stderr=%q", forbidden, stdout.String(), stderr.String())
		}
	}
}
