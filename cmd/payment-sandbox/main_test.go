package main

import (
	"encoding/base64"
	"path/filepath"
	"testing"
)

func TestLoadConfigRequiresDurableStatePath(t *testing.T) {
	t.Parallel()

	values := validSandboxEnvironment(t)
	delete(values, "PAYMENT_SANDBOX_STATE_PATH")
	if _, err := loadConfig(func(key string) string { return values[key] }); err == nil {
		t.Fatal("sandbox config without durable state path unexpectedly succeeded")
	}

	values["PAYMENT_SANDBOX_STATE_PATH"] = "relative/provider-state.jsonl"
	if _, err := loadConfig(func(key string) string { return values[key] }); err == nil {
		t.Fatal("sandbox config with relative durable state path unexpectedly succeeded")
	}

	values["PAYMENT_SANDBOX_STATE_PATH"] = filepath.Join(t.TempDir(), "provider-state.jsonl")
	config, err := loadConfig(func(key string) string { return values[key] })
	if err != nil {
		t.Fatalf("load valid durable sandbox config: %v", err)
	}
	if config.sandbox.StateStore == nil {
		t.Fatal("valid sandbox config omitted durable state store")
	}
}

func validSandboxEnvironment(t *testing.T) map[string]string {
	t.Helper()
	return map[string]string{
		"PAYMENT_SANDBOX_ENVIRONMENT":          "test",
		"PAYMENT_SANDBOX_WEBHOOK_ISSUE_KEY_ID": "current",
		"PAYMENT_SANDBOX_WEBHOOK_KEYRING":      "current=" + base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef")),
		"PAYMENT_SANDBOX_STATE_PATH":           filepath.Join(t.TempDir(), "provider-state.jsonl"),
		"PAYMENT_SANDBOX_STATE_MAX_BYTES":      "1048576",
	}
}
