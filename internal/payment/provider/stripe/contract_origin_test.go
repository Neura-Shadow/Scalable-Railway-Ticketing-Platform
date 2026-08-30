package stripe

import "testing"

func TestParseOriginAllowsReservedTestDNSOnlyWithExplicitTestFlag(t *testing.T) {
	t.Parallel()

	if _, err := parseOrigin("http://payment-stripe-contract.test:8100", true); err != nil {
		t.Fatalf("reserved test origin rejected: %v", err)
	}
	for _, raw := range []string{
		"http://payment-stripe-contract.test:8100",
		"http://payment-stripe-contract.example:8100",
		"http://10.0.0.8:8100",
	} {
		if _, err := parseOrigin(raw, false); err == nil {
			t.Fatalf("non-test runtime accepted insecure origin %q", raw)
		}
	}
	if _, err := parseOrigin("http://payment-stripe-contract.example:8100", true); err == nil {
		t.Fatal("test runtime accepted a non-reserved DNS origin")
	}
}
