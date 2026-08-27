package provider_test

import (
	"errors"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
)

func TestDescriptorRejectsMissingRequiredCapability(t *testing.T) {
	t.Parallel()
	descriptor := provider.Descriptor{
		Name: "bounded", APIVersion: "v1",
		Capabilities: provider.CapabilitySet{HostedCheckout: true},
	}
	if err := descriptor.Require(provider.SagaCapabilities()); !errors.Is(err, provider.ErrUnsupportedCapability) {
		t.Fatalf("Require() error = %v, want ErrUnsupportedCapability", err)
	}
}

func TestDescriptorAcceptsExactTypedCapabilities(t *testing.T) {
	t.Parallel()
	capabilities := provider.SagaCapabilities()
	capabilities.PartialRefund = true
	descriptor := provider.Descriptor{Name: "stripe", APIVersion: "2026-07-29.dahlia", Capabilities: capabilities}
	if err := descriptor.Require(provider.SagaCapabilities()); err != nil {
		t.Fatalf("Require() error = %v", err)
	}
}
