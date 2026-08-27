package provider

import (
	"errors"
	"regexp"
)

var (
	ErrInvalidDescriptor     = errors.New("payment provider descriptor is invalid")
	ErrUnsupportedCapability = errors.New("required payment provider capability is unsupported")
	providerNamePattern      = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
	apiVersionPattern        = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
)

// CapabilitySet is deliberately closed and typed. Adding a provider feature
// requires a code review rather than accepting an arbitrary configuration key.
type CapabilitySet struct {
	HostedCheckout         bool
	Authorize              bool
	Capture                bool
	Void                   bool
	FullRefund             bool
	PartialRefund          bool
	PaymentStatusQuery     bool
	SettlementTransactions bool
	PayoutReports          bool
	WebhookSignatures      bool
	WebhookKeyRotation     bool
}

type Descriptor struct {
	Name         string
	APIVersion   string
	Capabilities CapabilitySet
}

func (descriptor Descriptor) Validate() error {
	if !providerNamePattern.MatchString(descriptor.Name) || !apiVersionPattern.MatchString(descriptor.APIVersion) {
		return ErrInvalidDescriptor
	}
	return nil
}

func (descriptor Descriptor) Require(required CapabilitySet) error {
	if err := descriptor.Validate(); err != nil {
		return err
	}
	actual := descriptor.Capabilities
	if required.HostedCheckout && !actual.HostedCheckout ||
		required.Authorize && !actual.Authorize ||
		required.Capture && !actual.Capture || required.Void && !actual.Void ||
		required.FullRefund && !actual.FullRefund || required.PartialRefund && !actual.PartialRefund ||
		required.PaymentStatusQuery && !actual.PaymentStatusQuery ||
		required.SettlementTransactions && !actual.SettlementTransactions ||
		required.PayoutReports && !actual.PayoutReports ||
		required.WebhookSignatures && !actual.WebhookSignatures ||
		required.WebhookKeyRotation && !actual.WebhookKeyRotation {
		return ErrUnsupportedCapability
	}
	return nil
}

func SagaCapabilities() CapabilitySet {
	return CapabilitySet{
		HostedCheckout: true, Authorize: true, Capture: true, Void: true,
		FullRefund: true, PaymentStatusQuery: true, WebhookSignatures: true,
	}
}

type Described interface {
	Descriptor() Descriptor
}
