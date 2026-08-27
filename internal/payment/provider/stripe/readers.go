package stripe

import (
	"context"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	sdkstripe "github.com/stripe/stripe-go/v86"
)

type ListOptions = provider.EvidenceListOptions
type BalanceTransaction = provider.BalanceTransaction
type BalanceTransactionPage = provider.BalanceTransactionPage
type BalanceTransactionReader = provider.BalanceTransactionReader
type Payout = provider.Payout
type PayoutPage = provider.PayoutPage
type PayoutReader = provider.PayoutReader

var (
	_ provider.Client             = (*Client)(nil)
	_ provider.RefundLookupReader = (*Client)(nil)
	_ BalanceTransactionReader    = (*Client)(nil)
	_ PayoutReader                = (*Client)(nil)
)

// LookupRefund performs one bounded, read-only Stripe list request and accepts
// only a refund carrying every stable merchant correlation value from the
// original operation. It does not replay POST /v1/refunds.
func (c *Client) LookupRefund(ctx context.Context, request provider.RefundLookupRequest) (provider.RefundLookupResult, error) {
	operationIdentity := request.Metadata["refund_operation_id"]
	requestIdentity := request.Metadata["refund_request_id"]
	if request.AmountMinor <= 0 || !validCurrency(request.Currency) ||
		!validIdentifier(request.PaymentIntentID, "") || !validIdentifier(request.ProviderPaymentID, "pi_") ||
		!validIdempotencyKey(request.IdempotencyKey) || provider.ValidateMetadata(request.Metadata) != nil ||
		!validIdentifier(operationIdentity, "") || !validIdentifier(requestIdentity, "") ||
		request.Metadata["refund_idempotency_key"] != request.IdempotencyKey || request.Limit < 1 || request.Limit > 100 {
		return provider.RefundLookupResult{}, validationError("lookup_refund")
	}
	form := url.Values{
		"limit":          {strconv.Itoa(request.Limit)},
		"payment_intent": {request.ProviderPaymentID},
	}
	var list sdkstripe.RefundList
	if _, err := c.doForm(ctx, http.MethodGet, "/v1/refunds", "lookup_refund", "", form, &list); err != nil {
		return provider.RefundLookupResult{}, err
	}
	result := provider.RefundLookupResult{Definitive: !list.HasMore}
	for _, item := range list.Data {
		if item == nil || item.PaymentIntent == nil || item.PaymentIntent.ID != request.ProviderPaymentID {
			return provider.RefundLookupResult{}, inconsistentError("lookup_refund")
		}
		matches := true
		for key, value := range request.Metadata {
			if item.Metadata[key] != value {
				matches = false
				break
			}
		}
		if !matches {
			continue
		}
		currency := strings.ToUpper(string(item.Currency))
		if result.Found || !validIdentifier(item.ID, "re_") || item.Status != sdkstripe.RefundStatusSucceeded ||
			item.Amount != request.AmountMinor || currency != request.Currency {
			return provider.RefundLookupResult{}, inconsistentError("lookup_refund")
		}
		result.Found = true
		result.Refund = provider.OperationResult{
			ProviderPaymentID: item.PaymentIntent.ID, ProviderOperationID: item.ID,
			Status: provider.StatusRefunded, AmountMinor: item.Amount, Currency: currency,
		}
	}
	return result, nil
}

func (c *Client) ListBalanceTransactions(ctx context.Context, options ListOptions) (BalanceTransactionPage, error) {
	form, err := listForm(options)
	if err != nil {
		return BalanceTransactionPage{}, validationError("list_balance_transactions")
	}
	form.Set("expand[]", "data.source")
	var list sdkstripe.BalanceTransactionList
	if _, err := c.doForm(ctx, http.MethodGet, "/v1/balance_transactions", "list_balance_transactions", "", form, &list); err != nil {
		return BalanceTransactionPage{}, err
	}
	page := BalanceTransactionPage{Items: make([]BalanceTransaction, 0, len(list.Data)), HasMore: list.HasMore}
	for _, value := range list.Data {
		if value == nil || !validIdentifier(value.ID, "txn_") || value.Fee < 0 || value.Amount < math.MinInt64+value.Fee || value.Net != value.Amount-value.Fee || value.Created <= 0 || value.AvailableOn <= 0 {
			return BalanceTransactionPage{}, inconsistentError("list_balance_transactions")
		}
		currency := strings.ToUpper(string(value.Currency))
		if !validCurrency(currency) || value.Type == "" || value.Status == "" {
			return BalanceTransactionPage{}, inconsistentError("list_balance_transactions")
		}
		sourceID := ""
		paymentCorrelation := ""
		if value.Source != nil {
			sourceID = value.Source.ID
			if sourceID != "" && !validIdentifier(sourceID, "") {
				return BalanceTransactionPage{}, inconsistentError("list_balance_transactions")
			}
			switch {
			case value.Source.Charge != nil && value.Source.Charge.PaymentIntent != nil:
				paymentCorrelation = value.Source.Charge.PaymentIntent.ID
			case value.Source.Refund != nil && value.Source.Refund.PaymentIntent != nil:
				paymentCorrelation = value.Source.Refund.PaymentIntent.ID
			}
			if paymentCorrelation != "" && !validIdentifier(paymentCorrelation, "pi_") {
				return BalanceTransactionPage{}, inconsistentError("list_balance_transactions")
			}
		}
		page.Items = append(page.Items, BalanceTransaction{
			ID: value.ID, GrossMinor: value.Amount, FeeMinor: value.Fee, NetMinor: value.Net,
			Currency: currency, Type: string(value.Type), ReportingCategory: string(value.ReportingCategory),
			Status: string(value.Status), SourceID: sourceID, PaymentCorrelation: paymentCorrelation,
			CreatedAt: time.Unix(value.Created, 0).UTC(), AvailableAt: time.Unix(value.AvailableOn, 0).UTC(),
		})
	}
	if page.HasMore {
		if len(page.Items) == 0 {
			return BalanceTransactionPage{}, inconsistentError("list_balance_transactions")
		}
		page.NextStartingAfter = page.Items[len(page.Items)-1].ID
	}
	return page, nil
}

func (c *Client) ListPayouts(ctx context.Context, options ListOptions) (PayoutPage, error) {
	form, err := listForm(options)
	if err != nil {
		return PayoutPage{}, validationError("list_payouts")
	}
	form.Set("expand[]", "data.balance_transaction")
	var list sdkstripe.PayoutList
	if _, err := c.doForm(ctx, http.MethodGet, "/v1/payouts", "list_payouts", "", form, &list); err != nil {
		return PayoutPage{}, err
	}
	page := PayoutPage{Items: make([]Payout, 0, len(list.Data)), HasMore: list.HasMore}
	for _, value := range list.Data {
		if value == nil || !validIdentifier(value.ID, "po_") || value.Amount <= 0 || value.Created <= 0 || value.ArrivalDate <= 0 || value.Status == "" {
			return PayoutPage{}, inconsistentError("list_payouts")
		}
		currency := strings.ToUpper(string(value.Currency))
		if !validCurrency(currency) {
			return PayoutPage{}, inconsistentError("list_payouts")
		}
		balanceTransactionID := ""
		if value.BalanceTransaction != nil {
			balanceTransactionID = value.BalanceTransaction.ID
			if !validIdentifier(balanceTransactionID, "txn_") {
				return PayoutPage{}, inconsistentError("list_payouts")
			}
		}
		page.Items = append(page.Items, Payout{
			ID: value.ID, AmountMinor: value.Amount, Currency: currency, Status: string(value.Status),
			BalanceTransactionID: balanceTransactionID, Automatic: value.Automatic,
			CreatedAt: time.Unix(value.Created, 0).UTC(), ArrivalAt: time.Unix(value.ArrivalDate, 0).UTC(),
		})
	}
	if page.HasMore {
		if len(page.Items) == 0 {
			return PayoutPage{}, inconsistentError("list_payouts")
		}
		page.NextStartingAfter = page.Items[len(page.Items)-1].ID
	}
	return page, nil
}

func listForm(options ListOptions) (url.Values, error) {
	if options.Limit < 1 || options.Limit > 100 || (options.StartingAfter != "" && !validIdentifier(options.StartingAfter, "")) {
		return nil, provider.ErrInconsistentFinancialObservation
	}
	form := url.Values{"limit": {strconv.Itoa(options.Limit)}}
	if options.StartingAfter != "" {
		form.Set("starting_after", options.StartingAfter)
	}
	return form, nil
}
