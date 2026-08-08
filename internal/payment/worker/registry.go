package worker

import "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"

type Providers map[string]provider.Client

func (providers Providers) Provider(name string) (provider.Client, bool) {
	client, ok := providers[name]
	return client, ok && client != nil
}
