package tnats

import (
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// WithUserInfo konfiguruje uwierzytelnianie loginem i hasłem.
func WithUserInfo(username, password string) Option {
	return func(t *Transport) {
		t.options = append(t.options, nats.UserInfo(username, password))
	}
}

// WithToken konfiguruje uwierzytelnianie pojedynczym tokenem dostępowym.
func WithToken(token string) Option {
	return func(t *Transport) {
		t.options = append(t.options, nats.Token(token))
	}
}

// WithNKeySeed konfiguruje uwierzytelnianie NKey na podstawie pliku z seedem.
// Jeśli plik nie istnieje lub jest uszkodzony, błąd rejestrowany jest bezpiecznie w t.initErr.
func WithNKeySeed(seedFile string) Option {
	return func(t *Transport) {
		opt, err := nats.NkeyOptionFromSeed(seedFile)
		if err != nil {
			t.mu.Lock()
			t.initErr = fmt.Errorf("tnats: invalid nkey seed file %q: %w", seedFile, err)
			t.mu.Unlock()

			return
		}

		t.options = append(t.options, opt)
	}
}

// WithNKeySeedFunc pobiera seed programistycznie (np. z KMS / Secret Managera).
func WithNKeySeedFunc(seedFunc func() (string, error)) Option {
	return func(t *Transport) {
		t.options = append(t.options, func(o *nats.Options) error {
			seed, err := seedFunc()
			if err != nil {
				return fmt.Errorf("tnats: nkey seed retrieval failed: %w", err)
			}

			kp, err := nkeys.FromSeed([]byte(seed))
			if err != nil {
				return fmt.Errorf("tnats: decode nkey seed: %w", err)
			}

			pub, err := kp.PublicKey()
			if err != nil {
				return fmt.Errorf("tnats: derive nkey public: %w", err)
			}

			o.Nkey = pub
			o.SignatureCB = func(nonce []byte) ([]byte, error) {
				return kp.Sign(nonce)
			}

			return nil
		})
	}
}

// WithJWT konfiguruje uwierzytelnianie plikiem .creds (zawierającym User JWT i NKey seed).
func WithJWT(credsFile string) Option {
	return func(t *Transport) {
		t.options = append(t.options, nats.UserCredentials(credsFile))
	}
}

// WithTLS konfiguruje certyfikaty mTLS i wymusza szyfrowanie TLS.
func WithTLS(rootCA, clientCert, clientKey string) Option {
	return func(t *Transport) {
		if rootCA != "" {
			t.options = append(t.options, nats.RootCAs(rootCA))
		}

		if clientCert != "" && clientKey != "" {
			t.options = append(t.options, nats.ClientCert(clientCert, clientKey))
		}

		t.options = append(t.options, nats.Secure())
	}
}

// WithURLs agreguje wiele adresów klastra NATS dla wsparcia automatycznego failoveru.
func WithURLs(primary string, extra ...string) Option {
	return func(t *Transport) {
		urls := append([]string{primary}, extra...)

		t.mu.Lock()
		t.url = joinNATSURLs(urls)
		t.mu.Unlock()
	}
}

func joinNATSURLs(urls []string) string {
	if len(urls) == 0 {
		return ""
	}

	out := urls[0]
	for _, u := range urls[1:] {
		out += "," + u
	}

	return out
}
