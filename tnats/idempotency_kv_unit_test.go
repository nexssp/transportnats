package tnats

import (
	"errors"
	"testing"
)

func TestDecodeKVClaimCorruptionSentinel(t *testing.T) {
	for _, test := range []struct {
		name string
		data string
	}{
		{name: "malformed JSON", data: "{"},
		{name: "missing token", data: `{"exp":1}`},
		{name: "missing expiry", data: `{"token":"claim"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeKVClaim([]byte(test.data)); !errors.Is(err, errCorruptIdempotencyLedger) {
				t.Fatalf("decodeKVClaim error = %v, want corruption sentinel", err)
			}
		})
	}

	claim, err := decodeKVClaim([]byte(`{"token":"claim","req_hash":"hash","exp":1}`))
	if err != nil || claim.Token != "claim" || claim.RequestHash != "hash" || claim.ExpiresAt != 1 {
		t.Fatalf("decoded claim = %#v, error = %v", claim, err)
	}
}
