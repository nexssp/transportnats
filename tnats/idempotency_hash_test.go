package tnats

import (
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestMsgHashSeparatesSubjectAndPayloadBoundaries(t *testing.T) {
	tests := []struct {
		name  string
		left  *nats.Msg
		right *nats.Msg
	}{
		{
			name:  "small message",
			left:  &nats.Msg{Subject: "a", Data: []byte("bc")},
			right: &nats.Msg{Subject: "ab", Data: []byte("c")},
		},
		{
			name:  "large message",
			left:  &nats.Msg{Subject: "a", Data: []byte(strings.Repeat("x", 512))},
			right: &nats.Msg{Subject: "ax", Data: []byte(strings.Repeat("x", 511))},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got, want := msgHash(test.left), msgHash(test.right); got == want {
				t.Fatalf("ambiguous request partitions produced the same hash %q", got)
			}
		})
	}
}
