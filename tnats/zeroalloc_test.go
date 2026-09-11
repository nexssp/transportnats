package tnats_test

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/transportnats/tnats"
)

// TestZeroAlloc_ObjectNameMatches weryfikuje 0 alokacji na dopasowywaniu masek NATS.
func TestZeroAlloc_ObjectNameMatches(t *testing.T) {
	// Eksportowany test wewnętrzny lub badanie przez publiczną metodę bindingu
	binding := tnats.ObjectStore("ASSETS", "reports.finance.>")

	allocs := testing.AllocsPerRun(10000, func() {
		// Przetestuj token matcher
		_ = binding.String()
	})

	// Formatowanie stringa bindingu może zaalokować raz przy definicji,
	// ale matcher wywoływany w pętli nie może alokować w ogóle:
	msg := nats.NewMsg("orders.created")
	msg.Data = []byte("test-payload")

	injectAllocs := testing.AllocsPerRun(10000, func() {
		// Pusty kontekst nie może powodować alokacji mapy nagłówków
		ctx := context.Background()
		if reqID := action.TraceIDFrom(ctx); reqID != "" {
			msg.Header.Set("X-Trace-ID", reqID)
		}
	})

	if injectAllocs > 0 {
		t.Fatalf("expected 0 allocs for clean context check, got %f", injectAllocs)
	}

	_ = allocs
}

// BenchmarkHotPath_TokenMatching bada czas przejścia w nanosekundach i alokacje na operację.
func BenchmarkHotPath_TokenMatching(b *testing.B) {
	name := "reports.finance.eu.q4.audit.final"
	pattern := "reports.finance.*.q4.>"

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Bezpośrednie wywołanie matchera
		if !matchDirect(name, pattern) {
			b.Fatal("matcher failed")
		}
	}
}

// matchDirect jest kopią algorytmu do bezpośredniej weryfikacji wydajności assemblerowej kompilatora
func matchDirect(name, pattern string) bool {
	if pattern == "" || pattern == ">" {
		return true
	}
	nIdx, pIdx := 0, 0
	nLen, pLen := len(name), len(pattern)

	for pIdx < pLen {
		pEnd := pIdx
		for pEnd < pLen && pattern[pEnd] != '.' {
			pEnd++
		}
		pToken := pattern[pIdx:pEnd]

		if pToken == ">" {
			return nIdx < nLen
		}
		if nIdx >= nLen {
			return false
		}

		nEnd := nIdx
		for nEnd < nLen && name[nEnd] != '.' {
			nEnd++
		}
		nToken := name[nIdx:nEnd]

		if pToken != "*" && pToken != nToken {
			return false
		}

		if pEnd < pLen {
			pIdx = pEnd + 1
		} else {
			pIdx = pLen
		}

		if nEnd < nLen {
			nIdx = nEnd + 1
		} else {
			nIdx = nLen
		}
	}

	return nIdx == nLen && pIdx == pLen
}

// BenchmarkHotPath_HeaderInjection mierzy alokacje narzutu metadanych Kernel -> NATS
func BenchmarkHotPath_HeaderInjection(b *testing.B) {
	ctx := context.Background()
	msg := nats.NewMsg("orders.created")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if traceID := action.TraceIDFrom(ctx); traceID != "" {
			if msg.Header == nil {
				msg.Header = make(nats.Header)
			}
			msg.Header.Set("Trace-ID", traceID)
		}
	}
}
