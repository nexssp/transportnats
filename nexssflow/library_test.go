package nexssflow

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/transportnats/tnats"

	"github.com/nexssp/flow/contracts"
	"github.com/nexssp/flow/core"
)

type testActionResolver map[string]action.AnyAction

func (r testActionResolver) Action(name string) (action.AnyAction, bool) {
	act, ok := r[name]
	return act, ok
}

func TestBundleRegistersAndBuildsWithCurrentFlow(t *testing.T) {
	factory, ok := core.Lookup(ID)
	if !ok {
		t.Fatal("transportnats Flow bundle was not registered")
	}

	bundle := factory(nil)
	if bundle.ID != ID {
		t.Fatalf("bundle ID = %q, want %q", bundle.ID, ID)
	}
	if len(bundle.Libraries) != 1 || bundle.Libraries[0].Name != ID {
		t.Fatalf("unexpected Flow libraries: %#v", bundle.Libraries)
	}
	if len(bundle.Modifiers) == 0 {
		t.Fatal("bundle has no NATS modifiers")
	}
}

func TestNATSRequestModifierCreatesRequestBinding(t *testing.T) {
	modifierTable := core.NewModifierTable(Modifiers()...)
	base := action.New("inventory.check", func(_ context.Context, req any) (any, error) {
		return req, nil
	}).Build()

	modified, err := modifierTable.ApplyAll(base, []string{"nats_request=inventory.check"})
	if err != nil {
		t.Fatalf("apply nats_request modifier: %v", err)
	}

	bindings := modified.GetBindings()
	if len(bindings) != 1 {
		t.Fatalf("binding count = %d, want 1", len(bindings))
	}
	binding, ok := bindings[0].(tnats.RequestBinding)
	if !ok {
		t.Fatalf("binding type = %T, want tnats.RequestBinding", bindings[0])
	}
	if binding.Subject != "inventory.check" {
		t.Fatalf("subject = %q, want inventory.check", binding.Subject)
	}
}

func TestListenStartsNATSAndStopsOnContextCancellation(t *testing.T) {
	type echoRequest struct {
		Message string `json:"message"`
	}
	type echoResponse struct {
		Message string `json:"message"`
	}

	endpoint := action.New("quickstart.echo", func(_ context.Context, req echoRequest) (echoResponse, error) {
		return echoResponse(req), nil
	}).Route(tnats.Request("quickstart.echo")).Build()

	transport := tnats.New("", tnats.WithEmbeddedServer(tnats.EmbeddedConfig{
		Port:   -1,
		NoLog:  true,
		NoSigs: true,
	}))

	listenCtx, cancelListen := context.WithCancel(contracts.WithActionResolver(
		context.Background(),
		testActionResolver{"quickstart.echo": endpoint},
	))
	defer cancelListen()

	listenDone := make(chan error, 1)
	go func() {
		_, err := action.InvokeAny(listenCtx, listenAction(transport), map[string]any{
			"endpoints": []any{"quickstart.echo"},
		})
		listenDone <- err
	}()

	readyCtx, cancelReady := context.WithTimeout(context.Background(), 5*time.Second)
	err := transport.WaitReady(readyCtx)
	cancelReady()
	if err != nil {
		cancelListen()
		waitForListener(t, listenDone)
		t.Fatalf("wait for listener readiness: %v", err)
	}

	requestCtx, cancelRequest := context.WithTimeout(context.Background(), time.Second)
	var response echoResponse
	err = transport.Request(requestCtx, "quickstart.echo", echoRequest{Message: "latest Flow"}, &response)
	cancelRequest()
	if err != nil {
		cancelListen()
		waitForListener(t, listenDone)
		t.Fatalf("request through listener: %v", err)
	}
	if response.Message != "latest Flow" {
		t.Fatalf("response message = %q, want %q", response.Message, "latest Flow")
	}

	cancelListen()
	if err := waitForListener(t, listenDone); err != nil {
		t.Fatalf("listener returned an error during shutdown: %v", err)
	}
}

func waitForListener(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for listener shutdown")
		return nil
	}
}

func TestNewBundleRejectsConflictingAuthentication(t *testing.T) {
	testToken := strings.Repeat("x", 32)
	credsFile := filepath.Join(t.TempDir(), "fixture.creds")
	_, err := NewBundle(Config{CredsFile: credsFile, Token: testToken})
	if err == nil {
		t.Fatal("expected credentials file and token to be rejected together")
	}
}

func TestNewBundleProductionValidatesCredentialsAndRootCA(t *testing.T) {
	tempDir := t.TempDir()
	credsFile := filepath.Join(tempDir, "service.creds")
	if err := os.WriteFile(credsFile, []byte("test credentials"), 0o600); err != nil {
		t.Fatalf("write credentials fixture: %v", err)
	}
	rootCAFile := filepath.Join(tempDir, "root-ca.pem")
	if err := os.WriteFile(rootCAFile, testRootCAPEM(t), 0o600); err != nil {
		t.Fatalf("write root CA fixture: %v", err)
	}

	bundle, err := NewBundle(Config{
		URL:        "tls://nats.example.invalid:4222",
		Name:       "production-config-test",
		CredsFile:  credsFile,
		RootCAFile: rootCAFile,
		Production: true,
	})
	if err != nil {
		t.Fatalf("build production Flow bundle: %v", err)
	}
	if bundle.ID != ID || len(bundle.Shutdowns) != 1 {
		t.Fatalf("unexpected production bundle: id=%q shutdowns=%d", bundle.ID, len(bundle.Shutdowns))
	}
}

func TestNewBundleProductionRejectsMissingSecurity(t *testing.T) {
	_, err := NewBundle(Config{URL: "tls://nats.example.invalid:4222", Production: true})
	if err == nil {
		t.Fatal("expected production bundle without credentials/CA to fail validation")
	}
}

func testRootCAPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate root CA key: %v", err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create root CA certificate: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
