package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nexssp/transportnats/nexssflow"

	"github.com/nexssp/flow/native"
	"github.com/nexssp/flow/runner"
)

//go:embed service.nflow
var flowSource embed.FS

const (
	defaultServiceName = "nexss-flow-catalog-quote"
	shutdownTimeout    = 15 * time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "flow-nats-service:", err)
		os.Exit(1)
	}
}

func run() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	config, err := loadConfig(os.LookupEnv)
	if err != nil {
		return err
	}

	transportBundle, err := nexssflow.NewBundle(config.flowConfig())
	if err != nil {
		return fmt.Errorf("configure Flow NATS bundle: %w", err)
	}

	bundles := append(native.Bundles(), transportBundle)
	flowConfig, err := runner.BuildConfig(bundles)
	if err != nil {
		return fmt.Errorf("build Flow runtime: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	host := runner.NewHost(runner.WithShutdownTimeout(shutdownTimeout))
	if ownErr := host.Own(transportBundle); ownErr != nil {
		return fmt.Errorf("assign Flow resource lifecycle: %w", ownErr)
	}

	source, err := flowSource.ReadFile("service.nflow")
	if err != nil {
		return fmt.Errorf("read embedded Flow source: %w", err)
	}

	logger.Info("starting Flow NATS service", "service", config.Name)
	err = host.Run(ctx, func(runCtx context.Context) error {
		_, runErr := runner.Execute(runCtx, flowConfig, string(source), "service.nflow", nil)
		if errors.Is(runErr, context.Canceled) {
			return nil
		}
		return runErr
	})
	if err != nil {
		return fmt.Errorf("run Flow service: %w", err)
	}
	logger.Info("Flow NATS service stopped", "service", config.Name)
	return nil
}

type serviceConfig struct {
	URL            string
	Name           string
	CredsFile      string
	RootCAFile     string
	ClientCertFile string
	ClientKeyFile  string
}

func loadConfig(lookup func(string) (string, bool)) (serviceConfig, error) {
	readRequired := func(name string) (string, error) {
		value, ok := lookup(name)
		if !ok || strings.TrimSpace(value) == "" {
			return "", fmt.Errorf("required environment variable %s is not set", name)
		}
		return strings.TrimSpace(value), nil
	}

	url, err := readRequired("NATS_URL")
	if err != nil {
		return serviceConfig{}, err
	}
	credsFile, err := readRequired("NATS_CREDS_FILE")
	if err != nil {
		return serviceConfig{}, err
	}
	rootCAFile, err := readRequired("NATS_ROOT_CA_FILE")
	if err != nil {
		return serviceConfig{}, err
	}

	name := defaultServiceName
	if value, ok := lookup("NATS_NAME"); ok && strings.TrimSpace(value) != "" {
		name = strings.TrimSpace(value)
	}
	clientCertFile, _ := lookup("NATS_CLIENT_CERT_FILE")
	clientKeyFile, _ := lookup("NATS_CLIENT_KEY_FILE")

	return serviceConfig{
		URL:            url,
		Name:           name,
		CredsFile:      credsFile,
		RootCAFile:     rootCAFile,
		ClientCertFile: strings.TrimSpace(clientCertFile),
		ClientKeyFile:  strings.TrimSpace(clientKeyFile),
	}, nil
}

func (c serviceConfig) flowConfig() nexssflow.Config {
	return nexssflow.Config{
		URL:            c.URL,
		Name:           c.Name,
		Timeout:        5 * time.Second,
		CredsFile:      c.CredsFile,
		RootCAFile:     c.RootCAFile,
		ClientCertFile: c.ClientCertFile,
		ClientKeyFile:  c.ClientKeyFile,
		Production:     true,
	}
}
