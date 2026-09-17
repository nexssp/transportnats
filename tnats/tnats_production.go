package tnats

import (
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
)

func (t *Transport) ValidateProduction() error {
	t.mu.RLock()
	u := t.url
	t.mu.RUnlock()

	if u == "" {
		return errors.New("NATS URL is required in production")
	}

	if !strings.HasPrefix(u, "tls://") && !strings.HasPrefix(u, "nats://") {
		return errors.New("NATS URL must use nats:// or tls:// scheme")
	}

	if t.productionSecurity == nil && t.productionValidator == nil {
		return errors.New(
			"NATS production security is required; configure " +
				"tnats.WithProductionSecurity or tnats.WithProductionValidation",
		)
	}

	if t.productionSecurity == nil {
		return t.productionValidator()
	}

	if t.productionSecurity.CredentialsFile == "" {
		return errors.New("NATS credentials file is required in production")
	}

	if t.productionSecurity.RootCAFile == "" {
		return errors.New("NATS root CA file is required in production")
	}

	if err := validateProductionFiles(t.productionSecurity); err != nil {
		return err
	}

	if err := validateRootCA(t.productionSecurity.RootCAFile); err != nil {
		return err
	}

	if t.productionValidator != nil {
		return t.productionValidator()
	}

	return nil
}

func validateProductionFiles(sec *ProductionSecurity) error {
	if err := validateFile("credentials", sec.CredentialsFile); err != nil {
		return err
	}

	if err := validateFile("root CA", sec.RootCAFile); err != nil {
		return err
	}

	if sec.ClientCertFile != "" || sec.ClientKeyFile != "" {
		if err := validateFile("client cert", sec.ClientCertFile); err != nil {
			return err
		}

		if err := validateFile("client key", sec.ClientKeyFile); err != nil {
			return err
		}
	}

	return nil
}

func validateRootCA(path string) error {
	rootPEM, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read NATS root CA file: %w", err)
	}

	if !x509.NewCertPool().AppendCertsFromPEM(rootPEM) {
		return fmt.Errorf("parse NATS root CA file %q", path)
	}

	return nil
}

func validateFile(label, path string) error {
	if path == "" {
		return fmt.Errorf("NATS %s path is required", label)
	}

	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("NATS %s file: %w", label, err)
	}

	if info.IsDir() {
		return fmt.Errorf("NATS %s file %q is a directory", label, path)
	}

	return nil
}
