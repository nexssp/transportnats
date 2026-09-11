package tnats

import (
	"net"
	"os"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nexssp/kernel/xerr"
)

// EmbeddedConfig konfiguruje wbudowany in-process serwer NATS (np. dla testów CI/CD lub wdrożeń sidecar).
type EmbeddedConfig struct {
	Host           string
	Port           int
	JetStream      bool
	StoreDir       string
	ServerName     string
	MaxPayload     int32
	MaxConnections int
	Username       string
	Password       string
	Token          string
	Debug          bool
	Trace          bool
	NoLog          bool
	NoSigs         bool
	StartTimeout   time.Duration
	Options        func(*natsserver.Options)
}

func (c *EmbeddedConfig) applyDefaults() {
	if c.Host == "" {
		c.Host = "127.0.0.1"
	}

	if c.ServerName == "" {
		c.ServerName = "nexss-embedded"
	}

	if c.StartTimeout <= 0 {
		c.StartTimeout = 5 * time.Second
	}

	if c.MaxPayload == 0 {
		c.MaxPayload = 8 * 1024 * 1024
	}
}

// WithEmbeddedServer uruchamia in-process serwer NATS na czas trwania Transport.Do().
func WithEmbeddedServer(cfg EmbeddedConfig) Option {
	return func(t *Transport) {
		cp := cfg
		cp.applyDefaults()
		t.embedded = &cp
	}
}

func (t *Transport) startEmbedded() error {
	cfg := t.embedded
	if cfg == nil {
		return nil
	}

	if cfg.JetStream && cfg.StoreDir == "" {
		return xerr.BadRequest("tnats: embedded JetStream requires StoreDir")
	}

	if cfg.JetStream {
		if err := os.MkdirAll(cfg.StoreDir, 0o750); err != nil {
			return xerr.Internal("tnats: cannot create jetstream store dir", err)
		}
	}

	opts := &natsserver.Options{
		Host:          cfg.Host,
		Port:          cfg.Port,
		ServerName:    cfg.ServerName,
		JetStream:     cfg.JetStream,
		StoreDir:      cfg.StoreDir,
		MaxPayload:    cfg.MaxPayload,
		MaxConn:       cfg.MaxConnections,
		Username:      cfg.Username,
		Password:      cfg.Password,
		Authorization: cfg.Token,
		Debug:         cfg.Debug,
		Trace:         cfg.Trace,
		NoLog:         cfg.NoLog,
		NoSigs:        cfg.NoSigs,
	}
	if cfg.Options != nil {
		cfg.Options(opts)
	}

	srv, err := natsserver.NewServer(opts)
	if err != nil {
		return xerr.Internal("tnats: embedded server construction failed", err)
	}

	srv.Start()

	if !srv.ReadyForConnections(cfg.StartTimeout) {
		srv.Shutdown()
		srv.WaitForShutdown()

		return xerr.Unavailable("tnats: embedded server startup timed out")
	}

	clientURL := srv.ClientURL()

	t.mu.Lock()
	t.embeddedSrv = srv
	t.url = clientURL
	t.mu.Unlock()

	addr, _ := srv.Addr().(*net.TCPAddr)
	t.log.Info("nats_embedded_started",
		"url", clientURL,
		"host", cfg.Host,
		"port", addrPort(addr),
		"jetstream", cfg.JetStream,
	)

	return nil
}

func (t *Transport) stopEmbedded() {
	t.mu.Lock()
	srv := t.embeddedSrv
	t.embeddedSrv = nil
	t.mu.Unlock()

	if srv == nil {
		return
	}

	srv.Shutdown()
	srv.WaitForShutdown()
	t.log.Info("nats_embedded_stopped")
}

func addrPort(a *net.TCPAddr) int {
	if a == nil {
		return 0
	}

	return a.Port
}
