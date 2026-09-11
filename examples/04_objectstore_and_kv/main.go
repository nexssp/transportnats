package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/transportnats/tnats"
)

type ConfigPayload struct {
	MaxWorkers int    `json:"max_workers"`
	LogLevel   string `json:"log_level"`
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	storeDir, _ := os.MkdirTemp("", "nats_obj_kv_*")
	defer os.RemoveAll(storeDir)

	// 1. Boot the broker first so infrastructure exists before actions mount.
	srv, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: storeDir,
		NoLog: true, NoSigs: true,
	})
	if err != nil {
		logger.Error("embedded broker construction failed", "error", err)

		return
	}

	srv.Start()

	if !srv.ReadyForConnections(5 * time.Second) {
		logger.Error("embedded broker startup timed out")

		return
	}

	defer func() { srv.Shutdown(); srv.WaitForShutdown() }()

	bootstrapNC, err := nats.Connect(srv.ClientURL())
	if err != nil {
		logger.Error("embedded broker connection failed", "error", err)

		return
	}

	bootstrapJS, err := bootstrapNC.JetStream()
	if err != nil {
		bootstrapNC.Close()
		logger.Error("jetstream initialization failed", "error", err)

		return
	}

	setupBuckets(bootstrapJS, logger)
	bootstrapNC.Close()

	tr := tnats.New(srv.ClientURL())

	// 2. Action A: Dynamic Config Watcher (JetStream KV)
	configAction := action.New("config.watch", func(ctx context.Context, cfg ConfigPayload) (string, error) {
		logger.Info("[ConfigWatcher] Dynamic configuration updated",
			"max_workers", cfg.MaxWorkers,
			"log_level", cfg.LogLevel,
		)

		return "APPLIED", nil
	}).
		Route(tnats.KV("DYNAMIC_CONFIGS", "runtime.settings")).
		Build()

	// 3. Action B: ObjectStore Blob Ingestion Watcher (Large Files)
	objectAction := action.New("objects.ingest", func(ctx context.Context, ev tnats.ObjectEvent) (string, error) {
		logger.Info("[ObjectStoreWatcher] Object mutation detected",
			"bucket", ev.Bucket,
			"name", ev.Name,
			"type", ev.Type.String(),
			"size_bytes", ev.Size,
		)

		if ev.Type == tnats.ObjectPut && len(ev.Data) > 0 {
			logger.Info(
				"[ObjectStoreWatcher] Processed embedded payload",
				"preview", string(ev.Data[:previewLen(len(ev.Data), 32)]),
			)
		}

		return "INGESTED", nil
	}).
		Route(tnats.ObjectStore("ASSETS_BUCKET", "reports.>").WithData(10 * 1024 * 1024)).
		Build()

	tr.Mount([]action.AnyAction{configAction, objectAction})

	go func() {
		if _, transportErr := tr.Do(ctx, nil); transportErr != nil && !errors.Is(transportErr, context.Canceled) {
			logger.Error("Transport error", "error", transportErr)
		}
	}()

	if readyErr := tr.WaitReady(ctx); readyErr != nil {
		logger.Error("WaitReady timeout", "error", readyErr)

		return
	}

	js := tr.JetStream()

	logger.Info(">>> System operational. Mutating KV config and uploading ObjectStore blobs...")

	// 4. Update KV configuration dynamically
	kv, err := js.KeyValue("DYNAMIC_CONFIGS")
	if err == nil {
		time.Sleep(100 * time.Millisecond)

		_, _ = kv.Put("runtime.settings", []byte(`{"max_workers": 64, "log_level": "DEBUG"}`))
	}

	// 5. Upload binary blob to ObjectStore
	time.Sleep(100 * time.Millisecond)

	largeData := bytes.Repeat([]byte("AUDIT_LOG_ROW_DATA\n"), 1000) // ~19 KB

	info, putErr := tr.PutObject(ctx, "ASSETS_BUCKET", "reports.finance.audit_2026.csv", largeData)
	if putErr != nil {
		logger.Error("PutObject error", "error", putErr)
	} else {
		logger.Info("Object uploaded successfully", "name", info.Name, "size", info.Size)
	}

	// Read it back verified
	data, getErr := tr.GetObject(ctx, "ASSETS_BUCKET", "reports.finance.audit_2026.csv")
	if getErr != nil || len(data) != len(largeData) {
		logger.Error("GetObject mismatch", "error", getErr)
	} else {
		logger.Info("GetObject verified successfully", "bytes_read", len(data))
	}

	// Delete object
	time.Sleep(100 * time.Millisecond)

	if err := tr.DeleteObject(ctx, "ASSETS_BUCKET", "reports.finance.audit_2026.csv"); err != nil {
		logger.Error("DeleteObject error", "error", err)
	}

	time.Sleep(300 * time.Millisecond)
	logger.Info("Shutdown demonstration completed cleanly.")
}

func setupBuckets(js nats.JetStreamContext, log *slog.Logger) {
	_, _ = js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:  "DYNAMIC_CONFIGS",
		Storage: nats.MemoryStorage,
	})
	_, _ = js.CreateObjectStore(&nats.ObjectStoreConfig{
		Bucket:  "ASSETS_BUCKET",
		Storage: nats.MemoryStorage,
	})
}

func previewLen(a, b int) int {
	if a < b {
		return a
	}

	return b
}
