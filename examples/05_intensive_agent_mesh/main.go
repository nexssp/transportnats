package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/transportnats/tnats"
)

type Submit struct {
	JobID string `json:"job_id"`
	Agent string `json:"agent"`
	Input string `json:"input"`
}

type Command struct {
	JobID string `json:"job_id"`
	Agent string `json:"agent"`
	Input string `json:"input"`
}

type Task struct {
	JobID string `json:"job_id"`
	Agent string `json:"agent"`
	Step  int    `json:"step"`
}

type Result struct {
	JobID string `json:"job_id"`
	Agent string `json:"agent"`
	Step  int    `json:"step"`
	Value string `json:"value"`
}

type Audit struct {
	JobID string    `json:"job_id"`
	Agent string    `json:"agent"`
	At    time.Time `json:"at"`
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	storeDir, err := os.MkdirTemp("", "nats_agent_mesh_*")
	if err != nil {
		logger.Error("store directory", "error", err)

		return
	}
	defer os.RemoveAll(storeDir)

	broker := tnats.New("", tnats.WithEmbeddedServer(tnats.EmbeddedConfig{
		Port: 0, JetStream: true, StoreDir: storeDir, NoLog: true, NoSigs: true,
	}))
	go runTransport(ctx, broker, "broker", logger)

	if err := broker.WaitReady(ctx); err != nil {
		logger.Error("broker ready", "error", err)

		return
	}

	url := broker.Conn().ConnectedUrl()
	auditBinding := tnats.DurableWork("AGENT_AUDIT", "mesh.audit", "audit-v1", "mesh.audit.dlq").
		WithDeliveryPolicy(5, 10*time.Second, 100*time.Millisecond, 128)

	var completed atomic.Int32

	gateway := tnats.New(url)
	planner := tnats.New(url)
	scheduler := tnats.New(url)
	workerA := tnats.New(url)
	workerB := tnats.New(url)
	auditor := tnats.New(url)
	collector := tnats.New(url)

	gateway.Mount([]action.AnyAction{action.New("mesh.gateway", func(ctx context.Context, req Submit) (string, error) {
		if err := planner.Publish(
			ctx, "mesh.command", Command(req),
		); err != nil {
			return "", err
		}

		return "accepted", nil
	}).Idempotent().Route(tnats.Request("mesh.submit", 2*time.Second)).Build()})

	planner.Mount([]action.AnyAction{action.New("mesh.planner", func(ctx context.Context, cmd Command) (string, error) {
		var accepted bool
		if err := scheduler.Request(ctx, "mesh.scheduler.validate", cmd, &accepted); err != nil {
			return "", err
		}

		if !accepted {
			return "", errors.New("scheduler rejected command")
		}

		return "planned", planner.Publish(ctx, "mesh.task.assigned", Task{JobID: cmd.JobID, Agent: cmd.Agent, Step: 1})
	}).Route(tnats.Topic("mesh.command", "planner-workers")).Build()})

	scheduler.Mount([]action.AnyAction{action.New(
		"mesh.scheduler", func(ctx context.Context, cmd Command) (bool, error) {
			return cmd.JobID != "" && cmd.Agent != "", nil
		}).Route(tnats.Request("mesh.scheduler.validate", 2*time.Second)).Build()})

	workerAction := func(worker string) action.AnyAction {
		return action.New("mesh.worker."+worker, func(ctx context.Context, task Task) (string, error) {
			result := Result{JobID: task.JobID, Agent: task.Agent, Step: task.Step, Value: worker + " completed"}
			if err := collector.Publish(ctx, "mesh.result", result); err != nil {
				return "", err
			}

			if err := auditor.PublishDurable(
				ctx, auditBinding,
				Audit{JobID: task.JobID, Agent: task.Agent, At: time.Now().UTC()},
			); err != nil {
				return "", err
			}

			return result.Value, nil
		}).Route(tnats.Topic("mesh.task.assigned", "worker-pool")).
			Route(tnats.Request("mesh.worker."+worker+".health", 2*time.Second)).Build()
	}
	workerA.Mount([]action.AnyAction{workerAction("worker-a")})
	workerB.Mount([]action.AnyAction{workerAction("worker-b")})

	auditor.Mount([]action.AnyAction{action.New("mesh.audit", func(ctx context.Context, event Audit) (string, error) {
		logger.Info("audit event", "job_id", event.JobID, "agent", event.Agent)

		return "audited", nil
	}).Route(auditBinding).Build()})

	collector.Mount([]action.AnyAction{action.New(
		"mesh.collector", func(ctx context.Context, result Result) (string, error) {
			count := completed.Add(1)
			logger.Info("result collected", "job_id", result.JobID, "worker", result.Value, "completed", count)

			return "collected", nil
		}).Route(tnats.Topic("mesh.result", "result-workers")).Build()})

	services := []*tnats.Transport{gateway, planner, scheduler, workerA, workerB, auditor, collector}
	for i, service := range services {
		go runTransport(ctx, service, fmt.Sprintf("service-%d", i), logger)
	}

	for _, service := range services {
		if err := service.WaitReady(ctx); err != nil {
			logger.Error("service ready", "error", err)

			return
		}
	}

	var response string

	for i := 1; i <= 100; i++ {
		jobID := fmt.Sprintf("job-%04d", i)

		traceCtx := action.WithTraceContext(ctx, "trace-"+jobID, "span-gateway")
		if err := gateway.Request(
			traceCtx, "mesh.submit",
			Submit{JobID: jobID, Agent: "research-agent", Input: "summarize"}, &response,
		); err != nil {
			logger.Error("submit failed", "job_id", jobID, "error", err)
		}
	}

	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()

	for completed.Load() < 100 {
		select {
		case <-deadline.C:
			logger.Error("mesh completion timeout", "completed", completed.Load(), "expected", 100)
			stop()

			return
		case <-time.After(25 * time.Millisecond):
		}
	}

	logger.Info("intensive agent mesh completed", "jobs", completed.Load(), "services", len(services))
	stop()
}

func runTransport(ctx context.Context, tr *tnats.Transport, name string, logger *slog.Logger) {
	if _, err := tr.Do(ctx, nil); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("transport stopped", "name", name, "error", err)
	}
}
