package tnats_test

import (
	"context"
	"testing"

	"github.com/nexssp/transportnats/tnats"
)

func TestPublishDurableConcurrentStreamSubjectsAreMerged(t *testing.T) {
	t.Parallel()

	_, natsURL := runEmbeddedNATS(t)
	transport := tnats.New(natsURL)
	defer transport.Close()

	bindings := []tnats.DurableBinding{
		tnats.DurableWork("RACE_EVENTS", "events.first", "worker_first", "events.dead.first"),
		tnats.DurableWork("RACE_EVENTS", "events.second", "worker_second", "events.dead.second"),
	}
	start := make(chan struct{})
	errs := make(chan error, len(bindings))
	for _, binding := range bindings {
		go func() {
			<-start
			errs <- transport.PublishDurable(context.Background(), binding, map[string]string{"ok": "yes"})
		}()
	}
	close(start)
	for range bindings {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent durable publish: %v", err)
		}
	}

	info, err := transport.JetStream().StreamInfo("RACE_EVENTS")
	if err != nil {
		t.Fatalf("read stream info: %v", err)
	}
	configured := make(map[string]bool, len(info.Config.Subjects))
	for _, subject := range info.Config.Subjects {
		configured[subject] = true
	}
	for _, subject := range []string{"events.first", "events.second"} {
		if !configured[subject] {
			t.Errorf("stream is missing concurrently requested subject %q: %v", subject, info.Config.Subjects)
		}
	}

	dlqInfo, err := transport.JetStream().StreamInfo("RACE_EVENTS_DLQ")
	if err != nil {
		t.Fatalf("read DLQ stream info: %v", err)
	}
	configured = make(map[string]bool, len(dlqInfo.Config.Subjects))
	for _, subject := range dlqInfo.Config.Subjects {
		configured[subject] = true
	}
	for _, subject := range []string{"events.dead.first", "events.dead.second"} {
		if !configured[subject] {
			t.Errorf("DLQ stream is missing concurrently requested subject %q: %v", subject, dlqInfo.Config.Subjects)
		}
	}
}

func TestPublishDurableConcurrentSubjectsOnExistingStreamAreMerged(t *testing.T) {
	t.Parallel()
	_, natsURL := runEmbeddedNATS(t)
	transport := tnats.New(natsURL)
	defer transport.Close()

	initial := tnats.DurableWork("RACE_EXISTING", "events.existing", "worker_existing", "events.dead.existing")
	if err := transport.PublishDurable(context.Background(), initial, map[string]string{"ok": "initial"}); err != nil {
		t.Fatalf("create initial stream configuration: %v", err)
	}

	bindings := []tnats.DurableBinding{
		tnats.DurableWork("RACE_EXISTING", "events.first", "worker_first", "events.dead.first"),
		tnats.DurableWork("RACE_EXISTING", "events.second", "worker_second", "events.dead.second"),
	}
	start := make(chan struct{})
	errs := make(chan error, len(bindings))
	for _, binding := range bindings {
		go func() {
			<-start
			errs <- transport.PublishDurable(context.Background(), binding, map[string]string{"ok": "yes"})
		}()
	}
	close(start)
	for range bindings {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent durable publish: %v", err)
		}
	}

	assertStreamHasSubjects(t, transport, "RACE_EXISTING", "events.existing", "events.first", "events.second")
	assertStreamHasSubjects(t, transport, "RACE_EXISTING_DLQ", "events.dead.existing", "events.dead.first", "events.dead.second")
}

func assertStreamHasSubjects(t *testing.T, transport *tnats.Transport, name string, subjects ...string) {
	t.Helper()
	info, err := transport.JetStream().StreamInfo(name)
	if err != nil {
		t.Fatalf("read stream %q: %v", name, err)
	}
	configured := make(map[string]struct{}, len(info.Config.Subjects))
	for _, subject := range info.Config.Subjects {
		configured[subject] = struct{}{}
	}
	for _, subject := range subjects {
		if _, ok := configured[subject]; !ok {
			t.Errorf("stream %q is missing subject %q: %v", name, subject, info.Config.Subjects)
		}
	}
}
