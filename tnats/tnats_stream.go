package tnats

import (
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/xerr"
)

func (t *Transport) ensureJetStream() (nats.JetStreamContext, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.js != nil {
		return t.js, nil
	}

	if t.conn == nil {
		return nil, xerr.Unavailable("nats: connection not ready")
	}

	js, err := t.conn.JetStream()
	if err != nil {
		return nil, MapError(err)
	}

	t.js = js

	return js, nil
}

func (t *Transport) ensureDurableInfra(js nats.JetStreamContext, b DurableBinding) error {
	if err := t.ensureStream(js, b.Stream, b.Subject); err != nil {
		return err
	}

	return t.ensureStream(js, b.Stream+"_DLQ", b.DeadLetterSubject)
}

func (t *Transport) ensureStream(js nats.JetStreamContext, name string, subjects ...string) error {
	if len(subjects) == 0 {
		return nil
	}
	t.streamConfigMu.Lock()
	defer t.streamConfigMu.Unlock()

	for range 3 {
		info, err := js.StreamInfo(name)
		if errors.Is(err, nats.ErrStreamNotFound) {
			cfg := &nats.StreamConfig{
				Name:      name,
				Subjects:  subjects,
				Storage:   nats.FileStorage,
				Retention: nats.LimitsPolicy,
				Discard:   nats.DiscardOld,
				MaxAge:    7 * 24 * time.Hour,
			}
			if t.streamConfigModifier != nil {
				t.streamConfigModifier(cfg)
			}

			_, addErr := js.AddStream(cfg)
			if addErr == nil {
				return nil
			}
			if errors.Is(addErr, nats.ErrStreamNameAlreadyInUse) {
				continue
			}
			return MapError(addErr)
		}
		if err != nil {
			return MapError(err)
		}

		updated := false
		for _, subject := range subjects {
			found := false
			for _, configured := range info.Config.Subjects {
				if configured == subject || subjectPatternCovers(configured, subject) {
					found = true
					break
				}
			}
			if !found {
				info.Config.Subjects = append(info.Config.Subjects, subject)
				updated = true
			}
		}
		if !updated {
			return nil
		}
		if _, updateErr := js.UpdateStream(&info.Config); updateErr != nil {
			return MapError(updateErr)
		}
	}

	return xerr.Conflict("nats: stream configuration changed repeatedly during provisioning")
}

func subjectPatternCovers(pattern, subject string) bool {
	if pattern == subject {
		return true
	}

	patternStart, subjectStart := 0, 0
	for {
		patternEnd := patternStart
		for patternEnd < len(pattern) && pattern[patternEnd] != '.' {
			patternEnd++
		}

		subjectEnd := subjectStart
		for subjectEnd < len(subject) && subject[subjectEnd] != '.' {
			subjectEnd++
		}

		patternToken := pattern[patternStart:patternEnd]
		if patternToken == ">" {
			return subjectStart < len(subject)
		}

		if subjectStart >= len(subject) || (patternToken != "*" && patternToken != subject[subjectStart:subjectEnd]) {
			return false
		}

		if patternEnd == len(pattern) || subjectEnd == len(subject) {
			return patternEnd == len(pattern) && subjectEnd == len(subject)
		}

		patternStart = patternEnd + 1
		subjectStart = subjectEnd + 1
	}
}
