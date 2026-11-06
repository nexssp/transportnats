package tnats

import "testing"

func TestSubjectPatternCovers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pattern string
		subject string
		want    bool
	}{
		{pattern: ">", subject: "events.created", want: true},
		{pattern: "events.>", subject: "events.created", want: true},
		{pattern: "events.>", subject: "events.created.v1", want: true},
		{pattern: "events.>", subject: "events", want: false},
		{pattern: ">", subject: "", want: false},
		{pattern: "events.*", subject: "events.created", want: true},
		{pattern: "events.*", subject: "events.created.v1", want: false},
		{pattern: "events.*", subject: "events", want: false},
		{pattern: "events.created", subject: "events.created", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+" covers "+tt.subject, func(t *testing.T) {
			t.Parallel()
			if got := subjectPatternCovers(tt.pattern, tt.subject); got != tt.want {
				t.Fatalf("subjectPatternCovers(%q, %q) = %t, want %t", tt.pattern, tt.subject, got, tt.want)
			}
		})
	}
}
