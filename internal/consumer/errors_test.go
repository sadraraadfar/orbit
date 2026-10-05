package consumer

import (
	"errors"
	"fmt"
	"testing"
)

func TestClassifyDefaultsToTransient(t *testing.T) {
	if got := Classify(errors.New("boom")); got != ClassTransient {
		t.Fatalf("unknown error classified as %s, want transient", got)
	}
}

func TestClassifyExplicitClasses(t *testing.T) {
	cases := []struct {
		err  error
		want Class
	}{
		{Transient(errors.New("db down")), ClassTransient},
		{Permanent(errors.New("bad state")), ClassPermanent},
		{Invalid(errors.New("bad json")), ClassInvalid},
	}
	for _, tc := range cases {
		if got := Classify(tc.err); got != tc.want {
			t.Errorf("Classify = %s, want %s", got, tc.want)
		}
	}
}

func TestClassifyUnwraps(t *testing.T) {
	wrapped := fmt.Errorf("context: %w", Permanent(errors.New("root")))
	if got := Classify(wrapped); got != ClassPermanent {
		t.Fatalf("wrapped error classified as %s, want permanent", got)
	}
}
