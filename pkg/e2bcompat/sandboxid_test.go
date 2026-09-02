package e2bcompat

import "testing"

func TestPublicIDIsDNSLabelSafe(t *testing.T) {
	for _, tt := range []struct {
		name string
		in   string
		want string
	}{
		{"sandboxd claim id", "sb_0123456789abcdef", "sb-0123456789abcdef"},
		{"already safe is untouched", "sb-0123456789abcdef", "sb-0123456789abcdef"},
		{"uppercase is lowered", "SB_ABCDEF", "sb-abcdef"},
		{"empty stays empty", "", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := publicID(tt.in)
			if got != tt.want {
				t.Fatalf("publicID(%q) = %q, want %q", tt.in, got, tt.want)
			}
			for _, r := range got {
				if !isDNSSafe(r) {
					t.Errorf("publicID(%q) = %q contains %q, illegal in a DNS label", tt.in, got, r)
				}
			}
		})
	}
}

func TestMatchesIDAcceptsBothForms(t *testing.T) {
	const claim = "sb_0123456789abcdef"

	if !matchesID(claim, claim) {
		t.Error("raw claim id must match itself")
	}
	if !matchesID(claim, "sb-0123456789abcdef") {
		t.Error("published DNS-safe id must match its claim id")
	}
	if matchesID(claim, "sb-ffffffffffffffff") {
		t.Error("a different id must not match")
	}
}

func TestMatchesIDRejectsEmpty(t *testing.T) {
	if matchesID("", "") {
		t.Error("empty must not match empty")
	}
	if matchesID("sb_0123456789abcdef", "") {
		t.Error("empty request must not match a real id")
	}
	if matchesID("", "sb-0123456789abcdef") {
		t.Error("a sandbox with no claim id must not match a real request")
	}
}
