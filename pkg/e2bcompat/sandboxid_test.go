package e2bcompat

import "testing"

// TestPublicIDIsDNSLabelSafe is the reason this mapping exists: the e2b SDK
// builds the in-sandbox envd host as "{port}-{sandboxID}.{domain}". A sandboxd
// claim id is "sb_"+hex, and that underscore is illegal in a DNS label — an id
// published raw yields a host that cannot resolve, so the sandbox is created
// but unreachable.
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

// TestMatchesIDAcceptsBothForms: an id observed through either surface has to
// keep working. A client that saw the published (DNS-safe) id sends that back;
// something reading the raw claim id from the node sends the original.
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

// TestMatchesIDRejectsEmpty guards the lookup loop: a sandbox whose node has
// not published a claim id has an empty annotation, and an empty request path
// is equally meaningless. Matching those would return an arbitrary sandbox.
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
