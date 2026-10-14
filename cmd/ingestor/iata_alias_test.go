package main

import "testing"

// MeshTexas: observers still sending a retired code (GGG) are stored under its
// replacement (TYR); other codes and the client topic are untouched.
func TestResolveIATAAlias(t *testing.T) {
	cfg := &Config{IATAAliases: map[string]string{"ggg": "tyr"}}
	cases := map[string]string{"GGG": "TYR", "ggg": "TYR", " GGG ": "TYR", "TYR": "TYR", "DFW": "DFW", "client": "client"}
	for in, want := range cases {
		if got := cfg.ResolveIATAAlias(in); got != want {
			t.Errorf("ResolveIATAAlias(%q) = %q, want %q", in, got, want)
		}
	}
	var none *Config
	if got := none.ResolveIATAAlias("GGG"); got != "GGG" {
		t.Errorf("nil config changed the code: %q", got)
	}
	if got := (&Config{}).ResolveIATAAlias("GGG"); got != "GGG" {
		t.Errorf("no aliases changed the code: %q", got)
	}
}
