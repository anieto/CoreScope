package main

import (
	"reflect"
	"testing"
)

func TestBuildRegionMembership(t *testing.T) {
	rows := []regionHearing{
		// Heard directly only in AUS, relayed into SAT: AUS only (#1879).
		{pubkey: "aus", iata: "AUS", total: 40, direct: 12},
		{pubkey: "aus", iata: "SAT", total: 9, direct: 0},
		// Boundary node heard directly in both, 50/50: both.
		{pubkey: "edge", iata: "SAT", total: 10, direct: 5},
		{pubkey: "edge", iata: "AUS", total: 10, direct: 5},
		// One freak long-range direct hearing out of 10: below 20%, dropped.
		{pubkey: "tropo", iata: "DFW", total: 30, direct: 9},
		{pubkey: "tropo", iata: "AUS", total: 2, direct: 1},
		// Exactly 20% is kept.
		{pubkey: "share", iata: "SAT", total: 8, direct: 8},
		{pubkey: "share", iata: "CRP", total: 2, direct: 2},
		// Never heard directly: every region that heard it (fallback).
		{pubkey: "far", iata: "SAT", total: 3, direct: 0},
		{pubkey: "far", iata: "AUS", total: 1, direct: 0},
	}
	members, direct := buildRegionMembership(rows)

	want := map[string]map[string]bool{
		"aus":   {"AUS": true},
		"edge":  {"SAT": true, "AUS": true},
		"tropo": {"DFW": true},
		"share": {"SAT": true, "CRP": true},
		"far":   {"SAT": true, "AUS": true},
	}
	if !reflect.DeepEqual(members, want) {
		t.Fatalf("members = %v, want %v", members, want)
	}
	if direct != 4 {
		t.Fatalf("placed by direct hearings = %d, want 4", direct)
	}
}

func TestRegionSnapshotMembers(t *testing.T) {
	regionAdvertSnapMu.Lock()
	saved := regionAdvertSnap
	regionAdvertSnap = nil
	regionAdvertSnapMu.Unlock()
	defer func() {
		regionAdvertSnapMu.Lock()
		regionAdvertSnap = saved
		regionAdvertSnapMu.Unlock()
	}()

	if _, _, ok := regionSnapshotMembers([]string{"SAT"}); ok {
		t.Fatal("ok before the first refresh")
	}

	members, _ := buildRegionMembership([]regionHearing{
		{pubkey: "aus", iata: "AUS", total: 4, direct: 4},
		{pubkey: "aus", iata: "SAT", total: 4, direct: 0},
		{pubkey: "sat", iata: "SAT", total: 2, direct: 2},
	})
	regionAdvertSnapMu.Lock()
	regionAdvertSnap = &regionSnapshot{members: members}
	regionAdvertSnapMu.Unlock()

	keys, known, ok := regionSnapshotMembers([]string{"SAT"})
	if !ok || !reflect.DeepEqual(keys, []string{"sat"}) {
		t.Fatalf("SAT keys = %v (ok=%v), want [sat]", keys, ok)
	}
	// "aus" is known, so the in-memory scan must not re-add it to SAT.
	if !known("aus") || known("new") {
		t.Fatalf("known(aus)=%v known(new)=%v", known("aus"), known("new"))
	}
}
