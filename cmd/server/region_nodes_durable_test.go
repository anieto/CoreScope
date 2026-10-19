package main

import (
	"reflect"
	"testing"
	"time"
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
		// Never heard directly: the regions at its fewest hops, not every
		// region that heard it.
		{pubkey: "relayed", iata: "SAT", total: 3, direct: 0, minHops: 1},
		{pubkey: "relayed", iata: "AUS", total: 1, direct: 0, minHops: 4},
		// Tied fewest hops: both.
		{pubkey: "tie", iata: "SAT", total: 2, direct: 0, minHops: 2},
		{pubkey: "tie", iata: "HOU", total: 5, direct: 0, minHops: 2},
		{pubkey: "tie", iata: "DFW", total: 9, direct: 0, minHops: 3},
		// Exactly fallbackMaxHops is still placed.
		{pubkey: "edgehops", iata: "DFW", total: 1, direct: 0, minHops: fallbackMaxHops},
		// Closest hearing beyond fallbackMaxHops: no region, but still known.
		{pubkey: "gulf", iata: "AUS", total: 6, direct: 0, minHops: fallbackMaxHops + 1},
		{pubkey: "gulf", iata: "HOU", total: 2, direct: 0, minHops: 7},
	}
	members, direct, unplaced := buildRegionMembership(rows)

	want := map[string]map[string]bool{
		"aus":      {"AUS": true},
		"edge":     {"SAT": true, "AUS": true},
		"tropo":    {"DFW": true},
		"share":    {"SAT": true, "CRP": true},
		"relayed":  {"SAT": true},
		"tie":      {"SAT": true, "HOU": true},
		"edgehops": {"DFW": true},
		"gulf":     {},
	}
	if !reflect.DeepEqual(members, want) {
		t.Fatalf("members = %v, want %v", members, want)
	}
	if direct != 4 {
		t.Fatalf("placed by direct hearings = %d, want 4", direct)
	}
	if unplaced != 1 {
		t.Fatalf("in no region = %d, want 1", unplaced)
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

	members, _, _ := buildRegionMembership([]regionHearing{
		{pubkey: "aus", iata: "AUS", total: 4, direct: 4},
		{pubkey: "aus", iata: "SAT", total: 4, direct: 0, minHops: 2},
		{pubkey: "sat", iata: "SAT", total: 2, direct: 2},
		{pubkey: "gulf", iata: "SAT", total: 3, direct: 0, minHops: 9},
	})
	regionAdvertSnapMu.Lock()
	regionAdvertSnap = &regionSnapshot{members: members}
	regionAdvertSnapMu.Unlock()

	keys, known, ok := regionSnapshotMembers([]string{"SAT"})
	if !ok || !reflect.DeepEqual(keys, []string{"sat"}) {
		t.Fatalf("SAT keys = %v (ok=%v), want [sat]", keys, ok)
	}
	// "aus" and "gulf" are known, so the in-memory scan must not re-add them.
	if !known("aus") || !known("gulf") || known("new") {
		t.Fatalf("known(aus)=%v known(gulf)=%v known(new)=%v", known("aus"), known("gulf"), known("new"))
	}
}

func TestApplyRegionPins(t *testing.T) {
	members, _, _ := buildRegionMembership([]regionHearing{
		{pubkey: "blanco", iata: "AUS", total: 9, direct: 0, minHops: 2},
		{pubkey: "blanco", iata: "SAT", total: 3, direct: 0, minHops: 3},
	})
	applyRegionPins(members, map[string][]string{
		" BLANCO ": {"sat"},
		"quiet":    {"SAT", " "},
		"":         {"AUS"},
	})
	if want := map[string]bool{"AUS": true, "SAT": true}; !reflect.DeepEqual(members["blanco"], want) {
		t.Fatalf("blanco = %v, want %v", members["blanco"], want)
	}
	if want := map[string]bool{"SAT": true}; !reflect.DeepEqual(members["quiet"], want) {
		t.Fatalf("quiet = %v, want %v", members["quiet"], want)
	}
	if _, ok := members[""]; ok {
		t.Fatal("empty pubkey pinned")
	}
}

func TestRegionAdvertHearingsMinHops(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	now := time.Now().UTC().Format(time.RFC3339)
	db.conn.Exec(`INSERT INTO observers (id, name, iata) VALUES ('o1', 'Obs1', 'SAT')`)
	db.conn.Exec(`INSERT INTO observers (id, name, iata) VALUES ('o2', 'Obs2', 'aus ')`)
	db.conn.Exec(`INSERT INTO transmissions (raw_hex, hash, first_seen, route_type, payload_type, from_pubkey)
		VALUES ('AA', 'h1', ?, 1, 4, 'ABCD')`, now)
	db.conn.Exec(`INSERT INTO transmissions (raw_hex, hash, first_seen, route_type, payload_type, from_pubkey)
		VALUES ('BB', 'h2', ?, 1, 4, 'ABCD')`, now)
	// SAT hears it at 3 and 2 hops, AUS directly once and at 5 hops.
	for _, o := range []struct {
		tx, obs int
		path    interface{}
	}{
		{1, 1, `["aa","bb","cc"]`},
		{2, 1, `["aa","bb"]`},
		{1, 2, `[]`},
		{2, 2, `["aa","bb","cc","dd","ee"]`},
	} {
		if _, err := db.conn.Exec(`INSERT INTO observations (transmission_id, observer_idx, timestamp, path_json)
			VALUES (?, ?, 0, ?)`, o.tx, o.obs, o.path); err != nil {
			t.Fatal(err)
		}
	}

	rows, err := db.regionAdvertHearings(14)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]regionHearing{}
	for _, r := range rows {
		got[r.iata] = r
	}
	want := map[string]regionHearing{
		"SAT": {pubkey: "abcd", iata: "SAT", total: 2, direct: 0, minHops: 2},
		"AUS": {pubkey: "abcd", iata: "AUS", total: 2, direct: 1, minHops: 0},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("hearings = %+v, want %+v", got, want)
	}
}
