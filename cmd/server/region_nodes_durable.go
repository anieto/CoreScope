package main

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

// MeshTexas: region membership from direct hearings, kept durably.
//
// Upstream's RegionNodePubkeys (#2101) puts a node in a region when any of its
// adverts held in memory was heard there. Two problems on our deployment:
//   - the in-memory window is ~10h under packetStore.maxMemoryMB, so repeaters
//     with 12-47h advert intervals drop out of region filters between adverts;
//   - relayed adverts count, so a node heard only after a relay chain (or a
//     tropo event) leaks into neighbouring regions (#1879).
//
// This keeps a snapshot over the last retention.nodeDays of adverts in the DB,
// refreshed in the background, and decides membership per node:
//   - heard directly (empty path) by any region's observer: the regions holding
//     at least directShareThreshold of the node's direct hearings (a set, so a
//     boundary node can be in two regions, and one freak long-range hearing
//     doesn't pin it somewhere it isn't);
//   - never heard directly: every region that heard any of its adverts (the old
//     rule, same fallback as #1025).
// RegionNodePubkeys uses the snapshot for every node it knows, and upstream's
// in-memory scan only for nodes newer than the last refresh. Requests never run
// the query; until the first refresh lands the in-memory scan is used alone.

const (
	regionAdvertSnapshotInterval = 10 * time.Minute
	directShareThreshold         = 0.2
)

// regionHearing is one (node, region) row of the snapshot query.
type regionHearing struct {
	pubkey, iata  string
	total, direct int
}

type regionSnapshot struct {
	members map[string]map[string]bool // pubkey -> IATA codes it belongs to
}

var (
	regionAdvertSnapMu sync.RWMutex
	regionAdvertSnap   *regionSnapshot
)

// startRegionAdvertSnapshot refreshes the snapshot now and then every
// regionAdvertSnapshotInterval, for the life of the process.
func startRegionAdvertSnapshot(db *DB, nodeDays int) {
	go func() {
		for {
			t0 := time.Now()
			rows, err := db.regionAdvertHearings(nodeDays)
			if err != nil {
				log.Printf("[region] advert snapshot failed: %v", err)
			} else {
				members, direct := buildRegionMembership(rows)
				regionAdvertSnapMu.Lock()
				regionAdvertSnap = &regionSnapshot{members: members}
				regionAdvertSnapMu.Unlock()
				log.Printf("[region] advert snapshot: %d nodes (%d by direct hearings) in %v",
					len(members), direct, time.Since(t0).Round(time.Millisecond))
			}
			time.Sleep(regionAdvertSnapshotInterval)
		}
	}()
}

// buildRegionMembership applies the membership rule above to snapshot rows.
// It returns pubkey -> region set, and how many nodes were placed by direct
// hearings rather than the fallback.
func buildRegionMembership(rows []regionHearing) (map[string]map[string]bool, int) {
	type node struct {
		any         []string
		direct      map[string]int
		directTotal int
	}
	nodes := make(map[string]*node)
	for _, r := range rows {
		n := nodes[r.pubkey]
		if n == nil {
			n = &node{direct: map[string]int{}}
			nodes[r.pubkey] = n
		}
		n.any = append(n.any, r.iata)
		if r.direct > 0 {
			n.direct[r.iata] += r.direct
			n.directTotal += r.direct
		}
	}
	members := make(map[string]map[string]bool, len(nodes))
	placedByDirect := 0
	for pk, n := range nodes {
		set := make(map[string]bool)
		if n.directTotal > 0 {
			placedByDirect++
			for iata, c := range n.direct {
				if float64(c) >= directShareThreshold*float64(n.directTotal) {
					set[iata] = true
				}
			}
		} else {
			for _, iata := range n.any {
				set[iata] = true
			}
		}
		members[pk] = set
	}
	return members, placedByDirect
}

// regionSnapshotMembers returns the snapshot's pubkeys in any of codes (already
// normalized) and a lookup for whether the snapshot knows a node at all, or
// ok=false before the first refresh.
func regionSnapshotMembers(codes []string) (keys []string, known func(string) bool, ok bool) {
	regionAdvertSnapMu.RLock()
	snap := regionAdvertSnap
	regionAdvertSnapMu.RUnlock()
	if snap == nil {
		return nil, nil, false
	}
	for pk, set := range snap.members {
		for _, c := range codes {
			if set[c] {
				keys = append(keys, pk)
				break
			}
		}
	}
	known = func(pk string) bool { _, hit := snap.members[pk]; return hit }
	return keys, known, true
}

// regionAdvertHearings counts, per node and observer region, the observations
// of the node's adverts in the last nodeDays and how many were heard directly
// (empty path). Observers' current IATA codes are used, like
// resolveRegionObservers.
func (db *DB) regionAdvertHearings(nodeDays int) ([]regionHearing, error) {
	joinCond := "obs.rowid = o.observer_idx"
	if !db.isV3 {
		joinCond = "obs.id = o.observer_id"
	}
	since := time.Now().UTC().AddDate(0, 0, -nodeDays).Format(time.RFC3339)
	rows, err := db.conn.Query(fmt.Sprintf(`
		SELECT t.from_pubkey, UPPER(TRIM(obs.iata)), COUNT(*),
		       SUM(CASE WHEN o.path_json IS NULL OR o.path_json IN ('', '[]') THEN 1 ELSE 0 END)
		FROM transmissions t
		JOIN observations o ON o.transmission_id = t.id
		JOIN observers obs ON %s
		WHERE t.payload_type = 4 AND t.from_pubkey IS NOT NULL
		AND obs.iata IS NOT NULL AND TRIM(obs.iata) != ''
		AND t.first_seen >= ?
		GROUP BY 1, 2
	`, joinCond), since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []regionHearing
	for rows.Next() {
		var r regionHearing
		if err := rows.Scan(&r.pubkey, &r.iata, &r.total, &r.direct); err != nil {
			continue
		}
		r.pubkey = strings.ToLower(r.pubkey)
		out = append(out, r)
	}
	return out, rows.Err()
}
