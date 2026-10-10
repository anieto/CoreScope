package main

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// MeshTexas: RegionNodePubkeys (#2101) only scans the adverts the in-memory
// store holds. Under packetStore.maxMemoryMB that is ~10h here, so a repeater
// whose last advert is older than that (flood adverts are 12-47h apart)
// dropped out of every region filter until it adverted again. This keeps a
// DB-wide pubkey -> IATA snapshot, refreshed in the background, and
// RegionNodePubkeys unions it with the live scan. Requests never run the
// query; until the first refresh lands the live scan is used alone.

const regionAdvertSnapshotInterval = 10 * time.Minute

var (
	regionAdvertSnapMu sync.RWMutex
	regionAdvertSnap   map[string]map[string]bool // pubkey -> IATA codes
)

// startRegionAdvertSnapshot refreshes the snapshot now and then every
// regionAdvertSnapshotInterval, for the life of the process.
func startRegionAdvertSnapshot(db *DB) {
	go func() {
		for {
			t0 := time.Now()
			m, err := db.allRegionAdvertMemberships()
			if err != nil {
				log.Printf("[region] advert snapshot failed: %v", err)
			} else {
				regionAdvertSnapMu.Lock()
				regionAdvertSnap = m
				regionAdvertSnapMu.Unlock()
				log.Printf("[region] advert snapshot: %d nodes in %v", len(m), time.Since(t0).Round(time.Millisecond))
			}
			time.Sleep(regionAdvertSnapshotInterval)
		}
	}()
}

// regionAdvertSnapshotKeys returns the snapshot's pubkeys heard in any of
// codes (already normalized), or nil before the first refresh.
func regionAdvertSnapshotKeys(codes []string) []string {
	regionAdvertSnapMu.RLock()
	defer regionAdvertSnapMu.RUnlock()
	if regionAdvertSnap == nil {
		return nil
	}
	var out []string
	for pk, iatas := range regionAdvertSnap {
		for _, c := range codes {
			if iatas[c] {
				out = append(out, pk)
				break
			}
		}
	}
	return out
}

// allRegionAdvertMemberships maps every advertising pubkey to the IATA codes
// of the observers that heard its adverts, using observers' current codes
// like resolveRegionObservers does.
func (db *DB) allRegionAdvertMemberships() (map[string]map[string]bool, error) {
	joinCond := "obs.rowid = o.observer_idx"
	if !db.isV3 {
		joinCond = "obs.id = o.observer_id"
	}
	rows, err := db.conn.Query(fmt.Sprintf(`
		SELECT DISTINCT t.from_pubkey, UPPER(TRIM(obs.iata))
		FROM transmissions t
		JOIN observations o ON o.transmission_id = t.id
		JOIN observers obs ON %s
		WHERE t.payload_type = 4 AND t.from_pubkey IS NOT NULL
		AND obs.iata IS NOT NULL AND TRIM(obs.iata) != ''
	`, joinCond))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]map[string]bool)
	for rows.Next() {
		var pk, iata string
		if err := rows.Scan(&pk, &iata); err != nil {
			continue
		}
		if out[pk] == nil {
			out[pk] = make(map[string]bool)
		}
		out[pk][iata] = true
	}
	return out, rows.Err()
}
