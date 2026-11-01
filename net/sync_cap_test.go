package net

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/xeprotocol/xe/core"
	"github.com/xeprotocol/xe/store"
)

func TestSyncServerCapTruncationSignal(t *testing.T) {
	s := store.NewMemStore()
	l := core.NewLedger(s, core.LedgerConfig{})
	const account = "synthetic-offline-test"

	// Create 10,001 blocks (1 over the 10,000 maxServerBlocks cap)
	const totalBlocks = 10001
	blocks := make([]*core.Block, totalBlocks)
	for i := range blocks {
		blocks[i] = &core.Block{Account: account, Hash: fmt.Sprintf("%064x", i+1)}
	}
	if err := s.PutAccountChain(account, &core.AccountChain{Blocks: blocks}); err != nil {
		t.Fatalf("PutAccountChain: %v", err)
	}
	if err := s.PutFrontier(account, blocks[totalBlocks-1].Hash); err != nil {
		t.Fatalf("PutFrontier: %v", err)
	}

	request := l.Frontiers()
	delete(request, account) // remote peer has not seen account

	type pageStats struct {
		count     int
		last      string
		hasMore   bool
		truncated bool
	}

	collect := func(frontiers map[string]string) []pageStats {
		var pages []pageStats
		for resp := range findMissingBlocksPaginated(frontiers, l, 64) {
			last := ""
			if len(resp.Blocks) > 0 {
				last = resp.Blocks[len(resp.Blocks)-1].Hash
			}
			pages = append(pages, pageStats{
				count:     len(resp.Blocks),
				last:      last,
				hasMore:   resp.HasMore,
				truncated: resp.Truncated,
			})
		}
		return pages
	}

	// Round 1: Peer requests from genesis. Server caps at 10,000 blocks.
	pages1 := collect(request)
	totalDelivered1 := 0
	for _, p := range pages1 {
		totalDelivered1 += p.count
	}
	if totalDelivered1 != 10000 {
		t.Fatalf("round 1 expected 10,000 capped blocks, got %d", totalDelivered1)
	}

	lastPage1 := pages1[len(pages1)-1]
	// The final page of round 1 MUST NOT indicate terminal false-completion.
	// It must explicitly flag HasMore: true and Truncated: true.
	if !lastPage1.hasMore {
		t.Fatalf("round 1 final page indicated hasMore=false despite unsent blocks remaining on source chain")
	}
	if !lastPage1.truncated {
		t.Fatalf("round 1 final page missing truncated=true indicator")
	}

	// Round 2: Receiver advances its frontier to block 10,000 and requests again.
	request[account] = lastPage1.last
	pages2 := collect(request)
	totalDelivered2 := 0
	for _, p := range pages2 {
		totalDelivered2 += p.count
	}
	if totalDelivered2 != 1 {
		t.Fatalf("round 2 expected remaining 1 block, got %d", totalDelivered2)
	}

	lastPage2 := pages2[len(pages2)-1]
	if lastPage2.hasMore {
		t.Fatalf("round 2 final page should indicate hasMore=false upon full convergence, got true")
	}
	if lastPage2.truncated {
		t.Fatalf("round 2 final page should indicate truncated=false, got true")
	}
	if lastPage2.last != blocks[totalBlocks-1].Hash {
		t.Fatalf("round 2 final block hash=%s, want %s", lastPage2.last, blocks[totalBlocks-1].Hash)
	}
}

func TestSyncResponseTruncatedJSON(t *testing.T) {
	respTruncated := SyncResponse{
		HasMore:   true,
		Truncated: true,
	}
	data, err := json.Marshal(respTruncated)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(data), `"truncated":true`) {
		t.Fatalf("expected JSON to contain 'truncated':true, got %s", string(data))
	}

	respClean := SyncResponse{
		HasMore:   false,
		Truncated: false,
	}
	cleanData, err := json.Marshal(respClean)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(cleanData), `"truncated"`) {
		t.Fatalf("expected omitempty to drop 'truncated' field when false, got %s", string(cleanData))
	}
}

func TestSyncEqualFrontiersAcknowledgesConvergence(t *testing.T) {
	s := store.NewMemStore()
	l := core.NewLedger(s, core.LedgerConfig{})
	const account = "synced-peer"

	block := &core.Block{Account: account, Hash: fmt.Sprintf("%064x", 1)}
	if err := s.PutAccountChain(account, &core.AccountChain{Blocks: []*core.Block{block}}); err != nil {
		t.Fatalf("PutAccountChain: %v", err)
	}
	if err := s.PutFrontier(account, block.Hash); err != nil {
		t.Fatalf("PutFrontier: %v", err)
	}

	// Remote peer already has identical frontiers
	request := l.Frontiers()

	var responses []SyncResponse
	for resp := range findMissingBlocksPaginated(request, l, 64) {
		responses = append(responses, resp)
	}

	if len(responses) != 1 {
		t.Fatalf("expected exactly 1 terminal frame for identical frontiers, got %d", len(responses))
	}
	if responses[0].HasMore {
		t.Fatalf("expected hasMore=false for identical frontiers, got true")
	}
	if responses[0].Truncated {
		t.Fatalf("expected truncated=false for identical frontiers, got true")
	}
	if len(responses[0].Blocks) != 0 {
		t.Fatalf("expected 0 blocks, got %d", len(responses[0].Blocks))
	}
}

func TestSyncExactCap10000Blocks(t *testing.T) {
	s := store.NewMemStore()
	l := core.NewLedger(s, core.LedgerConfig{})
	const account = "exact-10000-peer"

	const totalBlocks = 10000
	blocks := make([]*core.Block, totalBlocks)
	for i := range blocks {
		blocks[i] = &core.Block{Account: account, Hash: fmt.Sprintf("%064x", i+1)}
	}
	if err := s.PutAccountChain(account, &core.AccountChain{Blocks: blocks}); err != nil {
		t.Fatalf("PutAccountChain: %v", err)
	}
	if err := s.PutFrontier(account, blocks[totalBlocks-1].Hash); err != nil {
		t.Fatalf("PutFrontier: %v", err)
	}

	request := l.Frontiers()
	delete(request, account)

	var allBlocks []*core.Block
	var lastResp SyncResponse
	for resp := range findMissingBlocksPaginated(request, l, 64) {
		allBlocks = append(allBlocks, resp.Blocks...)
		lastResp = resp
	}

	if len(allBlocks) != 10000 {
		t.Fatalf("expected exactly 10,000 blocks, got %d", len(allBlocks))
	}
	if lastResp.HasMore {
		t.Fatalf("expected hasMore=false for exact 10,000-block chain, got true")
	}
	if lastResp.Truncated {
		t.Fatalf("expected truncated=false for exact 10,000-block chain, got true")
	}
}
