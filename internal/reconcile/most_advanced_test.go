package reconcile

import (
	"testing"

	"github.com/openweft/weft-ha-postgresql/internal/dcs"
)

// TestIsMostAdvanced_SoloNodeWins : with no peers in the snapshot the
// node is trivially the most-advanced. Tests the "len(others)==0"
// early-exit ; without it a single-node bootstrap could deadlock.
func TestIsMostAdvanced_SoloNodeWins(t *testing.T) {
	r := &Reconciler{}
	snap := Snapshot{
		Self: dcs.Member{Name: "n1", LSN: 100},
		Members: []dcs.Member{
			{Name: "n1", LSN: 100},
		},
	}
	if !r.isMostAdvanced(snap) {
		t.Error("solo node should always be most-advanced")
	}
}

// TestIsMostAdvanced_HigherLSNPeerWins : the PRIMARY regression we are
// closing. Old code compared on name only ; an "aa-fresh" replica with
// LSN=10 would beat a "zz-canonical" replica with LSN=999, truncating
// 989 bytes of WAL on promotion. With the fix, LSN wins outright.
func TestIsMostAdvanced_HigherLSNPeerWins(t *testing.T) {
	r := &Reconciler{}
	snap := Snapshot{
		Self: dcs.Member{Name: "aa-fresh", LSN: 10},
		Members: []dcs.Member{
			{Name: "aa-fresh", LSN: 10},
			{Name: "zz-canonical", LSN: 999},
		},
	}
	if r.isMostAdvanced(snap) {
		t.Error("aa-fresh@10 must NOT win against zz-canonical@999 ; lexical-name tie-break would silently truncate WAL")
	}
}

// TestIsMostAdvanced_HigherLSNSelfWins : the symmetric case. Self has
// the most WAL ; the lexical tie-break doesn't matter.
func TestIsMostAdvanced_HigherLSNSelfWins(t *testing.T) {
	r := &Reconciler{}
	snap := Snapshot{
		Self: dcs.Member{Name: "zz-canonical", LSN: 999},
		Members: []dcs.Member{
			{Name: "aa-fresh", LSN: 10},
			{Name: "zz-canonical", LSN: 999},
		},
	}
	if !r.isMostAdvanced(snap) {
		t.Error("zz-canonical@999 should win against aa-fresh@10 ; the LSN gap is decisive")
	}
}

// TestIsMostAdvanced_EqualLSNLexTieBreak : when LSNs are equal we MUST
// converge deterministically across all nodes — every replica runs
// isMostAdvanced locally and the winner must agree. Lexical order is
// the canonical tie-break.
func TestIsMostAdvanced_EqualLSNLexTieBreak_LowerNameWins(t *testing.T) {
	r := &Reconciler{}
	snap := Snapshot{
		Self: dcs.Member{Name: "aa", LSN: 500},
		Members: []dcs.Member{
			{Name: "aa", LSN: 500},
			{Name: "zz", LSN: 500},
		},
	}
	if !r.isMostAdvanced(snap) {
		t.Error("equal-LSN : lexically lower name (aa) should win against zz")
	}
}

// TestIsMostAdvanced_EqualLSNLexTieBreak_HigherNameLoses : the mirror
// case — confirms only ONE node believes it's the most-advanced.
func TestIsMostAdvanced_EqualLSNLexTieBreak_HigherNameLoses(t *testing.T) {
	r := &Reconciler{}
	snap := Snapshot{
		Self: dcs.Member{Name: "zz", LSN: 500},
		Members: []dcs.Member{
			{Name: "aa", LSN: 500},
			{Name: "zz", LSN: 500},
		},
	}
	if r.isMostAdvanced(snap) {
		t.Error("equal-LSN : lexically higher name (zz) must NOT win ; would create competing campaigns")
	}
}

// TestIsMostAdvanced_PeerWithZeroLSNIgnored : LSN==0 is the "not yet
// observed" sentinel ; a peer that just started up shouldn't gate the
// promotion of an already-running replica. Otherwise a rolling reboot
// could indefinitely block failover.
func TestIsMostAdvanced_PeerWithZeroLSNIgnored(t *testing.T) {
	r := &Reconciler{}
	snap := Snapshot{
		Self: dcs.Member{Name: "n1", LSN: 100},
		Members: []dcs.Member{
			{Name: "n1", LSN: 100},
			{Name: "n2", LSN: 0}, // cold-start, no observation yet
		},
	}
	if !r.isMostAdvanced(snap) {
		t.Error("a peer with LSN=0 (cold-start window) should NOT gate promotion")
	}
}

// TestIsMostAdvanced_ThreeWayEqualLSN : at least one of the three
// nodes must win. Verifies the lex tie-break is total : not "all
// three see themselves as winner" (no convergence) nor "none does"
// (failover never happens).
func TestIsMostAdvanced_ThreeWayEqualLSN(t *testing.T) {
	r := &Reconciler{}
	members := []dcs.Member{
		{Name: "alpha", LSN: 42},
		{Name: "bravo", LSN: 42},
		{Name: "charlie", LSN: 42},
	}
	winners := 0
	for _, self := range members {
		snap := Snapshot{Self: self, Members: members}
		if r.isMostAdvanced(snap) {
			winners++
		}
	}
	if winners != 1 {
		t.Errorf("3-way equal-LSN should produce exactly one winner ; got %d", winners)
	}
}

// TestIsMostAdvanced_ThreeWayMixedLSN : the highest-LSN node wins
// even if its name is lex-last ; confirms LSN dominates name.
func TestIsMostAdvanced_ThreeWayMixedLSN(t *testing.T) {
	r := &Reconciler{}
	members := []dcs.Member{
		{Name: "alpha", LSN: 10},
		{Name: "bravo", LSN: 50},
		{Name: "charlie", LSN: 999}, // highest LSN, lex-last name
	}
	for _, self := range members {
		snap := Snapshot{Self: self, Members: members}
		got := r.isMostAdvanced(snap)
		want := self.Name == "charlie"
		if got != want {
			t.Errorf("self=%s LSN=%d : got isMostAdvanced=%v ; want %v (highest-LSN wins regardless of name)", self.Name, self.LSN, got, want)
		}
	}
}

// TestIsMostAdvanced_AllPeersUnobserved : every peer reports LSN=0
// (e.g. just after a cluster-wide restart). We should NOT block on
// the cold-start window — Self with any observed LSN wins.
func TestIsMostAdvanced_AllPeersUnobserved(t *testing.T) {
	r := &Reconciler{}
	snap := Snapshot{
		Self: dcs.Member{Name: "n1", LSN: 100},
		Members: []dcs.Member{
			{Name: "n1", LSN: 100},
			{Name: "n2", LSN: 0},
			{Name: "n3", LSN: 0},
		},
	}
	if !r.isMostAdvanced(snap) {
		t.Error("when all peers are unobserved, self with any LSN should win")
	}
}
