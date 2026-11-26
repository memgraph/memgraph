// The matcher's symbol index under incremental updates, proved with Verus.
//
// The rewriter keeps one index across passes. It never removes an entry: an
// entry whose e-class was merged away stays, and is resolved to its current
// class when read. After each pass it indexes only the e-classes that pass
// inserted. This file proves that this yields the same candidate classes as
// rebuilding the index from scratch:
//
//  1. Complete: every e-node outside the pending set has an entry for its
//     symbol that resolves to the e-node's current class.
//  2. Sound: every entry resolves to a class holding an e-node with the
//     entry's symbol.
//
// Both hold initially and are kept by inserting an e-node (which goes
// pending), by any coarsening of the partition (a merge, or everything a
// rebuild does), and by the incremental update, which empties the pending
// set. With nothing pending they give the theorem: for every symbol, the
// classes the kept index yields are exactly the classes holding it.
//
// The model abstracts the e-graph:
//  - Each id is an e-node with a symbol; `rep` gives its class
//    representative, idempotent as union_find.rs proves.
//  - A class holds a symbol when one of its e-nodes has it. Every e-node ever
//    inserted is counted, live or not: a duplicate a rebuild retires sits in
//    the class of the e-node it duplicates, which has the same symbol.
//  - The update indexes, for every pending id, every e-node of the id's
//    current class under that class's representative: the C++
//    MatcherIndex::rebuild_index(span) and the port's SymbolIndex::add.
//
// Check with: verus src/planner/rust/verus/match_index.rs

use vstd::prelude::*;

verus! {

broadcast use {
    vstd::set::group_set_lemmas,
    vstd::seq::group_seq_axioms,
};

pub struct Idx {
    pub rep: Seq<int>,
    pub sym: Seq<int>,
    /// (symbol, id) pairs; an id may be stale.
    pub entries: Set<(int, int)>,
    /// Ids inserted since the last update.
    pub pending: Set<int>,
}

impl Idx {
    pub open spec fn n(self) -> int {
        self.sym.len() as int
    }
}

pub open spec fn rep_wf(rep: Seq<int>, n: int) -> bool {
    &&& rep.len() == n
    &&& forall|i: int| 0 <= i < n ==> 0 <= #[trigger] rep[i] < n && rep[rep[i]] == rep[i]
}

pub open spec fn wf(x: Idx) -> bool {
    &&& rep_wf(x.rep, x.n())
    &&& forall|p: int| #[trigger] x.pending.contains(p) ==> 0 <= p < x.n()
    &&& forall|s: int, e: int| #[trigger] x.entries.contains((s, e)) ==> 0 <= e < x.n()
}

pub open spec fn complete(x: Idx) -> bool {
    forall|i: int|
        0 <= i < x.n() && !x.pending.contains(i) ==> exists|e: int|
            #[trigger] x.entries.contains((x.sym[i], e)) && x.rep[e] == x.rep[i]
}

pub open spec fn sound(x: Idx) -> bool {
    forall|s: int, e: int|
        #[trigger] x.entries.contains((s, e)) ==> exists|j: int| 0 <= j < x.n() && x.sym[j] == s && x.rep[j] == x.rep[e]
}

pub open spec fn inv(x: Idx) -> bool {
    wf(x) && complete(x) && sound(x)
}

/// `y` is `x` with one new e-node of symbol `s`, alone in its class and pending.
pub open spec fn inserted(x: Idx, y: Idx, s: int) -> bool {
    &&& y.sym == x.sym.push(s)
    &&& y.rep == x.rep.push(x.n())
    &&& y.entries == x.entries
    &&& y.pending == x.pending.insert(x.n())
}

/// `y`'s partition is `x`'s with classes joined: what merges and rebuilds do.
pub open spec fn coarsened(x: Idx, y: Idx) -> bool {
    &&& y.sym == x.sym
    &&& y.entries == x.entries
    &&& y.pending == x.pending
    &&& rep_wf(y.rep, x.n())
    &&& forall|i: int, j: int| 0 <= i < x.n() && 0 <= j < x.n() && x.rep[i] == x.rep[j] ==> y.rep[i] == y.rep[j]
}

/// The entries the update adds: every e-node of a pending id's class, under
/// that class's representative.
pub open spec fn update_entry(x: Idx, t: (int, int)) -> bool {
    exists|p: int, j: int|
        #![trigger x.pending.contains(p), x.rep[j]]
        x.pending.contains(p) && 0 <= j < x.n() && x.rep[j] == x.rep[p] && t == (x.sym[j], x.rep[p])
}

/// `y` is `x` after the incremental update.
pub open spec fn updated(x: Idx, y: Idx) -> bool {
    &&& y.sym == x.sym
    &&& y.rep == x.rep
    &&& y.pending == Set::<int>::empty()
    &&& forall|t: (int, int)| #[trigger] y.entries.contains(t) <==> (x.entries.contains(t) || update_entry(x, t))
}

pub proof fn lemma_empty_index(x: Idx)
    requires
        x.n() == 0,
        x.rep.len() == 0,
        x.entries == Set::<(int, int)>::empty(),
        x.pending == Set::<int>::empty(),
    ensures
        inv(x),
{
}

pub proof fn lemma_insert(x: Idx, y: Idx, s: int)
    requires
        inv(x),
        inserted(x, y, s),
    ensures
        inv(y),
{
    let n = x.n();
    assert forall|i: int| 0 <= i < y.n() implies 0 <= #[trigger] y.rep[i] < y.n() && y.rep[y.rep[i]] == y.rep[i] by {
        if i < n {
            assert(y.rep[i] == x.rep[i]);
            assert(y.rep[x.rep[i]] == x.rep[x.rep[i]]);
        }
    }
    assert forall|i: int| 0 <= i < y.n() && !y.pending.contains(i) implies exists|e: int|
        #[trigger] y.entries.contains((y.sym[i], e)) && y.rep[e] == y.rep[i] by {
        assert(i < n);
        let e = choose|e: int| #[trigger] x.entries.contains((x.sym[i], e)) && x.rep[e] == x.rep[i];
        assert(y.entries.contains((y.sym[i], e)) && y.rep[e] == y.rep[i]);
    }
    assert forall|s2: int, e: int| #[trigger] y.entries.contains((s2, e)) implies exists|j: int|
        0 <= j < y.n() && y.sym[j] == s2 && y.rep[j] == y.rep[e] by {
        let j = choose|j: int| 0 <= j < x.n() && x.sym[j] == s2 && x.rep[j] == x.rep[e];
        assert(0 <= j < y.n() && y.sym[j] == s2 && y.rep[j] == y.rep[e]);
    }
}

pub proof fn lemma_coarsen(x: Idx, y: Idx)
    requires
        inv(x),
        coarsened(x, y),
    ensures
        inv(y),
{
    assert forall|i: int| 0 <= i < y.n() && !y.pending.contains(i) implies exists|e: int|
        #[trigger] y.entries.contains((y.sym[i], e)) && y.rep[e] == y.rep[i] by {
        let e = choose|e: int| #[trigger] x.entries.contains((x.sym[i], e)) && x.rep[e] == x.rep[i];
        assert(y.entries.contains((y.sym[i], e)) && y.rep[e] == y.rep[i]);
    }
    assert forall|s: int, e: int| #[trigger] y.entries.contains((s, e)) implies exists|j: int|
        0 <= j < y.n() && y.sym[j] == s && y.rep[j] == y.rep[e] by {
        let j = choose|j: int| 0 <= j < x.n() && x.sym[j] == s && x.rep[j] == x.rep[e];
        assert(0 <= j < y.n() && y.sym[j] == s && y.rep[j] == y.rep[e]);
    }
}

pub proof fn lemma_update(x: Idx, y: Idx)
    requires
        inv(x),
        updated(x, y),
    ensures
        inv(y),
{
    assert forall|s: int, e: int| #[trigger] y.entries.contains((s, e)) implies 0 <= e < y.n() by {
        if !x.entries.contains((s, e)) {
            let (p, j) = choose|p: int, j: int|
                #![trigger x.pending.contains(p), x.rep[j]]
                x.pending.contains(p) && 0 <= j < x.n() && x.rep[j] == x.rep[p] && (s, e) == (x.sym[j], x.rep[p]);
        }
    }
    assert forall|i: int| 0 <= i < y.n() && !y.pending.contains(i) implies exists|e: int|
        #[trigger] y.entries.contains((y.sym[i], e)) && y.rep[e] == y.rep[i] by {
        if x.pending.contains(i) {
            let e = x.rep[i];
            assert(update_entry(x, (x.sym[i], e)));
            assert(y.entries.contains((y.sym[i], e)) && y.rep[e] == y.rep[i]);
        } else {
            let e = choose|e: int| #[trigger] x.entries.contains((x.sym[i], e)) && x.rep[e] == x.rep[i];
            assert(y.entries.contains((y.sym[i], e)) && y.rep[e] == y.rep[i]);
        }
    }
    assert forall|s: int, e: int| #[trigger] y.entries.contains((s, e)) implies exists|j: int|
        0 <= j < y.n() && y.sym[j] == s && y.rep[j] == y.rep[e] by {
        if x.entries.contains((s, e)) {
            let j = choose|j: int| 0 <= j < x.n() && x.sym[j] == s && x.rep[j] == x.rep[e];
            assert(0 <= j < y.n() && y.sym[j] == s && y.rep[j] == y.rep[e]);
        } else {
            let (p, j) = choose|p: int, j: int|
                #![trigger x.pending.contains(p), x.rep[j]]
                x.pending.contains(p) && 0 <= j < x.n() && x.rep[j] == x.rep[p] && (s, e) == (x.sym[j], x.rep[p]);
            assert(y.rep[e] == y.rep[x.rep[p]]);
            assert(0 <= j < y.n() && y.sym[j] == s && y.rep[j] == y.rep[e]);
        }
    }
}

/// With nothing pending, a class is a candidate for a symbol exactly when it
/// holds an e-node with that symbol: what a full rebuild of the index gives.
pub proof fn theorem_same_candidates_as_full_rebuild(x: Idx, s: int, c: int)
    requires
        inv(x),
        x.pending == Set::<int>::empty(),
    ensures
        (exists|e: int| #[trigger] x.entries.contains((s, e)) && x.rep[e] == c) <==> (exists|j: int|
            0 <= j < x.n() && x.sym[j] == s && #[trigger] x.rep[j] == c),
{
    if exists|e: int| #[trigger] x.entries.contains((s, e)) && x.rep[e] == c {
        let e = choose|e: int| #[trigger] x.entries.contains((s, e)) && x.rep[e] == c;
        let j = choose|j: int| 0 <= j < x.n() && x.sym[j] == s && x.rep[j] == x.rep[e];
        assert(0 <= j < x.n() && x.sym[j] == s && x.rep[j] == c);
    }
    if exists|j: int| 0 <= j < x.n() && x.sym[j] == s && #[trigger] x.rep[j] == c {
        let j = choose|j: int| 0 <= j < x.n() && x.sym[j] == s && #[trigger] x.rep[j] == c;
        assert(!x.pending.contains(j));
        let e = choose|e: int| #[trigger] x.entries.contains((x.sym[j], e)) && x.rep[e] == x.rep[j];
        assert(x.entries.contains((s, e)) && x.rep[e] == c);
    }
}

fn main() {
}

} // verus!
