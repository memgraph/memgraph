// The e-graph core with deferred rebuilding, as a state machine, proved with
// Verus:
//
//  1. Every operation preserves the representation invariant: `emplace`,
//     `union` (a caller's merge), and each repair step `rebuild` takes.
//  2. `rebuild` restores congruence: it terminates, and when it stops, two
//     e-nodes with the same symbol and disambiguator whose children are
//     equivalent are themselves equivalent.
//  3. The result is exact: after `rebuild`, two terms share a class exactly
//     when the caller's merges and congruence force them together. Nothing
//     is merged that they do not force, and nothing they force is missed.
//
// The model abstracts the port's data structures:
//  - `rep` gives each id its representative; the union-find that computes it
//    is proved separately (union_find.rs).
//  - `hc` is the hashcons as a map from an e-node's content to its id, the
//    contract the port's hashcons implementations are held to.
//  - `pending` holds the e-nodes awaiting repair. A merge adds every live
//    e-node that reads either class; the port reaches the same e-nodes through
//    its per-class parent lists, whose completeness its tests check.
//  - A merge keeps the first argument's representative. Which root survives is
//    a performance choice (union by rank) that no property here depends on.
//  - `terms` and `eqs` are ghost: each id's e-node as first built, and the
//    caller's merges. They state what the e-graph is required to compute.
//
// Check with: verus src/planner/rust/verus/egraph.rs

use vstd::prelude::*;
use vstd::set_lib::*;

verus! {

broadcast use {
    vstd::set::group_set_lemmas,
    vstd::set_lib::group_set_lib_default,
    vstd::map::group_map_lemmas,
    vstd::seq::group_seq_axioms,
};

/// An e-node: a symbol, a disambiguator and the ids of its children.
pub struct Node {
    pub sym: int,
    pub dis: int,
    pub kids: Seq<int>,
}

pub struct EG {
    pub rep: Seq<int>,
    pub nodes: Seq<Node>,
    pub dead: Seq<bool>,
    pub hc: Map<Node, int>,
    pub pending: Set<int>,
    pub terms: Seq<Node>,
    pub eqs: Seq<(int, int)>,
}

impl EG {
    pub open spec fn n(self) -> int {
        self.nodes.len() as int
    }
}

/// The e-node with each child replaced by its representative.
pub open spec fn canon(rep: Seq<int>, x: Node) -> Node {
    Node { sym: x.sym, dis: x.dis, kids: Seq::new(x.kids.len(), |j: int| rep[x.kids[j]]) }
}

pub open spec fn is_canonical(rep: Seq<int>, x: Node) -> bool {
    canon(rep, x) == x
}

pub open spec fn rep_wf(rep: Seq<int>, n: int) -> bool {
    &&& rep.len() == n
    &&& forall|i: int| 0 <= i < n ==> 0 <= #[trigger] rep[i] < n && rep[rep[i]] == rep[i]
}

pub open spec fn in_range(x: Node, n: int) -> bool {
    forall|j: int| 0 <= j < x.kids.len() ==> 0 <= #[trigger] x.kids[j] < n
}

// ---------------------------------------------------------------------------
// What the e-graph is required to compute: the least congruence over the
// terms that contains the caller's merges.

/// The children of terms `i` and `j` are pairwise related by `r`, and the two
/// terms have the same symbol, disambiguator and arity.
pub open spec fn cong_args(terms: Seq<Node>, r: spec_fn(int, int) -> bool, i: int, j: int) -> bool {
    &&& terms[i].sym == terms[j].sym
    &&& terms[i].dis == terms[j].dis
    &&& terms[i].kids.len() == terms[j].kids.len()
    &&& forall|k: int| 0 <= k < terms[i].kids.len() ==> #[trigger] r(terms[i].kids[k], terms[j].kids[k])
}

/// `r` is an equivalence, contains every caller's merge, and is closed under
/// congruence over the terms.
pub open spec fn closed_rel(terms: Seq<Node>, eqs: Seq<(int, int)>, r: spec_fn(int, int) -> bool) -> bool {
    &&& forall|a: int| #[trigger] r(a, a)
    &&& forall|a: int, b: int| #[trigger] r(a, b) ==> r(b, a)
    &&& forall|a: int, b: int, c: int| #[trigger] r(a, b) && #[trigger] r(b, c) ==> r(a, c)
    &&& forall|e: int| 0 <= e < eqs.len() ==> #[trigger] r(eqs[e].0, eqs[e].1)
    &&& forall|i: int, j: int|
        0 <= i < terms.len() && 0 <= j < terms.len() && #[trigger] cong_args(terms, r, i, j) ==> r(i, j)
}

/// The least congruence containing the caller's merges relates `i` and `j`:
/// every relation closed in that way does.
pub open spec fn equiv(terms: Seq<Node>, eqs: Seq<(int, int)>, i: int, j: int) -> bool {
    forall|r: spec_fn(int, int) -> bool| #[trigger] closed_rel(terms, eqs, r) ==> r(i, j)
}

pub proof fn lemma_refl(terms: Seq<Node>, eqs: Seq<(int, int)>, i: int)
    ensures
        equiv(terms, eqs, i, i),
{
}

pub proof fn lemma_trans(terms: Seq<Node>, eqs: Seq<(int, int)>, i: int, m: int, j: int)
    requires
        equiv(terms, eqs, i, m),
        equiv(terms, eqs, m, j),
    ensures
        equiv(terms, eqs, i, j),
{
    assert forall|r: spec_fn(int, int) -> bool| #[trigger] closed_rel(terms, eqs, r) implies r(i, j) by {
        assert(r(i, m) && r(m, j));
    }
}

pub proof fn lemma_eq(terms: Seq<Node>, eqs: Seq<(int, int)>, e: int)
    requires
        0 <= e < eqs.len(),
    ensures
        equiv(terms, eqs, eqs[e].0, eqs[e].1),
        equiv(terms, eqs, eqs[e].1, eqs[e].0),
{
    assert forall|r: spec_fn(int, int) -> bool| #[trigger] closed_rel(terms, eqs, r) implies r(
        eqs[e].1,
        eqs[e].0,
    ) by {
        assert(r(eqs[e].0, eqs[e].1));
    }
}

/// Terms of the same shape whose children are pairwise equivalent are
/// equivalent.
pub proof fn lemma_cong(terms: Seq<Node>, eqs: Seq<(int, int)>, i: int, j: int)
    requires
        0 <= i < terms.len(),
        0 <= j < terms.len(),
        terms[i].sym == terms[j].sym,
        terms[i].dis == terms[j].dis,
        terms[i].kids.len() == terms[j].kids.len(),
        forall|k: int|
            0 <= k < terms[i].kids.len() ==> #[trigger] equiv(terms, eqs, terms[i].kids[k], terms[j].kids[k]),
    ensures
        equiv(terms, eqs, i, j),
{
    assert forall|r: spec_fn(int, int) -> bool| #[trigger] closed_rel(terms, eqs, r) implies r(i, j) by {
        assert forall|k: int| 0 <= k < terms[i].kids.len() implies #[trigger] r(
            terms[i].kids[k],
            terms[j].kids[k],
        ) by {
            assert(equiv(terms, eqs, terms[i].kids[k], terms[j].kids[k]));
        }
        assert(cong_args(terms, r, i, j));
    }
}

/// Equivalence survives appending a term: a relation closed over more terms
/// is closed over fewer.
pub proof fn lemma_equiv_push_term(terms: Seq<Node>, eqs: Seq<(int, int)>, x: Node, i: int, j: int)
    requires
        equiv(terms, eqs, i, j),
    ensures
        equiv(terms.push(x), eqs, i, j),
{
    let t2 = terms.push(x);
    assert forall|r: spec_fn(int, int) -> bool| #[trigger] closed_rel(t2, eqs, r) implies r(i, j) by {
        assert forall|a: int, b: int|
            0 <= a < terms.len() && 0 <= b < terms.len() && #[trigger] cong_args(terms, r, a, b) implies r(
            a,
            b,
        ) by {
            assert(t2[a] == terms[a] && t2[b] == terms[b]);
            assert(cong_args(t2, r, a, b));
        }
        assert(closed_rel(terms, eqs, r));
    }
}

/// Equivalence survives recording another merge.
pub proof fn lemma_equiv_push_eq(terms: Seq<Node>, eqs: Seq<(int, int)>, e: (int, int), i: int, j: int)
    requires
        equiv(terms, eqs, i, j),
    ensures
        equiv(terms, eqs.push(e), i, j),
{
    let q2 = eqs.push(e);
    assert forall|r: spec_fn(int, int) -> bool| #[trigger] closed_rel(terms, q2, r) implies r(i, j) by {
        assert forall|k: int| 0 <= k < eqs.len() implies #[trigger] r(eqs[k].0, eqs[k].1) by {
            assert(q2[k] == eqs[k]);
        }
        assert(closed_rel(terms, eqs, r));
    }
}

/// Every pair sharing a representative is derivably equal.
pub open spec fn sound(s: EG) -> bool {
    forall|i: int, j: int|
        0 <= i < s.n() && 0 <= j < s.n() && #[trigger] s.rep[i] == #[trigger] s.rep[j] ==> equiv(
            s.terms,
            s.eqs,
            i,
            j,
        )
}

/// After a rebuild: two terms share a class exactly when they are derivably
/// equal.
pub open spec fn exact(s: EG) -> bool {
    forall|i: int, j: int|
        0 <= i < s.n() && 0 <= j < s.n() ==> (#[trigger] s.rep[i] == #[trigger] s.rep[j] <==> equiv(
            s.terms,
            s.eqs,
            i,
            j,
        ))
}

// ---------------------------------------------------------------------------
// The invariant.

/// A dead e-node is congruent to a live e-node of its own class.
pub open spec fn has_witness(s: EG, d: int) -> bool {
    exists|k: int|
        0 <= k < s.n() && !s.dead[k] && s.rep[k] == s.rep[d] && canon(s.rep, s.nodes[d]) == canon(
            s.rep,
            s.nodes[k],
        )
}

/// Every e-node of `t` denotes, under `t`'s representatives, the same term
/// as the e-node of `old` at the same id.
pub open spec fn same_terms(old: Seq<Node>, t: EG) -> bool {
    forall|i: int| 0 <= i < old.len() ==> #[trigger] canon(t.rep, t.nodes[i]) == canon(t.rep, old[i])
}

pub open spec fn inv(s: EG) -> bool {
    let n = s.n();
    &&& rep_wf(s.rep, n)
    &&& s.dead.len() == n
    &&& forall|i: int| 0 <= i < n ==> in_range(#[trigger] s.nodes[i], n)
    // The hashcons holds every live e-node under its content ...
    &&& forall|i: int|
        0 <= i < n && !s.dead[i] ==> s.hc.dom().contains(#[trigger] s.nodes[i]) && s.hc[s.nodes[i]]
            == i
    // ... and nothing else.
    &&& forall|x: Node| #[trigger]
        s.hc.dom().contains(x) ==> 0 <= s.hc[x] < n && !s.dead[s.hc[x]] && s.nodes[s.hc[x]] == x
    // A live e-node that is not canonical is awaiting repair.
    &&& forall|i: int|
        0 <= i < n && !s.dead[i] && !is_canonical(s.rep, #[trigger] s.nodes[i]) ==> s.pending.contains(
            i,
        )
    &&& forall|d: int| 0 <= d < n && #[trigger] s.dead[d] ==> has_witness(s, d)
    &&& forall|p: int| #[trigger] s.pending.contains(p) ==> 0 <= p < n
    // The stored e-nodes denote the terms as built, and every caller's merge
    // holds.
    &&& s.terms.len() == n
    &&& forall|i: int| 0 <= i < n ==> in_range(#[trigger] s.terms[i], n)
    &&& same_terms(s.terms, s)
    &&& forall|e: int|
        0 <= e < s.eqs.len() ==> 0 <= (#[trigger] s.eqs[e]).0 < n && 0 <= s.eqs[e].1 < n && s.rep[s.eqs[e].0]
            == s.rep[s.eqs[e].1]
    // Nothing is merged that the caller's merges and congruence do not force.
    &&& sound(s)
}

pub open spec fn congruence_closed(s: EG) -> bool {
    forall|i: int, j: int|
        0 <= i < s.n() && 0 <= j < s.n() && #[trigger] canon(s.rep, s.nodes[i]) == #[trigger] canon(
            s.rep,
            s.nodes[j],
        ) ==> s.rep[i] == s.rep[j]
}

// ---------------------------------------------------------------------------
// Representatives.

pub proof fn lemma_canon_in_range(rep: Seq<int>, n: int, x: Node)
    requires
        rep_wf(rep, n),
        in_range(x, n),
    ensures
        in_range(canon(rep, x), n),
{
    let c = canon(rep, x);
    assert forall|j: int| 0 <= j < c.kids.len() implies 0 <= #[trigger] c.kids[j] < n by {
        assert(0 <= x.kids[j] < n);
    }
}

/// Canonicalising twice changes nothing.
pub proof fn lemma_canon_idempotent(rep: Seq<int>, n: int, x: Node)
    requires
        rep_wf(rep, n),
        in_range(x, n),
    ensures
        canon(rep, canon(rep, x)) == canon(rep, x),
        is_canonical(rep, canon(rep, x)),
{
    let c = canon(rep, x);
    let cc = canon(rep, c);
    assert forall|j: int| 0 <= j < cc.kids.len() implies #[trigger] cc.kids[j] == c.kids[j] by {
        assert(0 <= x.kids[j] < n);
    }
    assert(cc.kids =~= c.kids);
}

/// `new` coarsens `old`: it only joins classes `old` keeps apart.
pub open spec fn coarsens(old: Seq<int>, new: Seq<int>, n: int) -> bool {
    &&& rep_wf(old, n)
    &&& rep_wf(new, n)
    &&& forall|i: int| 0 <= i < n ==> #[trigger] new[old[i]] == new[i]
}

pub proof fn lemma_coarsens_refl(rep: Seq<int>, n: int)
    requires
        rep_wf(rep, n),
    ensures
        coarsens(rep, rep, n),
{
}

pub proof fn lemma_coarsens_trans(a: Seq<int>, b: Seq<int>, c: Seq<int>, n: int)
    requires
        coarsens(a, b, n),
        coarsens(b, c, n),
    ensures
        coarsens(a, c, n),
{
    assert forall|i: int| 0 <= i < n implies #[trigger] c[a[i]] == c[i] by {
        assert(0 <= a[i] < n);
        assert(c[b[a[i]]] == c[a[i]]);
        assert(b[a[i]] == b[i]);
        assert(c[b[i]] == c[i]);
    }
}

/// Under a coarsening, canonicalising with the old representatives first
/// makes no difference, so e-nodes congruent before stay congruent.
pub proof fn lemma_coarsen_canon(old: Seq<int>, new: Seq<int>, n: int, x: Node)
    requires
        coarsens(old, new, n),
        in_range(x, n),
    ensures
        canon(new, canon(old, x)) == canon(new, x),
{
    let a = canon(new, canon(old, x));
    let b = canon(new, x);
    assert forall|j: int| 0 <= j < a.kids.len() implies #[trigger] a.kids[j] == b.kids[j] by {
        assert(0 <= x.kids[j] < n);
        assert(new[old[x.kids[j]]] == new[x.kids[j]]);
    }
    assert(a.kids =~= b.kids);
}

pub proof fn lemma_coarsen_congruent(old: Seq<int>, new: Seq<int>, n: int, x: Node, y: Node)
    requires
        coarsens(old, new, n),
        in_range(x, n),
        in_range(y, n),
        canon(old, x) == canon(old, y),
    ensures
        canon(new, x) == canon(new, y),
{
    lemma_coarsen_canon(old, new, n, x);
    lemma_coarsen_canon(old, new, n, y);
}

/// The stored e-nodes keep denoting the original terms across a coarsening.
pub proof fn lemma_same_terms_coarsen(terms: Seq<Node>, before: EG, after: EG, n: int)
    requires
        terms.len() == n,
        before.n() == n,
        after.n() == n,
        forall|i: int| 0 <= i < n ==> in_range(#[trigger] terms[i], n),
        forall|i: int| 0 <= i < n ==> in_range(#[trigger] before.nodes[i], n),
        coarsens(before.rep, after.rep, n),
        same_terms(terms, before),
        same_terms(before.nodes, after),
    ensures
        same_terms(terms, after),
{
    assert forall|i: int| 0 <= i < n implies #[trigger] canon(after.rep, after.nodes[i]) == canon(
        after.rep,
        terms[i],
    ) by {
        assert(canon(before.rep, before.nodes[i]) == canon(before.rep, terms[i]));
        lemma_coarsen_congruent(before.rep, after.rep, n, before.nodes[i], terms[i]);
    }
}

/// The number of classes.
pub open spec fn num_roots(s: EG) -> nat {
    set_int_range(0, s.n()).filter(|i: int| s.rep[i] == i).len()
}

// ---------------------------------------------------------------------------
// Merge.

/// Representatives after joining roots `ra` and `rb` under `ra`.
pub open spec fn joined(rep: Seq<int>, ra: int, rb: int) -> Seq<int> {
    Seq::new(rep.len(), |i: int| if rep[i] == ra || rep[i] == rb { ra } else { rep[i] })
}

pub open spec fn reads(s: EG, i: int, ra: int, rb: int) -> bool {
    exists|j: int|
        0 <= j < s.nodes[i].kids.len() && (s.rep[s.nodes[i].kids[j]] == ra || s.rep[
            s.nodes[i].kids[j]
        ] == rb)
}

/// The live e-nodes reading either class.
pub open spec fn readers(s: EG, ra: int, rb: int) -> Set<int> {
    set_int_range(0, s.n()).filter(|i: int| !s.dead[i] && reads(s, i, ra, rb))
}

pub open spec fn merge(s: EG, a: int, b: int) -> EG {
    let ra = s.rep[a];
    let rb = s.rep[b];
    if ra == rb {
        s
    } else {
        EG {
            rep: joined(s.rep, ra, rb),
            nodes: s.nodes,
            dead: s.dead,
            hc: s.hc,
            pending: s.pending + readers(s, ra, rb),
            terms: s.terms,
            eqs: s.eqs,
        }
    }
}

/// What a merge establishes, needing only the structural part of the
/// invariant, since repair merges while one e-node is briefly unregistered.
pub proof fn lemma_merge(s: EG, a: int, b: int)
    requires
        rep_wf(s.rep, s.n()),
        s.dead.len() == s.n(),
        forall|i: int| 0 <= i < s.n() ==> in_range(#[trigger] s.nodes[i], s.n()),
        0 <= a < s.n(),
        0 <= b < s.n(),
    ensures
        ({
            let m = merge(s, a, b);
            &&& coarsens(s.rep, m.rep, s.n())
            &&& m.rep[a] == m.rep[b]
            &&& m.nodes == s.nodes && m.dead == s.dead && m.hc == s.hc
            &&& m.terms == s.terms && m.eqs == s.eqs
            &&& s.pending.subset_of(m.pending)
            &&& forall|p: int| #[trigger] m.pending.contains(p) ==> s.pending.contains(p) || 0 <= p
                < s.n()
            // An e-node a merge leaves non-canonical was non-canonical already
            // or has been queued.
            &&& forall|i: int|
                0 <= i < s.n() && !s.dead[i] && !is_canonical(m.rep, #[trigger] s.nodes[i])
                    ==> !is_canonical(s.rep, s.nodes[i]) || m.pending.contains(i)
            &&& s.rep[a] != s.rep[b] ==> num_roots(m) + 1 == num_roots(s)
            &&& s.rep[a] == s.rep[b] ==> m == s
        }),
{
    let n = s.n();
    let ra = s.rep[a];
    let rb = s.rep[b];
    let m = merge(s, a, b);
    if ra == rb {
        assert forall|i: int| 0 <= i < n implies #[trigger] m.rep[s.rep[i]] == m.rep[i] by {}
        return;
    }
    let new = joined(s.rep, ra, rb);
    assert(rep_wf(new, n)) by {
        assert forall|i: int| 0 <= i < n implies 0 <= #[trigger] new[i] < n && new[new[i]]
            == new[i] by {
            assert(0 <= s.rep[i] < n);
        }
    }
    assert forall|i: int| 0 <= i < n implies #[trigger] new[s.rep[i]] == new[i] by {
        assert(0 <= s.rep[i] < n);
    }
    assert forall|i: int|
        0 <= i < n && !s.dead[i] && !is_canonical(new, #[trigger] s.nodes[i]) implies !is_canonical(
        s.rep,
        s.nodes[i],
    ) || m.pending.contains(i) by {
        let x = s.nodes[i];
        assert(in_range(x, n));
        if is_canonical(s.rep, x) {
            // Some child moved: it was a root that stopped being one, so it
            // was rb, and this e-node reads rb.
            let cn = canon(new, x);
            assert(!(cn.kids =~= x.kids));
            let j = choose|j: int| 0 <= j < x.kids.len() && cn.kids[j] != x.kids[j];
            assert(canon(s.rep, x).kids[j] == x.kids[j]);
            assert(s.rep[x.kids[j]] == x.kids[j]);
            assert(reads(s, i, ra, rb));
            assert(readers(s, ra, rb).contains(i));
        }
    }
    let roots = set_int_range(0, n).filter(|i: int| s.rep[i] == i);
    let new_roots = set_int_range(0, n).filter(|i: int| new[i] == i);
    assert(new_roots =~= roots.remove(rb));
    assert(roots.contains(rb));
}

/// A merge of two derivably equal ids keeps the representatives sound.
pub proof fn lemma_merge_sound(s: EG, a: int, b: int)
    requires
        rep_wf(s.rep, s.n()),
        s.terms.len() == s.n(),
        0 <= a < s.n(),
        0 <= b < s.n(),
        sound(s),
        equiv(s.terms, s.eqs, a, b),
        equiv(s.terms, s.eqs, b, a),
    ensures
        sound(merge(s, a, b)),
{
    let n = s.n();
    let m = merge(s, a, b);
    let ra = s.rep[a];
    let rb = s.rep[b];
    if ra == rb {
        return;
    }
    assert forall|i: int, j: int|
        0 <= i < n && 0 <= j < n && #[trigger] m.rep[i] == #[trigger] m.rep[j] implies equiv(
        s.terms,
        s.eqs,
        i,
        j,
    ) by {
        assert(0 <= s.rep[i] < n && 0 <= s.rep[j] < n);
        if s.rep[i] == s.rep[j] {
        } else if s.rep[i] == ra && s.rep[j] == rb {
            assert(s.rep[i] == s.rep[a]);
            assert(s.rep[b] == s.rep[j]);
            lemma_trans(s.terms, s.eqs, i, a, b);
            lemma_trans(s.terms, s.eqs, i, b, j);
        } else {
            assert(s.rep[i] == rb && s.rep[j] == ra);
            assert(s.rep[i] == s.rep[b]);
            assert(s.rep[a] == s.rep[j]);
            lemma_trans(s.terms, s.eqs, i, b, a);
            lemma_trans(s.terms, s.eqs, i, a, j);
        }
    }
}

// ---------------------------------------------------------------------------
// Emplace and union: the caller's operations.

pub open spec fn emplace(s: EG, x: Node) -> EG {
    if s.hc.dom().contains(x) {
        s
    } else {
        EG {
            rep: s.rep.push(s.n()),
            nodes: s.nodes.push(x),
            dead: s.dead.push(false),
            hc: s.hc.insert(x, s.n()),
            pending: s.pending,
            terms: s.terms.push(x),
            eqs: s.eqs,
        }
    }
}

pub proof fn lemma_emplace(s: EG, x: Node)
    requires
        inv(s),
        in_range(x, s.n()),
        is_canonical(s.rep, x),
    ensures
        inv(emplace(s, x)),
        emplace(s, x).hc.dom().contains(x),
{
    if s.hc.dom().contains(x) {
        return;
    }
    let e = emplace(s, x);
    let n = s.n();
    assert forall|i: int| 0 <= i < n + 1 implies 0 <= #[trigger] e.rep[i] < n + 1 && e.rep[e.rep[i]]
        == e.rep[i] by {
        if i < n {
            assert(0 <= s.rep[i] < n);
        }
    }
    // Old e-nodes canonicalise the same way: their children are old ids.
    assert forall|y: Node| in_range(y, n) implies #[trigger] canon(e.rep, y) == canon(s.rep, y) by {
        assert(canon(e.rep, y).kids =~= canon(s.rep, y).kids) by {
            assert forall|j: int| 0 <= j < y.kids.len() implies canon(e.rep, y).kids[j] == canon(
                s.rep,
                y,
            ).kids[j] by {
                assert(0 <= y.kids[j] < n);
            }
        }
    }
    assert forall|i: int| 0 <= i < n + 1 implies in_range(#[trigger] e.nodes[i], n + 1) by {
        if i < n {
            assert(in_range(s.nodes[i], n));
        }
    }
    assert forall|i: int| 0 <= i < n + 1 implies in_range(#[trigger] e.terms[i], n + 1) by {
        if i < n {
            assert(in_range(s.terms[i], n));
        }
    }
    assert(canon(e.rep, x) == canon(s.rep, x));
    assert forall|i: int|
        0 <= i < n + 1 && !e.dead[i] && !is_canonical(e.rep, #[trigger] e.nodes[i]) implies e.pending.contains(
        i,
    ) by {
        assert(i < n);
        assert(in_range(s.nodes[i], n));
    }
    assert forall|i: int|
        0 <= i < n + 1 && !e.dead[i] implies e.hc.dom().contains(#[trigger] e.nodes[i]) && e.hc[e.nodes[i]]
        == i by {
        if i < n {
            assert(s.hc.dom().contains(s.nodes[i]));
            assert(s.nodes[i] != x);
        }
    }
    assert forall|y: Node| #[trigger] e.hc.dom().contains(y) implies 0 <= e.hc[y] < n + 1 && !e.dead[e.hc[y]]
        && e.nodes[e.hc[y]] == y by {
        if y != x {
            assert(s.hc.dom().contains(y));
        }
    }
    assert forall|d: int| 0 <= d < n + 1 && #[trigger] e.dead[d] implies has_witness(e, d) by {
        assert(d < n);
        assert(has_witness(s, d));
        let k = choose|k: int|
            0 <= k < n && !s.dead[k] && s.rep[k] == s.rep[d] && canon(s.rep, s.nodes[d]) == canon(
                s.rep,
                s.nodes[k],
            );
        assert(in_range(s.nodes[d], n));
        assert(in_range(s.nodes[k], n));
        assert(canon(e.rep, e.nodes[d]) == canon(e.rep, e.nodes[k]));
    }
    assert forall|i: int| 0 <= i < n + 1 implies #[trigger] canon(e.rep, e.nodes[i]) == canon(
        e.rep,
        e.terms[i],
    ) by {
        if i < n {
            assert(in_range(s.nodes[i], n));
            assert(in_range(s.terms[i], n));
            assert(canon(s.rep, s.nodes[i]) == canon(s.rep, s.terms[i]));
        }
    }
    // Old derivations still hold with one more term; the new id is alone in
    // its class.
    assert forall|i: int, j: int|
        0 <= i < n + 1 && 0 <= j < n + 1 && #[trigger] e.rep[i] == #[trigger] e.rep[j] implies equiv(
        e.terms,
        e.eqs,
        i,
        j,
    ) by {
        if i == j {
            lemma_refl(e.terms, e.eqs, i);
        } else {
            assert(i < n && j < n);
            assert(equiv(s.terms, s.eqs, i, j));
            lemma_equiv_push_term(s.terms, s.eqs, x, i, j);
        }
    }
}

/// The caller's merge: records the equation, then merges.
pub open spec fn union(s: EG, a: int, b: int) -> EG {
    let t = EG {
        rep: s.rep,
        nodes: s.nodes,
        dead: s.dead,
        hc: s.hc,
        pending: s.pending,
        terms: s.terms,
        eqs: s.eqs.push((a, b)),
    };
    merge(t, a, b)
}

pub proof fn lemma_union(s: EG, a: int, b: int)
    requires
        inv(s),
        0 <= a < s.n(),
        0 <= b < s.n(),
    ensures
        inv(union(s, a, b)),
        union(s, a, b).rep[a] == union(s, a, b).rep[b],
{
    let n = s.n();
    let t = EG {
        rep: s.rep,
        nodes: s.nodes,
        dead: s.dead,
        hc: s.hc,
        pending: s.pending,
        terms: s.terms,
        eqs: s.eqs.push((a, b)),
    };
    let u = merge(t, a, b);
    // Soundness carries over to the longer equation list, and the new
    // equation justifies the merge.
    assert(sound(t)) by {
        assert forall|i: int, j: int|
            0 <= i < n && 0 <= j < n && #[trigger] t.rep[i] == #[trigger] t.rep[j] implies equiv(
            t.terms,
            t.eqs,
            i,
            j,
        ) by {
            assert(equiv(s.terms, s.eqs, i, j));
            lemma_equiv_push_eq(s.terms, s.eqs, (a, b), i, j);
        }
    }
    lemma_eq(t.terms, t.eqs, s.eqs.len() as int);
    lemma_merge(t, a, b);
    lemma_merge_sound(t, a, b);
    assert forall|i: int|
        0 <= i < n && !u.dead[i] && !is_canonical(u.rep, #[trigger] u.nodes[i]) implies u.pending.contains(
        i,
    ) by {
        if !is_canonical(s.rep, s.nodes[i]) {
            assert(s.pending.contains(i));
        }
    }
    assert forall|d: int| 0 <= d < n && #[trigger] u.dead[d] implies has_witness(u, d) by {
        assert(has_witness(s, d));
        let k = choose|k: int|
            0 <= k < n && !s.dead[k] && s.rep[k] == s.rep[d] && canon(s.rep, s.nodes[d]) == canon(
                s.rep,
                s.nodes[k],
            );
        assert(in_range(s.nodes[d], n));
        assert(in_range(s.nodes[k], n));
        lemma_coarsen_congruent(s.rep, u.rep, n, s.nodes[d], s.nodes[k]);
    }
    assert(same_terms(t.nodes, u)) by {
        assert forall|i: int| 0 <= i < n implies #[trigger] canon(u.rep, u.nodes[i]) == canon(
            u.rep,
            t.nodes[i],
        ) by {}
    }
    lemma_same_terms_coarsen(s.terms, t, u, n);
    assert forall|e: int| 0 <= e < u.eqs.len() implies 0 <= (#[trigger] u.eqs[e]).0 < n && 0 <= u.eqs[e].1
        < n && u.rep[u.eqs[e].0] == u.rep[u.eqs[e].1] by {
        if e < s.eqs.len() {
            assert(s.eqs[e] == u.eqs[e]);
            assert(s.rep[s.eqs[e].0] == s.rep[s.eqs[e].1]);
        }
    }
}

// ---------------------------------------------------------------------------
// Repair: one step of rebuild.

pub open spec fn repair(s: EG, p: int) -> EG {
    let s1 = EG {
        rep: s.rep,
        nodes: s.nodes,
        dead: s.dead,
        hc: s.hc,
        pending: s.pending.remove(p),
        terms: s.terms,
        eqs: s.eqs,
    };
    if s.dead[p] {
        s1
    } else {
        let old = s.nodes[p];
        let x = canon(s.rep, old);
        let s2 = EG {
            rep: s1.rep,
            nodes: s1.nodes.update(p, x),
            dead: s1.dead,
            hc: s1.hc.remove(old),
            pending: s1.pending,
            terms: s1.terms,
            eqs: s1.eqs,
        };
        if s2.hc.dom().contains(x) {
            let s3 = merge(s2, s2.hc[x], p);
            EG {
                rep: s3.rep,
                nodes: s3.nodes,
                dead: s3.dead.update(p, true),
                hc: s3.hc,
                pending: s3.pending,
                terms: s3.terms,
                eqs: s3.eqs,
            }
        } else {
            EG {
                rep: s2.rep,
                nodes: s2.nodes,
                dead: s2.dead,
                hc: s2.hc.insert(x, p),
                pending: s2.pending,
                terms: s2.terms,
                eqs: s2.eqs,
            }
        }
    }
}

/// Lexicographic: fewer classes, or as many classes and fewer pending.
pub open spec fn smaller(t: EG, s: EG) -> bool {
    num_roots(t) < num_roots(s) || (num_roots(t) == num_roots(s) && t.pending.len() < s.pending.len())
}

pub proof fn lemma_repair(s: EG, p: int)
    requires
        inv(s),
        s.pending.contains(p),
    ensures
        inv(repair(s, p)),
        smaller(repair(s, p), s),
        repair(s, p).n() == s.n(),
        coarsens(s.rep, repair(s, p).rep, s.n()),
        same_terms(s.nodes, repair(s, p)),
        repair(s, p).terms == s.terms,
        repair(s, p).eqs == s.eqs,
{
    let n = s.n();
    let r = repair(s, p);
    assert(0 <= p < n);
    assert(s.pending.remove(p).len() + 1 == s.pending.len());
    if s.dead[p] {
        assert forall|d: int| 0 <= d < n && #[trigger] r.dead[d] implies has_witness(r, d) by {
            assert(has_witness(s, d));
        }
        lemma_coarsens_refl(s.rep, n);
        assert(r.nodes == s.nodes);
        return;
    }
    let old = s.nodes[p];
    let x = canon(s.rep, old);
    assert(in_range(old, n));
    lemma_canon_in_range(s.rep, n, old);
    lemma_canon_idempotent(s.rep, n, old);
    assert(s.hc.dom().contains(old) && s.hc[old] == p);
    let s2 = EG {
        rep: s.rep,
        nodes: s.nodes.update(p, x),
        dead: s.dead,
        hc: s.hc.remove(old),
        pending: s.pending.remove(p),
        terms: s.terms,
        eqs: s.eqs,
    };
    assert forall|i: int| 0 <= i < n implies in_range(#[trigger] s2.nodes[i], n) by {
        if i != p {
            assert(in_range(s.nodes[i], n));
        }
    }
    // No other live e-node had the old content: the hashcons maps it to p.
    assert forall|i: int| 0 <= i < n && i != p && !s.dead[i] implies #[trigger] s.nodes[i] != old by {
        assert(s.hc[s.nodes[i]] == i);
    }

    if s2.hc.dom().contains(x) {
        let kept = s2.hc[x];
        assert(s.hc.dom().contains(x) && x != old);
        assert(0 <= kept < n && !s.dead[kept] && s.nodes[kept] == x);
        assert(kept != p);
        // The merge is forced: the original terms of kept and p have the same
        // shape and pairwise equivalent children.
        assert(canon(s.rep, s.terms[kept]) == canon(s.rep, s.terms[p])) by {
            assert(canon(s.rep, s.nodes[kept]) == canon(s.rep, s.terms[kept]));
            assert(canon(s.rep, s.nodes[p]) == canon(s.rep, s.terms[p]));
            assert(canon(s.rep, x) == x);
        }
        let tk = s.terms[kept];
        let tp = s.terms[p];
        assert(in_range(tk, n) && in_range(tp, n));
        assert(tk.sym == tp.sym && tk.dis == tp.dis && tk.kids.len() == tp.kids.len()) by {
            assert(canon(s.rep, tk).kids.len() == canon(s.rep, tp).kids.len());
        }
        assert forall|k: int| 0 <= k < tk.kids.len() implies #[trigger] equiv(
            s.terms,
            s.eqs,
            tk.kids[k],
            tp.kids[k],
        ) by {
            assert(canon(s.rep, tk).kids[k] == canon(s.rep, tp).kids[k]);
            assert(0 <= tk.kids[k] < n && 0 <= tp.kids[k] < n);
            assert(s.rep[tk.kids[k]] == s.rep[tp.kids[k]]);
        }
        assert forall|k: int| 0 <= k < tp.kids.len() implies #[trigger] equiv(
            s.terms,
            s.eqs,
            tp.kids[k],
            tk.kids[k],
        ) by {
            assert(canon(s.rep, tk).kids[k] == canon(s.rep, tp).kids[k]);
            assert(0 <= tk.kids[k] < n && 0 <= tp.kids[k] < n);
            assert(s.rep[tk.kids[k]] == s.rep[tp.kids[k]]);
        }
        lemma_cong(s.terms, s.eqs, kept, p);
        lemma_cong(s.terms, s.eqs, p, kept);

        lemma_merge(s2, kept, p);
        lemma_merge_sound(s2, kept, p);
        let s3 = merge(s2, kept, p);
        assert(coarsens(s.rep, s3.rep, n));
        assert forall|i: int| 0 <= i < n implies #[trigger] canon(r.rep, r.nodes[i]) == canon(
            r.rep,
            s.nodes[i],
        ) by {
            if i == p {
                lemma_coarsen_canon(s.rep, r.rep, n, old);
            }
        }
        assert(r.rep == s3.rep && r.nodes == s2.nodes && r.hc == s2.hc && r.pending == s3.pending);
        assert(r.dead == s.dead.update(p, true));
        if s.rep[kept] == s.rep[p] {
            assert(s3 == s2);
        }
        assert forall|i: int|
            0 <= i < n && !r.dead[i] implies r.hc.dom().contains(#[trigger] r.nodes[i]) && r.hc[r.nodes[i]]
            == i by {
            assert(i != p);
            assert(s.nodes[i] != old);
            assert(s.hc.dom().contains(s.nodes[i]));
        }
        assert forall|y: Node| #[trigger] r.hc.dom().contains(y) implies 0 <= r.hc[y] < n && !r.dead[r.hc[y]]
            && r.nodes[r.hc[y]] == y by {
            assert(s.hc.dom().contains(y) && y != old);
            assert(s.hc[y] != p);
        }
        assert forall|i: int|
            0 <= i < n && !r.dead[i] && !is_canonical(r.rep, #[trigger] r.nodes[i]) implies r.pending.contains(
            i,
        ) by {
            assert(i != p);
            assert(s2.nodes[i] == s.nodes[i]);
            if !is_canonical(s.rep, s.nodes[i]) {
                assert(s.pending.contains(i));
            }
        }
        assert forall|d: int| 0 <= d < n && #[trigger] r.dead[d] implies has_witness(r, d) by {
            if d == p {
                lemma_canon_idempotent(s.rep, n, old);
                assert(canon(r.rep, r.nodes[p]) == canon(r.rep, r.nodes[kept]));
                assert(r.rep[p] == r.rep[kept]);
            } else {
                assert(has_witness(s, d));
                let k = choose|k: int|
                    0 <= k < n && !s.dead[k] && s.rep[k] == s.rep[d] && canon(s.rep, s.nodes[d])
                        == canon(s.rep, s.nodes[k]);
                assert(in_range(s.nodes[d], n));
                assert(in_range(s.nodes[k], n));
                if k == p {
                    // p is gone; kept is congruent to it and in its class.
                    lemma_coarsen_congruent(s.rep, r.rep, n, s.nodes[d], old);
                    lemma_coarsen_canon(s.rep, r.rep, n, old);
                    assert(canon(r.rep, r.nodes[d]) == canon(r.rep, r.nodes[kept]));
                    assert(r.rep[d] == r.rep[kept]);
                } else {
                    lemma_coarsen_congruent(s.rep, r.rep, n, s.nodes[d], s.nodes[k]);
                    assert(r.rep[d] == r.rep[k]);
                }
            }
        }
        lemma_same_terms_coarsen(s.terms, s, r, n);
        assert forall|e: int| 0 <= e < r.eqs.len() implies 0 <= (#[trigger] r.eqs[e]).0 < n && 0
            <= r.eqs[e].1 < n && r.rep[r.eqs[e].0] == r.rep[r.eqs[e].1] by {
            assert(s.rep[s.eqs[e].0] == s.rep[s.eqs[e].1]);
            assert(0 <= s.eqs[e].0 < n && 0 <= s.eqs[e].1 < n);
            assert(r.rep[s.rep[s.eqs[e].0]] == r.rep[s.eqs[e].0]);
            assert(r.rep[s.rep[s.eqs[e].1]] == r.rep[s.eqs[e].1]);
        }
        assert(sound(r)) by {
            assert(sound(s3));
        }
    } else {
        assert(r.rep == s.rep && r.dead == s.dead && r.nodes == s2.nodes);
        assert(r.hc == s.hc.remove(old).insert(x, p));
        assert(r.pending == s.pending.remove(p));
        lemma_coarsens_refl(s.rep, n);
        assert forall|i: int| 0 <= i < n implies #[trigger] canon(r.rep, r.nodes[i]) == canon(
            r.rep,
            s.nodes[i],
        ) by {
            if i == p {
                lemma_canon_idempotent(s.rep, n, old);
            }
        }
        lemma_same_terms_coarsen(s.terms, s, r, n);
        assert(num_roots(r) == num_roots(s)) by {
            assert(set_int_range(0, n).filter(|i: int| r.rep[i] == i) =~= set_int_range(0, n).filter(
                |i: int| s.rep[i] == i,
            ));
        }
        assert forall|i: int|
            0 <= i < n && !r.dead[i] implies r.hc.dom().contains(#[trigger] r.nodes[i]) && r.hc[r.nodes[i]]
            == i by {
            if i != p {
                assert(s.nodes[i] != old);
                assert(s.hc.dom().contains(s.nodes[i]));
                if s.nodes[i] == x {
                    assert(s.hc.dom().contains(x));
                }
            }
        }
        assert forall|y: Node| #[trigger] r.hc.dom().contains(y) implies 0 <= r.hc[y] < n && !r.dead[r.hc[y]]
            && r.nodes[r.hc[y]] == y by {
            if y != x {
                assert(s.hc.dom().contains(y) && y != old);
                assert(s.hc[y] != p);
            }
        }
        assert forall|i: int|
            0 <= i < n && !r.dead[i] && !is_canonical(r.rep, #[trigger] r.nodes[i]) implies r.pending.contains(
            i,
        ) by {
            assert(i != p);
            assert(s.pending.contains(i));
        }
        assert forall|d: int| 0 <= d < n && #[trigger] r.dead[d] implies has_witness(r, d) by {
            assert(d != p);
            assert(has_witness(s, d));
            let k = choose|k: int|
                0 <= k < n && !s.dead[k] && s.rep[k] == s.rep[d] && canon(s.rep, s.nodes[d]) == canon(
                    s.rep,
                    s.nodes[k],
                );
            if k == p {
                assert(canon(r.rep, r.nodes[p]) == canon(s.rep, old));
            }
            assert(canon(r.rep, r.nodes[d]) == canon(r.rep, r.nodes[k]));
        }
    }
}

// ---------------------------------------------------------------------------
// Rebuild.

/// With nothing awaiting repair, the invariant gives congruence: congruent
/// live e-nodes are canonical, hence identical, hence the same hashcons entry;
/// a dead e-node stands for a congruent live one of its class.
pub proof fn lemma_closed(s: EG)
    requires
        inv(s),
        s.pending.is_empty(),
    ensures
        congruence_closed(s),
{
    let n = s.n();
    assert forall|i: int, j: int|
        0 <= i < n && 0 <= j < n && #[trigger] canon(s.rep, s.nodes[i]) == #[trigger] canon(
            s.rep,
            s.nodes[j],
        ) implies s.rep[i] == s.rep[j] by {
        let li = if s.dead[i] {
            assert(has_witness(s, i));
            choose|k: int|
                0 <= k < n && !s.dead[k] && s.rep[k] == s.rep[i] && canon(s.rep, s.nodes[i]) == canon(
                    s.rep,
                    s.nodes[k],
                )
        } else {
            i
        };
        let lj = if s.dead[j] {
            assert(has_witness(s, j));
            choose|k: int|
                0 <= k < n && !s.dead[k] && s.rep[k] == s.rep[j] && canon(s.rep, s.nodes[j]) == canon(
                    s.rep,
                    s.nodes[k],
                )
        } else {
            j
        };
        assert(!s.pending.contains(li) && !s.pending.contains(lj));
        assert(is_canonical(s.rep, s.nodes[li]));
        assert(is_canonical(s.rep, s.nodes[lj]));
        assert(s.nodes[li] == s.nodes[lj]);
        assert(s.hc[s.nodes[li]] == li && s.hc[s.nodes[lj]] == lj);
    }
}

/// With nothing awaiting repair, "same representative" is an equivalence that
/// contains the caller's merges and is closed under congruence, so it contains
/// the least such relation.
pub proof fn lemma_rep_closed(s: EG)
    requires
        inv(s),
        s.pending.is_empty(),
    ensures
        closed_rel(s.terms, s.eqs, |a: int, b: int| s.rep[a] == s.rep[b]),
{
    let r = |a: int, b: int| s.rep[a] == s.rep[b];
    let n = s.n();
    lemma_closed(s);
    assert forall|e: int| 0 <= e < s.eqs.len() implies #[trigger] r(s.eqs[e].0, s.eqs[e].1) by {
        assert(s.rep[s.eqs[e].0] == s.rep[s.eqs[e].1]);
    }
    assert forall|i: int, j: int|
        0 <= i < n && 0 <= j < n && #[trigger] cong_args(s.terms, r, i, j) implies r(i, j) by {
        let ti = s.terms[i];
        let tj = s.terms[j];
        assert forall|k: int| 0 <= k < ti.kids.len() implies canon(s.rep, ti).kids[k] == canon(
            s.rep,
            tj,
        ).kids[k] by {
            assert(r(ti.kids[k], tj.kids[k]));
        }
        assert(canon(s.rep, ti).kids =~= canon(s.rep, tj).kids);
        assert(canon(s.rep, ti) == canon(s.rep, tj));
        assert(canon(s.rep, s.nodes[i]) == canon(s.rep, ti));
        assert(canon(s.rep, s.nodes[j]) == canon(s.rep, tj));
    }
}

pub proof fn lemma_exact(s: EG)
    requires
        inv(s),
        s.pending.is_empty(),
    ensures
        exact(s),
{
    lemma_rep_closed(s);
    let r = |a: int, b: int| s.rep[a] == s.rep[b];
    assert forall|i: int, j: int| 0 <= i < s.n() && 0 <= j < s.n() implies (#[trigger] s.rep[i]
        == #[trigger] s.rep[j] <==> equiv(s.terms, s.eqs, i, j)) by {
        if equiv(s.terms, s.eqs, i, j) {
            assert(r(i, j));
        }
    }
}

/// Rebuild repairs pending e-nodes until none is left.
pub open spec fn rebuild(s: EG) -> EG
    decreases num_roots(s), s.pending.len(),
    when inv(s)
    via rebuild_decreases
{
    if s.pending.is_empty() {
        s
    } else {
        rebuild(repair(s, s.pending.choose()))
    }
}

#[via_fn]
proof fn rebuild_decreases(s: EG) {
    if !s.pending.is_empty() {
        lemma_repair(s, s.pending.choose());
    }
}

/// From any state satisfying the invariant, rebuild terminates (the recursion
/// above is well founded) and leaves the invariant holding, nothing pending,
/// the e-graph congruence-closed and exact: two terms share a class exactly
/// when the caller's merges and congruence force them together. It only ever
/// joins classes, and the stored e-nodes still denote the original terms.
pub proof fn lemma_rebuild(s: EG)
    requires
        inv(s),
    ensures
        inv(rebuild(s)),
        rebuild(s).pending.is_empty(),
        congruence_closed(rebuild(s)),
        exact(rebuild(s)),
        rebuild(s).n() == s.n(),
        rebuild(s).terms == s.terms,
        rebuild(s).eqs == s.eqs,
        coarsens(s.rep, rebuild(s).rep, s.n()),
        same_terms(s.nodes, rebuild(s)),
    decreases num_roots(s), s.pending.len(),
{
    if s.pending.is_empty() {
        lemma_closed(s);
        lemma_coarsens_refl(s.rep, s.n());
        lemma_exact(s);
    } else {
        let p = s.pending.choose();
        let r = repair(s, p);
        lemma_repair(s, p);
        lemma_rebuild(r);
        let f = rebuild(s);
        lemma_coarsens_trans(s.rep, r.rep, f.rep, s.n());
        assert forall|i: int| 0 <= i < s.n() implies #[trigger] canon(f.rep, f.nodes[i]) == canon(
            f.rep,
            s.nodes[i],
        ) by {
            assert(canon(f.rep, f.nodes[i]) == canon(f.rep, r.nodes[i]));
            assert(canon(r.rep, r.nodes[i]) == canon(r.rep, s.nodes[i]));
            assert(in_range(r.nodes[i], s.n()));
            assert(in_range(s.nodes[i], s.n()));
            lemma_coarsen_congruent(r.rep, f.rep, s.n(), r.nodes[i], s.nodes[i]);
        }
    }
}

// ---------------------------------------------------------------------------
// The invariant is satisfiable, and the lemmas compose on a concrete run.

pub open spec fn empty_graph() -> EG {
    EG {
        rep: Seq::empty(),
        nodes: Seq::empty(),
        dead: Seq::empty(),
        hc: Map::empty(),
        pending: Set::empty(),
        terms: Seq::empty(),
        eqs: Seq::empty(),
    }
}

pub proof fn lemma_empty_inv()
    ensures
        inv(empty_graph()),
{
}

pub open spec fn leaf(sym: int, dis: int) -> Node {
    Node { sym, dis, kids: Seq::empty() }
}

pub open spec fn unary(sym: int, kid: int) -> Node {
    Node { sym, dis: 0, kids: seq![kid] }
}

/// Leaves a and b, then F(a) and F(b); merge a with b; rebuild. F(a) and F(b)
/// end in one class, which congruence forces and no merge stated directly.
/// a and F(a) stay apart: nothing forces them together, and exactness rules
/// out merging what is not forced.
pub proof fn lemma_example()
{
    let g0 = empty_graph();
    lemma_empty_inv();
    let a = leaf(0, 0);
    assert(canon(g0.rep, a).kids =~= a.kids);
    lemma_emplace(g0, a);
    let g1 = emplace(g0, a);
    let b = leaf(0, 1);
    assert(canon(g1.rep, b).kids =~= b.kids);
    lemma_emplace(g1, b);
    let g2 = emplace(g1, b);
    let fa = unary(1, 0);
    assert(canon(g2.rep, fa).kids =~= fa.kids);
    lemma_emplace(g2, fa);
    let g3 = emplace(g2, fa);
    let fb = unary(1, 1);
    assert(canon(g3.rep, fb).kids =~= fb.kids);
    assert(fb.kids[0] != fa.kids[0]);
    assert(fb != fa);
    lemma_emplace(g3, fb);
    let g4 = emplace(g3, fb);
    assert(g4.n() == 4);
    assert(g4.nodes[2] == fa && g4.nodes[3] == fb);

    lemma_union(g4, 0, 1);
    let g5 = union(g4, 0, 1);
    lemma_rebuild(g5);
    let g6 = rebuild(g5);
    assert(g6.rep[0] == g6.rep[1]);
    // Rebuild rewrote the stored e-nodes, but not the terms they denote.
    assert(canon(g6.rep, g6.nodes[2]) == canon(g6.rep, fa));
    assert(canon(g6.rep, g6.nodes[3]) == canon(g6.rep, fb));
    assert(canon(g6.rep, fa) == canon(g6.rep, fb)) by {
        assert(canon(g6.rep, fa).kids =~= canon(g6.rep, fb).kids);
    }
    assert(g6.rep[2] == g6.rep[3]);

    // A relation that is closed in every required way yet keeps the leaves
    // apart from the F terms, so the least such relation does too.
    let r = |x: int, y: int| x == y || (0 <= x <= 1 && 0 <= y <= 1) || (2 <= x <= 3 && 2 <= y <= 3);
    assert(g6.terms == g5.terms && g5.terms == g4.terms);
    assert(g6.eqs == g5.eqs && g5.eqs == seq![(0int, 1int)]);
    assert(g4.terms =~= seq![a, b, fa, fb]);
    assert(closed_rel(g6.terms, g6.eqs, r)) by {
        assert forall|i: int, j: int|
            0 <= i < g6.terms.len() && 0 <= j < g6.terms.len() && #[trigger] cong_args(g6.terms, r, i, j) implies r(
            i,
            j,
        ) by {
            if g6.terms[i].kids.len() == 1 {
                assert(2 <= i <= 3 && 2 <= j <= 3);
            } else {
                assert(0 <= i <= 1 && 0 <= j <= 1);
            }
        }
    }
    assert(!equiv(g6.terms, g6.eqs, 0, 2));
    assert(g6.rep[0] != g6.rep[2]);
}

fn main() {
}

} // verus!
