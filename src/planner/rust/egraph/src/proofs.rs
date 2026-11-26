//! Inductive-step proofs, checked with `cargo kani`.
//!
//! Each harness starts from an arbitrary state that satisfies the invariant,
//! applies one operation, and checks the invariant and the operation's effect.
//! Because the starting state is arbitrary rather than built by a sequence of
//! calls, a passing harness covers every sequence of operations, of any
//! length, over states of the bounded size.

use super::*;

/// Elements in the union-find states the harnesses range over.
const N: usize = 4;

/// The union-find invariant: parents are in range, and rank strictly
/// increases along every link from a non-root to its parent, which makes the
/// parent links acyclic.
fn wf(parent: &[u32; N], rank: &[u8; N]) -> bool {
    (0..N).all(|i| {
        let p = parent[i] as usize;
        p < N && (p == i || rank[i] < rank[p])
    })
}

/// The representative of `i`: following parents N times always reaches it,
/// since rank strictly increases along each link and there are N elements.
fn root_of(parent: &[u32; N], i: usize) -> usize {
    let mut x = i;
    for _ in 0..N {
        x = parent[x] as usize;
    }
    x
}

fn any_wf_state() -> ([u32; N], [u8; N]) {
    let parent: [u32; N] = kani::any();
    let rank: [u8; N] = kani::any();
    kani::assume(wf(&parent, &rank));
    (parent, rank)
}

fn union_find_of(parent: &[u32; N], rank: &[u8; N]) -> UnionFind {
    UnionFind { parent: parent.iter().map(|&p| Cell::new(p)).collect(), rank: rank.to_vec() }
}

fn state_of(uf: &UnionFind) -> ([u32; N], [u8; N]) {
    let mut parent = [0u32; N];
    let mut rank = [0u8; N];
    for i in 0..N {
        parent[i] = uf.parent[i].get();
        rank[i] = uf.rank[i];
    }
    (parent, rank)
}

/// `find` answers the representative, keeps the invariant, and leaves every
/// element's representative where it was: path halving only shortens paths.
#[kani::proof]
#[kani::unwind(6)]
fn find_answers_the_root_and_moves_no_root() {
    let (parent, rank) = any_wf_state();
    let x: usize = kani::any();
    kani::assume(x < N);
    let uf = union_find_of(&parent, &rank);

    let r = uf.find(x as u32) as usize;
    kani::cover!(root_of(&parent, x) != parent[x] as usize, "x is two or more links below its root");
    kani::cover!(uf.parent[x].get() != parent[x], "path halving moved x's link");

    assert_eq!(r, root_of(&parent, x));
    let (after_parent, after_rank) = state_of(&uf);
    assert!(wf(&after_parent, &after_rank));
    assert_eq!(after_rank, rank);
    for i in 0..N {
        assert_eq!(root_of(&after_parent, i), root_of(&parent, i));
    }
}

/// Linking two distinct roots keeps the invariant, gives every member of
/// either class the surviving root, and leaves every other element alone.
#[kani::proof]
#[kani::unwind(6)]
fn union_roots_joins_exactly_the_two_classes() {
    let (parent, rank) = any_wf_state();
    let (a, b): (usize, usize) = (kani::any(), kani::any());
    kani::assume(a < N && b < N && a != b);
    kani::assume(parent[a] as usize == a && parent[b] as usize == b);
    // Ranks stay below 32 for any state the operations reach; the Verus proof
    // establishes that bound, so here it is assumed rather than re-proved.
    kani::assume(rank[a] < 32 && rank[b] < 32);
    let mut uf = union_find_of(&parent, &rank);

    let root = uf.union_roots(a as u32, b as u32) as usize;
    kani::cover!(rank[a] == rank[b], "equal ranks: the survivor's rank grows");
    kani::cover!(rank[a] != rank[b], "unequal ranks");
    kani::cover!((0..N).any(|i| i != a && i != b && root_of(&parent, i) == b), "a third element moves with b's class");

    assert!(root == a || root == b);
    let (after_parent, after_rank) = state_of(&uf);
    assert!(wf(&after_parent, &after_rank));
    for i in 0..N {
        let before = root_of(&parent, i);
        let expected = if before == a || before == b { root } else { before };
        assert_eq!(root_of(&after_parent, i), expected);
    }
}

/// A new element is its own root, and nobody else's root changes.
#[kani::proof]
#[kani::unwind(6)]
fn make_set_adds_a_singleton() {
    let parent: [u32; N - 1] = kani::any();
    let rank: [u8; N - 1] = kani::any();
    let mut full_parent = [0u32; N];
    let mut full_rank = [0u8; N];
    full_parent[..N - 1].copy_from_slice(&parent);
    full_rank[..N - 1].copy_from_slice(&rank);
    // Element N-1 does not exist yet: give it a self-link so `wf` reads the
    // first N-1 elements alone.
    full_parent[N - 1] = (N - 1) as u32;
    kani::assume(wf(&full_parent, &full_rank));
    kani::assume(parent.iter().all(|&p| (p as usize) < N - 1));
    let mut uf = UnionFind {
        parent: parent.iter().map(|&p| Cell::new(p)).collect(),
        rank: rank.to_vec(),
    };

    let id = uf.make_set() as usize;

    assert_eq!(id, N - 1);
    let (after_parent, after_rank) = state_of(&uf);
    assert!(wf(&after_parent, &after_rank));
    assert_eq!(after_rank[id], 0);
    assert_eq!(root_of(&after_parent, id), id);
    for i in 0..N - 1 {
        assert_eq!(root_of(&after_parent, i), root_of(&full_parent, i));
    }
}
