// Union-find with union by rank and path halving, the algorithm of the e-graph
// port's union-find, proved correct with Verus.
//
// Each element's representative is tracked as ghost state, `roots`. The proofs
// establish that `find` returns the representative and that path halving
// changes nobody's representative, that `union_roots` joins exactly the two
// classes, and that ranks stay below 32, so the one-byte rank field cannot
// overflow for any number of elements a u32 id can name.
//
// Check with: verus src/planner/rust/verus/union_find.rs

use vstd::arithmetic::power2::*;
use vstd::prelude::*;
use vstd::set_lib::*;

verus! {

broadcast use {vstd::set::group_set_lemmas, vstd::set_lib::group_set_lib_default};

/// The elements whose representative is `r`.
pub open spec fn class(roots: Seq<int>, r: int) -> Set<int> {
    set_int_range(0, roots.len() as int).filter(|j: int| roots[j] == r)
}

pub open spec fn is_root(parent: Seq<u32>, i: int) -> bool {
    parent[i] as int == i
}

/// The representation invariant, over the three sequences that make up a
/// union-find: parent links, ranks and (ghost) representatives.
pub open spec fn wf_parts(parent: Seq<u32>, rank: Seq<u8>, roots: Seq<int>) -> bool {
    let n = parent.len() as int;
    &&& rank.len() == n
    &&& roots.len() == n
    &&& n <= u32::MAX
    &&& forall|i: int| 0 <= i < n ==> #[trigger] parent[i] < n
    &&& forall|i: int| 0 <= i < n ==> 0 <= #[trigger] roots[i] < n && is_root(parent, roots[i])
    &&& forall|i: int| 0 <= i < n ==> roots[#[trigger] parent[i] as int] == roots[i]
    &&& forall|i: int| 0 <= i < n && #[trigger] parent[i] as int == i ==> roots[i] == i
    &&& forall|i: int|
        0 <= i < n && #[trigger] parent[i] as int != i ==> rank[i] < rank[parent[i] as int]
    &&& forall|r: int|
        0 <= r < n && #[trigger] parent[r] as int == r ==> pow2(rank[r] as nat) <= class(
            roots,
            r,
        ).len()
}

/// Every rank is below 32: a root of rank r has at least 2^r members, and
/// there are fewer than 2^32 elements.
pub proof fn lemma_rank_below_32(parent: Seq<u32>, rank: Seq<u8>, roots: Seq<int>, r: int)
    requires
        wf_parts(parent, rank, roots),
        0 <= r < parent.len(),
        is_root(parent, r),
    ensures
        rank[r] < 32,
{
    let n = parent.len() as int;
    assert(parent[r] as int == r);
    lemma_int_range(0, n);
    lemma_len_subset(class(roots, r), set_int_range(0, n));
    lemma2_to64();
    if rank[r] >= 32 {
        lemma_pow2_strictly_increases(32, rank[r] as nat);
    }
}

/// Pointing a non-root at its grandparent keeps the invariant and every
/// representative.
pub proof fn lemma_halve(parent: Seq<u32>, rank: Seq<u8>, roots: Seq<int>, cur: int)
    requires
        wf_parts(parent, rank, roots),
        0 <= cur < parent.len(),
        !is_root(parent, cur),
    ensures
        wf_parts(parent.update(cur, parent[parent[cur] as int]), rank, roots),
{
    let n = parent.len() as int;
    let p = parent[cur] as int;
    let gp = parent[p];
    let next = parent.update(cur, gp);
    assert(parent[cur] as int != cur);
    assert(rank[cur] < rank[p]);
    assert(roots[p] == roots[cur]);
    assert(gp < n);
    assert(roots[gp as int] == roots[p]);
    if parent[p] as int != p {
        assert(rank[p] < rank[gp as int]);
    }
    assert(gp as int != cur);
    assert forall|i: int| 0 <= i < n implies #[trigger] next[i] < n by {
        if i != cur {
            assert(parent[i] < n);
        }
    }
    assert forall|i: int| 0 <= i < n implies 0 <= #[trigger] roots[i] < n && is_root(
        next,
        roots[i],
    ) by {
        assert(is_root(parent, roots[i]));
        assert(parent[roots[i]] as int == roots[i]);
        assert(roots[i] != cur);
    }
    assert forall|i: int| 0 <= i < n implies roots[#[trigger] next[i] as int] == roots[i] by {
        if i != cur {
            assert(parent[i] < n);
            assert(roots[parent[i] as int] == roots[i]);
        }
    }
    assert forall|i: int| 0 <= i < n && #[trigger] next[i] as int == i implies roots[i] == i by {
        assert(i != cur);
        assert(parent[i] as int == i);
    }
    assert forall|i: int| 0 <= i < n && #[trigger] next[i] as int != i implies rank[i] < rank[
        next[i] as int] by {
        if i != cur {
            assert(parent[i] as int != i);
        }
    }
    assert forall|r: int| 0 <= r < n && #[trigger] next[r] as int == r implies pow2(
        rank[r] as nat,
    ) <= class(roots, r).len() by {
        assert(r != cur);
        assert(parent[r] as int == r);
    }
}

/// The representatives after joining roots `a` and `b` under `root`.
pub open spec fn joined(roots: Seq<int>, a: int, b: int, root: int) -> Seq<int> {
    Seq::new(roots.len(), |j: int| if roots[j] == a || roots[j] == b { root } else { roots[j] })
}

/// Linking one root under another, by rank, keeps the invariant with the two
/// classes joined.
pub proof fn lemma_link(
    parent: Seq<u32>,
    rank: Seq<u8>,
    roots: Seq<int>,
    root: int,
    child: int,
    new_rank: Seq<u8>,
)
    requires
        wf_parts(parent, rank, roots),
        0 <= root < parent.len(),
        0 <= child < parent.len(),
        root != child,
        is_root(parent, root),
        is_root(parent, child),
        rank[child] <= rank[root],
        rank[root] < 32,
        new_rank == (if rank[child] == rank[root] {
            rank.update(root, (rank[root] + 1) as u8)
        } else {
            rank
        }),
    ensures
        wf_parts(parent.update(child, root as u32), new_rank, joined(roots, root, child, root)),
{
    let n = parent.len() as int;
    let next = parent.update(child, root as u32);
    let new_roots = joined(roots, root, child, root);
    assert(parent[root] as int == root);
    assert(parent[child] as int == child);
    assert(roots[root] == root);
    assert(roots[child] == child);

    assert forall|i: int| 0 <= i < n implies #[trigger] next[i] < n by {
        if i != child {
            assert(parent[i] < n);
        }
    }
    assert forall|i: int| 0 <= i < n implies 0 <= #[trigger] new_roots[i] < n && is_root(
        next,
        new_roots[i],
    ) by {
        assert(0 <= roots[i] < n && is_root(parent, roots[i]));
        assert(parent[roots[i]] as int == roots[i]);
    }
    assert forall|i: int| 0 <= i < n implies new_roots[#[trigger] next[i] as int] == new_roots[i] by {
        if i != child {
            assert(parent[i] < n);
            assert(roots[parent[i] as int] == roots[i]);
        }
    }
    assert forall|i: int| 0 <= i < n && #[trigger] next[i] as int == i implies new_roots[i] == i by {
        assert(i != child);
        assert(parent[i] as int == i);
    }
    assert forall|i: int| 0 <= i < n && #[trigger] next[i] as int != i implies new_rank[i] < new_rank[
        next[i] as int] by {
        if i != child {
            assert(parent[i] as int != i);
            assert(rank[i] < rank[parent[i] as int]);
            assert(i != root);
        }
    }

    let joined_class = class(new_roots, root);
    assert(joined_class =~= class(roots, root) + class(roots, child));
    assert(class(roots, root).disjoint(class(roots, child)));
    lemma_set_disjoint_lens(class(roots, root), class(roots, child));
    assert forall|r: int| 0 <= r < n && #[trigger] next[r] as int == r implies pow2(
        new_rank[r] as nat,
    ) <= class(new_roots, r).len() by {
        assert(r != child);
        assert(parent[r] as int == r);
        if r == root {
            assert(pow2(rank[root] as nat) <= class(roots, root).len());
            assert(pow2(rank[child] as nat) <= class(roots, child).len());
            if rank[child] == rank[root] {
                lemma_pow2_unfold((rank[root] + 1) as nat);
            }
        } else {
            assert(class(new_roots, r) =~= class(roots, r));
        }
    }
}

/// Appending a new singleton element keeps the invariant.
pub proof fn lemma_push(parent: Seq<u32>, rank: Seq<u8>, roots: Seq<int>)
    requires
        wf_parts(parent, rank, roots),
        parent.len() < u32::MAX,
    ensures
        wf_parts(parent.push(parent.len() as u32), rank.push(0), roots.push(parent.len() as int)),
{
    let n = parent.len() as int;
    let next = parent.push(n as u32);
    let new_rank = rank.push(0);
    let new_roots = roots.push(n);
    assert forall|i: int| 0 <= i < n + 1 implies #[trigger] next[i] < n + 1 by {
        if i < n {
            assert(parent[i] < n);
        }
    }
    assert forall|i: int| 0 <= i < n + 1 implies 0 <= #[trigger] new_roots[i] < n + 1 && is_root(
        next,
        new_roots[i],
    ) by {
        if i < n {
            assert(0 <= roots[i] < n && is_root(parent, roots[i]));
        }
    }
    assert forall|i: int| 0 <= i < n + 1 implies new_roots[#[trigger] next[i] as int] == new_roots[i] by {
        if i < n {
            assert(parent[i] < n);
        }
    }
    assert forall|i: int| 0 <= i < n + 1 && #[trigger] next[i] as int == i implies new_roots[i] == i by {
        if i < n {
            assert(parent[i] as int == i);
        }
    }
    assert forall|i: int| 0 <= i < n + 1 && #[trigger] next[i] as int != i implies new_rank[i] < new_rank[
        next[i] as int] by {
        assert(i < n);
        assert(parent[i] as int != i);
        assert(parent[i] < n);
    }
    assert forall|r: int| 0 <= r < n + 1 && #[trigger] next[r] as int == r implies pow2(
        new_rank[r] as nat,
    ) <= class(new_roots, r).len() by {
        if r == n {
            assert(class(new_roots, r) =~= Set::empty().insert(r));
            lemma2_to64();
        } else {
            assert(parent[r] as int == r);
            assert(class(new_roots, r) =~= class(roots, r));
        }
    }
}

pub struct UnionFind {
    pub parent: Vec<u32>,
    pub rank: Vec<u8>,
    pub roots: Ghost<Seq<int>>,
}

impl UnionFind {
    pub open spec fn len(&self) -> int {
        self.parent@.len() as int
    }

    pub open spec fn wf(&self) -> bool {
        wf_parts(self.parent@, self.rank@, self.roots@)
    }

    pub fn new() -> (uf: Self)
        ensures
            uf.wf(),
            uf.len() == 0,
    {
        UnionFind { parent: Vec::new(), rank: Vec::new(), roots: Ghost(Seq::empty()) }
    }

    /// Adds an element in a class of its own.
    pub fn make_set(&mut self) -> (id: u32)
        requires
            old(self).wf(),
            old(self).len() < u32::MAX,
        ensures
            final(self).wf(),
            id == old(self).len(),
            final(self).roots@ == old(self).roots@.push(id as int),
    {
        let id = self.parent.len() as u32;
        proof {
            lemma_push(self.parent@, self.rank@, self.roots@);
        }
        self.parent.push(id);
        self.rank.push(0);
        self.roots = Ghost(self.roots@.push(id as int));
        id
    }

    /// The representative of `x`, halving the path on the way.
    pub fn find(&mut self, x: u32) -> (r: u32)
        requires
            old(self).wf(),
            x < old(self).len(),
        ensures
            final(self).wf(),
            r == old(self).roots@[x as int],
            final(self).roots@ == old(self).roots@,
            final(self).rank@ == old(self).rank@,
            final(self).len() == old(self).len(),
    {
        let ghost target = self.roots@[x as int];
        let ghost roots0 = self.roots@;
        let ghost rank0 = self.rank@;
        let ghost len0 = self.len();
        let mut current = x;
        while self.parent[current as usize] != current
            invariant
                self.wf(),
                current < self.len(),
                self.roots@ == roots0,
                self.rank@ == rank0,
                self.len() == len0,
                roots0[current as int] == target,
            decreases 256 - self.rank@[current as int],
        {
            let parent = self.parent[current as usize];
            let grandparent = self.parent[parent as usize];
            proof {
                assert(self.parent@[current as int] as int != current as int);
                assert(self.rank@[current as int] < self.rank@[parent as int]);
                if self.parent@[parent as int] as int != parent as int {
                    assert(self.rank@[parent as int] < self.rank@[grandparent as int]);
                }
                assert(self.roots@[parent as int] == self.roots@[current as int]);
                lemma_halve(self.parent@, self.rank@, self.roots@, current as int);
            }
            self.parent.set(current as usize, grandparent);
            current = grandparent;
        }
        proof {
            assert(self.parent@[current as int] as int == current as int);
        }
        current
    }

    /// Joins the classes of two distinct roots and returns the surviving root.
    pub fn union_roots(&mut self, a: u32, b: u32) -> (root: u32)
        requires
            old(self).wf(),
            a < old(self).len(),
            b < old(self).len(),
            a != b,
            is_root(old(self).parent@, a as int),
            is_root(old(self).parent@, b as int),
        ensures
            final(self).wf(),
            root == a || root == b,
            final(self).len() == old(self).len(),
            final(self).roots@ == joined(old(self).roots@, a as int, b as int, root as int),
    {
        let rank_a = self.rank[a as usize];
        let rank_b = self.rank[b as usize];
        let (root, child) = if rank_a > rank_b {
            (a, b)
        } else {
            (b, a)
        };
        let ghost parent0 = self.parent@;
        let ghost rank0 = self.rank@;
        let ghost roots0 = self.roots@;
        proof {
            lemma_rank_below_32(parent0, rank0, roots0, root as int);
        }
        if rank_a == rank_b {
            let r = self.rank[root as usize];
            self.rank.set(root as usize, r + 1);
        }
        self.parent.set(child as usize, root);
        self.roots = Ghost(joined(roots0, root as int, child as int, root as int));
        proof {
            lemma_link(parent0, rank0, roots0, root as int, child as int, self.rank@);
            assert(joined(roots0, root as int, child as int, root as int) =~= joined(
                roots0,
                a as int,
                b as int,
                root as int,
            ));
        }
        root
    }
}

fn main() {
    let mut uf = UnionFind::new();
    let a = uf.make_set();
    let b = uf.make_set();
    let ra = uf.find(a);
    let rb = uf.find(b);
    if ra != rb {
        uf.union_roots(ra, rb);
    }
}

} // verus!
