//! Reference model of the e-graph core, written from its specification.
//!
//! A term is an e-node as it was added: a symbol, a disambiguator and the
//! terms it was built from. Two e-nodes are identical when symbol,
//! disambiguator and children are all equal, for leaves and inner nodes
//! alike. The model keeps every term and every asserted merge, and answers
//! questions about their congruence closure: the smallest equivalence over
//! the terms that contains the merges and in which two terms with the same
//! symbol and disambiguator and pairwise equivalent children are equivalent.
//!
//! That closure is what an e-graph must hold after `rebuild()`. The model
//! recomputes it from scratch, quadratically, so that it can be checked by
//! reading; it is meant for graphs of up to a few hundred terms.

use std::collections::BTreeSet;

pub const NONE: u32 = u32::MAX;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Term {
    pub symbol: u8,
    pub disambiguator: u64,
    pub children: Vec<usize>,
}

#[derive(Default, Clone, Debug)]
pub struct Model {
    terms: Vec<Term>,
    merges: Vec<(usize, usize)>,
    /// Block label of each term under the closure, valid until the next
    /// mutation. A label is the smallest term index in its block.
    blocks: Option<Vec<usize>>,
}

/// An e-node with its children replaced by their blocks: the identity an
/// e-node has once the graph is canonical.
type CanonicalENode = (u8, u64, Vec<usize>);

impl Model {
    /// Adds a term and returns its index. Children are earlier term indices.
    pub fn add(&mut self, symbol: u8, disambiguator: u64, children: Vec<usize>) -> usize {
        assert!(children.iter().all(|&c| c < self.terms.len()), "child refers to a later term");
        self.terms.push(Term { symbol, disambiguator, children });
        self.blocks = None;
        self.terms.len() - 1
    }

    pub fn merge(&mut self, a: usize, b: usize) {
        assert!(a < self.terms.len() && b < self.terms.len(), "merge of an unknown term");
        self.merges.push((a, b));
        self.blocks = None;
    }

    pub fn num_terms(&self) -> usize {
        self.terms.len()
    }

    /// Computes the closure of everything asserted so far. Queries below
    /// answer from the most recent call.
    pub fn close(&mut self) {
        if self.blocks.is_none() {
            self.blocks = Some(congruence_closure(&self.terms, &self.merges));
        }
    }

    fn blocks(&self) -> &[usize] {
        self.blocks.as_deref().expect("call close() after the last mutation")
    }

    pub fn block_of(&self, term: usize) -> usize {
        self.blocks()[term]
    }

    pub fn equivalent(&self, a: usize, b: usize) -> bool {
        self.blocks()[a] == self.blocks()[b]
    }

    pub fn num_blocks(&self) -> usize {
        self.blocks().iter().enumerate().filter(|&(i, &b)| i == b).count()
    }

    fn canonical(&self, term: usize) -> CanonicalENode {
        let t = &self.terms[term];
        (t.symbol, t.disambiguator, t.children.iter().map(|&c| self.blocks()[c]).collect())
    }

    /// The distinct canonical e-nodes: what remains after duplicates are removed.
    fn canonical_enodes(&self) -> BTreeSet<CanonicalENode> {
        (0..self.terms.len()).map(|t| self.canonical(t)).collect()
    }

    pub fn num_enodes(&self) -> usize {
        self.canonical_enodes().len()
    }

    pub fn enodes_in_block(&self, block: usize) -> usize {
        let in_block: BTreeSet<_> =
            (0..self.terms.len()).filter(|&t| self.blocks()[t] == block).map(|t| self.canonical(t)).collect();
        in_block.len()
    }

    /// The block holding the canonical e-node `(symbol, disambiguator,
    /// child_blocks)`, if any term has that form.
    pub fn find_enode(&self, symbol: u8, disambiguator: u64, child_blocks: &[usize]) -> Option<usize> {
        (0..self.terms.len())
            .find(|&t| self.canonical(t) == (symbol, disambiguator, child_blocks.to_vec()))
            .map(|t| self.blocks()[t])
    }
}

fn congruent(terms: &[Term], blocks: &[usize], a: usize, b: usize) -> bool {
    let (x, y) = (&terms[a], &terms[b]);
    x.symbol == y.symbol
        && x.disambiguator == y.disambiguator
        && x.children.len() == y.children.len()
        && x.children.iter().zip(&y.children).all(|(&c, &d)| blocks[c] == blocks[d])
}

/// Joins the blocks of `a` and `b`, keeping the smaller label so a label stays
/// the smallest term index in its block. Returns whether they were apart.
fn join(blocks: &mut [usize], a: usize, b: usize) -> bool {
    let (keep, drop) = (blocks[a].min(blocks[b]), blocks[a].max(blocks[b]));
    if keep == drop {
        return false;
    }
    for label in blocks.iter_mut() {
        if *label == drop {
            *label = keep;
        }
    }
    true
}

/// Least fixpoint: start from the merges, then join congruent pairs until no
/// pair is left to join.
pub fn congruence_closure(terms: &[Term], merges: &[(usize, usize)]) -> Vec<usize> {
    let n = terms.len();
    let mut blocks: Vec<usize> = (0..n).collect();
    for &(a, b) in merges {
        join(&mut blocks, a, b);
    }
    loop {
        let mut changed = false;
        for a in 0..n {
            for b in a + 1..n {
                if blocks[a] != blocks[b] && congruent(terms, &blocks, a, b) {
                    changed |= join(&mut blocks, a, b);
                }
            }
        }
        if !changed {
            return blocks;
        }
    }
}

// C ABI used by the differential test. Term indices are u32; `NONE` stands
// for "absent". Queries other than add/merge read the closure computed by the
// last `egraph_oracle_close`.

#[unsafe(no_mangle)]
pub extern "C" fn egraph_oracle_new() -> *mut Model {
    Box::into_raw(Box::default())
}

/// # Safety
/// `model` must come from `egraph_oracle_new` and not have been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn egraph_oracle_free(model: *mut Model) {
    if !model.is_null() {
        drop(unsafe { Box::from_raw(model) });
    }
}

unsafe fn model_mut<'a>(model: *mut Model) -> &'a mut Model {
    unsafe { model.as_mut() }.expect("null oracle handle")
}

unsafe fn ids<'a>(ptr: *const u32, len: usize) -> Vec<usize> {
    if len == 0 {
        return Vec::new();
    }
    unsafe { std::slice::from_raw_parts(ptr, len) }.iter().map(|&i| i as usize).collect()
}

fn to_u32(value: usize) -> u32 {
    u32::try_from(value).expect("index exceeds u32")
}

/// # Safety
/// `model` must be a live handle; `children` must point to `len` term indices.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn egraph_oracle_add(
    model: *mut Model,
    symbol: u8,
    disambiguator: u64,
    children: *const u32,
    len: usize,
) -> u32 {
    let children = unsafe { ids(children, len) };
    to_u32(unsafe { model_mut(model) }.add(symbol, disambiguator, children))
}

/// # Safety
/// `model` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn egraph_oracle_merge(model: *mut Model, a: u32, b: u32) {
    unsafe { model_mut(model) }.merge(a as usize, b as usize)
}

/// # Safety
/// `model` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn egraph_oracle_close(model: *mut Model) {
    unsafe { model_mut(model) }.close()
}

/// # Safety
/// `model` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn egraph_oracle_num_terms(model: *mut Model) -> u32 {
    to_u32(unsafe { model_mut(model) }.num_terms())
}

/// # Safety
/// `model` must be a live, closed handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn egraph_oracle_block_of(model: *mut Model, term: u32) -> u32 {
    to_u32(unsafe { model_mut(model) }.block_of(term as usize))
}

/// # Safety
/// `model` must be a live, closed handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn egraph_oracle_equivalent(model: *mut Model, a: u32, b: u32) -> bool {
    unsafe { model_mut(model) }.equivalent(a as usize, b as usize)
}

/// # Safety
/// `model` must be a live, closed handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn egraph_oracle_num_blocks(model: *mut Model) -> u32 {
    to_u32(unsafe { model_mut(model) }.num_blocks())
}

/// # Safety
/// `model` must be a live, closed handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn egraph_oracle_num_enodes(model: *mut Model) -> u32 {
    to_u32(unsafe { model_mut(model) }.num_enodes())
}

/// # Safety
/// `model` must be a live, closed handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn egraph_oracle_enodes_in_block(model: *mut Model, block: u32) -> u32 {
    to_u32(unsafe { model_mut(model) }.enodes_in_block(block as usize))
}

/// # Safety
/// `model` must be a live, closed handle; `child_blocks` must point to `len`
/// block labels.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn egraph_oracle_find_enode(
    model: *mut Model,
    symbol: u8,
    disambiguator: u64,
    child_blocks: *const u32,
    len: usize,
) -> u32 {
    let child_blocks = unsafe { ids(child_blocks, len) };
    match unsafe { model_mut(model) }.find_enode(symbol, disambiguator, &child_blocks) {
        Some(block) => to_u32(block),
        None => NONE,
    }
}

// The model's own laws. The closure is checked against two derivations that
// share no code with it before it is trusted to judge an e-graph.
#[cfg(test)]
mod laws {
    use super::*;
    use proptest::prelude::*;
    use std::collections::HashMap;

    #[derive(Debug, Clone)]
    enum Op {
        Add { symbol: u8, disambiguator: u64, children: Vec<u32> },
        Merge(u32, u32),
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            3 => (0u8..3, 0u64..2, prop::collection::vec(any::<u32>(), 0..3))
                .prop_map(|(symbol, disambiguator, children)| Op::Add { symbol, disambiguator, children }),
            2 => (any::<u32>(), any::<u32>()).prop_map(|(a, b)| Op::Merge(a, b)),
        ]
    }

    /// Replays ops, resolving the raw indices modulo the terms present.
    fn build(ops: &[Op], with_merges: bool) -> (Vec<Term>, Vec<(usize, usize)>) {
        let (mut terms, mut merges) = (Vec::<Term>::new(), Vec::new());
        for op in ops {
            match op {
                Op::Add { symbol, disambiguator, children } => {
                    if terms.is_empty() && !children.is_empty() {
                        continue;
                    }
                    let children = children.iter().map(|&c| c as usize % terms.len().max(1)).collect();
                    terms.push(Term { symbol: *symbol, disambiguator: *disambiguator, children });
                }
                Op::Merge(a, b) if with_merges && !terms.is_empty() => {
                    merges.push((*a as usize % terms.len(), *b as usize % terms.len()));
                }
                Op::Merge(..) => {}
            }
        }
        (terms, merges)
    }

    /// Independent derivation: union-find, and a hash table of canonical
    /// signatures rebuilt until it finds nothing more to union.
    fn worklist_closure(terms: &[Term], merges: &[(usize, usize)]) -> Vec<usize> {
        fn find(parent: &mut Vec<usize>, mut x: usize) -> usize {
            while parent[x] != x {
                x = parent[x];
            }
            x
        }
        let mut parent: Vec<usize> = (0..terms.len()).collect();
        for &(a, b) in merges {
            let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
            parent[ra] = rb;
        }
        loop {
            let mut seen: HashMap<(u8, u64, Vec<usize>), usize> = HashMap::new();
            let mut unioned = false;
            for (i, t) in terms.iter().enumerate() {
                let sig = (t.symbol, t.disambiguator, t.children.iter().map(|&c| find(&mut parent, c)).collect());
                let ri = find(&mut parent, i);
                match seen.get(&sig) {
                    Some(&other) => {
                        let ro = find(&mut parent, other);
                        if ro != ri {
                            parent[ri] = ro;
                            unioned = true;
                        }
                    }
                    None => {
                        seen.insert(sig, i);
                    }
                }
            }
            if !unioned {
                break;
            }
        }
        let roots: Vec<usize> = (0..terms.len()).map(|i| find(&mut parent, i)).collect();
        (0..terms.len()).map(|i| (0..terms.len()).find(|&j| roots[j] == roots[i]).unwrap()).collect()
    }

    /// The tree a term denotes, fully expanded.
    fn tree(terms: &[Term], t: usize) -> String {
        let term = &terms[t];
        let kids: Vec<String> = term.children.iter().map(|&c| tree(terms, c)).collect();
        format!("{}#{}({})", term.symbol, term.disambiguator, kids.join(","))
    }

    proptest! {
        #[test]
        fn closure_contains_the_merges(ops in prop::collection::vec(op(), 0..48)) {
            let (terms, merges) = build(&ops, true);
            let blocks = congruence_closure(&terms, &merges);
            for (a, b) in merges {
                prop_assert_eq!(blocks[a], blocks[b]);
            }
        }

        #[test]
        fn closure_is_closed_under_congruence(ops in prop::collection::vec(op(), 0..48)) {
            let (terms, merges) = build(&ops, true);
            let blocks = congruence_closure(&terms, &merges);
            for a in 0..terms.len() {
                for b in 0..terms.len() {
                    if congruent(&terms, &blocks, a, b) {
                        prop_assert_eq!(blocks[a], blocks[b], "congruent terms {} and {} apart", a, b);
                    }
                }
            }
        }

        #[test]
        fn closure_agrees_with_the_worklist_derivation(ops in prop::collection::vec(op(), 0..48)) {
            let (terms, merges) = build(&ops, true);
            prop_assert_eq!(congruence_closure(&terms, &merges), worklist_closure(&terms, &merges));
        }

        #[test]
        fn without_merges_equivalence_is_equality_of_trees(ops in prop::collection::vec(op(), 0..32)) {
            let (terms, merges) = build(&ops, false);
            let blocks = congruence_closure(&terms, &merges);
            for a in 0..terms.len() {
                for b in 0..terms.len() {
                    prop_assert_eq!(blocks[a] == blocks[b], tree(&terms, a) == tree(&terms, b));
                }
            }
        }

        #[test]
        fn a_merge_never_splits_a_block(ops in prop::collection::vec(op(), 1..48), a: u32, b: u32) {
            let (terms, merges) = build(&ops, true);
            prop_assume!(!terms.is_empty());
            let before = congruence_closure(&terms, &merges);
            let mut more = merges.clone();
            more.push((a as usize % terms.len(), b as usize % terms.len()));
            let after = congruence_closure(&terms, &more);
            for x in 0..terms.len() {
                for y in 0..terms.len() {
                    if before[x] == before[y] {
                        prop_assert_eq!(after[x], after[y]);
                    }
                }
            }
        }

        #[test]
        fn a_redundant_merge_changes_nothing(ops in prop::collection::vec(op(), 1..48), a: u32) {
            let (terms, merges) = build(&ops, true);
            prop_assume!(!terms.is_empty());
            let before = congruence_closure(&terms, &merges);
            let x = a as usize % terms.len();
            let y = before.iter().position(|&l| l == before[x]).unwrap();
            let mut more = merges.clone();
            more.push((x, y));
            prop_assert_eq!(before, congruence_closure(&terms, &more));
        }

        #[test]
        fn labels_are_the_smallest_index_in_the_block(ops in prop::collection::vec(op(), 0..48)) {
            let (terms, merges) = build(&ops, true);
            let blocks = congruence_closure(&terms, &merges);
            for (i, &label) in blocks.iter().enumerate() {
                prop_assert!(label <= i);
                prop_assert_eq!(blocks[label], label);
            }
        }
    }
}
