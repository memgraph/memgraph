//! E-graph with deferred congruence maintenance.
//!
//! An e-graph holds e-nodes (a symbol, a disambiguator and child e-classes)
//! partitioned into e-classes of equivalent e-nodes. `emplace` adds an e-node
//! or finds the identical one, `merge` asserts two e-classes equal, and
//! `rebuild` restores the invariants that merges defer: every e-node is
//! canonical, congruent e-nodes share an e-class, and no e-node is held twice.
//! Between rebuilds, `find` reflects every merge asserted but may not yet
//! reflect the merges congruence implies.
//!
//! E-class ids and e-node ids share one id space: the e-node created with an
//! id starts out alone in the e-class of the same id.

use std::cell::Cell;
use std::hash::{Hash, Hasher};

use rustc_hash::FxHasher;
use smallvec::SmallVec;

mod hashcons;
pub use hashcons::{TableHashCons, VecHashCons};
use hashcons::HashCons;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Id(u32);

impl Id {
    pub fn new(raw: u32) -> Self {
        Id(raw)
    }
    pub fn raw(self) -> u32 {
        self.0
    }
    fn index(self) -> usize {
        self.0 as usize
    }
}

/// Facts kept per e-class. A merge joins the facts of the two e-classes.
pub trait Analysis {
    fn merge(&mut self, other: &Self);
}

impl Analysis for () {
    fn merge(&mut self, _other: &Self) {}
}

pub type Children = SmallVec<[Id; 2]>;

#[derive(Clone, Debug)]
pub struct ENode<S> {
    symbol: S,
    disambiguator: u64,
    children: Children,
    hash: u64,
}

impl<S: Hash + Eq> ENode<S> {
    fn new(symbol: S, disambiguator: u64, children: Children) -> Self {
        let mut node = ENode { symbol, disambiguator, children, hash: 0 };
        node.hash = node.compute_hash();
        node
    }

    fn compute_hash(&self) -> u64 {
        let mut hasher = FxHasher::default();
        self.symbol.hash(&mut hasher);
        self.disambiguator.hash(&mut hasher);
        self.children.hash(&mut hasher);
        hasher.finish()
    }

    fn same(&self, other: &Self) -> bool {
        self.hash == other.hash
            && self.symbol == other.symbol
            && self.disambiguator == other.disambiguator
            && self.children == other.children
    }

    pub fn symbol(&self) -> &S {
        &self.symbol
    }
    pub fn disambiguator(&self) -> u64 {
        self.disambiguator
    }
    pub fn children(&self) -> &[Id] {
        &self.children
    }
}

#[derive(Clone, Debug)]
pub struct EClass<A> {
    nodes: Vec<Id>,
    parents: Vec<Id>,
    analysis: A,
}

impl<A> EClass<A> {
    pub fn nodes(&self) -> &[Id] {
        &self.nodes
    }
    pub fn len(&self) -> usize {
        self.nodes.len()
    }
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
    pub fn analysis(&self) -> &A {
        &self.analysis
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct EmplaceResult {
    /// The e-class holding the e-node; canonical until the next merge.
    pub eclass: Id,
    /// The e-node; valid until a rebuild removes it as a duplicate.
    pub enode: Id,
    pub did_insert: bool,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct MergeResult {
    pub eclass: Id,
    pub did_merge: bool,
}

/// Union by rank with path halving. Parents live in cells so that `find`
/// halves paths through a shared reference.
#[derive(Default, Clone, Debug)]
struct UnionFind {
    parent: Vec<Cell<u32>>,
    rank: Vec<u8>,
}

impl UnionFind {
    fn make_set(&mut self) -> u32 {
        let id = u32::try_from(self.parent.len()).expect("more than u32::MAX e-nodes");
        self.parent.push(Cell::new(id));
        self.rank.push(0);
        id
    }

    /// The representative of `x`, without halving any path: for checks,
    /// which must read a state without changing it.
    fn root_of(&self, mut x: u32) -> u32 {
        loop {
            let parent = self.parent[x as usize].get();
            if parent == x {
                return x;
            }
            x = parent;
        }
    }

    fn find(&self, mut x: u32) -> u32 {
        loop {
            let parent = self.parent[x as usize].get();
            if parent == x {
                return x;
            }
            let grandparent = self.parent[parent as usize].get();
            self.parent[x as usize].set(grandparent);
            x = grandparent;
        }
    }

    /// Joins two distinct roots and returns the surviving root.
    fn union_roots(&mut self, a: u32, b: u32) -> u32 {
        let (rank_a, rank_b) = (self.rank[a as usize], self.rank[b as usize]);
        let (root, child) = if rank_a > rank_b { (a, b) } else { (b, a) };
        self.parent[child as usize].set(root);
        if rank_a == rank_b {
            self.rank[root as usize] += 1;
        }
        root
    }

    fn len(&self) -> usize {
        self.parent.len()
    }

    fn clear(&mut self) {
        self.parent.clear();
        self.rank.clear();
    }
}

/// The first clause of `EGraph::check_invariant` found broken, and the id it
/// was found at.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct Violation {
    pub clause: u8,
    pub at: usize,
}

impl Violation {
    fn new(clause: u8, at: usize) -> Self {
        Violation { clause, at }
    }
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invariant clause {} broken at id {}", self.clause, self.at)
    }
}

/// Where a refresh starts. Both reach the same facts; `Changed` visits only
/// what the classes created or merged since the last refresh can affect.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Scope {
    Changed,
    Everything,
}

pub struct EGraph<S, A = ()> {
    union_find: UnionFind,
    nodes: Vec<ENode<S>>,
    dead: Vec<bool>,
    /// Indexed by id; `Some` exactly for canonical e-classes.
    classes: Vec<Option<EClass<A>>>,
    hashcons: HashCons,
    worklist: Vec<Id>,
    touched: Vec<Id>,
    /// Classes created or merged since the last refresh, as raw ids.
    analysis_dirty: Vec<Id>,
    tracks_analysis: bool,
    num_classes: usize,
    num_dead: usize,
    // Scratch reused across rebuilds.
    pending: Vec<Id>,
    survivors: Vec<Id>,
    with_dead: Vec<Id>,
}

impl<S, A> Default for EGraph<S, A> {
    fn default() -> Self {
        EGraph {
            union_find: UnionFind::default(),
            nodes: Vec::new(),
            dead: Vec::new(),
            classes: Vec::new(),
            hashcons: HashCons::default(),
            worklist: Vec::new(),
            touched: Vec::new(),
            analysis_dirty: Vec::new(),
            tracks_analysis: true,
            num_classes: 0,
            num_dead: 0,
            pending: Vec::new(),
            survivors: Vec::new(),
            with_dead: Vec::new(),
        }
    }
}

impl<S: Hash + Eq + Clone, A: Analysis + Default> EGraph<S, A> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let mut graph = Self::default();
        graph.nodes.reserve(capacity);
        graph.classes.reserve(capacity);
        graph.hashcons.reserve(capacity, |_| 0);
        graph
    }

    pub fn find(&self, id: Id) -> Id {
        Id(self.union_find.find(id.0))
    }

    /// Adds the e-node, or finds the identical one. Children must be canonical.
    pub fn emplace(&mut self, symbol: S, children: &[Id], disambiguator: u64) -> EmplaceResult {
        self.emplace_with(symbol, children, disambiguator, A::default())
    }

    pub fn emplace_with(&mut self, symbol: S, children: &[Id], disambiguator: u64, seed: A) -> EmplaceResult {
        debug_assert!(children.iter().all(|&c| self.find(c) == c), "e-node children must be canonical");
        let node = ENode::new(symbol, disambiguator, Children::from_slice(children));
        let nodes = &self.nodes;
        if let Some(existing) = self.hashcons.find(node.hash, |id| nodes[id.index()].same(&node)) {
            return EmplaceResult { eclass: self.find(existing), enode: existing, did_insert: false };
        }

        let id = Id(self.union_find.make_set());
        let hash = node.hash;
        for (i, &child) in node.children.iter().enumerate() {
            if !node.children[..i].contains(&child) {
                self.classes[child.index()].as_mut().expect("child e-class is canonical").parents.push(id);
            }
        }
        self.nodes.push(node);
        self.dead.push(false);
        self.classes.push(Some(EClass { nodes: vec![id], parents: Vec::new(), analysis: seed }));
        let nodes = &self.nodes;
        self.hashcons.insert(hash, id, |other| nodes[other.index()].hash);
        self.mark_changed(id);
        self.num_classes += 1;
        EmplaceResult { eclass: id, enode: id, did_insert: true }
    }

    pub fn merge(&mut self, a: Id, b: Id) -> MergeResult {
        let (root_a, root_b) = (self.union_find.find(a.0), self.union_find.find(b.0));
        if root_a == root_b {
            return MergeResult { eclass: Id(root_a), did_merge: false };
        }
        let root = self.union_find.union_roots(root_a, root_b);
        let other = if root == root_a { root_b } else { root_a };
        let mut absorbed = self.classes[other as usize].take().expect("merged e-class is canonical");
        let survivor = self.classes[root as usize].as_mut().expect("merged e-class is canonical");
        if absorbed.nodes.len() > survivor.nodes.len() {
            std::mem::swap(&mut absorbed.nodes, &mut survivor.nodes);
        }
        survivor.nodes.extend_from_slice(&absorbed.nodes);
        if absorbed.parents.len() > survivor.parents.len() {
            std::mem::swap(&mut absorbed.parents, &mut survivor.parents);
        }
        survivor.parents.extend_from_slice(&absorbed.parents);
        survivor.analysis.merge(&absorbed.analysis);

        self.worklist.push(Id(root));
        self.mark_changed(Id(root));
        self.num_classes -= 1;
        MergeResult { eclass: Id(root), did_merge: true }
    }

    pub fn needs_rebuild(&self) -> bool {
        !self.worklist.is_empty()
    }

    /// Restores canonical e-nodes, congruence and duplicate-freedom.
    pub fn rebuild(&mut self) {
        while !self.worklist.is_empty() {
            let mut batch = std::mem::take(&mut self.worklist);
            for id in batch.iter_mut() {
                *id = self.find(*id);
            }
            batch.sort_unstable();
            batch.dedup();
            for &class in &batch {
                self.repair_parents_of(class);
            }
            batch.clear();
            if self.worklist.is_empty() {
                self.worklist = batch;
            }
        }
        self.drop_dead_from_classes();
    }

    /// Re-canonicalises every e-node reading `class`, merging the e-classes of
    /// e-nodes that turn out identical and keeping one of them.
    fn repair_parents_of(&mut self, class: Id) {
        let class = self.find(class);
        let mut pending = std::mem::take(&mut self.pending);
        pending.clear();
        pending.append(&mut self.classes[class.index()].as_mut().expect("canonical").parents);
        pending.sort_unstable();
        pending.dedup();

        let mut survivors = std::mem::take(&mut self.survivors);
        survivors.clear();
        for &parent in &pending {
            if self.dead[parent.index()] {
                continue;
            }
            let old_hash = self.nodes[parent.index()].hash;
            self.hashcons.remove(old_hash, parent);
            self.canonicalize(parent);

            let nodes = &self.nodes;
            let node = &nodes[parent.index()];
            match self.hashcons.find(node.hash, |id| nodes[id.index()].same(node)) {
                Some(kept) => {
                    let (kept_class, parent_class) = (self.find(kept), self.find(parent));
                    if kept_class != parent_class {
                        self.merge(kept_class, parent_class);
                    }
                    self.dead[parent.index()] = true;
                    self.num_dead += 1;
                    self.with_dead.push(parent);
                }
                None => {
                    let hash = node.hash;
                    self.hashcons.insert(hash, parent, |other| nodes[other.index()].hash);
                    survivors.push(parent);
                }
            }
        }
        let class = self.find(class);
        self.classes[class.index()].as_mut().expect("canonical").parents.extend_from_slice(&survivors);
        self.pending = pending;
        self.survivors = survivors;
    }

    fn canonicalize(&mut self, enode: Id) {
        let union_find = &self.union_find;
        let node = &mut self.nodes[enode.index()];
        let mut changed = false;
        for child in node.children.iter_mut() {
            let canonical = union_find.find(child.0);
            if canonical != child.0 {
                *child = Id(canonical);
                changed = true;
            }
        }
        if changed {
            node.hash = node.compute_hash();
        }
    }

    fn drop_dead_from_classes(&mut self) {
        if self.with_dead.is_empty() {
            return;
        }
        let mut with_dead = std::mem::take(&mut self.with_dead);
        for id in with_dead.iter_mut() {
            *id = self.find(*id);
        }
        with_dead.sort_unstable();
        with_dead.dedup();
        for &class in &with_dead {
            let dead = &self.dead;
            self.classes[class.index()].as_mut().expect("canonical").nodes.retain(|n| !dead[n.index()]);
        }
        with_dead.clear();
        self.with_dead = with_dead;
    }

    pub fn eclass(&self, id: Id) -> &EClass<A> {
        self.classes[id.index()].as_ref().expect("id needs to be canonical")
    }

    pub fn has_class(&self, id: Id) -> bool {
        self.classes.get(id.index()).is_some_and(Option::is_some)
    }

    pub fn get_enode(&self, id: Id) -> &ENode<S> {
        &self.nodes[id.index()]
    }

    pub fn canonical_classes(&self) -> impl Iterator<Item = (Id, &EClass<A>)> {
        self.classes.iter().enumerate().filter_map(|(i, c)| c.as_ref().map(|c| (Id(i as u32), c)))
    }

    pub fn num_classes(&self) -> usize {
        self.num_classes
    }
    pub fn num_nodes(&self) -> usize {
        self.union_find.len()
    }
    pub fn num_dead_nodes(&self) -> usize {
        self.num_dead
    }
    pub fn num_live_nodes(&self) -> usize {
        self.num_nodes() - self.num_dead
    }
    pub fn is_empty(&self) -> bool {
        self.num_classes == 0
    }

    /// Canonical e-classes inserted or merged into since `clear_touched`,
    /// including merges a rebuild's congruence cascade made; sorted, distinct.
    pub fn touched_into(&self, out: &mut Vec<Id>) {
        out.clear();
        out.extend(self.touched.iter().map(|&id| self.find(id)));
        out.sort_unstable();
        out.dedup();
    }

    pub fn clear_touched(&mut self) {
        self.touched.clear();
    }

    /// A class that is new or newly merged: the touched set always records it,
    /// and the next `Changed` refresh starts from it when analysis is tracked.
    fn mark_changed(&mut self, id: Id) {
        self.touched.push(id);
        if self.tracks_analysis {
            self.analysis_dirty.push(id);
        }
    }

    /// Whether any symbol derives facts from its children. With tracking off,
    /// only an `Everything` refresh sees changes; set it once, before use.
    pub fn set_tracks_analysis(&mut self, tracks: bool) {
        self.tracks_analysis = tracks;
    }

    /// The facts of `id`'s class.
    pub fn analysis_of(&self, id: Id) -> &A {
        &self.eclass(self.find(id)).analysis
    }

    /// Widens facts until every e-node's `update` adds nothing, and answers how
    /// many updates widened a class. `update(enode, facts_of)` gives the facts
    /// the e-node derives from its children's, or `None` for a symbol that
    /// derives none; they are joined into the facts of the e-node's class.
    /// `update` must be monotone and `Analysis::merge` a join, which makes the
    /// result the least fixpoint whichever scope or order the walk takes.
    pub fn refresh_analysis<F>(&mut self, scope: Scope, mut update: F) -> usize
    where
        A: Clone + PartialEq,
        F: FnMut(&ENode<S>, &dyn Fn(Id) -> A) -> Option<A>,
    {
        let mut worklist: Vec<Id> = Vec::new();
        match scope {
            Scope::Changed => {
                let mut dirty: Vec<Id> = self.analysis_dirty.iter().map(|&id| self.find(id)).collect();
                dirty.sort_unstable();
                dirty.dedup();
                for class in dirty {
                    if let Some(eclass) = self.classes[class.index()].as_ref() {
                        worklist.extend_from_slice(&eclass.nodes);
                        worklist.extend_from_slice(&eclass.parents);
                    }
                }
            }
            Scope::Everything => {
                for eclass in self.classes.iter().flatten() {
                    worklist.extend_from_slice(&eclass.nodes);
                }
            }
        }
        self.analysis_dirty.clear();

        let mut widened = 0;
        while let Some(enode) = worklist.pop() {
            if self.dead[enode.index()] {
                continue;
            }
            let derived = {
                let graph = &*self;
                let facts_of = |id: Id| graph.analysis_of(id).clone();
                update(&graph.nodes[enode.index()], &facts_of)
            };
            let Some(derived) = derived else { continue };
            let class = self.find(enode);
            let eclass = self.classes[class.index()].as_mut().expect("canonical");
            let mut joined = eclass.analysis.clone();
            joined.merge(&derived);
            if joined != eclass.analysis {
                eclass.analysis = joined;
                widened += 1;
                worklist.extend_from_slice(&eclass.parents);
                self.touched.push(class);
            }
        }
        widened
    }

    /// The representation invariant, which every public operation preserves.
    /// It costs quadratic time, for tests and proofs; the error names the
    /// first clause that fails.
    ///
    ///  1. One union-find element, e-node, dead flag and class slot per id.
    ///  2. Each e-node's cached hash is the hash of its content, and its
    ///     children are ids in range.
    ///  3. A class slot is filled exactly when its id is canonical, and the
    ///     class and dead-node counters match the flags they count.
    ///  4. Every live e-node sits in exactly one class, its canonical one, and
    ///     no class holds a dead e-node.
    ///  5. The hashcons holds exactly the live e-nodes, each under its current
    ///     hash, and no two of them have identical content.
    ///  6. Every live e-node is a parent of the class of each of its children,
    ///     so a rebuild that repairs a class reaches everything reading it.
    ///  7. A live e-node with a non-canonical child has that child's class on
    ///     the worklist, so a rebuild will repair it.
    ///  8. With the worklist empty, every live e-node is canonical.
    pub fn check_invariant(&self) -> Result<(), Violation> {
        let n = self.union_find.len();
        if self.nodes.len() != n || self.dead.len() != n || self.classes.len() != n {
            return Err(Violation::new(1, 0));
        }
        for (i, node) in self.nodes.iter().enumerate() {
            if node.hash != node.compute_hash() {
                return Err(Violation::new(2, i));
            }
            if node.children.iter().any(|c| c.index() >= n) {
                return Err(Violation::new(2, i));
            }
        }
        let mut live_classes = 0;
        for i in 0..n {
            let canonical = self.union_find.root_of(i as u32) as usize == i;
            if self.classes[i].is_some() != canonical {
                return Err(Violation::new(3, i));
            }
            live_classes += usize::from(canonical);
        }
        if live_classes != self.num_classes {
            return Err(Violation::new(3, 0));
        }
        if self.dead.iter().filter(|&&d| d).count() != self.num_dead {
            return Err(Violation::new(3, 0));
        }
        let mut seen = vec![0u32; n];
        for (c, class) in self.classes.iter().enumerate() {
            for &member in class.iter().flat_map(|class| &class.nodes) {
                if self.dead[member.index()] {
                    return Err(Violation::new(4, c));
                }
                if Id(self.union_find.root_of(member.0)).index() != c {
                    return Err(Violation::new(4, c));
                }
                seen[member.index()] += 1;
            }
        }
        for i in 0..n {
            let expected = u32::from(!self.dead[i]);
            if seen[i] != expected {
                return Err(Violation::new(4, i));
            }
        }
        let live = n - self.num_dead;
        if self.hashcons.len() != live {
            return Err(Violation::new(5, 0));
        }
        for i in 0..n {
            let id = Id(i as u32);
            let registered = self.hashcons.find(self.nodes[i].hash, |other| other == id).is_some();
            if registered == self.dead[i] {
                return Err(Violation::new(5, i));
            }
        }
        for i in 0..n {
            for j in i + 1..n {
                if !self.dead[i] && !self.dead[j] && self.nodes[i].same(&self.nodes[j]) {
                    return Err(Violation::new(5, i));
                }
            }
        }
        let queued: Vec<Id> = self.worklist.iter().map(|&w| Id(self.union_find.root_of(w.0))).collect();
        for i in (0..n).filter(|&i| !self.dead[i]) {
            for &child in &self.nodes[i].children {
                let class = Id(self.union_find.root_of(child.0));
                let parents = &self.classes[class.index()].as_ref().expect("canonical").parents;
                if !parents.contains(&Id(i as u32)) {
                    return Err(Violation::new(6, i));
                }
                if class != child {
                    if !queued.contains(&class) {
                        return Err(Violation::new(7, i));
                    }
                    if self.worklist.is_empty() {
                        return Err(Violation::new(8, i));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn clear(&mut self) {
        self.union_find.clear();
        self.nodes.clear();
        self.dead.clear();
        self.classes.clear();
        self.hashcons.clear();
        self.worklist.clear();
        self.touched.clear();
        self.analysis_dirty.clear();
        self.with_dead.clear();
        self.num_classes = 0;
        self.num_dead = 0;
    }
}

#[cfg(kani)]
mod proofs;
