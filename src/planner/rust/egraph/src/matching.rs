//! E-matching: finding the substitutions under which a rule's patterns occur
//! in a rebuilt e-graph.
//!
//! A pattern is a tree of variables, wildcards and symbol nodes. A symbol node
//! matches an e-class holding an e-node with that symbol and as many children
//! as the node has, each child matching the corresponding child pattern; the
//! disambiguator plays no part. A symbol node may also bind a variable to the
//! e-class it matched. A rule's patterns share one substitution, so a variable
//! named in two patterns joins them and patterns that share none combine as a
//! cross product. Each substitution is reported once.
//!
//! The graph must be rebuilt before matching: the matcher reads e-classes as
//! canonical and does not look for congruences a rebuild would find.

use std::hash::Hash;

use hashbrown::HashMap;
use hashbrown::HashSet;
use rustc_hash::FxBuildHasher;
use smallvec::SmallVec;

use crate::{Analysis, EGraph, Id};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PatternNode<S> {
    Var(u8),
    Wild,
    Sym { symbol: S, children: SmallVec<[u32; 4]>, bind: Option<u8> },
}

/// A pattern's nodes, children before parents; the last node is the root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pattern<S> {
    nodes: Vec<PatternNode<S>>,
}

#[derive(Default)]
pub struct PatternBuilder<S> {
    nodes: Vec<PatternNode<S>>,
}

impl<S> PatternBuilder<S> {
    pub fn new() -> Self {
        PatternBuilder { nodes: Vec::new() }
    }
    pub fn var(&mut self, var: u8) -> u32 {
        self.push(PatternNode::Var(var))
    }
    pub fn wild(&mut self) -> u32 {
        self.push(PatternNode::Wild)
    }
    /// Children are nodes this builder returned earlier.
    pub fn sym(&mut self, symbol: S, children: &[u32], bind: Option<u8>) -> u32 {
        assert!(children.iter().all(|&c| (c as usize) < self.nodes.len()), "child added after its parent");
        self.push(PatternNode::Sym { symbol, children: children.into(), bind })
    }
    fn push(&mut self, node: PatternNode<S>) -> u32 {
        self.nodes.push(node);
        (self.nodes.len() - 1) as u32
    }
    /// The node added last becomes the root.
    pub fn build(self) -> Pattern<S> {
        assert!(!self.nodes.is_empty(), "a pattern needs a node");
        Pattern { nodes: self.nodes }
    }
}

impl<S> Pattern<S> {
    pub fn nodes(&self) -> &[PatternNode<S>] {
        &self.nodes
    }
    fn root(&self) -> u32 {
        (self.nodes.len() - 1) as u32
    }
    fn vars(&self) -> impl Iterator<Item = u8> + '_ {
        self.nodes.iter().filter_map(|n| match n {
            PatternNode::Var(v) => Some(*v),
            PatternNode::Sym { bind, .. } => *bind,
            PatternNode::Wild => None,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum RuleError {
    NoPattern,
    /// The index of a pattern that names no variable.
    NoVariable(usize),
    /// A variable two symbol nodes bind.
    BoundTwice(u8),
}

const NO_SLOT: u8 = u8::MAX;

/// A rule's patterns, checked and put in the order they are joined.
pub struct Matcher<S> {
    /// In join order.
    patterns: Vec<Pattern<S>>,
    /// For each pattern, whether each node's subtree names no variable. Such a
    /// subtree only needs to occur once; which e-node it occurs as is not part
    /// of the match.
    ground: Vec<Vec<bool>>,
    vars: Vec<u8>,
    slot_of: [u8; 256],
}

impl<S: Clone> Matcher<S> {
    pub fn new(patterns: &[Pattern<S>]) -> Result<Self, RuleError> {
        if patterns.is_empty() {
            return Err(RuleError::NoPattern);
        }
        let mut bound = [false; 256];
        for (i, p) in patterns.iter().enumerate() {
            if p.vars().next().is_none() {
                return Err(RuleError::NoVariable(i));
            }
            for node in &p.nodes {
                if let PatternNode::Sym { bind: Some(v), .. } = node {
                    if std::mem::replace(&mut bound[*v as usize], true) {
                        return Err(RuleError::BoundTwice(*v));
                    }
                }
            }
        }
        let mut vars: Vec<u8> = patterns.iter().flat_map(|p| p.vars()).collect();
        vars.sort_unstable();
        vars.dedup();
        let mut slot_of = [NO_SLOT; 256];
        for (slot, &v) in vars.iter().enumerate() {
            slot_of[v as usize] = slot as u8;
        }
        let ordered: Vec<Pattern<S>> = join_order(patterns).into_iter().map(|i| patterns[i].clone()).collect();
        let ground = ordered.iter().map(ground_nodes).collect();
        Ok(Matcher { patterns: ordered, ground, vars, slot_of })
    }
}

impl<S> Matcher<S> {
    /// The variables a match binds, ascending; a match lists their e-classes
    /// in this order.
    pub fn vars(&self) -> &[u8] {
        &self.vars
    }
}

/// The pattern naming the most variables first, then repeatedly the one
/// sharing most variables with those already placed, then the one adding
/// fewest new ones, then the earliest.
fn join_order<S>(patterns: &[Pattern<S>]) -> Vec<usize> {
    let var_sets: Vec<Vec<u8>> = patterns
        .iter()
        .map(|p| {
            let mut v: Vec<u8> = p.vars().collect();
            v.sort_unstable();
            v.dedup();
            v
        })
        .collect();
    let mut placed = vec![false; patterns.len()];
    let mut known = [false; 256];
    let mut order = Vec::with_capacity(patterns.len());
    let first = (0..patterns.len()).max_by_key(|&i| (var_sets[i].len(), std::cmp::Reverse(i))).unwrap();
    for _ in 0..patterns.len() {
        let next = if order.is_empty() {
            first
        } else {
            (0..patterns.len())
                .filter(|&i| !placed[i])
                .max_by_key(|&i| {
                    let shared = var_sets[i].iter().filter(|&&v| known[v as usize]).count();
                    (shared, std::cmp::Reverse(var_sets[i].len() - shared), std::cmp::Reverse(i))
                })
                .unwrap()
        };
        placed[next] = true;
        for &v in &var_sets[next] {
            known[v as usize] = true;
        }
        order.push(next);
    }
    order
}

fn ground_nodes<S>(pattern: &Pattern<S>) -> Vec<bool> {
    let mut ground = vec![false; pattern.nodes.len()];
    for (i, node) in pattern.nodes.iter().enumerate() {
        ground[i] = match node {
            PatternNode::Var(_) => false,
            PatternNode::Wild => true,
            PatternNode::Sym { children, bind, .. } => {
                bind.is_none() && children.iter().all(|&c| ground[c as usize])
            }
        };
    }
    ground
}

/// The e-classes holding each symbol. A full rebuild lists every canonical
/// e-class once per symbol it holds; `add` appends without removing what
/// merges made stale, and readers canonicalise what they read.
pub struct SymbolIndex<S> {
    classes: HashMap<S, Vec<Id>, FxBuildHasher>,
}

impl<S: Hash + Eq + Clone> Default for SymbolIndex<S> {
    fn default() -> Self {
        SymbolIndex { classes: HashMap::default() }
    }
}

impl<S: Hash + Eq + Clone> SymbolIndex<S> {
    pub fn rebuild<A: Analysis + Default>(&mut self, graph: &EGraph<S, A>) {
        for list in self.classes.values_mut() {
            list.clear();
        }
        for (id, class) in graph.canonical_classes() {
            self.add_class(graph, id, class.nodes());
        }
    }

    /// Indexes the e-classes of `ids` without rebuilding.
    pub fn add<A: Analysis + Default>(&mut self, graph: &EGraph<S, A>, ids: &[Id]) {
        for &id in ids {
            let id = graph.find(id);
            self.add_class(graph, id, graph.eclass(id).nodes());
        }
    }

    fn add_class<A: Analysis + Default>(&mut self, graph: &EGraph<S, A>, id: Id, nodes: &[Id]) {
        for &n in nodes {
            let symbol = graph.get_enode(n).symbol();
            match self.classes.get_mut(symbol) {
                Some(list) if list.last() == Some(&id) => {}
                Some(list) => list.push(id),
                None => {
                    self.classes.insert(symbol.clone(), vec![id]);
                }
            }
        }
    }

    fn of(&self, symbol: &S) -> &[Id] {
        self.classes.get(symbol).map_or(&[], Vec::as_slice)
    }
}

/// The matches of one run, each `vars().len()` e-classes long.
#[derive(Default)]
pub struct Matches {
    width: usize,
    flat: Vec<Id>,
    seen: HashSet<SmallVec<[Id; 4]>, FxBuildHasher>,
}

impl Matches {
    pub fn len(&self) -> usize {
        if self.width == 0 { 0 } else { self.flat.len() / self.width }
    }
    pub fn is_empty(&self) -> bool {
        self.flat.is_empty()
    }
    pub fn iter(&self) -> impl Iterator<Item = &[Id]> {
        self.flat.chunks(self.width.max(1))
    }
    fn reset(&mut self, width: usize) {
        self.width = width;
        self.flat.clear();
        self.seen.clear();
    }
    fn push(&mut self, slots: &[Id]) {
        if self.seen.insert(SmallVec::from_slice(slots)) {
            self.flat.extend_from_slice(slots);
        }
    }
}

const UNBOUND: Id = Id(u32::MAX);

struct Search<'a, S, A> {
    matcher: &'a Matcher<S>,
    graph: &'a EGraph<S, A>,
    index: &'a SymbolIndex<S>,
    slots: SmallVec<[Id; 8]>,
    /// Pending (pattern node, e-class) goals of the current pattern.
    goals: Vec<(u32, Id)>,
}

impl<S: Hash + Eq + Clone> Matcher<S> {
    /// Fills `out` with every match in `graph`, which must be rebuilt.
    /// `index` must hold every e-class's symbols, possibly under stale ids.
    pub fn run<A: Analysis + Default>(&self, graph: &EGraph<S, A>, index: &SymbolIndex<S>, out: &mut Matches) {
        debug_assert!(!graph.needs_rebuild(), "matching needs a rebuilt graph");
        out.reset(self.vars.len());
        let mut search = Search {
            matcher: self,
            graph,
            index,
            slots: SmallVec::from_elem(UNBOUND, self.vars.len()),
            goals: Vec::new(),
        };
        search.pattern(0, out);
    }
}

impl<S: Hash + Eq + Clone, A: Analysis + Default> Search<'_, S, A> {
    fn slot(&self, var: u8) -> usize {
        self.matcher.slot_of[var as usize] as usize
    }

    /// Matches pattern `p` and the ones after it, given the slots bound so far.
    fn pattern(&mut self, p: usize, out: &mut Matches) {
        let Some(pattern) = self.matcher.patterns.get(p) else {
            out.push(&self.slots);
            return;
        };
        let root = pattern.root();
        let graph = self.graph;
        match &pattern.nodes[root as usize] {
            PatternNode::Var(v) if self.slots[self.slot(*v)] != UNBOUND => {
                let class = self.slots[self.slot(*v)];
                self.start(p, root, class, out);
            }
            PatternNode::Sym { symbol, .. } => {
                let index = self.index;
                for &id in index.of(symbol) {
                    self.start(p, root, graph.find(id), out);
                }
            }
            _ => {
                for (id, _) in graph.canonical_classes() {
                    self.start(p, root, id, out);
                }
            }
        }
    }

    fn start(&mut self, p: usize, root: u32, class: Id, out: &mut Matches) {
        debug_assert!(self.goals.is_empty());
        self.goals.push((root, class));
        self.solve(p, out);
        self.goals.clear();
    }

    /// Discharges the pending goals of pattern `p`, then moves to the next.
    fn solve(&mut self, p: usize, out: &mut Matches) {
        let Some((node, class)) = self.goals.pop() else {
            self.pattern(p + 1, out);
            return;
        };
        let matcher = self.matcher;
        match &matcher.patterns[p].nodes[node as usize] {
            PatternNode::Wild => self.solve(p, out),
            PatternNode::Var(v) => self.bind_then(*v, class, |s| s.solve(p, out)),
            PatternNode::Sym { bind: Some(v), .. } => {
                let v = *v;
                self.bind_then(v, class, |s| s.expand(p, node, class, out));
            }
            PatternNode::Sym { .. } => self.expand(p, node, class, out),
        }
        self.goals.push((node, class));
    }

    /// Binds `var` to `class` unless it holds another e-class, runs `then`,
    /// and undoes the binding.
    fn bind_then(&mut self, var: u8, class: Id, then: impl FnOnce(&mut Self)) {
        let slot = self.slot(var);
        let held = self.slots[slot];
        if held == UNBOUND {
            self.slots[slot] = class;
            then(self);
            self.slots[slot] = UNBOUND;
        } else if held == class {
            then(self);
        }
    }

    /// Tries each e-node of `class` that fits symbol node `node`.
    fn expand(&mut self, p: usize, node: u32, class: Id, out: &mut Matches) {
        let matcher = self.matcher;
        let graph = self.graph;
        let PatternNode::Sym { symbol, children, .. } = &matcher.patterns[p].nodes[node as usize] else {
            unreachable!("expand takes a symbol node");
        };
        if matcher.ground[p][node as usize] {
            if self.occurs(p, node, class) {
                self.solve(p, out);
            }
            return;
        }
        let depth = self.goals.len();
        for &n in graph.eclass(class).nodes() {
            let enode = graph.get_enode(n);
            if enode.symbol() != symbol || enode.children().len() != children.len() {
                continue;
            }
            for (&child, &kid) in children.iter().zip(enode.children()).rev() {
                self.goals.push((child, graph.find(kid)));
            }
            self.solve(p, out);
            self.goals.truncate(depth);
        }
    }

    /// Whether the variable-free subtree at `node` occurs in `class`.
    fn occurs(&self, p: usize, node: u32, class: Id) -> bool {
        let graph = self.graph;
        match &self.matcher.patterns[p].nodes[node as usize] {
            PatternNode::Wild => true,
            PatternNode::Var(_) => unreachable!("a ground subtree names no variable"),
            PatternNode::Sym { symbol, children, .. } => graph.eclass(class).nodes().iter().any(|&n| {
                let enode = graph.get_enode(n);
                enode.symbol() == symbol
                    && enode.children().len() == children.len()
                    && children.iter().zip(enode.children()).all(|(&c, &k)| self.occurs(p, c, graph.find(k)))
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn graph_of(build: impl FnOnce(&mut EGraph<u8>) -> Vec<Id>) -> (EGraph<u8>, Vec<Id>) {
        let mut g = EGraph::<u8>::new();
        let ids = build(&mut g);
        g.rebuild();
        (g, ids)
    }

    fn run(g: &EGraph<u8>, patterns: &[Pattern<u8>]) -> Vec<Vec<Id>> {
        let matcher = Matcher::new(patterns).unwrap();
        let mut index = SymbolIndex::default();
        index.rebuild(g);
        let mut out = Matches::default();
        matcher.run(g, &index, &mut out);
        let mut v: Vec<Vec<Id>> = out.iter().map(<[Id]>::to_vec).collect();
        v.sort();
        v
    }

    #[test]
    fn a_match_reached_twice_is_reported_once() {
        // x, S(x,x,x) twice under different disambiguators: S(?1, ?0, ?0)
        // reaches (x, x) from both roots.
        let (g, ids) = graph_of(|g| {
            let x = g.emplace(0, &[], 0).eclass;
            g.emplace(0, &[x, x, x], 0);
            g.emplace(0, &[x, x, x], 1);
            vec![x]
        });
        let mut b = PatternBuilder::new();
        let (v1, v0a, v0b) = (b.var(1), b.var(0), b.var(0));
        b.sym(0, &[v1, v0a, v0b], None);
        assert_eq!(run(&g, &[b.build()]), vec![vec![ids[0], ids[0]]]);
    }

    #[test]
    fn rejects_rules_without_variables() {
        let mut b = PatternBuilder::<u8>::new();
        b.wild();
        assert_eq!(Matcher::new(&[b.build()]).err(), Some(RuleError::NoVariable(0)));
        assert_eq!(Matcher::<u8>::new(&[]).err(), Some(RuleError::NoPattern));
    }
}
