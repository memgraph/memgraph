//! Reference model of e-matching, written from its specification.
//!
//! A pattern is a tree of variables, wildcards and symbol nodes. It matches an
//! e-class under a substitution, a map from variables to e-classes, when:
//!
//! - a variable is mapped to that e-class;
//! - a wildcard matches any e-class and binds nothing;
//! - a symbol node matches when the e-class holds an e-node with that symbol
//!   and as many children as the node has, each child e-class matching the
//!   corresponding child pattern. A symbol node that names a variable also
//!   maps that variable to the e-class it matched.
//!
//! The disambiguator plays no part. A rule's patterns share one substitution,
//! so a variable named in two patterns joins them, and patterns that share no
//! variable combine as a cross product. The matches of a rule are the
//! substitutions under which every pattern matches some e-class, restricted to
//! the variables the patterns name. They form a set: a matcher reports each
//! substitution once, however many ways it can be reached.
//!
//! A rule is rejected when it has no pattern, when a pattern names no
//! variable, or when one variable is bound by two symbol nodes.
//!
//! E-classes are the model's blocks, so the model answers for the graph after
//! `rebuild()`. It enumerates every candidate e-class for every pattern node
//! and is meant for graphs of a few dozen terms and patterns of a few nodes.

use std::collections::{BTreeMap, BTreeSet};

use crate::Term;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Pat {
    Var(u8),
    Wild,
    Sym { symbol: u8, children: Vec<Pat>, bind: Option<u8> },
}

impl Pat {
    pub fn vars(&self, out: &mut BTreeSet<u8>) {
        match self {
            Pat::Var(v) => {
                out.insert(*v);
            }
            Pat::Wild => {}
            Pat::Sym { children, bind, .. } => {
                out.extend(bind.iter().copied());
                for child in children {
                    child.vars(out);
                }
            }
        }
    }

    /// The prefix encoding the replay driver's `match` command reads.
    pub fn encode(&self, out: &mut String) {
        match self {
            Pat::Var(v) => out.push_str(&format!(" v {v}")),
            Pat::Wild => out.push_str(" w"),
            Pat::Sym { symbol, children, bind } => {
                let bind = bind.map_or(-1, i32::from);
                out.push_str(&format!(" s {symbol} {} {bind}", children.len()));
                for child in children {
                    child.encode(out);
                }
            }
        }
    }
}

pub type Subst = BTreeMap<u8, usize>;

#[derive(Debug, PartialEq, Eq)]
pub enum Rejected {
    NoPattern,
    /// The index of a pattern that names no variable.
    NoVariable(usize),
    /// A variable two symbol nodes bind.
    BoundTwice(u8),
}

impl Pat {
    fn symbol_binds(&self, out: &mut Vec<u8>) {
        if let Pat::Sym { children, bind, .. } = self {
            out.extend(bind.iter().copied());
            for child in children {
                child.symbol_binds(out);
            }
        }
    }
}

/// The variable that more than one symbol node of `patterns` binds, if any.
pub fn bound_twice(patterns: &[Pat]) -> Option<u8> {
    let mut binds = vec![];
    for p in patterns {
        p.symbol_binds(&mut binds);
    }
    binds.sort_unstable();
    binds.windows(2).find(|w| w[0] == w[1]).map(|w| w[0])
}

/// The canonical e-nodes of the graph whose blocks are `labels`: for each
/// block, its distinct (symbol, child blocks) shapes.
fn shapes(terms: &[Term], labels: &[usize]) -> BTreeMap<usize, BTreeSet<(u8, Vec<usize>)>> {
    let mut out: BTreeMap<usize, BTreeSet<(u8, Vec<usize>)>> = BTreeMap::new();
    for (t, term) in terms.iter().enumerate() {
        let kids = term.children.iter().map(|&c| labels[c]).collect();
        out.entry(labels[t]).or_default().insert((term.symbol, kids));
    }
    out
}

type Shapes = BTreeMap<usize, BTreeSet<(u8, Vec<usize>)>>;

/// Every extension of `subst` under which `pat` matches `class`.
fn match_at(shapes: &Shapes, pat: &Pat, class: usize, subst: Subst) -> Vec<Subst> {
    match pat {
        Pat::Wild => vec![subst],
        Pat::Var(v) => bind(subst, *v, class).into_iter().collect(),
        Pat::Sym { symbol, children, bind: binding } => {
            let subst = match binding {
                Some(v) => match bind(subst, *v, class) {
                    Some(s) => s,
                    None => return vec![],
                },
                None => subst,
            };
            let mut out = vec![];
            for (sym, kids) in &shapes[&class] {
                if sym != symbol || kids.len() != children.len() {
                    continue;
                }
                let mut partial = vec![subst.clone()];
                for (child, &kid) in children.iter().zip(kids) {
                    partial = partial.into_iter().flat_map(|s| match_at(shapes, child, kid, s)).collect();
                }
                out.extend(partial);
            }
            out
        }
    }
}

fn bind(mut subst: Subst, var: u8, class: usize) -> Option<Subst> {
    match subst.get(&var) {
        Some(&bound) if bound != class => None,
        Some(_) => Some(subst),
        None => {
            subst.insert(var, class);
            Some(subst)
        }
    }
}

/// The matches of the rule made of `patterns` on the graph whose blocks are
/// `labels`, as substitutions from variables to block labels.
pub fn matches(terms: &[Term], labels: &[usize], patterns: &[Pat]) -> Result<BTreeSet<Subst>, Rejected> {
    if patterns.is_empty() {
        return Err(Rejected::NoPattern);
    }
    for (i, p) in patterns.iter().enumerate() {
        let mut vars = BTreeSet::new();
        p.vars(&mut vars);
        if vars.is_empty() {
            return Err(Rejected::NoVariable(i));
        }
    }
    if let Some(v) = bound_twice(patterns) {
        return Err(Rejected::BoundTwice(v));
    }
    let shapes = shapes(terms, labels);
    let mut substs = vec![Subst::new()];
    for pat in patterns {
        let mut next = vec![];
        for s in &substs {
            for &class in shapes.keys() {
                next.extend(match_at(&shapes, pat, class, s.clone()));
            }
        }
        substs = next;
    }
    Ok(substs.into_iter().collect())
}

#[cfg(test)]
mod examples {
    use super::*;

    fn leaf(symbol: u8) -> Term {
        Term { symbol, disambiguator: 0, children: vec![] }
    }

    fn node(symbol: u8, children: Vec<usize>) -> Term {
        Term { symbol, disambiguator: 0, children }
    }

    fn sym(symbol: u8, children: Vec<Pat>) -> Pat {
        Pat::Sym { symbol, children, bind: None }
    }

    fn subst(pairs: &[(u8, usize)]) -> Subst {
        pairs.iter().copied().collect()
    }

    #[test]
    fn repeated_variable_requires_one_class() {
        // 0: a, 1: b, 2: F(a, a), 3: F(a, b)
        let terms = vec![leaf(0), leaf(1), node(5, vec![0, 0]), node(5, vec![0, 1])];
        let labels = vec![0, 1, 2, 3];
        let got = matches(&terms, &labels, &[sym(5, vec![Pat::Var(0), Pat::Var(0)])]).unwrap();
        assert_eq!(got, BTreeSet::from([subst(&[(0, 0)])]));
    }

    #[test]
    fn arity_must_agree() {
        let terms = vec![leaf(0), node(5, vec![0]), node(5, vec![0, 0])];
        let labels = vec![0, 1, 2];
        let got = matches(&terms, &labels, &[sym(5, vec![Pat::Var(0)])]).unwrap();
        assert_eq!(got, BTreeSet::from([subst(&[(0, 0)])]));
    }

    #[test]
    fn wildcards_collapse_into_one_match() {
        // F(a, b) and F(a, c) merged: F(?0, _) has one match.
        let terms = vec![leaf(0), leaf(1), leaf(2), node(5, vec![0, 1]), node(5, vec![0, 2])];
        let labels = vec![0, 1, 2, 3, 3];
        let got = matches(&terms, &labels, &[sym(5, vec![Pat::Var(0), Pat::Wild])]).unwrap();
        assert_eq!(got, BTreeSet::from([subst(&[(0, 0)])]));
    }

    #[test]
    fn shared_variables_join_patterns() {
        // F(a), G(a), G(b): F(?0) joined with G(?0) leaves ?0 = a.
        let terms = vec![leaf(0), leaf(1), node(5, vec![0]), node(6, vec![0]), node(6, vec![1])];
        let labels = vec![0, 1, 2, 3, 4];
        let got =
            matches(&terms, &labels, &[sym(5, vec![Pat::Var(0)]), sym(6, vec![Pat::Var(0)])]).unwrap();
        assert_eq!(got, BTreeSet::from([subst(&[(0, 0)])]));
        let disjoint =
            matches(&terms, &labels, &[sym(5, vec![Pat::Var(0)]), sym(6, vec![Pat::Var(1)])]).unwrap();
        assert_eq!(disjoint.len(), 2);
    }

    #[test]
    fn cyclic_class_matches_at_every_depth() {
        // a merged with F(a): F(F(?0)) matches with ?0 = that class.
        let terms = vec![leaf(0), node(5, vec![0])];
        let labels = vec![0, 0];
        let got = matches(&terms, &labels, &[sym(5, vec![sym(5, vec![Pat::Var(0)])])]).unwrap();
        assert_eq!(got, BTreeSet::from([subst(&[(0, 0)])]));
    }

    #[test]
    fn patterns_without_variables_are_rejected() {
        let terms = vec![leaf(0)];
        assert_eq!(matches(&terms, &[0], &[]), Err(Rejected::NoPattern));
        assert_eq!(matches(&terms, &[0], &[Pat::Var(0), sym(0, vec![])]), Err(Rejected::NoVariable(1)));
        let bound = |v| Pat::Sym { symbol: 0, children: vec![], bind: Some(v) };
        assert_eq!(matches(&terms, &[0], &[bound(2), bound(2)]), Err(Rejected::BoundTwice(2)));
    }
}
