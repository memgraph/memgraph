//! The analysis the e-graph tests use, and the facts a refresh must reach.
//!
//! Facts are a 16-bit set and a join is their union, so the facts form a
//! lattice of finite height. Each symbol either seeds facts when an e-node is
//! made, derives them from its children, or neither:
//!
//!  - `SYM_LEAF` seeds bit `disambiguator % 16`;
//!  - `SYM_OPAQUE` seeds nothing; it gains facts only from what is merged in;
//!  - `SYM_PLUS` derives the union of its children's facts;
//!  - every other symbol neither seeds nor derives.
//!
//! A seed is a function of the e-node's content, so identical e-nodes always
//! carry identical seeds.
//!
//! What a refresh must reach, for a given partition of the terms into classes:
//! the least assignment of facts to classes such that a class holds the seed
//! of every term in it, and the class of a deriving term holds what that term
//! derives from its children's classes.

use std::collections::BTreeMap;

use crate::Term;

pub const SYM_LEAF: u8 = 0;
pub const SYM_OPAQUE: u8 = 1;
pub const SYM_PLUS: u8 = 2;
pub const SYM_F: u8 = 3;

pub fn seed(symbol: u8, disambiguator: u64) -> u16 {
    if symbol == SYM_LEAF { 1 << (disambiguator % 16) } else { 0 }
}

pub fn derives(symbol: u8) -> bool {
    symbol == SYM_PLUS
}

/// What one term contributes to its class, given the facts of every class.
fn contribution(term: &Term, labels: &[usize], facts: &BTreeMap<usize, u16>) -> u16 {
    let mut add = seed(term.symbol, term.disambiguator);
    if derives(term.symbol) {
        for &child in &term.children {
            add |= facts.get(&labels[child]).copied().unwrap_or(0);
        }
    }
    add
}

/// The least facts per term: `labels[t]` names the class of term `t`, and the
/// answer gives, for each term, the facts of its class.
pub fn least_facts(terms: &[Term], labels: &[usize]) -> Vec<u16> {
    least_facts_in_order(terms, labels, &(0..terms.len()).collect::<Vec<_>>())
}

/// Kleene iteration visiting terms in `order` each round.
fn least_facts_in_order(terms: &[Term], labels: &[usize], order: &[usize]) -> Vec<u16> {
    let mut facts: BTreeMap<usize, u16> = BTreeMap::new();
    loop {
        let mut changed = false;
        for &t in order {
            let add = contribution(&terms[t], labels, &facts);
            let slot = facts.entry(labels[t]).or_insert(0);
            if *slot | add != *slot {
                *slot |= add;
                changed = true;
            }
        }
        if !changed {
            return (0..terms.len()).map(|t| facts.get(&labels[t]).copied().unwrap_or(0)).collect();
        }
    }
}

#[cfg(test)]
mod laws {
    use super::*;
    use proptest::prelude::*;

    fn terms_strategy() -> impl Strategy<Value = Vec<Term>> {
        prop::collection::vec((0u8..5, 0u64..20, prop::collection::vec(any::<u32>(), 0..3)), 0..30).prop_map(
            |raw| {
                let mut terms: Vec<Term> = Vec::new();
                for (symbol, disambiguator, kids) in raw {
                    let n = terms.len();
                    let children = if n == 0 { Vec::new() } else { kids.iter().map(|&k| k as usize % n).collect() };
                    terms.push(Term { symbol, disambiguator, children });
                }
                terms
            },
        )
    }

    /// Labels for a partition: each term points at a smaller-or-equal term's label.
    fn partition(n: usize, raw: &[u32]) -> Vec<usize> {
        let mut labels: Vec<usize> = (0..n).collect();
        for (i, r) in raw.iter().enumerate().take(n) {
            let target = *r as usize % (i + 1);
            labels[i] = labels[target];
        }
        labels
    }

    /// Facts of a term on a graph with no merges, by recursion on the term.
    fn tree_facts(terms: &[Term], t: usize) -> u16 {
        let term = &terms[t];
        let mut f = seed(term.symbol, term.disambiguator);
        if derives(term.symbol) {
            for &c in &term.children {
                f |= tree_facts(terms, c);
            }
        }
        f
    }

    proptest! {
        #[test]
        fn the_result_satisfies_every_constraint(terms in terms_strategy(), raw in prop::collection::vec(any::<u32>(), 30)) {
            let labels = partition(terms.len(), &raw);
            let facts = least_facts(&terms, &labels);
            for (t, term) in terms.iter().enumerate() {
                let mut need = seed(term.symbol, term.disambiguator);
                if derives(term.symbol) {
                    for &c in &term.children {
                        need |= facts[c];
                    }
                }
                prop_assert_eq!(facts[t] & need, need, "term {} lacks facts it is owed", t);
                prop_assert_eq!(facts[t], facts.iter().zip(&labels).find(|(_, l)| **l == labels[t]).map(|(f, _)| *f).unwrap());
            }
        }

        #[test]
        fn visit_order_does_not_matter(terms in terms_strategy(), raw in prop::collection::vec(any::<u32>(), 30)) {
            let labels = partition(terms.len(), &raw);
            let reverse: Vec<usize> = (0..terms.len()).rev().collect();
            prop_assert_eq!(least_facts(&terms, &labels), least_facts_in_order(&terms, &labels, &reverse));
        }

        #[test]
        fn without_merges_facts_are_those_of_the_tree(terms in terms_strategy()) {
            let labels: Vec<usize> = (0..terms.len()).collect();
            let facts = least_facts(&terms, &labels);
            for t in 0..terms.len() {
                prop_assert_eq!(facts[t], tree_facts(&terms, t));
            }
        }

        #[test]
        fn merging_classes_only_adds_facts(terms in terms_strategy(), raw in prop::collection::vec(any::<u32>(), 30), a: u32, b: u32) {
            prop_assume!(!terms.is_empty());
            let labels = partition(terms.len(), &raw);
            let before = least_facts(&terms, &labels);
            let (la, lb) = (labels[a as usize % terms.len()], labels[b as usize % terms.len()]);
            let coarser: Vec<usize> = labels.iter().map(|&l| if l == lb { la } else { l }).collect();
            let after = least_facts(&terms, &coarser);
            for t in 0..terms.len() {
                prop_assert_eq!(after[t] & before[t], before[t]);
            }
        }
    }
}
