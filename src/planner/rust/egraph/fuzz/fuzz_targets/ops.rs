//! Coverage-guided fuzzing of the e-graph port and its matcher against the
//! reference model. The input decodes to the operations the proptest harness
//! generates; after every step the port's invariant must hold, after every
//! rebuild its partition must be the model's congruence closure, and every
//! match, over a fresh or a kept index, must be the model's set of matches,
//! with no match reported twice.

#![no_main]

use std::collections::BTreeSet;

use arbitrary::Arbitrary;
use egraph_oracle::Model;
use egraph_oracle::matching::{Pat, matches};
use libfuzzer_sys::fuzz_target;
use mg_planner_egraph::matching::{Matcher, Matches, Pattern, PatternBuilder, SymbolIndex};
use mg_planner_egraph::{EGraph, Id};

#[derive(Arbitrary, Debug)]
enum FPat {
    Var(u8),
    Wild,
    Sym(u8, Vec<FPat>, Option<u8>),
}

#[derive(Arbitrary, Debug)]
enum Op {
    Add(u8, u8, Vec<u16>),
    Merge(u16, u16),
    Rebuild,
    Match(Vec<FPat>, bool),
}

/// Bounds the decoded pattern to the harness's shapes: few symbols and
/// variables, depth three, three children.
fn to_pat(p: &FPat, depth: usize) -> Pat {
    match p {
        FPat::Var(v) => Pat::Var(v % 3),
        FPat::Wild => Pat::Wild,
        FPat::Sym(s, kids, bind) => Pat::Sym {
            symbol: s % 4,
            children: if depth == 0 { vec![] } else { kids.iter().take(3).map(|k| to_pat(k, depth - 1)).collect() },
            bind: bind.map(|v| v % 3),
        },
    }
}

fn to_port(p: &Pat, b: &mut PatternBuilder<u8>) -> u32 {
    match p {
        Pat::Var(v) => b.var(*v),
        Pat::Wild => b.wild(),
        Pat::Sym { symbol, children, bind } => {
            let kids: Vec<u32> = children.iter().map(|c| to_port(c, b)).collect();
            b.sym(*symbol, &kids, *bind)
        }
    }
}

struct State {
    graph: EGraph<u8>,
    model: Model,
    terms: Vec<Id>,
    kept: Option<SymbolIndex<u8>>,
    inserted: Vec<Id>,
}

impl State {
    fn rebuild(&mut self) {
        self.graph.rebuild();
        self.model.close();
        for a in 0..self.terms.len() {
            for b in a + 1..self.terms.len() {
                let same = self.graph.find(self.terms[a]) == self.graph.find(self.terms[b]);
                assert_eq!(same, self.model.equivalent(a, b), "terms {a} and {b} after rebuild");
            }
        }
    }

    fn match_rule(&mut self, rule: &[Pat], incremental: bool) {
        self.rebuild();
        let patterns: Vec<Pattern<u8>> = rule
            .iter()
            .map(|p| {
                let mut b = PatternBuilder::new();
                to_port(p, &mut b);
                b.build()
            })
            .collect();
        let (matcher, expected) = match (Matcher::new(&patterns), matches(self.model.terms(), &self.labels(), rule)) {
            (Ok(m), Ok(e)) => (m, e),
            (Err(_), Err(_)) => return,
            (got, want) => panic!("port and model disagree on whether {rule:?} is valid: {:?} vs {want:?}", got.err()),
        };
        let mut fresh = SymbolIndex::default();
        let index = if incremental {
            match &mut self.kept {
                Some(index) => index.add(&self.graph, &self.inserted),
                None => {
                    let mut index = SymbolIndex::default();
                    index.rebuild(&self.graph);
                    self.kept = Some(index);
                }
            }
            self.inserted.clear();
            self.kept.as_ref().unwrap()
        } else {
            fresh.rebuild(&self.graph);
            &fresh
        };
        let mut out = Matches::default();
        matcher.run(&self.graph, index, &mut out);
        let got: Vec<Vec<usize>> = out.iter().map(|t| t.iter().map(|id| id.raw() as usize).collect()).collect();
        let got_set: BTreeSet<Vec<usize>> = got.iter().cloned().collect();
        assert_eq!(got.len(), got_set.len(), "a match of {rule:?} reported twice");
        let want: BTreeSet<Vec<usize>> = expected.iter().map(|s| s.values().copied().collect()).collect();
        assert_eq!(got_set, want, "matches of {rule:?}");
    }

    fn labels(&self) -> Vec<usize> {
        self.terms.iter().map(|&t| self.graph.find(t).raw() as usize).collect()
    }
}

fuzz_target!(|ops: Vec<Op>| {
    let mut st =
        State { graph: EGraph::new(), model: Model::default(), terms: Vec::new(), kept: None, inserted: Vec::new() };
    for op in ops.iter().take(64) {
        match op {
            Op::Add(symbol, dis, kids) => {
                if st.terms.is_empty() && !kids.is_empty() {
                    continue;
                }
                let kids: Vec<usize> = kids.iter().take(3).map(|&k| k as usize % st.terms.len().max(1)).collect();
                let children: Vec<Id> = kids.iter().map(|&k| st.terms[k]).collect();
                let result = st.graph.emplace(symbol % 4, &children, (*dis % 3) as u64);
                st.terms.push(result.eclass);
                if result.did_insert {
                    st.inserted.push(result.eclass);
                }
                st.model.add(symbol % 4, (*dis % 3) as u64, kids);
            }
            Op::Merge(a, b) => {
                if st.terms.is_empty() {
                    continue;
                }
                let (a, b) = (*a as usize % st.terms.len(), *b as usize % st.terms.len());
                st.graph.merge(st.terms[a], st.terms[b]);
                st.model.merge(a, b);
            }
            Op::Rebuild => st.rebuild(),
            Op::Match(rule, incremental) => {
                let rule: Vec<Pat> = rule.iter().take(3).map(|p| to_pat(p, 3)).collect();
                st.match_rule(&rule, *incremental);
            }
        }
        if let Err(v) = st.graph.check_invariant() {
            panic!("invariant after {op:?}: {v}");
        }
    }
});
