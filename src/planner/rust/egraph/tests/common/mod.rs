//! What an e-graph is held to, written once against `Subject` so the same
//! checks run on the Rust port and on the C++ e-graph.
//!
//! The e-graph holds the congruence closure the reference model computes
//! whenever it has just been rebuilt:
//!
//!  - two terms share an e-class exactly when the model puts them in one block;
//!  - there is one e-class per block, holding exactly the distinct canonical
//!    e-nodes of that block;
//!  - no rebuild is pending;
//!  - the touched set names every e-class inserted, merged or merged by a
//!    rebuild's congruence cascade since it was last cleared.
//!
//! Between rebuilds: a merge of terms the closure keeps apart reports
//! did_merge, a merge on a rebuilt graph reports did_merge exactly when the
//! closure keeps them apart, and an emplace on a rebuilt graph reports
//! did_insert exactly when no term has that canonical form. A subject that can
//! check its own representation invariant does so after every operation.
//!
//! A subject that runs the analysis (a 16-bit set of facts, see
//! `egraph_oracle::analysis`) is also held to these after every refresh:
//!
//!  - P1: every term's facts are the least fixpoint over the subject's current
//!    partition, whichever scope the refresh used;
//!  - P2: an immediate further refresh of everything changes nothing;
//!  - P6: the touched set names every class a refresh widened, and, straight
//!    after a clear, only those;
//!  - P8: a refresh reports 0 exactly when no term's facts changed;
//!
//! and to these after every operation: facts only grow (P4), a new term's class
//! holds its seed, and a merge leaves both sides with the union of their facts (P5).

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use egraph_oracle::Model;
use egraph_oracle::analysis::{derives, least_facts, seed};
use mg_planner_egraph::{Analysis, EGraph, Id, Scope};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

/// An e-node as a subject reports it: symbol, disambiguator, child e-classes.
pub type Reported = (u8, u64, Vec<u32>);

/// An e-graph under test. Terms are numbered in the order `add` creates them;
/// e-class ids are the subject's own.
pub trait Subject {
    fn reset(&mut self);
    /// Adds `symbol(children)` over the canonical classes of the child terms.
    fn add(&mut self, symbol: u8, disambiguator: u64, children: &[usize]) -> (bool, u32);
    fn merge(&mut self, a: usize, b: usize) -> (bool, u32);
    fn rebuild(&mut self);
    fn clear_touched(&mut self);
    fn find(&mut self, term: usize) -> u32;
    fn needs_rebuild(&mut self) -> bool;
    /// Number of e-classes and of live e-nodes.
    fn stats(&mut self) -> (usize, usize);
    /// The e-nodes of a canonical e-class, children canonicalised; `None` if
    /// the id is not a canonical e-class.
    fn class(&mut self, eclass: u32) -> Option<Vec<Reported>>;
    fn touched(&mut self) -> Vec<u32>;
    fn check_invariant(&mut self) -> Result<(), String> {
        Ok(())
    }
    /// Whether the subject runs the analysis; the three below matter only then.
    fn supports_analysis(&self) -> bool {
        false
    }
    fn facts(&mut self, _term: usize) -> u16 {
        0
    }
    /// Refreshes the analysis, of everything or of what changed, and answers
    /// how many updates widened a class.
    fn refresh(&mut self, _everything: bool) -> usize {
        0
    }
}

/// Facts as a 16-bit set; a join is the union.
#[derive(Default, Clone, Copy, PartialEq, Eq, Debug)]
pub struct Mask(pub u16);

impl Analysis for Mask {
    fn merge(&mut self, other: &Self) {
        self.0 |= other.0;
    }
}

/// The Rust port, in-process.
#[derive(Default)]
pub struct Port {
    graph: EGraph<u8, Mask>,
    terms: Vec<Id>,
}

impl Subject for Port {
    fn reset(&mut self) {
        *self = Port::default();
    }
    fn add(&mut self, symbol: u8, disambiguator: u64, children: &[usize]) -> (bool, u32) {
        let children: Vec<Id> = children.iter().map(|&t| self.graph.find(self.terms[t])).collect();
        let result = self.graph.emplace_with(symbol, &children, disambiguator, Mask(seed(symbol, disambiguator)));
        self.terms.push(result.eclass);
        (result.did_insert, result.eclass.raw())
    }
    fn merge(&mut self, a: usize, b: usize) -> (bool, u32) {
        let result = self.graph.merge(self.terms[a], self.terms[b]);
        (result.did_merge, result.eclass.raw())
    }
    fn rebuild(&mut self) {
        self.graph.rebuild();
    }
    fn clear_touched(&mut self) {
        self.graph.clear_touched();
    }
    fn find(&mut self, term: usize) -> u32 {
        self.graph.find(self.terms[term]).raw()
    }
    fn needs_rebuild(&mut self) -> bool {
        self.graph.needs_rebuild()
    }
    fn stats(&mut self) -> (usize, usize) {
        (self.graph.num_classes(), self.graph.num_live_nodes())
    }
    fn class(&mut self, eclass: u32) -> Option<Vec<Reported>> {
        let id = Id::new(eclass);
        if !self.graph.has_class(id) {
            return None;
        }
        let graph = &self.graph;
        Some(
            graph
                .eclass(id)
                .nodes()
                .iter()
                .map(|&n| {
                    let node = graph.get_enode(n);
                    let children = node.children().iter().map(|&c| graph.find(c).raw()).collect();
                    (*node.symbol(), node.disambiguator(), children)
                })
                .collect(),
        )
    }
    fn touched(&mut self) -> Vec<u32> {
        let mut out = Vec::new();
        self.graph.touched_into(&mut out);
        out.into_iter().map(Id::raw).collect()
    }
    fn check_invariant(&mut self) -> Result<(), String> {
        self.graph.check_invariant().map_err(|v| v.to_string())
    }
    fn supports_analysis(&self) -> bool {
        true
    }
    fn facts(&mut self, term: usize) -> u16 {
        self.graph.analysis_of(self.terms[term]).0
    }
    fn refresh(&mut self, everything: bool) -> usize {
        let scope = if everything { Scope::Everything } else { Scope::Changed };
        self.graph.refresh_analysis(scope, |node, facts_of| {
            derives(*node.symbol()).then(|| Mask(node.children().iter().fold(0, |acc, &c| acc | facts_of(c).0)))
        })
    }
}

/// The C++ e-graph, through the replay driver running as a subprocess.
pub struct Cpp {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    line: String,
}

impl Cpp {
    /// The replay driver, from `MG_EGRAPH_REPLAY` or the default build tree.
    pub fn binary() -> Option<PathBuf> {
        let path = std::env::var_os("MG_EGRAPH_REPLAY").map(PathBuf::from).unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../../build/tests/oracle/egraph_replay")
        });
        path.is_file().then_some(path)
    }

    pub fn spawn(binary: &PathBuf) -> Cpp {
        let mut child = Command::new(binary)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("cannot start {}: {e}", binary.display()));
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        Cpp { child, stdin, stdout, line: String::new() }
    }

    /// Sends one command and answers the fields after `ok`.
    fn ask(&mut self, command: &str) -> Vec<u64> {
        writeln!(self.stdin, "{command}").expect("replay driver closed its input");
        self.line.clear();
        self.stdout.read_line(&mut self.line).expect("replay driver closed its output");
        let mut fields = self.line.split_whitespace();
        let status = fields.next().unwrap_or("");
        assert_eq!(status, "ok", "replay driver answered `{}` to `{command}`", self.line.trim());
        fields.map(|f| f.parse().expect("numeric field")).collect()
    }
}

impl Drop for Cpp {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Subject for Cpp {
    fn reset(&mut self) {
        self.ask("reset");
    }
    fn add(&mut self, symbol: u8, disambiguator: u64, children: &[usize]) -> (bool, u32) {
        let mut command = format!("add {symbol} {disambiguator} {}", children.len());
        for c in children {
            command += &format!(" {c}");
        }
        let f = self.ask(&command);
        (f[0] == 1, f[1] as u32)
    }
    fn merge(&mut self, a: usize, b: usize) -> (bool, u32) {
        let f = self.ask(&format!("merge {a} {b}"));
        (f[0] == 1, f[1] as u32)
    }
    fn rebuild(&mut self) {
        self.ask("rebuild");
    }
    fn clear_touched(&mut self) {
        self.ask("clear_touched");
    }
    fn find(&mut self, term: usize) -> u32 {
        self.ask(&format!("find {term}"))[0] as u32
    }
    fn needs_rebuild(&mut self) -> bool {
        self.ask("needs_rebuild")[0] == 1
    }
    fn stats(&mut self) -> (usize, usize) {
        let f = self.ask("stats");
        (f[0] as usize, f[1] as usize)
    }
    fn class(&mut self, eclass: u32) -> Option<Vec<Reported>> {
        writeln!(self.stdin, "class {eclass}").expect("replay driver closed its input");
        self.line.clear();
        self.stdout.read_line(&mut self.line).expect("replay driver closed its output");
        if self.line.starts_with("error") {
            return None;
        }
        let f: Vec<u64> = self.line.split_whitespace().skip(1).map(|x| x.parse().unwrap()).collect();
        let (mut at, mut nodes) = (1, Vec::new());
        for _ in 0..f[0] {
            let (symbol, disambiguator, arity) = (f[at] as u8, f[at + 1], f[at + 2] as usize);
            let children = f[at + 3..at + 3 + arity].iter().map(|&c| c as u32).collect();
            nodes.push((symbol, disambiguator, children));
            at += 3 + arity;
        }
        Some(nodes)
    }
    fn touched(&mut self) -> Vec<u32> {
        let f = self.ask("touched");
        let mut ids: Vec<u32> = f[1..].iter().map(|&x| x as u32).collect();
        ids.sort_unstable();
        ids
    }
}

#[derive(Debug, Clone)]
pub enum Op {
    Add { symbol: u8, disambiguator: u64, children: Vec<u32> },
    Merge(u32, u32),
    Rebuild,
    ClearTouched,
    /// Refresh the analysis: of everything (true) or of what changed (false).
    Refresh(bool),
}

/// Few symbols and disambiguators, so adds collide with existing terms and
/// merges set off congruence cascades. Child and merge operands are raw draws
/// taken modulo the terms present.
pub fn ops(max_len: usize) -> impl Strategy<Value = Vec<Op>> {
    let op = prop_oneof![
        6 => (0u8..4, 0u64..3, prop::collection::vec(any::<u32>(), 0..4))
            .prop_map(|(symbol, disambiguator, children)| Op::Add { symbol, disambiguator, children }),
        3 => (any::<u32>(), any::<u32>()).prop_map(|(a, b)| Op::Merge(a, b)),
        1 => Just(Op::Rebuild),
        1 => Just(Op::ClearTouched),
        2 => any::<bool>().prop_map(Op::Refresh),
    ];
    prop::collection::vec(op, 0..max_len)
}

pub struct Harness<'s, S: Subject> {
    subject: &'s mut S,
    model: Model,
    terms: usize,
    changed_since_clear: Vec<usize>,
    /// The facts last seen for each term, which may only grow.
    facts_seen: Vec<u16>,
    /// Nothing but a clear has happened to the touched set since it was cleared.
    just_cleared: bool,
}

/// Runs one generated case against a subject that has just been reset.
pub fn run_case<S: Subject>(subject: &mut S, ops: &[Op]) -> Result<(), TestCaseError> {
    let mut harness = Harness {
        subject,
        model: Model::default(),
        terms: 0,
        changed_since_clear: Vec::new(),
        facts_seen: Vec::new(),
        just_cleared: false,
    };
    for op in ops {
        harness.apply(op)?;
    }
    harness.rebuild()
}

impl<S: Subject> Harness<'_, S> {
    fn term(&self, raw: u32) -> usize {
        raw as usize % self.terms
    }

    fn apply(&mut self, op: &Op) -> Result<(), TestCaseError> {
        match op {
            Op::Add { symbol, disambiguator, children } => self.add(*symbol, *disambiguator, children)?,
            Op::Merge(a, b) => self.merge(*a, *b)?,
            Op::Rebuild => self.rebuild()?,
            Op::ClearTouched => {
                self.subject.clear_touched();
                self.changed_since_clear.clear();
            }
            Op::Refresh(everything) => {
                if self.subject.supports_analysis() {
                    self.refresh(*everything)?;
                }
            }
        }
        self.just_cleared = matches!(op, Op::ClearTouched);
        if let Err(clause) = self.subject.check_invariant() {
            return Err(TestCaseError::fail(format!("after {op:?}: {clause}")));
        }
        if self.subject.supports_analysis() {
            for t in 0..self.terms {
                let now = self.subject.facts(t);
                let before = self.facts_seen[t];
                prop_assert_eq!(now & before, before, "facts of term {} shrank after {:?}", t, op);
                self.facts_seen[t] = now;
            }
        }
        Ok(())
    }

    /// P1, P2, P6 and P8 around one refresh.
    fn refresh(&mut self, everything: bool) -> Result<(), TestCaseError> {
        let was_cleared = self.just_cleared;
        let before: Vec<u16> = (0..self.terms).map(|t| self.subject.facts(t)).collect();
        let widened = self.subject.refresh(everything);
        let after: Vec<u16> = (0..self.terms).map(|t| self.subject.facts(t)).collect();

        let labels: Vec<usize> = (0..self.terms).map(|t| self.subject.find(t) as usize).collect();
        let expected = least_facts(self.model.terms(), &labels);
        for t in 0..self.terms {
            prop_assert_eq!(after[t], expected[t], "term {} after refresh(everything={})", t, everything);
        }
        prop_assert_eq!(widened == 0, before == after, "refresh reported {} widenings", widened);

        let changed: Vec<usize> = (0..self.terms).filter(|&t| before[t] != after[t]).collect();
        let touched = self.subject.touched();
        for &t in &changed {
            prop_assert!(touched.binary_search(&(labels[t] as u32)).is_ok(), "touched set misses widened term {}", t);
        }
        if was_cleared {
            let mut widened_classes: Vec<u32> = changed.iter().map(|&t| labels[t] as u32).collect();
            widened_classes.sort_unstable();
            widened_classes.dedup();
            prop_assert_eq!(touched, widened_classes, "touched set after clear and refresh");
        }
        self.changed_since_clear.extend(changed);

        prop_assert_eq!(self.subject.refresh(true), 0, "a second refresh widened something");
        for t in 0..self.terms {
            prop_assert_eq!(self.subject.facts(t), after[t]);
        }
        Ok(())
    }

    fn add(&mut self, symbol: u8, disambiguator: u64, raw_children: &[u32]) -> Result<(), TestCaseError> {
        if self.terms == 0 && !raw_children.is_empty() {
            return Ok(());
        }
        let children: Vec<usize> = raw_children.iter().map(|&c| self.term(c)).collect();
        let clean = !self.subject.needs_rebuild();
        let existing = if clean {
            self.model.close();
            let blocks: Vec<usize> = children.iter().map(|&c| self.model.block_of(c)).collect();
            self.model.find_enode(symbol, disambiguator, &blocks)
        } else {
            None
        };

        let (did_insert, _) = self.subject.add(symbol, disambiguator, &children);
        if clean {
            prop_assert_eq!(did_insert, existing.is_none());
        }
        let term = self.model.add(symbol, disambiguator, children);
        self.terms += 1;
        if self.subject.supports_analysis() {
            let facts = self.subject.facts(term);
            let own = seed(symbol, disambiguator);
            prop_assert_eq!(facts & own, own, "term {} lacks its seed", term);
            self.facts_seen.push(facts);
        }
        if did_insert {
            self.changed_since_clear.push(term);
        }
        Ok(())
    }

    fn merge(&mut self, raw_a: u32, raw_b: u32) -> Result<(), TestCaseError> {
        if self.terms == 0 {
            return Ok(());
        }
        let (a, b) = (self.term(raw_a), self.term(raw_b));
        let clean = !self.subject.needs_rebuild();
        self.model.close();
        let apart = !self.model.equivalent(a, b);

        let joined = if self.subject.supports_analysis() {
            self.subject.facts(a) | self.subject.facts(b)
        } else {
            0
        };
        let (did_merge, _) = self.subject.merge(a, b);
        if self.subject.supports_analysis() {
            prop_assert_eq!(self.subject.facts(a), joined, "merge did not join facts");
            prop_assert_eq!(self.subject.facts(b), joined, "merge did not join facts");
        }
        if apart {
            prop_assert!(did_merge);
        }
        if clean {
            prop_assert_eq!(did_merge, apart);
        }
        if did_merge {
            prop_assert!(self.subject.needs_rebuild());
            self.changed_since_clear.push(a);
        }
        self.model.merge(a, b);
        Ok(())
    }

    fn rebuild(&mut self) -> Result<(), TestCaseError> {
        let before: Vec<u32> = (0..self.terms).map(|t| self.subject.find(t)).collect();
        self.subject.rebuild();
        self.model.close();
        prop_assert!(!self.subject.needs_rebuild());

        let after: Vec<u32> = (0..self.terms).map(|t| self.subject.find(t)).collect();
        let mut first_before: BTreeMap<u32, u32> = BTreeMap::new();
        for t in 0..self.terms {
            let prior = *first_before.entry(after[t]).or_insert(before[t]);
            if prior != before[t] {
                self.changed_since_clear.push(t);
            }
        }

        let mut block_of_class: BTreeMap<u32, usize> = BTreeMap::new();
        let mut class_of_block: BTreeMap<usize, u32> = BTreeMap::new();
        for t in 0..self.terms {
            let block = self.model.block_of(t);
            prop_assert_eq!(*block_of_class.entry(after[t]).or_insert(block), block);
            prop_assert_eq!(*class_of_block.entry(block).or_insert(after[t]), after[t]);
        }
        let (classes, live) = self.subject.stats();
        prop_assert_eq!(classes, self.model.num_blocks());
        prop_assert_eq!(live, self.model.num_enodes());

        for (&class, &block) in &block_of_class {
            let nodes = self.subject.class(class);
            prop_assert!(nodes.is_some(), "class {} is not canonical", class);
            let nodes = nodes.unwrap();
            prop_assert_eq!(nodes.len(), self.model.enodes_in_block(block));
            for (symbol, disambiguator, children) in nodes {
                let mut child_blocks = Vec::new();
                for child in children {
                    let child_block = block_of_class.get(&child);
                    prop_assert!(child_block.is_some(), "child e-class holds no term");
                    child_blocks.push(*child_block.unwrap());
                }
                prop_assert_eq!(self.model.find_enode(symbol, disambiguator, &child_blocks), Some(block));
            }
        }

        let touched = self.subject.touched();
        for &t in &self.changed_since_clear {
            prop_assert!(touched.binary_search(&after[t]).is_ok(), "touched set misses term {}", t);
        }
        for class in &touched {
            prop_assert!(block_of_class.contains_key(class));
        }
        Ok(())
    }
}
