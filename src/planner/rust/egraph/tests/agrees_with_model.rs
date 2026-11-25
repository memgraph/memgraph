//! The e-graph holds the congruence closure the reference model computes
//! whenever it has just been rebuilt. The C++ e-graph is held to the same
//! observations:
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
//! did_insert exactly when no term has that canonical form.

use std::collections::BTreeMap;

use egraph_oracle::Model;
use mg_planner_egraph::{EGraph, Id};
use proptest::prelude::*;

#[derive(Debug, Clone)]
enum Op {
    Add { symbol: u8, disambiguator: u64, children: Vec<u32> },
    Merge(u32, u32),
    Rebuild,
    ClearTouched,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (0u8..4, 0u64..3, prop::collection::vec(any::<u32>(), 0..4))
            .prop_map(|(symbol, disambiguator, children)| Op::Add { symbol, disambiguator, children }),
        3 => (any::<u32>(), any::<u32>()).prop_map(|(a, b)| Op::Merge(a, b)),
        1 => Just(Op::Rebuild),
        1 => Just(Op::ClearTouched),
    ]
}

#[derive(Default)]
struct Harness {
    graph: EGraph<u8>,
    model: Model,
    ids: Vec<Id>,
    changed_since_clear: Vec<usize>,
}

impl Harness {
    fn term(&self, raw: u32) -> usize {
        raw as usize % self.ids.len()
    }

    fn find(&self, term: usize) -> Id {
        self.graph.find(self.ids[term])
    }

    fn apply(&mut self, op: &Op) -> Result<(), TestCaseError> {
        match op {
            Op::Add { symbol, disambiguator, children } => self.add(*symbol, *disambiguator, children),
            Op::Merge(a, b) => self.merge(*a, *b),
            Op::Rebuild => self.rebuild(),
            Op::ClearTouched => {
                self.graph.clear_touched();
                self.changed_since_clear.clear();
                Ok(())
            }
        }
    }

    fn add(&mut self, symbol: u8, disambiguator: u64, raw_children: &[u32]) -> Result<(), TestCaseError> {
        if self.ids.is_empty() && !raw_children.is_empty() {
            return Ok(());
        }
        let children: Vec<usize> = raw_children.iter().map(|&c| self.term(c)).collect();
        let graph_children: Vec<Id> = children.iter().map(|&c| self.find(c)).collect();

        let clean = !self.graph.needs_rebuild();
        let existing = if clean {
            self.model.close();
            let blocks: Vec<usize> = children.iter().map(|&c| self.model.block_of(c)).collect();
            self.model.find_enode(symbol, disambiguator, &blocks)
        } else {
            None
        };

        let result = self.graph.emplace(symbol, &graph_children, disambiguator);
        if clean {
            prop_assert_eq!(result.did_insert, existing.is_none());
        }
        self.ids.push(result.eclass);
        let term = self.model.add(symbol, disambiguator, children);
        if result.did_insert {
            self.changed_since_clear.push(term);
        }
        Ok(())
    }

    fn merge(&mut self, raw_a: u32, raw_b: u32) -> Result<(), TestCaseError> {
        if self.ids.is_empty() {
            return Ok(());
        }
        let (a, b) = (self.term(raw_a), self.term(raw_b));
        let clean = !self.graph.needs_rebuild();
        self.model.close();
        let apart = !self.model.equivalent(a, b);

        let result = self.graph.merge(self.ids[a], self.ids[b]);
        if apart {
            prop_assert!(result.did_merge);
        }
        if clean {
            prop_assert_eq!(result.did_merge, apart);
        }
        if result.did_merge {
            prop_assert!(self.graph.needs_rebuild());
            self.changed_since_clear.push(a);
        }
        self.model.merge(a, b);
        Ok(())
    }

    fn rebuild(&mut self) -> Result<(), TestCaseError> {
        let before: Vec<Id> = (0..self.ids.len()).map(|t| self.find(t)).collect();
        self.graph.rebuild();
        self.model.close();
        prop_assert!(!self.graph.needs_rebuild());

        let mut first_before: BTreeMap<Id, Id> = BTreeMap::new();
        for t in 0..self.ids.len() {
            let prior = *first_before.entry(self.find(t)).or_insert(before[t]);
            if prior != before[t] {
                self.changed_since_clear.push(t);
            }
        }

        let mut block_of_class: BTreeMap<Id, usize> = BTreeMap::new();
        let mut class_of_block: BTreeMap<usize, Id> = BTreeMap::new();
        for t in 0..self.ids.len() {
            let class = self.find(t);
            let block = self.model.block_of(t);
            prop_assert!(self.graph.has_class(class));
            prop_assert_eq!(*block_of_class.entry(class).or_insert(block), block);
            prop_assert_eq!(*class_of_block.entry(block).or_insert(class), class);
        }
        prop_assert_eq!(self.graph.num_classes(), self.model.num_blocks());
        prop_assert_eq!(self.graph.num_live_nodes(), self.model.num_enodes());

        for (&class, &block) in &block_of_class {
            let eclass = self.graph.eclass(class);
            prop_assert_eq!(eclass.len(), self.model.enodes_in_block(block));
            for &enode_id in eclass.nodes() {
                let enode = self.graph.get_enode(enode_id);
                let mut child_blocks = Vec::new();
                for &child in enode.children() {
                    let block = block_of_class.get(&self.graph.find(child));
                    prop_assert!(block.is_some(), "child e-class holds no term");
                    child_blocks.push(*block.unwrap());
                }
                prop_assert_eq!(
                    self.model.find_enode(*enode.symbol(), enode.disambiguator(), &child_blocks),
                    Some(block)
                );
            }
        }

        let mut touched = Vec::new();
        self.graph.touched_into(&mut touched);
        for &t in &self.changed_since_clear {
            prop_assert!(touched.binary_search(&self.find(t)).is_ok(), "touched set misses term {}", t);
        }
        for class in &touched {
            prop_assert!(block_of_class.contains_key(class));
        }
        Ok(())
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    #[test]
    fn holds_the_congruence_closure_after_every_rebuild(ops in prop::collection::vec(op(), 0..100)) {
        let mut harness = Harness::default();
        for op in &ops {
            harness.apply(op)?;
        }
        harness.rebuild()?;
    }
}
