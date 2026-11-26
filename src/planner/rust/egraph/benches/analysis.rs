//! The C++ planner's analysis-refresh workloads (bench_analysis.cpp), built
//! the same way, so the two implementations can be compared on one machine.
//!
//! The analysis is a 64-bit set joined by union. A leaf is seeded with its
//! disambiguator's bits and `Add` derives the union of its operands' facts;
//! nothing else recomputes. Each graph is built untimed, and only the refresh
//! of what changed is timed.

use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use mg_planner_egraph::{Analysis, EGraph, Id, Scope};

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
enum Op {
    Var,
    Const,
    Add,
}

#[derive(Default, Clone, Copy, PartialEq, Eq, Debug)]
struct Mask(u64);

impl Analysis for Mask {
    fn merge(&mut self, other: &Self) {
        self.0 |= other.0;
    }
}

type Graph = EGraph<Op, Mask>;

fn leaf(graph: &mut Graph, op: Op, bits: u64) -> Id {
    graph.emplace_with(op, &[], bits, Mask(bits)).eclass
}

fn add(graph: &mut Graph, lhs: Id, rhs: Id) -> Id {
    graph.emplace_with(Op::Add, &[lhs, rhs], 0, Mask(0)).eclass
}

fn refresh(graph: &mut Graph) -> usize {
    graph.refresh_analysis(Scope::Changed, |node, facts_of| {
        (*node.symbol() == Op::Add).then(|| Mask(node.children().iter().fold(0, |acc, &c| acc | facts_of(c).0)))
    })
}

/// `width` independent Adds over their own leaves, plus one Add whose right
/// operand is unseeded; that operand is then merged with a new leaf.
fn one_change(width: u64) -> Graph {
    let mut graph = Graph::new();
    for i in 0..width {
        let a = leaf(&mut graph, Op::Var, i + 1);
        let b = leaf(&mut graph, Op::Const, i + 16);
        add(&mut graph, a, b);
    }
    let opaque = leaf(&mut graph, Op::Var, 0);
    let seeded = leaf(&mut graph, Op::Const, 4);
    add(&mut graph, seeded, opaque);
    refresh(&mut graph);
    graph.clear_touched();
    let widen = leaf(&mut graph, Op::Const, 8);
    graph.merge(opaque, widen);
    graph
}

/// `fanout` Adds that all read one leaf, which is then merged with a new leaf.
fn fan_out(fanout: u64) -> Graph {
    let mut graph = Graph::new();
    let shared = leaf(&mut graph, Op::Var, 0);
    for i in 0..fanout {
        let other = leaf(&mut graph, Op::Const, i);
        add(&mut graph, shared, other);
    }
    refresh(&mut graph);
    let widen = leaf(&mut graph, Op::Const, 1 << 20);
    graph.merge(shared, widen);
    graph
}

/// Five towers of `depth` Adds over a shared leaf, their bases merged
/// pairwise and collapsed by one rebuild.
fn cascade(depth: u64) -> Graph {
    let mut graph = Graph::new();
    let shared = leaf(&mut graph, Op::Const, 1);
    let mut bases = Vec::new();
    for c in 0..5u64 {
        let mut previous = leaf(&mut graph, Op::Var, 1 << (c + 1));
        bases.push(previous);
        for _ in 0..depth {
            previous = add(&mut graph, previous, shared);
        }
    }
    for pair in bases.windows(2) {
        graph.merge(pair[1], pair[0]);
    }
    graph.rebuild();
    graph
}

fn bench_refresh(c: &mut Criterion, name: &str, sizes: &[u64], build: fn(u64) -> Graph) {
    let mut group = c.benchmark_group(name);
    for &size in sizes {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || build(size),
                |mut graph| {
                    black_box(refresh(&mut graph));
                    graph
                },
                BatchSize::LargeInput,
            )
        });
    }
    group.finish();
}

fn refresh_benches(c: &mut Criterion) {
    bench_refresh(c, "BM_RefreshAfterOneChange", &[0, 256, 4096, 16384], one_change);
    bench_refresh(c, "BM_RefreshFanOut", &[1, 64, 1024, 8192], fan_out);
    bench_refresh(c, "BM_RefreshAfterCascade", &[16, 256, 2048], cascade);
}

/// Building the graph with the analysis tracked, as BM_BuildWithUpdatingAnalysis.
fn build_benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("BM_BuildWithUpdatingAnalysis");
    for width in [256u64, 4096] {
        group.bench_with_input(BenchmarkId::from_parameter(width), &width, |b, &width| {
            b.iter(|| {
                let mut graph = Graph::new();
                for i in 0..width {
                    let a = leaf(&mut graph, Op::Var, i + 1);
                    let b = leaf(&mut graph, Op::Const, i + 16);
                    add(&mut graph, a, b);
                }
                black_box(graph.num_classes())
            })
        });
    }
    group.finish();
}

criterion_group!(benches, refresh_benches, build_benches);
criterion_main!(benches);
