//! The C++ planner's e-graph rebuild workload, built the same way, so the two
//! implementations can be compared on one machine.

use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use mg_planner_egraph::EGraph;

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
enum Op {
    Const,
    F,
}

/// Five chains `F(F(...F(Const_i)))` of the given length over distinct
/// constants, with the chain heads merged pairwise. Rebuilding collapses the
/// five chains into one, level by level.
fn congruence_chain(length: usize) -> EGraph<Op> {
    let mut graph = EGraph::new();
    let mut heads = Vec::new();
    for chain in 0..5u64 {
        let mut previous = graph.emplace(Op::Const, &[], chain).eclass;
        heads.push(previous);
        for _ in 0..length {
            previous = graph.emplace(Op::F, &[previous], 0).eclass;
        }
    }
    for pair in heads.windows(2) {
        graph.merge(pair[1], pair[0]);
    }
    graph
}

fn rebuild_congruence_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("EGraph/Rebuild/CongruenceChain");
    for length in [100usize, 512, 4096, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(length), &length, |b, &length| {
            b.iter_batched(
                || congruence_chain(length),
                |mut graph| {
                    graph.rebuild();
                    black_box(graph.num_classes());
                    graph
                },
                BatchSize::LargeInput,
            )
        });
    }
    group.finish();
}

fn build_congruence_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("EGraph/Build/CongruenceChain");
    for length in [100usize, 512, 4096, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(length), &length, |b, &length| {
            b.iter(|| black_box(congruence_chain(length)))
        });
    }
    group.finish();
}

fn check_shape() {
    let mut graph = congruence_chain(100);
    graph.rebuild();
    assert_eq!(graph.num_classes(), 101, "five chains of 100 collapse to one");
}

fn benches(c: &mut Criterion) {
    check_shape();
    rebuild_congruence_chain(c);
    build_congruence_chain(c);
}

criterion_group!(group, benches);
criterion_main!(group);
