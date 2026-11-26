//! The C++ planner's matching workloads (bench_match.cpp and the builders in
//! test_support), built the same way, so the two implementations can be
//! compared on one machine. Benchmark names and arguments follow the C++ ones.
//!
//! Each graph and its symbol index are built once; only the match is timed.
//! `ctx:fresh` allocates a new match buffer per run and `ctx:reused` clears
//! one, as the C++ does with its match context.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use mg_planner_egraph::matching::{Matcher, Matches, Pattern, PatternBuilder, SymbolIndex};
use mg_planner_egraph::{EGraph, Id};

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
enum Op {
    Var,
    Const,
    Add,
    Mul,
    Neg,
    F,
}

type Graph = EGraph<Op>;

const X: u8 = 0;
const Y: u8 = 1;
const R: u8 = 2;

fn leaf(g: &mut Graph, op: Op, dis: u64) -> Id {
    g.emplace(op, &[], dis).eclass
}

fn node(g: &mut Graph, op: Op, children: &[Id]) -> Id {
    g.emplace(op, children, 0).eclass
}

// ---- patterns (test_support/patterns.hpp) ----

fn pattern_binary(op: Op, a: u8, b: u8) -> Pattern<Op> {
    let mut p = PatternBuilder::new();
    let (x, y) = (p.var(a), p.var(b));
    p.sym(op, &[x, y], None);
    p.build()
}

fn pattern_unary(op: Op) -> Pattern<Op> {
    let mut p = PatternBuilder::new();
    let x = p.var(X);
    p.sym(op, &[x], None);
    p.build()
}

fn pattern_nested(op: Op, depth: usize) -> Pattern<Op> {
    let mut p = PatternBuilder::new();
    let mut cur = p.var(X);
    for _ in 0..depth {
        cur = p.sym(op, &[cur], None);
    }
    p.build()
}

fn pattern_selective() -> Pattern<Op> {
    let mut p = PatternBuilder::new();
    let x = p.var(X);
    let neg = p.sym(Op::Neg, &[x], None);
    let y = p.var(Y);
    p.sym(Op::Add, &[neg, y], None);
    p.build()
}

fn pattern_hoist_anchor() -> Pattern<Op> {
    let mut p = PatternBuilder::new();
    let x = p.var(X);
    p.sym(Op::F, &[x], Some(R));
    p.build()
}

// ---- graphs (test_support/builders.hpp) ----

fn independent_adds(n: u64) -> Graph {
    let mut g = Graph::new();
    for i in 0..n {
        let x = leaf(&mut g, Op::Var, i * 2);
        let y = leaf(&mut g, Op::Var, i * 2 + 1);
        node(&mut g, Op::Add, &[x, y]);
    }
    g
}

fn neg_chain(depth: u64) -> Graph {
    let mut g = Graph::new();
    let mut cur = leaf(&mut g, Op::Var, 0);
    for _ in 0..depth {
        cur = node(&mut g, Op::Neg, &[cur]);
    }
    g
}

fn mixed_adds(n: u64) -> Graph {
    let mut g = Graph::new();
    let vars: Vec<Id> = (0..n).map(|i| leaf(&mut g, Op::Var, i)).collect();
    let half = (n / 2) as usize;
    for i in 0..half {
        node(&mut g, Op::Add, &[vars[i], vars[i]]);
    }
    for i in 0..half {
        let j = (i + 1) % half;
        if i != j {
            node(&mut g, Op::Add, &[vars[i], vars[j]]);
        }
    }
    g
}

fn merged_add_mul(n: u64) -> Graph {
    let mut g = Graph::new();
    let vars: Vec<Id> = (0..n).map(|i| leaf(&mut g, Op::Var, i)).collect();
    let mut pairs = Vec::new();
    for i in 0..vars.len().saturating_sub(1) {
        let add = node(&mut g, Op::Add, &[vars[i], vars[i + 1]]);
        let mul = node(&mut g, Op::Mul, &[vars[i], vars[i + 1]]);
        pairs.push((add, mul));
    }
    for (add, mul) in pairs {
        g.merge(add, mul);
    }
    g.rebuild();
    g
}

fn adds_with_one_neg(n: u64) -> Graph {
    let mut g = Graph::new();
    let vars: Vec<Id> = (0..n).map(|i| leaf(&mut g, Op::Var, i)).collect();
    for i in 0..vars.len() - 1 {
        node(&mut g, Op::Add, &[vars[i], vars[i + 1]]);
    }
    let neg = node(&mut g, Op::Neg, &[vars[0]]);
    node(&mut g, Op::Add, &[neg, vars[1]]);
    g
}

fn high_parent_hub(parents_f: u64, parents_neg: u64) -> Graph {
    let mut g = Graph::new();
    let hub = leaf(&mut g, Op::Const, 0);
    for i in 0..parents_f {
        let other = leaf(&mut g, Op::Const, i + 1);
        node(&mut g, Op::F, &[hub, other]);
    }
    for _ in 0..parents_neg {
        node(&mut g, Op::Neg, &[hub]);
    }
    g
}

fn self_referential() -> Graph {
    let mut g = Graph::new();
    let n0 = leaf(&mut g, Op::Const, 42);
    let n1 = node(&mut g, Op::F, &[n0]);
    let n2 = node(&mut g, Op::F, &[n1]);
    g.merge(n1, n2);
    g.rebuild();
    g
}

fn nested_join(leaves: u64) -> Graph {
    let mut g = Graph::new();
    for i in 0..leaves {
        let l = leaf(&mut g, Op::Const, i);
        node(&mut g, Op::F, &[l]);
        let f1 = node(&mut g, Op::F, &[l]);
        let f2 = node(&mut g, Op::F, &[f1]);
        let f3 = node(&mut g, Op::F, &[f2]);
        node(&mut g, Op::F, &[f3]);
    }
    g
}

fn parent_diversity(num_leaves: u64, parents_per_leaf: u64) -> Graph {
    let mut g = Graph::new();
    let leaves: Vec<Id> = (0..num_leaves).map(|i| leaf(&mut g, Op::Const, i)).collect();
    let symbols = [Op::Add, Op::Mul, Op::Neg, Op::F];
    let mut rng: u64 = 42;
    let mut next = || {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        rng
    };
    for &l in &leaves {
        for _ in 0..parents_per_leaf {
            let sym = symbols[(next() % 4) as usize];
            if sym == Op::Neg {
                node(&mut g, Op::Neg, &[l]);
            } else {
                let other = leaves[(next() % leaves.len() as u64) as usize];
                node(&mut g, sym, &[l, other]);
            }
        }
    }
    g
}

fn eclass_hoist(num_eclasses: u64, enodes_per_class: u64, parents: u64) -> Graph {
    let mut g = Graph::new();
    for ec in 0..num_eclasses {
        let leaf0 = leaf(&mut g, Op::Const, ec * 1000);
        let first_f = node(&mut g, Op::F, &[leaf0]);
        for e in 1..enodes_per_class {
            let l = leaf(&mut g, Op::Const, ec * 1000 + e);
            let f = node(&mut g, Op::F, &[l]);
            g.merge(first_f, f);
        }
        for p in 0..parents {
            let unique = leaf(&mut g, Op::Const, 100_000 + ec * 1000 + p);
            node(&mut g, Op::Mul, &[first_f, unique]);
        }
    }
    g.rebuild();
    g
}

// ---- harness ----

/// Times matching `patterns` against `graph`, with a fresh buffer each run
/// and, if `reused` is given, with one buffer cleared each run.
fn bench_match(c: &mut Criterion, name: &str, param: &str, graph: &Graph, patterns: &[Pattern<Op>], reused: Option<&str>) {
    let matcher = Matcher::new(patterns).expect("benchmark rules are valid");
    let mut index = SymbolIndex::default();
    index.rebuild(graph);
    c.bench_function(&format!("{name}/{param}"), |b| {
        b.iter(|| {
            let mut out = Matches::default();
            matcher.run(graph, &index, &mut out);
            black_box(out.len())
        })
    });
    if let Some(name) = reused {
        let mut out = Matches::default();
        c.bench_function(&format!("{name}/{param}"), |b| {
            b.iter(|| {
                matcher.run(graph, &index, &mut out);
                black_box(out.len())
            })
        });
    }
}

fn match_benches(c: &mut Criterion) {
    for n in [10u64, 100, 1000, 50000] {
        let g = independent_adds(n);
        let p = [pattern_binary(Op::Add, X, Y)];
        bench_match(c, "Match/Simple/ctx:fresh", &format!("size:{n}"), &g, &p, Some("Match/Simple/ctx:reused"));
    }
    for depth in [1u64, 5, 10, 50, 100] {
        let g = neg_chain(depth);
        let p = [pattern_nested(Op::Neg, depth as usize)];
        bench_match(c, "Match/Deep/ctx:fresh", &format!("depth:{depth}"), &g, &p, Some("Match/Deep/ctx:reused"));
    }
    for n in [10u64, 100, 1000] {
        let p = [pattern_binary(Op::Add, X, X)];
        bench_match(c, "Match/SameVar/ctx:fresh", &format!("size:{n}"), &mixed_adds(n), &p, Some("Match/SameVar/ctx:reused"));
        let p = [pattern_binary(Op::Add, X, Y)];
        bench_match(c, "Match/Merged/ctx:fresh", &format!("size:{n}"), &merged_add_mul(n), &p, Some("Match/Merged/ctx:reused"));
        let p = [pattern_selective()];
        let g = adds_with_one_neg(n);
        bench_match(c, "Match/Selective/ctx:fresh", &format!("size:{n}"), &g, &p, Some("Match/Selective/ctx:reused"));
    }
    for f in [1000u64, 5000, 10000] {
        let g = high_parent_hub(f, 100);
        bench_match(c, "Match/HighParent", &format!("F_parents:{f}/Neg_parents:100"), &g, &[pattern_unary(Op::Neg)], None);
    }
    {
        let g = self_referential();
        bench_match(c, "Match", "SelfRef", &g, &[pattern_nested(Op::F, 2)], None);
    }
    for (leaves, per) in [(100u64, 20u64), (500, 20), (100, 50)] {
        let g = parent_diversity(leaves, per);
        let param = format!("leaves:{leaves}/parents_per:{per}");
        bench_match(c, "Match/ParentDiversity", &param, &g, &[pattern_unary(Op::Neg)], None);
    }
    for leaves in [10u64, 50, 100, 500] {
        let g = nested_join(leaves);
        let p = [pattern_unary(Op::F), pattern_nested(Op::F, 4)];
        bench_match(c, "Match/NestedJoin", &format!("leaves:{leaves}"), &g, &p, None);
    }
    let hoist = [
        (100u64, 1u64, 1u64),
        (1000, 1, 1),
        (100, 10, 1),
        (1000, 10, 1),
        (100, 20, 1),
        (1000, 20, 1),
        (100, 10, 5),
        (100, 10, 20),
        (100, 10, 50),
        (100, 20, 20),
        (1000, 20, 20),
    ];
    for (ec, per, parents) in hoist {
        let g = eclass_hoist(ec, per, parents);
        let p = [pattern_hoist_anchor(), pattern_binary(Op::Mul, R, Y)];
        let param = format!("eclasses:{ec}/enodes_per:{per}/parents:{parents}");
        bench_match(c, "Match/EclassHoist", &param, &g, &p, None);
    }
}

criterion_group!(benches, match_benches);
criterion_main!(benches);
