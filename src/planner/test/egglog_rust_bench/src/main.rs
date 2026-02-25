use egglog::ast::{Command, Expr, Schema};
use egglog::prelude::*;
use egglog::ArcSort;
use std::time::Instant;

const QUERY_NAMES: [&str; 5] = [
    "Neg(?x)",
    "Add(?x, ?x)",
    "F(Add(?x, ?y), Neg(?z))",
    "F2(F(Mul(?x, ?y), ?z))",
    "Test(?x) [expected no matches]",
];

fn call(sym: &str, args: Vec<Expr>) -> Expr {
    exprs::call(sym, args)
}

fn insert_expr(egraph: &mut EGraph, expr: Expr) -> Result<Expr, String> {
    egraph
        .eval_expr(&expr)
        .map_err(|e| format!("eval_expr failed: {e}"))?;
    Ok(expr)
}

fn union_exprs(egraph: &mut EGraph, a: Expr, b: Expr) -> Result<(), String> {
    egraph
        .run_program(vec![Command::Action(Action::Union(span!(), a, b))])
        .map_err(|e| format!("union action failed: {e}"))?;
    Ok(())
}

fn build_egraph() -> Result<(EGraph, ArcSort), String> {
    let mut egraph = EGraph::default();
    let declare_res: Result<(), egglog::Error> = (|| {
        datatype!(
            &mut egraph,
            (datatype Expr
                (Const i64)
                (Var i64)
                (Neg Expr)
                (Add Expr Expr)
                (Mul Expr Expr)
                (F Expr Expr)
                (F2 Expr)
                (F3 Expr Expr Expr)
                (Test Expr))
        );
        Ok(())
    })();
    declare_res.map_err(|e| format!("datatype declaration failed: {e}"))?;

    const NUM_CONSTS: usize = 180;
    const NUM_VARS: usize = 60;
    const NUM_LAYERS: usize = 320;

    let mut consts = Vec::with_capacity(NUM_CONSTS);
    let mut vars = Vec::with_capacity(NUM_VARS);

    for i in 0..NUM_CONSTS {
        consts.push(insert_expr(
            &mut egraph,
            call("Const", vec![exprs::int(i as i64)]),
        )?);
    }
    for i in 0..NUM_VARS {
        vars.push(insert_expr(
            &mut egraph,
            call("Var", vec![exprs::int(i as i64)]),
        )?);
    }

    for i in 0..NUM_LAYERS {
        let c1 = consts[i % consts.len()].clone();
        let c2 = consts[(i * 7 + 11) % consts.len()].clone();
        let v1 = vars[(i * 13 + 3) % vars.len()].clone();

        let add = insert_expr(&mut egraph, call("Add", vec![c1.clone(), c2.clone()]))?;
        let neg = insert_expr(&mut egraph, call("Neg", vec![c2.clone()]))?;
        let mul = insert_expr(&mut egraph, call("Mul", vec![add.clone(), neg.clone()]))?;
        let f = insert_expr(&mut egraph, call("F", vec![mul, v1]))?;
        let _f2 = insert_expr(&mut egraph, call("F2", vec![f]))?;

        if i % 5 == 0 {
            let add_swapped = insert_expr(&mut egraph, call("Add", vec![c2.clone(), c1.clone()]))?;
            union_exprs(&mut egraph, add.clone(), add_swapped)?;
        }

        if i % 9 == 0 {
            let neg2 = insert_expr(&mut egraph, call("Neg", vec![c2]))?;
            union_exprs(&mut egraph, neg.clone(), neg2)?;
        }
    }

    let expr_sort = egraph
        .get_sort_by_name("Expr")
        .cloned()
        .ok_or_else(|| "Sort Expr not found".to_string())?;
    Ok((egraph, expr_sort))
}

struct PreparedQuery {
    name: &'static str,
    ruleset: &'static str,
    relation: &'static str,
}

fn install_prepared_queries(egraph: &mut EGraph) -> Result<Vec<PreparedQuery>, String> {
    add_relation(egraph, "bench_match_0", vec!["Expr".to_owned()])
        .map_err(|e| format!("add_relation bench_match_0 failed: {e}"))?;
    add_relation(egraph, "bench_match_1", vec!["Expr".to_owned()])
        .map_err(|e| format!("add_relation bench_match_1 failed: {e}"))?;
    add_relation(
        egraph,
        "bench_match_2",
        vec!["Expr".to_owned(), "Expr".to_owned(), "Expr".to_owned()],
    )
    .map_err(|e| format!("add_relation bench_match_2 failed: {e}"))?;
    add_relation(
        egraph,
        "bench_match_3",
        vec!["Expr".to_owned(), "Expr".to_owned(), "Expr".to_owned()],
    )
    .map_err(|e| format!("add_relation bench_match_3 failed: {e}"))?;
    add_relation(egraph, "bench_match_4", vec!["Expr".to_owned()])
        .map_err(|e| format!("add_relation bench_match_4 failed: {e}"))?;

    add_ruleset(egraph, "bench_q0").map_err(|e| format!("add_ruleset bench_q0 failed: {e}"))?;
    add_ruleset(egraph, "bench_q1").map_err(|e| format!("add_ruleset bench_q1 failed: {e}"))?;
    add_ruleset(egraph, "bench_q2").map_err(|e| format!("add_ruleset bench_q2 failed: {e}"))?;
    add_ruleset(egraph, "bench_q3").map_err(|e| format!("add_ruleset bench_q3 failed: {e}"))?;
    add_ruleset(egraph, "bench_q4").map_err(|e| format!("add_ruleset bench_q4 failed: {e}"))?;

    rule(egraph, "bench_q0", facts![(Neg x)], actions![(bench_match_0 x)])
        .map_err(|e| format!("rule bench_q0 failed: {e}"))?;
    rule(egraph, "bench_q1", facts![(Add x x)], actions![(bench_match_1 x)])
        .map_err(|e| format!("rule bench_q1 failed: {e}"))?;
    rule(
        egraph,
        "bench_q2",
        facts![(F (Add x y) (Neg z))],
        actions![(bench_match_2 x y z)],
    )
    .map_err(|e| format!("rule bench_q2 failed: {e}"))?;
    rule(
        egraph,
        "bench_q3",
        facts![(F2 (F (Mul x y) z))],
        actions![(bench_match_3 x y z)],
    )
    .map_err(|e| format!("rule bench_q3 failed: {e}"))?;
    rule(egraph, "bench_q4", facts![(Test x)], actions![(bench_match_4 x)])
        .map_err(|e| format!("rule bench_q4 failed: {e}"))?;

    Ok(vec![
        PreparedQuery {
            name: QUERY_NAMES[0],
            ruleset: "bench_q0",
            relation: "bench_match_0",
        },
        PreparedQuery {
            name: QUERY_NAMES[1],
            ruleset: "bench_q1",
            relation: "bench_match_1",
        },
        PreparedQuery {
            name: QUERY_NAMES[2],
            ruleset: "bench_q2",
            relation: "bench_match_2",
        },
        PreparedQuery {
            name: QUERY_NAMES[3],
            ruleset: "bench_q3",
            relation: "bench_match_3",
        },
        PreparedQuery {
            name: QUERY_NAMES[4],
            ruleset: "bench_q4",
            relation: "bench_match_4",
        },
    ])
}

fn run_queries(egraph: &mut EGraph, prepared: &[PreparedQuery]) -> Result<Vec<usize>, String> {
    egraph.push();
    let mut counts = Vec::with_capacity(prepared.len());
    for q in prepared.iter() {
        run_ruleset(egraph, q.ruleset).map_err(|e| format!("run_ruleset {} failed: {e}", q.name))?;
        let n = egraph.get_size(q.relation);
        counts.push(n);
    }
    egraph.pop().map_err(|e| format!("egraph.pop failed: {e}"))?;
    Ok(counts)
}

fn run_baseline() -> Result<f64, String> {
    let start = Instant::now();
    let _ = build_egraph()?;
    Ok(start.elapsed().as_secs_f64() * 1000.0)
}

fn run_full(with_counts: bool) -> Result<(f64, Vec<usize>), String> {
    let start = Instant::now();
    let (mut egraph, _expr_sort) = build_egraph()?;
    let prepared = install_prepared_queries(&mut egraph)?;
    let counts = if with_counts {
        run_queries(&mut egraph, &prepared)?
    } else {
        let _ = run_queries(&mut egraph, &prepared)?;
        Vec::new()
    };
    Ok((start.elapsed().as_secs_f64() * 1000.0, counts))
}

fn parse_arg<T: std::str::FromStr>(args: &[String], name: &str, default: T) -> T {
    if let Some(pos) = args.iter().position(|a| a == name) {
        if let Some(v) = args.get(pos + 1) {
            if let Ok(parsed) = v.parse::<T>() {
                return parsed;
            }
        }
    }
    default
}

fn has_flag(args: &[String], name: &str) -> bool {
    args.iter().any(|a| a == name)
}

fn main() -> Result<(), String> {
    let args = std::env::args().collect::<Vec<_>>();
    let iterations = parse_arg(&args, "--iterations", 5usize);
    let compare_results = has_flag(&args, "--compare-results");

    // Query-only mode: build once, then run queries repeatedly on the same graph.
    let (mut prebuilt_egraph, _expr_sort) = build_egraph()?;
    let prepared = install_prepared_queries(&mut prebuilt_egraph)?;
    let _ = run_queries(&mut prebuilt_egraph, &prepared)?;
    let query_start = Instant::now();
    let mut counts = Vec::new();
    for _ in 0..iterations {
        counts = run_queries(&mut prebuilt_egraph, &prepared)?;
    }
    let query_only_ms = query_start.elapsed().as_secs_f64() * 1000.0;

    // End-to-end mode: build + query path (with baseline subtraction available for inspection).
    let mut total_baseline_ms = 0.0;
    let mut total_full_ms = 0.0;
    for _ in 0..iterations {
        let (full_ms, _) = run_full(false)?;
        let baseline_ms = run_baseline()?;
        total_full_ms += full_ms;
        total_baseline_ms += baseline_ms;
    }

    println!("RUST_EGGLOG_QUERY_ONLY_MS={query_only_ms:.6}");
    println!("RUST_EGGLOG_TOTAL_FULL_MS={total_full_ms:.6}");
    println!("RUST_EGGLOG_TOTAL_BASELINE_MS={total_baseline_ms:.6}");
    println!(
        "RUST_EGGLOG_MATCH_COUNTS={}",
        counts
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    println!(
        "RUST_EGGLOG_QUERIES={}",
        QUERY_NAMES.join("|")
    );

    if compare_results {
        // Exit code and printed counts are enough for parent benchmark to validate.
    }

    Ok(())
}
