//! The C++ e-graph holds the congruence closure the reference model computes;
//! see `common`. It runs through the replay driver, built from
//! tests/oracle/egraph/replay.cpp, so changing the model, the generators or
//! the checks needs no C++ rebuild.
//!
//! The driver is found through `MG_EGRAPH_REPLAY` or in the repository's
//! default build tree. Without it the test is skipped, unless
//! `MG_REQUIRE_CPP=1` asks for it to fail instead.

mod common;

use std::cell::RefCell;

use common::{Cpp, Subject, ops, run_case};
use proptest::test_runner::{Config, TestRunner};

#[test]
fn cpp_holds_the_congruence_closure_after_every_rebuild() {
    let Some(binary) = Cpp::binary() else {
        assert!(std::env::var_os("MG_REQUIRE_CPP").is_none(), "MG_REQUIRE_CPP is set but the replay driver is missing");
        eprintln!("SKIPPED: no egraph_replay driver; build memgraph__oracle__egraph_replay or set MG_EGRAPH_REPLAY");
        return;
    };
    let cases = std::env::var("MG_CPP_CASES").ok().and_then(|n| n.parse().ok()).unwrap_or(2000);
    let cpp = RefCell::new(Cpp::spawn(&binary));
    let mut runner = TestRunner::new(Config::with_cases(cases));
    let result = runner.run(&ops(100), |ops| {
        let mut cpp = cpp.borrow_mut();
        cpp.reset();
        run_case(&mut *cpp, &ops)
    });
    if let Err(failure) = result {
        panic!("{failure}");
    }
}
