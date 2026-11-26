//! The Rust port holds the congruence closure the reference model computes,
//! and its representation invariant, after every operation; see `common`.

mod common;

use common::{Port, Subject, ops, run_case};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    #[test]
    fn holds_the_congruence_closure_after_every_rebuild(ops in ops(100)) {
        let mut port = Port::default();
        port.reset();
        run_case(&mut port, &ops)?;
    }
}
