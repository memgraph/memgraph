// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

namespace memgraph::query::plan::v2 {

/// Empty analysis marker passed to core::EGraph<symbol, analysis> through
/// TypedEGraph. plan_v2 currently runs no e-class analysis; the type only
/// exists to satisfy the EGraph's Analysis template parameter. Kept in
/// plan_v2's namespace (rather than reusing core::NoAnalysis) so the
/// inheritance line `TypedEGraph<symbol, analysis, ...>` reads in one style.
struct analysis {};

}  // namespace memgraph::query::plan::v2
