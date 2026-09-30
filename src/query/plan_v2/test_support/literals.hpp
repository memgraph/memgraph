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

#include <cstdint>

#include "query/plan_v2/egraph/egraph.hpp"
#include "storage/v2/property_value.hpp"

namespace memgraph::query::plan::v2 {

/// Intern an integer-literal e-class. Shared by plan_v2 unit tests that build
/// egraphs directly and need a terse literal constructor.
inline auto IntLit(egraph &eg, int64_t v) -> eclass { return eg.MakeLiteral(storage::ExternalPropertyValue{v}); }

}  // namespace memgraph::query::plan::v2
