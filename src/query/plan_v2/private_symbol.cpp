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

// This translation unit is the canonical home for compile-time checks over the
// `symbol` enum that would otherwise be paid by every TU including
// private_symbol.hpp.  Adding a new symbol but forgetting to specialise
// `symbol_descriptor` produces the failure here, exactly once per build.

#include "query/plan_v2/private_symbol.hpp"

namespace memgraph::query::plan::v2 {

static_assert(detail::AllHaveDescriptorsImpl(AllSymbolsSeq{}),
              "every `symbol` enum value must have a `symbol_descriptor` specialisation; "
              "see private_symbol.hpp");

}  // namespace memgraph::query::plan::v2
