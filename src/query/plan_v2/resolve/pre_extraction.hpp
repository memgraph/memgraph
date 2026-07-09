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

#include <boost/container/small_vector.hpp>

#include "query/plan_v2/egraph/child_layout.hpp"
#include "query/plan_v2/resolve/extraction_env.hpp"
#include "query/plan_v2/resolve/variable_index.hpp"
#include "query/plan_v2/resolve/variable_set.hpp"

namespace memgraph::query::plan::v2 {

/// The pre-extraction walk's product: VariableIndex bits for Symbol e-classes
/// and the egraph-wide set of symbols some Identifier references. Both feed the
/// SymbolContext a cost/resolve pass reads.
struct PreExtractionData {
  VariableIndex variable_index;
  VariableSet referenced_syms;
};

/// One walk of canonical eclass ids: assign VariableIndex bits to Symbol
/// e-classes and collect Identifier-referenced symbols. O(num_enodes).
inline auto BuildPreExtractionData(EGraph const &core) -> PreExtractionData {
  using enum symbol;
  PreExtractionData out;
  boost::container::small_vector<planner::core::EClassId, 32> identifier_referenced;
  for (auto eclass_id : core.canonical_eclass_ids()) {
    for (auto enode_id : core.eclass(eclass_id).nodes()) {
      auto const &enode = core.get_enode(enode_id);
      auto const sym = enode.symbol();
      if (sym == Symbol) {
        out.variable_index.assign(eclass_id);
      } else if (sym == Identifier && !enode.children().empty()) {
        // The sym child is canonical: EGraph::emplace canonicalizes children
        // at insert time and rebuild() re-canonicalizes via canonicalize_in_place().
        identifier_referenced.push_back(enode.children()[child::identifier::sym]);
      }
    }
  }
  out.referenced_syms = out.variable_index.to_variable_set(identifier_referenced);
  return out;
}

}  // namespace memgraph::query::plan::v2
