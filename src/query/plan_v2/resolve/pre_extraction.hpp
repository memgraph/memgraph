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
  // DISTINCT / ORDER BY carry their dedup/remember columns as bare Symbol
  // children - a demand like an Identifier reference, but not Identifiers - so
  // collect them here and fold into referenced_syms after the walk. ORDER BY's
  // sort-key expression children are swept in too but discarded below by
  // `contains` (only Symbol e-classes carry a bit).
  boost::container::small_vector<planner::core::EClassId, 32> column_referenced;
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
      } else if (sym == Distinct) {
        for (auto child : enode.children().subspan(child::distinct::first_value)) column_referenced.push_back(child);
      } else if (sym == OrderBy) {
        for (auto child : enode.children().subspan(child::order_by::first_expr)) column_referenced.push_back(child);
      }
    }
  }
  out.referenced_syms = out.variable_index.to_variable_set(identifier_referenced);
  // `contains` skips sort-key children, keeping bit_of's registered-only contract.
  for (auto eclass_id : column_referenced) {
    if (out.variable_index.contains(eclass_id)) out.referenced_syms.set(out.variable_index.bit_of(eclass_id));
  }
  return out;
}

}  // namespace memgraph::query::plan::v2
