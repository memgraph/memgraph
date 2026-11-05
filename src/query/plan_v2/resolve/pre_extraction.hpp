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

#include "query/plan_v2/cost/extraction_env.hpp"
#include "query/plan_v2/egraph/child_layout.hpp"
#include "query/plan_v2/egraph/symbol_dispatch.hpp"
#include "query/plan_v2/scope/variable_index.hpp"
#include "query/plan_v2/scope/variable_set.hpp"

namespace memgraph::query::plan::v2 {

/// The pre-extraction walk's product: VariableIndex bits for Symbol e-classes
/// and the egraph-wide set of symbols something reads. Both feed the
/// SymbolContext a cost/resolve pass reads.
struct PreExtractionData {
  VariableIndex variable_index;
  VariableSet referenced_syms;
};

/// One walk of canonical eclass ids: assign VariableIndex bits to Symbol
/// e-classes and collect the symbols read by any e-node that reads one, as
/// `reads_symbol_v` declares. O(num_enodes).
inline auto BuildPreExtractionData(EGraph const &core) -> PreExtractionData {
  using enum symbol;
  PreExtractionData out;
  boost::container::small_vector<planner::core::EClassId, 32> read_symbols;
  // DISTINCT / ORDER BY carry their dedup and remember columns as bare Symbol
  // children: a demand like an Identifier reference, but not Identifiers. ORDER
  // BY's sort-key expression children are swept in with them and discarded
  // below by `contains`, since only Symbol e-classes carry a bit. Folding these
  // in after the walk is also what makes every Symbol e-class assigned by then.
  boost::container::small_vector<planner::core::EClassId, 32> read_columns;
  for (auto eclass_id : core.canonical_eclass_ids()) {
    for (auto enode_id : core.eclass(eclass_id).nodes()) {
      auto const &enode = core.get_enode(enode_id);
      if (enode.symbol() == Symbol) {
        out.variable_index.assign(eclass_id);
        continue;
      }
      auto const reads = DispatchBySymbol(enode.symbol(), [&]<symbol S>() { return reads_symbol_v<S>; });
      if (!reads || enode.children().empty()) continue;
      // Children are canonical: EGraph::emplace canonicalizes them at insert
      // time and rebuild() re-canonicalizes via canonicalize_in_place(). Each
      // reader takes the children its own layout puts the bindings at.
      if (enode.symbol() == Distinct) {
        for (auto child : enode.children().subspan(child::distinct::first_value)) read_columns.push_back(child);
      } else if (enode.symbol() == OrderBy) {
        for (auto child : enode.children().subspan(child::order_by::first_expr)) read_columns.push_back(child);
      } else {
        read_symbols.push_back(enode.children()[child::identifier::sym]);
      }
    }
  }
  out.referenced_syms = out.variable_index.to_variable_set(read_symbols);
  // `contains` skips sort-key children, keeping bit_of's registered-only contract.
  for (auto eclass_id : read_columns) {
    if (out.variable_index.contains(eclass_id)) out.referenced_syms.set(out.variable_index.bit_of(eclass_id));
  }
  return out;
}

}  // namespace memgraph::query::plan::v2
