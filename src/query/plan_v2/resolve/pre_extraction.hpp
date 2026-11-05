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
/// e-classes and collect the symbols read by any e-node that reads one.
/// O(num_enodes).
///
/// Which symbols read a binding is the `reads_symbol_v` partition, checked
/// exhaustive in symbol.hpp, so a symbol added without declaring itself cannot
/// be mistaken here for one that reads nothing.
inline auto BuildPreExtractionData(EGraph const &core) -> PreExtractionData {
  using enum symbol;
  PreExtractionData out;
  boost::container::small_vector<planner::core::EClassId, 32> read_symbols;
  for (auto eclass_id : core.canonical_eclass_ids()) {
    for (auto enode_id : core.eclass(eclass_id).nodes()) {
      auto const &enode = core.get_enode(enode_id);
      if (enode.symbol() == Symbol) {
        out.variable_index.assign(eclass_id);
        continue;
      }
      auto const reads = DispatchBySymbol(enode.symbol(), [&]<symbol S>() { return reads_symbol_v<S>; });
      if (!reads || enode.children().empty()) continue;
      // Every reader takes the binding at Identifier's sym index; a reader with
      // another layout would need that index from its own child_layout entry.
      // The child is canonical: EGraph::emplace canonicalizes children at insert
      // time and rebuild() re-canonicalizes via canonicalize_in_place().
      read_symbols.push_back(enode.children()[child::identifier::sym]);
    }
  }
  out.referenced_syms = out.variable_index.to_variable_set(read_symbols);
  return out;
}

}  // namespace memgraph::query::plan::v2
