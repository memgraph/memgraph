// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/plan_v2/ast_converter.hpp"

namespace memgraph::query::plan::v2 {

auto ConvertToEgraph(CypherQuery const &query, SymbolTable const &symbol_table) -> egraph { return egraph{}; }

}  // namespace memgraph::query::plan::v2

// struct ast_converter {
//   ast_converter(CypherQuery const &query, SymbolTable const &symbol_table)
//       : query_root_{query}, symbol_table_{symbol_table} {}
// private:
//   [[maybe_unused]] CypherQuery const &query_root_;
//   [[maybe_unused]] SymbolTable const &symbol_table_;
// };
