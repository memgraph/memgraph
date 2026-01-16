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

#pragma once

#include <deque>
#include <string>

#include "query/frontend/ast/query/aggregation.hpp"
#include "query/frontend/ast/query/exists.hpp"
#include "query/frontend/ast/query/identifier.hpp"
#include "query/frontend/ast/query/named_expression.hpp"
#include "query/frontend/ast/query/pattern_comprehension.hpp"
#include "query/frontend/semantic/symbol.hpp"
#include "utils/logging.hpp"

#include <absl/container/flat_hash_map.h>

namespace memgraph::query {

class SymbolTable final {
 public:
  SymbolTable() = default;
  const Symbol &CreateSymbol(std::string name, bool user_declared, Symbol::Type type = Symbol::Type::ANY,
                             int32_t token_position = -1) {
    MG_ASSERT(table_.size() <= std::numeric_limits<int32_t>::max(),
              "SymbolTable size doesn't fit into 32-bit integer!");
    auto position = table_.size();
    return table_.emplace_back(std::move(name), position, user_declared, type, token_position);
  }

  // TODO(buda): This is the same logic as in the cypher_main_visitor. During
  // parsing phase symbol table doesn't exist. Figure out a better solution.
  const Symbol &CreateAnonymousSymbol(Symbol::Type type = Symbol::Type::ANY) {
    while (true) {
      static const std::string &kAnonPrefix = "anon";
      std::string name_candidate = kAnonPrefix + std::to_string(anon_couter_++);
      if (std::ranges::find_if(table_, [&](const auto &item) -> bool { return item.name() == name_candidate; }) ==
          std::end(table_)) {
        return CreateSymbol(std::move(name_candidate), false, type);
      }
    }
  }

  const Symbol &at(const Identifier &ident) const { return table_.at(ident.symbol_pos_); }
  const Symbol &at(const NamedExpression &nexpr) const { return table_.at(nexpr.symbol_pos_); }
  const Symbol &at(const Aggregation &aggr) const { return table_.at(aggr.symbol_pos_); }
  const Symbol &at(const Exists &exists) const { return table_.at(exists.symbol_pos_); }
  const Symbol &at(const PatternComprehension &pattern_comprehension) const {
    return table_.at(pattern_comprehension.symbol_pos_);
  }

  // TODO: Remove these since members are public
  int32_t max_position() const { return static_cast<int32_t>(table_.size()); }

  const auto &table() const { return table_; }

  int anon_couter_ = 1;
  std::deque<Symbol> table_;
};

}  // namespace memgraph::query
