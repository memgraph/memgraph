#pragma once

#include <map>
#include <string>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol.hpp"

namespace query {

class SymbolTable final {
 public:
  SymbolTable() {}
  const Symbol &CreateSymbol(const std::string &name, bool user_declared,
                             Symbol::Type type = Symbol::Type::ANY,
                             int32_t token_position = -1) {
    CHECK(table_.size() <= std::numeric_limits<int32_t>::max())
        << "SymbolTable size doesn't fit into 32-bit integer!";
    int32_t position = static_cast<int32_t>(table_.size());
    table_.emplace_back(name, position, user_declared, type, token_position);
    return table_.back();
  }

  const Symbol &at(const Identifier &ident) const {
    return table_.at(ident.symbol_pos_);
  }
  const Symbol &at(const NamedExpression &nexpr) const {
    return table_.at(nexpr.symbol_pos_);
  }
  const Symbol &at(const Aggregation &aggr) const {
    return table_.at(aggr.symbol_pos_);
  }

  // TODO: Remove these since members are public
  int32_t max_position() const { return static_cast<int32_t>(table_.size()); }

  const auto &table() const { return table_; }

  std::vector<Symbol> table_;
};

}  // namespace query
