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
    auto got = table_.emplace(position_, Symbol(name, position_, user_declared,
                                                type, token_position));
    CHECK(got.second) << "Duplicate symbol ID!";
    position_++;
    return got.first->second;
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

  int32_t position_{0};
  std::map<int32_t, Symbol> table_;
};

}  // namespace query
