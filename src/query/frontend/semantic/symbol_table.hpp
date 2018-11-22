#pragma once

#include <map>
#include <string>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol.hpp"

namespace query {

class SymbolTable final {
 public:
  SymbolTable() {}
  Symbol CreateSymbol(const std::string &name, bool user_declared,
                      Symbol::Type type = Symbol::Type::ANY,
                      int token_position = -1) {
    int position = position_++;
    return Symbol(name, position, user_declared, type, token_position);
  }

  auto &operator[](const Tree &tree) { return table_[tree.uid_]; }

  Symbol &at(const Tree &tree) { return table_.at(tree.uid_); }
  const Symbol &at(const Tree &tree) const { return table_.at(tree.uid_); }

  // TODO: Remove these since members are public
  int max_position() const { return position_; }

  const auto &table() const { return table_; }

  int position_{0};
  std::map<int, Symbol> table_;
};


}  // namespace query
