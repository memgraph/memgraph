#pragma once

#include <map>
#include <string>

#include "query/frontend/ast/ast.hpp"

namespace query {
struct Symbol {
  Symbol() {}
  Symbol(const std::string& name, int position) : name_(name), position_(position) {}
  std::string name_;
  int position_;
};

class SymbolTable {
 public:
  Symbol CreateSymbol(const std::string& name) {
    int position = position_++;
    return Symbol(name, position);
  }

  auto& operator[](const Tree& tree) { return table_[tree.uid()]; }

  int max_position() const { return position_; }

 private:
  int position_{0};
  std::map<int, Symbol> table_;
};
}
