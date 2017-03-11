#pragma once

#include <map>
#include <string>

#include "query/frontend/ast/ast.hpp"

namespace query {
struct Symbol {
  Symbol(std::string& name, int position) : name_(name), position_(position) {}
  std::string name_;
  int position_;
};

class SymbolTable {
 public:
  Symbol CreateSymbol(std::string& name) {
    int position = position_++;
    return Symbol(name, position);
  }

  void AssignSymbol(const Tree& tree, Symbol symbol) {
    table_[tree.uid()] = symbol;
  }

  auto& operator[](const Tree& tree) { return table_[tree.uid()]; }

  int max_position() const { return position_; }

 private:
  int position_{0};
  std::map<int, Symbol> table_;
};
}
