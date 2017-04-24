#pragma once

#include <map>
#include <string>

#include "query/frontend/ast/ast.hpp"

namespace query {

class Symbol {
 public:
  // This is similar to TypedValue::Type, but this has `Any` type.
  enum class Type { Any, Vertex, Edge, Path, Number };

  static std::string TypeToString(Type type) {
    const char *enum_string[] = {"Any", "Vertex", "Edge", "Path", "Number"};
    return enum_string[static_cast<int>(type)];
  }

  // Calculates the Symbol hash based on its position.
  struct Hash {
    size_t operator()(const Symbol &symbol) const {
      return std::hash<int>{}(symbol.position_);
    }
  };

  Symbol() {}
  Symbol(const std::string &name, int position, Type type = Type::Any)
      : name_(name), position_(position), type_(type) {}

  std::string name_;
  int position_;
  Type type_{Type::Any};

  bool operator==(const Symbol &other) const {
    return position_ == other.position_ && name_ == other.name_ &&
           type_ == other.type_;
  }
  bool operator!=(const Symbol &other) const { return !operator==(other); }
};

class SymbolTable {
 public:
  Symbol CreateSymbol(const std::string &name,
                      Symbol::Type type = Symbol::Type::Any) {
    int position = position_++;
    return Symbol(name, position, type);
  }

  auto &operator[](const Tree &tree) { return table_[tree.uid()]; }

  Symbol &at(const Tree &tree) { return table_.at(tree.uid()); }
  const Symbol &at(const Tree &tree) const { return table_.at(tree.uid()); }

  int max_position() const { return position_; }

 private:
  int position_{0};
  std::map<int, Symbol> table_;
};

}
