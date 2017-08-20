#pragma once

#include <map>
#include <string>

#include "query/frontend/ast/ast.hpp"

namespace query {

class Symbol {
 public:
  // This is similar to TypedValue::Type, but this has `Any` type.
  // TODO: Make a better Type structure which can store a generic List.
  enum class Type { Any, Vertex, Edge, Path, Number, EdgeList };

  static std::string TypeToString(Type type) {
    const char *enum_string[] = {"Any",  "Vertex", "Edge",
                                 "Path", "Number", "EdgeList"};
    return enum_string[static_cast<int>(type)];
  }

  Symbol() {}
  Symbol(const std::string &name, int position, bool user_declared,
         Type type = Type::Any)
      : name_(name),
        position_(position),
        user_declared_(user_declared),
        type_(type) {}

  bool operator==(const Symbol &other) const {
    return position_ == other.position_ && name_ == other.name_ &&
           type_ == other.type_;
  }
  bool operator!=(const Symbol &other) const { return !operator==(other); }

  const auto &name() const { return name_; }
  int position() const { return position_; }
  Type type() const { return type_; }
  bool user_declared() const { return user_declared_; }

 private:
  std::string name_;
  int position_;
  bool user_declared_ = true;
  Type type_ = Type::Any;
};

class SymbolTable {
 public:
  Symbol CreateSymbol(const std::string &name, bool user_declared,
                      Symbol::Type type = Symbol::Type::Any) {
    int position = position_++;
    return Symbol(name, position, user_declared, type);
  }

  auto &operator[](const Tree &tree) { return table_[tree.uid()]; }

  Symbol &at(const Tree &tree) { return table_.at(tree.uid()); }
  const Symbol &at(const Tree &tree) const { return table_.at(tree.uid()); }

  int max_position() const { return position_; }

 private:
  int position_{0};
  std::map<int, Symbol> table_;
};

}  // namespace query

namespace std {

template <>
struct hash<query::Symbol> {
  size_t operator()(const query::Symbol &symbol) const {
    size_t prime = 265443599u;
    size_t hash = std::hash<int>{}(symbol.position());
    hash ^= prime * std::hash<std::string>{}(symbol.name());
    hash ^= prime * std::hash<bool>{}(symbol.user_declared());
    hash ^= prime * std::hash<int>{}(static_cast<int>(symbol.type()));
    return hash;
  }
};

}  // namespace std
