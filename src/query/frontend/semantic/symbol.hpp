#pragma once

#include <string>

#include "symbol.capnp.h"

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
         Type type = Type::Any, int token_position = -1)
      : name_(name),
        position_(position),
        user_declared_(user_declared),
        type_(type),
        token_position_(token_position) {}

  bool operator==(const Symbol &other) const {
    return position_ == other.position_ && name_ == other.name_ &&
           type_ == other.type_;
  }
  bool operator!=(const Symbol &other) const { return !operator==(other); }

  // TODO: Remove these since members are public
  const auto &name() const { return name_; }
  int position() const { return position_; }
  Type type() const { return type_; }
  bool user_declared() const { return user_declared_; }
  int token_position() const { return token_position_; }

  std::string name_;
  int position_;
  bool user_declared_ = true;
  Type type_ = Type::Any;
  int token_position_ = -1;
};

inline void Save(const Symbol &symbol, capnp::Symbol::Builder *builder) {
  builder->setName(symbol.name());
  builder->setPosition(symbol.position());
  builder->setUserDeclared(symbol.user_declared());
  builder->setTokenPosition(symbol.token_position());
  switch (symbol.type()) {
    case Symbol::Type::Any:
      builder->setType(capnp::Symbol::Type::ANY);
      break;
    case Symbol::Type::Edge:
      builder->setType(capnp::Symbol::Type::EDGE);
      break;
    case Symbol::Type::EdgeList:
      builder->setType(capnp::Symbol::Type::EDGE_LIST);
      break;
    case Symbol::Type::Number:
      builder->setType(capnp::Symbol::Type::NUMBER);
      break;
    case Symbol::Type::Path:
      builder->setType(capnp::Symbol::Type::PATH);
      break;
    case Symbol::Type::Vertex:
      builder->setType(capnp::Symbol::Type::VERTEX);
      break;
  }
}

inline void Load(Symbol *symbol, const capnp::Symbol::Reader &reader) {
  symbol->name_ = reader.getName();
  symbol->position_ = reader.getPosition();
  symbol->user_declared_ = reader.getUserDeclared();
  symbol->token_position_ = reader.getTokenPosition();
  switch (reader.getType()) {
    case capnp::Symbol::Type::ANY:
      symbol->type_ = Symbol::Type::Any;
      break;
    case capnp::Symbol::Type::EDGE:
      symbol->type_ = Symbol::Type::Edge;
      break;
    case capnp::Symbol::Type::EDGE_LIST:
      symbol->type_ = Symbol::Type::EdgeList;
      break;
    case capnp::Symbol::Type::NUMBER:
      symbol->type_ = Symbol::Type::Number;
      break;
    case capnp::Symbol::Type::PATH:
      symbol->type_ = Symbol::Type::Path;
      break;
    case capnp::Symbol::Type::VERTEX:
      symbol->type_ = Symbol::Type::Vertex;
      break;
  }
}

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
