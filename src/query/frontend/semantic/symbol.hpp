#pragma once

#include <string>

#include "boost/serialization/serialization.hpp"
#include "boost/serialization/string.hpp"

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

  const auto &name() const { return name_; }
  int position() const { return position_; }
  Type type() const { return type_; }
  bool user_declared() const { return user_declared_; }
  int token_position() const { return token_position_; }

  void Save(capnp::Symbol::Builder *builder) const {
    builder->setName(name_);
    builder->setPosition(position_);
    builder->setUserDeclared(user_declared_);
    builder->setTokenPosition(token_position_);
    switch (type_) {
      case Type::Any:
        builder->setType(capnp::Symbol::Type::ANY);
        break;
      case Type::Edge:
        builder->setType(capnp::Symbol::Type::EDGE);
        break;
      case Type::EdgeList:
        builder->setType(capnp::Symbol::Type::EDGE_LIST);
        break;
      case Type::Number:
        builder->setType(capnp::Symbol::Type::NUMBER);
        break;
      case Type::Path:
        builder->setType(capnp::Symbol::Type::PATH);
        break;
      case Type::Vertex:
        builder->setType(capnp::Symbol::Type::VERTEX);
        break;
    }
  }

  void Load(const capnp::Symbol::Reader &reader) {
    name_ = reader.getName();
    position_ = reader.getPosition();
    user_declared_ = reader.getUserDeclared();
    token_position_ = reader.getTokenPosition();
    switch (reader.getType()) {
      case capnp::Symbol::Type::ANY:
        type_ = Type::Any;
        break;
      case capnp::Symbol::Type::EDGE:
        type_ = Type::Edge;
        break;
      case capnp::Symbol::Type::EDGE_LIST:
        type_ = Type::EdgeList;
        break;
      case capnp::Symbol::Type::NUMBER:
        type_ = Type::Number;
        break;
      case capnp::Symbol::Type::PATH:
        type_ = Type::Path;
        break;
      case capnp::Symbol::Type::VERTEX:
        type_ = Type::Vertex;
        break;
    }
  }

 private:
  std::string name_;
  int position_;
  bool user_declared_ = true;
  Type type_ = Type::Any;
  int token_position_ = -1;

  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, const unsigned int) {
    ar & name_;
    ar & position_;
    ar & user_declared_;
    ar & type_;
    ar & token_position_;
  }
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
