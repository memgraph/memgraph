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

#include <array>
#include <cstdint>
#include <string>
#include <utility>

#include "utils/typeinfo.hpp"

namespace memgraph::query {

class Symbol {
 public:
  using Position_t = int64_t;
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  enum class Type : uint8_t { ANY, VERTEX, EDGE, PATH, NUMBER, EDGE_LIST };

  // TODO: Generate enum to string conversion from LCP. Note, that this is
  // displayed to the end user, so we may want to have a pretty name of each
  // value.
  static std::string_view TypeToString(Type type) {
    using namespace std::string_view_literals;
    static constexpr auto enum_string = std::array{"Any"sv, "Vertex"sv, "Edge"sv, "Path"sv, "Number"sv, "EdgeList"sv};
    return enum_string[static_cast<int>(type)];
  }

  Symbol() = default;
  Symbol(std::string name, int position, bool user_declared, Type type = Type::ANY, int token_position = -1)
      : name_(std::move(name)),
        position_(position),
        user_declared_(user_declared),
        type_(type),
        token_position_(token_position) {}

  bool operator==(const Symbol &other) const {
    return position_ == other.position_ && type_ == other.type_ && name_ == other.name_;
  }

  // TODO: Remove these since members are public
  auto name() const -> std::string const & { return name_; }
  Position_t position() const { return position_; }
  Type type() const { return type_; }
  bool user_declared() const { return user_declared_; }
  int64_t token_position() const { return token_position_; }

 private:
  std::string name_;
  Position_t position_;
  bool user_declared_{true};
  Type type_{Type::ANY};
  int64_t token_position_{-1};  // from ANTLR token stream
};

}  // namespace memgraph::query

namespace std {

template <>
struct hash<memgraph::query::Symbol> {
  size_t operator()(const memgraph::query::Symbol &symbol) const {
    size_t prime = 265443599u;
    size_t hash = std::hash<int>{}(symbol.position());
    hash ^= prime * std::hash<std::string>{}(symbol.name());
    hash ^= prime * std::hash<int>{}(static_cast<int>(symbol.type()));
    return hash;
  }
};

}  // namespace std
