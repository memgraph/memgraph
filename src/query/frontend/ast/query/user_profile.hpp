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

#include "query/frontend/ast/query/expression.hpp"
#include "query/frontend/ast/query/query.hpp"

#include <optional>
#include <string>
#include <vector>

namespace memgraph::query {
class UserProfileQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  UserProfileQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  struct LimitValueResult {
    enum class Type : uint8_t { UNLIMITED, MEMORY_LIMIT, QUANTITY };
    union {
      Type type{Type::UNLIMITED};
      struct {
        Type type;
        union {
          memgraph::query::Expression *expr;
          uint64_t value;
        };
        size_t scale;
      } mem_limit;
      struct {
        Type type;
        union {
          memgraph::query::Expression *expr;
          uint64_t value;
        };
      } quantity;
    };

    bool operator==(const auto &other) const {
      if (type != other.type) return false;
      switch (type) {
        case Type::UNLIMITED:
          return true;
        case Type::MEMORY_LIMIT:
          return mem_limit.value == other.mem_limit.value && mem_limit.scale == other.mem_limit.scale;
        case Type::QUANTITY:
          return quantity.value == other.quantity.value;
      }
    }
  };

  using limit_t = std::pair<std::string, LimitValueResult>;
  using limits_t = std::vector<limit_t>;

  enum class Action { CREATE, UPDATE, DROP, SHOW_ALL, SHOW_ONE, SHOW_USERS, SHOW_FOR, SET, CLEAR } action_;
  std::string profile_name_;
  limits_t limits_;  // Value type might change if int is not sufficient
  std::optional<std::string> user_or_role_;

  UserProfileQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<UserProfileQuery>();
    object->action_ = action_;
    object->profile_name_ = profile_name_;
    object->limits_ = limits_;
    object->user_or_role_ = user_or_role_;
    return object;
  }

 private:
  friend class AstStorage;
};

}  // namespace memgraph::query
