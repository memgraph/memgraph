// Copyright 2026 Memgraph Ltd.
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

#include <optional>
#include <string>
#include <vector>

#include "query/frontend/ast/query/expression.hpp"
#include "query/frontend/ast/query/query.hpp"
#include "query/frontend/ast/query/user_profile.hpp"

namespace memgraph::query {

class TenantProfileQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;

  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  TenantProfileQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  enum class Action : uint8_t {
    CREATE,
    ALTER,
    DROP,
    SHOW_ALL,
    SHOW_ONE,
    SET_ON_DATABASE,
    REMOVE_FROM_DATABASE,
  } action_{Action::CREATE};

  std::string profile_name_;
  std::string db_name_;

  using limit_t = std::pair<std::string, UserProfileQuery::LimitValueResult>;
  using limits_t = std::vector<limit_t>;
  limits_t limits_;

  TenantProfileQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<TenantProfileQuery>();
    object->action_ = action_;
    object->profile_name_ = profile_name_;
    object->db_name_ = db_name_;
    object->limits_ = limits_;
    return object;
  }

 private:
  friend class AstStorage;
};

}  // namespace memgraph::query
