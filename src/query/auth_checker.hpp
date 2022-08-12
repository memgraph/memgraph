// Copyright 2022 Memgraph Ltd.
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

#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"

namespace memgraph::query {

class UserBasedAuthChecker;

class AuthChecker {
 public:
  virtual ~AuthChecker() = default;

  [[nodiscard]] virtual bool IsUserAuthorized(const std::optional<std::string> &username,
                                              const std::vector<query::AuthQuery::Privilege> &privileges) const = 0;

  [[nodiscard]] virtual std::unique_ptr<UserBasedAuthChecker> GetUserBasedAuthChecker(
      const std::string &username) const = 0;
};

class UserBasedAuthChecker {
 public:
  virtual ~UserBasedAuthChecker() = default;

  [[nodiscard]] virtual bool Accept(const memgraph::query::DbAccessor &dba, const query::VertexAccessor &vertex,
                                    const memgraph::storage::View &view) const = 0;

  [[nodiscard]] virtual bool Accept(const memgraph::query::DbAccessor &dba, const query::EdgeAccessor &edge) const = 0;
};

class AllowEverythingUserBasedAuthChecker final : public query::UserBasedAuthChecker {
 public:
  bool Accept(const memgraph::query::DbAccessor &dba, const VertexAccessor &vertex,
              const memgraph::storage::View &view) const override {
    return true;
  }

  bool Accept(const memgraph::query::DbAccessor &dba, const memgraph::query::EdgeAccessor &edge) const override {
    return true;
  }
};

class AllowEverythingAuthChecker final : public query::AuthChecker {
 public:
  bool IsUserAuthorized(const std::optional<std::string> & /*username*/,
                        const std::vector<query::AuthQuery::Privilege> & /*privileges*/) const override {
    return true;
  }

  std::unique_ptr<UserBasedAuthChecker> GetUserBasedAuthChecker(const std::string & /*username*/) const override {
    return std::make_unique<AllowEverythingUserBasedAuthChecker>();
  }
};

}  // namespace memgraph::query
