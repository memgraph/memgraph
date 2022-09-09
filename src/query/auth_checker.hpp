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
#include "storage/v2/id_types.hpp"

namespace memgraph::query {

class FineGrainedAuthChecker;

class AuthChecker {
 public:
  virtual ~AuthChecker() = default;

  [[nodiscard]] virtual bool IsUserAuthorized(const std::optional<std::string> &username,
                                              const std::vector<query::AuthQuery::Privilege> &privileges) const = 0;

  [[nodiscard]] virtual std::unique_ptr<FineGrainedAuthChecker> GetFineGrainedAuthChecker(
      const std::string &username, const memgraph::query::DbAccessor &db_accessor) const = 0;
};

class FineGrainedAuthChecker {
 public:
  virtual ~FineGrainedAuthChecker() = default;

  [[nodiscard]] virtual bool Accept(const query::VertexAccessor &vertex, memgraph::storage::View view,
                                    query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const = 0;

  [[nodiscard]] virtual bool Accept(const query::EdgeAccessor &edge,
                                    query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const = 0;

  [[nodiscard]] virtual bool Accept(const std::vector<memgraph::storage::LabelId> &labels,
                                    query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const = 0;

  [[nodiscard]] virtual bool Accept(const memgraph::storage::EdgeTypeId &edge_type,
                                    query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const = 0;

  [[nodiscard]] virtual bool HasGlobalPermissionOnVertices(
      memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const = 0;

  [[nodiscard]] virtual bool HasGlobalPermissionOnEdges(
      memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const = 0;
};

class AllowEverythingFineGrainedAuthChecker final : public query::FineGrainedAuthChecker {
 public:
  bool Accept(const VertexAccessor & /*vertex*/, const memgraph::storage::View /*view*/,
              const query::AuthQuery::FineGrainedPrivilege /*fine_grained_permission*/) const override {
    return true;
  }

  bool Accept(const memgraph::query::EdgeAccessor & /*edge*/,
              const query::AuthQuery::FineGrainedPrivilege /*fine_grained_permission*/) const override {
    return true;
  }

  bool Accept(const std::vector<memgraph::storage::LabelId> & /*labels*/,
              const query::AuthQuery::FineGrainedPrivilege /*fine_grained_permission*/) const override {
    return true;
  }

  bool Accept(const memgraph::storage::EdgeTypeId & /*edge_type*/,
              const query::AuthQuery::FineGrainedPrivilege /*fine_grained_permission*/) const override {
    return true;
  }

  bool HasGlobalPermissionOnVertices(
      const memgraph::query::AuthQuery::FineGrainedPrivilege /*fine_grained_privilege*/) const override {
    return true;
  }

  bool HasGlobalPermissionOnEdges(
      const memgraph::query::AuthQuery::FineGrainedPrivilege /*fine_grained_privilege*/) const override {
    return true;
  }
};  // namespace memgraph::query

class AllowEverythingAuthChecker final : public query::AuthChecker {
 public:
  bool IsUserAuthorized(const std::optional<std::string> & /*username*/,
                        const std::vector<query::AuthQuery::Privilege> & /*privileges*/) const override {
    return true;
  }

  std::unique_ptr<FineGrainedAuthChecker> GetFineGrainedAuthChecker(const std::string & /*username*/,
                                                                    const query::DbAccessor & /*dba*/) const override {
    return std::make_unique<AllowEverythingFineGrainedAuthChecker>();
  }
};

}  // namespace memgraph::query
