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

#include <set>
#include <span>
#include <string>
#include <utility>

#include "query/auth_checker.hpp"

namespace memgraph::tests {

template <typename NameResolver>
class StubPropertyFGAChecker final : public query::FineGrainedAuthChecker {
 public:
  using DenySet = std::set<std::pair<std::string, std::string>>;

  explicit StubPropertyFGAChecker(NameResolver const *resolver, DenySet denied)
      : resolver_(resolver), denied_(std::move(denied)) {}

  bool Has(const query::VertexAccessor &, storage::View, query::AuthQuery::FineGrainedPrivilege) const override {
    return true;
  }

  bool Has(const query::EdgeAccessor &, query::AuthQuery::FineGrainedPrivilege) const override { return true; }

  bool Has(std::span<storage::LabelId const>, query::AuthQuery::FineGrainedPrivilege) const override { return true; }

  bool Has(storage::EdgeTypeId const &, query::AuthQuery::FineGrainedPrivilege) const override { return true; }

  bool HasGlobalPrivilegeOnVertices(query::AuthQuery::FineGrainedPrivilege) const override { return true; }

  bool HasGlobalPrivilegeOnEdges(query::AuthQuery::FineGrainedPrivilege) const override { return true; }

  bool HasAllGlobalPrivilegesOnVertices() const override { return true; }

  bool HasAllGlobalPrivilegesOnEdges() const override { return true; }

  bool HasUnrestrictedAccessToVertices() const override { return true; }

  bool HasUnrestrictedAccessToEdges() const override { return true; }

  bool HasUnrestrictedAccessToVertexProperties() const override { return denied_.empty(); }

  bool HasUnrestrictedAccessToEdgeTypeProperties() const override { return denied_.empty(); }

  bool NeedsFineGrainedAuthChecker() const override { return !denied_.empty(); }

  void MakeThreadSafe() const override {}

  bool IsThreadSafe() const override { return true; }

  bool HasPropertyPermission(std::span<storage::LabelId const> labels, storage::PropertyId property,
                             query::AuthQuery::PropertyPermissionType) const override {
    auto const &prop_name = resolver_->PropertyToName(property);
    for (auto label : labels) {
      if (denied_.contains({resolver_->LabelToName(label), prop_name})) return false;
    }
    return true;
  }

  bool HasPropertyPermission(storage::EdgeTypeId const &edge_type, storage::PropertyId property,
                             query::AuthQuery::PropertyPermissionType) const override {
    return !denied_.contains({resolver_->EdgeTypeToName(edge_type), resolver_->PropertyToName(property)});
  }

 private:
  NameResolver const *resolver_;
  DenySet denied_;
};

}  // namespace memgraph::tests
