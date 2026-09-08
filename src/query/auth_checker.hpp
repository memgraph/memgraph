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

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "dbms/constants.hpp"
#include "query/edge_accessor.hpp"
#include "query/query_user.hpp"
#include "query/vertex_accessor.hpp"
#include "storage/v2/id_types.hpp"

namespace memgraph::query {

class DbAccessor;
class FineGrainedAuthChecker;

class AuthChecker {
 public:
  virtual ~AuthChecker() = default;

  virtual std::shared_ptr<QueryUserOrRole> GenQueryUser(const std::optional<std::string> &username,
                                                        const std::vector<std::string> &rolenames) const = 0;

  virtual std::shared_ptr<QueryUserOrRole> GenEmptyUser() const = 0;

  [[nodiscard]] virtual std::unique_ptr<FineGrainedAuthChecker> GetFineGrainedAuthChecker(
      const QueryUserOrRole &user, const DbAccessor *db_accessor) const = 0;

 protected:
  AuthChecker() = default;
  AuthChecker(const AuthChecker &) = default;
  AuthChecker(AuthChecker &&) noexcept = default;
  AuthChecker &operator=(const AuthChecker &) = default;
  AuthChecker &operator=(AuthChecker &&) noexcept = default;
};

class FineGrainedAuthChecker {
 public:
  virtual ~FineGrainedAuthChecker() = default;

  [[nodiscard]] virtual bool Has(const VertexAccessor &vertex, memgraph::storage::View view,
                                 AuthQuery::FineGrainedPrivilege fine_grained_privilege) const = 0;

  [[nodiscard]] virtual bool Has(const EdgeAccessor &edge,
                                 AuthQuery::FineGrainedPrivilege fine_grained_privilege) const = 0;

  [[nodiscard]] virtual bool Has(std::span<memgraph::storage::LabelId const> labels,
                                 AuthQuery::FineGrainedPrivilege fine_grained_privilege) const = 0;

  [[nodiscard]] virtual bool Has(const memgraph::storage::EdgeTypeId &edge_type,
                                 AuthQuery::FineGrainedPrivilege fine_grained_privilege) const = 0;

  [[nodiscard]] virtual bool HasGlobalPrivilegeOnVertices(
      AuthQuery::FineGrainedPrivilege fine_grained_privilege) const = 0;

  [[nodiscard]] virtual bool HasGlobalPrivilegeOnEdges(
      AuthQuery::FineGrainedPrivilege fine_grained_privilege) const = 0;

  [[nodiscard]] virtual bool HasAllGlobalPrivilegesOnVertices() const = 0;

  [[nodiscard]] virtual bool HasAllGlobalPrivilegesOnEdges() const = 0;

  [[nodiscard]] virtual bool HasUnrestrictedAccessToVertices() const = 0;

  [[nodiscard]] virtual bool HasUnrestrictedAccessToEdges() const = 0;

  [[nodiscard]] virtual bool HasUnrestrictedAccessToVertexProperties() const = 0;

  [[nodiscard]] virtual bool HasUnrestrictedAccessToEdgeTypeProperties() const = 0;

  [[nodiscard]] bool HasPropertyRestrictions() const {
    if (!has_property_restrictions_) {
      has_property_restrictions_ =
          !HasUnrestrictedAccessToVertexProperties() || !HasUnrestrictedAccessToEdgeTypeProperties();
    }
    return *has_property_restrictions_;
  }

  /// True when a FineGrainedAuthChecker must be attached for correct
  /// authorization, defined by either per-Label/per-Edge rules, or per-Property
  /// rules. When false, the checker is redundant as no restrictions to labels,
  /// edges, or properties are defined for the current user.
  [[nodiscard]] virtual bool NeedsFineGrainedAuthChecker() const = 0;

  [[nodiscard]] virtual bool HasPropertyPermission(std::span<memgraph::storage::LabelId const> labels,
                                                   memgraph::storage::PropertyId property,
                                                   AuthQuery::PropertyPermissionType type) const = 0;

  [[nodiscard]] virtual bool HasPropertyPermission(memgraph::storage::EdgeTypeId const &edge_type,
                                                   memgraph::storage::PropertyId property,
                                                   AuthQuery::PropertyPermissionType type) const = 0;

  virtual void UpdateDbAccessor(DbAccessor const * /*dba*/) {}

  // Used to make the auth checker thread safe
  // throw if not possible
  virtual void MakeThreadSafe() const = 0;
  virtual bool IsThreadSafe() const = 0;

 protected:
  FineGrainedAuthChecker() = default;
  FineGrainedAuthChecker(const FineGrainedAuthChecker &) = default;
  FineGrainedAuthChecker(FineGrainedAuthChecker &&) noexcept = default;
  FineGrainedAuthChecker &operator=(const FineGrainedAuthChecker &) = default;
  FineGrainedAuthChecker &operator=(FineGrainedAuthChecker &&) noexcept = default;

  bool IsPropertyRestrictionsCached() const { return has_property_restrictions_.has_value(); }

  mutable std::optional<bool> has_property_restrictions_;
};

// single source of truth for "may this caller READ this property on this entity", reused by the
// expression evaluator (masks the value to NULL) and the search procedures (drop the hit)
inline bool PropertyReadAllowed(FineGrainedAuthChecker const *auth_checker, VertexAccessor const &vertex,
                                storage::View view, storage::PropertyId property) {
  if (!auth_checker || !auth_checker->HasPropertyRestrictions()) return true;
  const auto maybe_labels = vertex.Labels(view);
  if (!maybe_labels) return false;
  return auth_checker->HasPropertyPermission(*maybe_labels, property, AuthQuery::PropertyPermissionType::READ);
}

inline bool PropertyReadAllowed(FineGrainedAuthChecker const *auth_checker, EdgeAccessor const &edge,
                                storage::PropertyId property) {
  if (!auth_checker || !auth_checker->HasPropertyRestrictions()) return true;
  return auth_checker->HasPropertyPermission(edge.EdgeType(), property, AuthQuery::PropertyPermissionType::READ);
}

class AllowEverythingFineGrainedAuthChecker final : public FineGrainedAuthChecker {
 public:
  bool Has(const VertexAccessor & /*vertex*/, const memgraph::storage::View /*view*/,
           const AuthQuery::FineGrainedPrivilege /*fine_grained_privilege*/) const override {
    return true;
  }

  bool Has(const EdgeAccessor & /*edge*/,
           const AuthQuery::FineGrainedPrivilege /*fine_grained_privilege*/) const override {
    return true;
  }

  bool Has(std::span<memgraph::storage::LabelId const> /*labels*/,
           AuthQuery::FineGrainedPrivilege /*fine_grained_privilege*/) const override {
    return true;
  }

  bool Has(const memgraph::storage::EdgeTypeId & /*edge_type*/,
           const AuthQuery::FineGrainedPrivilege /*fine_grained_privilege*/) const override {
    return true;
  }

  bool HasGlobalPrivilegeOnVertices(const AuthQuery::FineGrainedPrivilege /*fine_grained_privilege*/) const override {
    return true;
  }

  bool HasGlobalPrivilegeOnEdges(const AuthQuery::FineGrainedPrivilege /*fine_grained_privilege*/) const override {
    return true;
  }

  bool HasAllGlobalPrivilegesOnVertices() const override { return true; }

  bool HasAllGlobalPrivilegesOnEdges() const override { return true; }

  bool HasUnrestrictedAccessToVertices() const override { return true; }

  bool HasUnrestrictedAccessToEdges() const override { return true; }

  bool HasUnrestrictedAccessToVertexProperties() const override { return true; }

  bool HasUnrestrictedAccessToEdgeTypeProperties() const override { return true; }

  bool NeedsFineGrainedAuthChecker() const override { return false; }

  bool HasPropertyPermission(std::span<memgraph::storage::LabelId const> /*labels*/,
                             memgraph::storage::PropertyId /*property*/,
                             AuthQuery::PropertyPermissionType /*type*/) const override {
    return true;
  }

  bool HasPropertyPermission(memgraph::storage::EdgeTypeId const & /*edge_type*/,
                             memgraph::storage::PropertyId /*property*/,
                             AuthQuery::PropertyPermissionType /*type*/) const override {
    return true;
  }

  void MakeThreadSafe() const override {
    // No-op
  }

  bool IsThreadSafe() const override { return true; }
};

class AllowEverythingAuthChecker final : public AuthChecker {
 public:
  struct User : query::QueryUserOrRole {
    User() : query::QueryUserOrRole{{}, {}} {}

    User(std::string name) : query::QueryUserOrRole{std::move(name), {}} {}

    bool IsAuthorized(const std::vector<AuthQuery::Privilege> & /*privileges*/,
                      std::optional<std::string_view> /*db_name*/, UserPolicy * /*policy*/) const override {
      return true;
    }

    std::shared_ptr<QueryUserOrRole> clone() const override { return std::make_shared<User>(*this); }

    std::vector<std::string> GetRolenames(std::optional<std::string> /*db_name*/) const override { return {}; }
#ifdef MG_ENTERPRISE
    bool CanImpersonate(const std::string & /*target*/, query::UserPolicy * /*policy*/,
                        std::optional<std::string_view> /*db_name*/ = std::nullopt) const override {
      return true;
    }

    std::string GetDefaultDB() const override { return std::string{dbms::kDefaultDB}; }
#endif
  };

  std::shared_ptr<query::QueryUserOrRole> GenQueryUser(const std::optional<std::string> &name,
                                                       const std::vector<std::string> & /*roles*/) const override {
    if (name) return std::make_shared<User>(*name);
    return std::make_shared<User>();
  }

  std::shared_ptr<QueryUserOrRole> GenEmptyUser() const override { return std::make_shared<User>(); }

  std::unique_ptr<FineGrainedAuthChecker> GetFineGrainedAuthChecker(const QueryUserOrRole & /*user*/,
                                                                    const DbAccessor * /*dba*/) const override {
    return std::make_unique<AllowEverythingFineGrainedAuthChecker>();
  }
};

struct CachedFineGrainedAuth {
  enum class State : uint8_t {
    EMPTY,            // No auth cached, need to to check licence and FGA
    NO_LICENSE,       // No enterprise license, so re-evaluate every query
    NO_RESTRICTIONS,  // Enterprise licensed, no FGA needed, and this is cached
    ACTIVE,           // Enterprise licensed, FGA active and auth cached
  };

  std::unique_ptr<FineGrainedAuthChecker> checker;
  std::string db_name;
  State state{State::EMPTY};

  FineGrainedAuthChecker const *get() const { return checker.get(); }

  void Refresh(AuthChecker const &auth_checker, QueryUserOrRole const &user, DbAccessor const *dba,
               std::string current_db) {
    bool const must_rebuild = state == State::EMPTY || state == State::NO_LICENSE || db_name != current_db;

    if (!must_rebuild) {
      if (checker) checker->UpdateDbAccessor(dba);
      return;
    }

    checker = auth_checker.GetFineGrainedAuthChecker(user, dba);

    if (!checker) {
      db_name = std::move(current_db);
      state = State::NO_LICENSE;
      return;
    }

    if (!checker->NeedsFineGrainedAuthChecker()) {
      checker.reset();
      db_name = std::move(current_db);
      state = State::NO_RESTRICTIONS;
      return;
    }

    db_name = std::move(current_db);
    state = State::ACTIVE;
  }

  void Reset() {
    checker.reset();
    db_name.clear();
    state = State::EMPTY;
  }
};

}  // namespace memgraph::query
