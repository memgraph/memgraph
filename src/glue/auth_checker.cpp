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

#include "glue/auth_checker.hpp"
#include <range/v3/all.hpp>

#include "auth/auth.hpp"
#include "auth/models.hpp"
#include "glue/auth.hpp"
#include "glue/query_user.hpp"
#include "license/license.hpp"
#include "query/auth_checker.hpp"
#include "query/common.hpp"
#include "query/constants.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/query_user.hpp"
#include "storage/v2/id_types.hpp"
#include "utils/logging.hpp"
#include "utils/synchronized.hpp"
#include "utils/variant_helpers.hpp"

#include <algorithm>
#include <optional>
#include <span>

#ifdef MG_ENTERPRISE

// Not ideal defining aliases inside an ifdef, but clang-tidy complains about
// these being unused when MG_ENTERPRISE is not defined if outside.
namespace r = ranges;
namespace rv = r::views;

namespace {

bool IsAuthorizedLabels(memgraph::auth::FineGrainedAccessPermissions const &permissions,
                        const memgraph::query::DbAccessor *dba, std::span<memgraph::storage::LabelId const> labels,
                        const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return true;
  }

  auto const label_names = labels |
                           rv::transform([dba](memgraph::storage::LabelId label) { return dba->LabelToName(label); }) |
                           r::to_vector;

  return permissions.Has(std::span<const std::string>(label_names),
                         memgraph::glue::FineGrainedPrivilegeToFineGrainedPermission(
                             fine_grained_privilege, memgraph::glue::FineGrainedPermissionType::LABEL)) ==
         memgraph::auth::PermissionLevel::GRANT;
}

bool IsAuthorizedGloballyLabels(memgraph::auth::FineGrainedAccessPermissions const &permissions,
                                const memgraph::auth::FineGrainedPermission fine_grained_permission) {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return true;
  }
  return permissions.HasGlobal(fine_grained_permission) == memgraph::auth::PermissionLevel::GRANT;
}

bool IsAuthorizedGloballyEdges(memgraph::auth::FineGrainedAccessPermissions const &permissions,
                               const memgraph::auth::FineGrainedPermission fine_grained_permission) {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return true;
  }
  return permissions.HasGlobal(fine_grained_permission) == memgraph::auth::PermissionLevel::GRANT;
}

bool IsAuthorizedEdgeType(memgraph::auth::FineGrainedAccessPermissions const &permissions,
                          const memgraph::query::DbAccessor *dba, const memgraph::storage::EdgeTypeId &edgeType,
                          const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return true;
  }

  auto const &edge_type_name = dba->EdgeTypeToName(edgeType);
  return permissions.Has(std::span{&edge_type_name, 1},
                         memgraph::glue::FineGrainedPrivilegeToFineGrainedPermission(
                             fine_grained_privilege, memgraph::glue::FineGrainedPermissionType::EDGE_TYPE)) ==
         memgraph::auth::PermissionLevel::GRANT;
}
}  // namespace
#endif
namespace memgraph::glue {

AuthChecker::AuthChecker(memgraph::auth::SynchedAuth *auth) : auth_(auth) {}

std::shared_ptr<query::QueryUserOrRole> AuthChecker::GenQueryUser(const std::optional<std::string> &username,
                                                                  const std::vector<std::string> &rolenames) const {
  const auto user_or_role = auth_->ReadLock()->GetUserOrRole(username, rolenames);
  if (user_or_role) {
    return std::make_shared<QueryUserOrRole>(auth_, *user_or_role);
  }
  // No user or role
  return std::make_shared<QueryUserOrRole>(auth_);
}

std::unique_ptr<query::QueryUserOrRole> AuthChecker::GenQueryUser(auth::SynchedAuth *auth,
                                                                  const std::optional<auth::UserOrRole> &user_or_role) {
  if (user_or_role) {
    return std::visit(
        utils::Overloaded{[&](auto &user_or_role) { return std::make_unique<QueryUserOrRole>(auth, user_or_role); }},
        *user_or_role);
  }
  // No user or role
  return std::make_unique<QueryUserOrRole>(auth);
}

std::shared_ptr<query::QueryUserOrRole> AuthChecker::GenEmptyUser() const {
  return std::make_shared<QueryUserOrRole>(auth_);
}

std::unique_ptr<memgraph::query::FineGrainedAuthChecker> AuthChecker::GetFineGrainedAuthChecker(
    const query::QueryUserOrRole &user_or_role, const memgraph::query::DbAccessor *dba) const {
#ifdef MG_ENTERPRISE
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return {};
  }
  if (!user_or_role) {
    throw query::QueryRuntimeException("No user specified for fine grained authorization!");
  }

  // Convert from query user to auth user or role
  // NOTE: Make a copy of the user/role. At preparation time, the interpreter check if the user/role is authorized, and
  // update (if needed), so no need to update after that.
  try {
    auto glue_user = dynamic_cast<const glue::QueryUserOrRole &>(user_or_role);
    DMG_ASSERT(dba, "DbAccessor must be non-null for fine-grained auth checking");
    if (glue_user.user_) {
      return std::make_unique<glue::FineGrainedAuthChecker>(glue_user.user_.value(), dba);
    }
    if (glue_user.roles_) {
      return std::make_unique<glue::FineGrainedAuthChecker>(
          auth::RoleWUsername{glue_user.username().value(), glue_user.roles_.value()}, dba);
    }
    DMG_ASSERT(false, "Glue user has neither user not role");
  } catch (std::bad_cast &) {
    DMG_ASSERT(false, "Using a non-glue user in glue...");
  }
#endif

  // Should never get here (enterprise) / always returns null (community)
  return {};
}

bool AuthChecker::IsUserAuthorized(const memgraph::auth::User &user,
                                   const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
                                   std::optional<std::string_view> db_name) {  // NOLINT
#ifdef MG_ENTERPRISE
  if (db_name && !user.HasAccess(db_name.value())) {
    return false;
  }
#endif
  const auto user_permissions = user.GetPermissions(db_name);
  return std::ranges::all_of(privileges, [&user_permissions](const auto privilege) {
    return user_permissions.Has(memgraph::glue::PrivilegeToPermission(privilege)) ==
           memgraph::auth::PermissionLevel::GRANT;
  });
}

bool AuthChecker::IsRoleAuthorized(const memgraph::auth::Roles &roles,
                                   const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
                                   std::optional<std::string_view> db_name) {  // NOLINT
#ifdef MG_ENTERPRISE
  if (db_name && !roles.HasAccess(db_name.value())) {
    return false;
  }
#endif
  const auto roles_permissions = roles.GetPermissions(db_name);
  return std::ranges::all_of(privileges, [&roles_permissions](const auto privilege) {
    return roles_permissions.Has(memgraph::glue::PrivilegeToPermission(privilege)) ==
           memgraph::auth::PermissionLevel::GRANT;
  });
}

bool AuthChecker::IsUserOrRoleAuthorized(const memgraph::auth::UserOrRole &user_or_role,
                                         const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
                                         std::optional<std::string_view> db_name) {
  return std::visit(
      utils::Overloaded{
          [&](const auth::User &user) -> bool { return AuthChecker::IsUserAuthorized(user, privileges, db_name); },
          [&](const auth::Roles &roles) -> bool { return AuthChecker::IsRoleAuthorized(roles, privileges, db_name); }},
      user_or_role);
}

#ifdef MG_ENTERPRISE
bool AuthChecker::CanImpersonate(const memgraph::auth::User &user, const memgraph::auth::User &target,
                                 std::optional<std::string_view> db_name) {
  return user.CanImpersonate(target, db_name);
}

bool AuthChecker::CanImpersonate(const memgraph::auth::Role &role, const memgraph::auth::User &target,
                                 std::optional<std::string_view> db_name) {
  return role.CanImpersonate(target, db_name);
}

bool AuthChecker::CanImpersonate(const memgraph::auth::Roles &roles, const memgraph::auth::User &target,
                                 std::optional<std::string_view> db_name) {
  return roles.CanImpersonate(target, db_name);
}
#endif

#ifdef MG_ENTERPRISE
FineGrainedAuthChecker::FineGrainedAuthChecker(auth::UserOrRole user_or_role, const memgraph::query::DbAccessor *dba)
    : user_or_role_{std::move(user_or_role)}, dba_(dba), db_name_{dba_->DatabaseName()} {};

auth::FineGrainedAccessPermissions const &FineGrainedAuthChecker::GetCachedLabelPermissions() const {
  if (!cached_label_permissions_) {
    cached_label_permissions_ = std::visit(memgraph::utils::Overloaded{[this](auto const &user_or_role) {
                                             return user_or_role.GetFineGrainedAccessLabelPermissions(db_name_);
                                           }},
                                           user_or_role_);
  }
  return *cached_label_permissions_;
}

auth::FineGrainedAccessPermissions const &FineGrainedAuthChecker::GetCachedEdgePermissions() const {
  if (!cached_edge_permissions_) {
    cached_edge_permissions_ = std::visit(memgraph::utils::Overloaded{[this](auto const &user_or_role) {
                                            return user_or_role.GetFineGrainedAccessEdgeTypePermissions(db_name_);
                                          }},
                                          user_or_role_);
  }
  return *cached_edge_permissions_;
}

bool FineGrainedAuthChecker::Has(const memgraph::query::VertexAccessor &vertex, const memgraph::storage::View view,
                                 const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const {
  if (HasUnrestrictedAccessToVertices()) return true;
  auto maybe_labels = vertex.Labels(view);
  if (!maybe_labels) {
    memgraph::query::ThrowVertexLabelsReadFailure(maybe_labels.error());
  }

  return IsAuthorizedLabels(GetCachedLabelPermissions(), dba_, *maybe_labels, fine_grained_privilege);
}

bool FineGrainedAuthChecker::Has(const memgraph::query::EdgeAccessor &edge,
                                 const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const {
  if (HasUnrestrictedAccessToEdges()) return true;
  return IsAuthorizedEdgeType(GetCachedEdgePermissions(), dba_, edge.EdgeType(), fine_grained_privilege);
}

bool FineGrainedAuthChecker::Has(std::span<memgraph::storage::LabelId const> labels,
                                 memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const {
  if (HasUnrestrictedAccessToVertices()) return true;
  return IsAuthorizedLabels(GetCachedLabelPermissions(), dba_, labels, fine_grained_privilege);
}

bool FineGrainedAuthChecker::Has(const memgraph::storage::EdgeTypeId &edge_type,
                                 const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const {
  if (HasUnrestrictedAccessToEdges()) return true;
  return IsAuthorizedEdgeType(GetCachedEdgePermissions(), dba_, edge_type, fine_grained_privilege);
}

bool FineGrainedAuthChecker::HasGlobalPrivilegeOnVertices(
    const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return true;
  }
  return IsAuthorizedGloballyLabels(
      GetCachedLabelPermissions(),
      FineGrainedPrivilegeToFineGrainedPermission(fine_grained_privilege, FineGrainedPermissionType::LABEL));
}

bool FineGrainedAuthChecker::HasGlobalPrivilegeOnEdges(
    const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return true;
  }
  return IsAuthorizedGloballyEdges(
      GetCachedEdgePermissions(),
      FineGrainedPrivilegeToFineGrainedPermission(fine_grained_privilege, FineGrainedPermissionType::EDGE_TYPE));
}

bool FineGrainedAuthChecker::HasAllGlobalPrivilegesOnVertices() const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return true;
  }
  auto const &permissions = GetCachedLabelPermissions();
  auto const &global_grants = permissions.GetGlobalGrants();
  auto const &global_denies = permissions.GetGlobalDenies();
  return global_grants.has_value() &&
         static_cast<memgraph::auth::FineGrainedPermission>(global_grants.value()) ==
             memgraph::auth::kAllLabelPermissions &&
         !global_denies.has_value();
}

bool FineGrainedAuthChecker::HasAllGlobalPrivilegesOnEdges() const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return true;
  }
  auto const &permissions = GetCachedEdgePermissions();
  auto const &global_grants = permissions.GetGlobalGrants();
  auto const &global_denies = permissions.GetGlobalDenies();
  return global_grants.has_value() &&
         static_cast<memgraph::auth::FineGrainedPermission>(global_grants.value()) ==
             memgraph::auth::kAllEdgeTypePermissions &&
         !global_denies.has_value();
}

bool FineGrainedAuthChecker::HasUnrestrictedAccessToVertices() const {
  if (!unrestricted_vertices_) {
    auto const &permissions = GetCachedLabelPermissions();
    unrestricted_vertices_ = HasAllGlobalPrivilegesOnVertices() && permissions.GetRules().empty();
  }
  return *unrestricted_vertices_;
}

bool FineGrainedAuthChecker::HasUnrestrictedAccessToEdges() const {
  if (!unrestricted_edges_) {
    auto const &permissions = GetCachedEdgePermissions();
    unrestricted_edges_ = HasAllGlobalPrivilegesOnEdges() && permissions.GetRules().empty();
  }
  return *unrestricted_edges_;
}

bool FineGrainedAuthChecker::HasUnrestrictedAccessToVertexProperties() const {
  if (!unrestricted_vertex_properties_) {
    unrestricted_vertex_properties_ = GetCachedPropertyLabelPermissions().HasUnrestrictedAccess();
  }
  return *unrestricted_vertex_properties_;
}

bool FineGrainedAuthChecker::HasUnrestrictedAccessToEdgeTypeProperties() const {
  if (!unrestricted_edge_properties_) {
    unrestricted_edge_properties_ = GetCachedPropertyEdgeTypePermissions().HasUnrestrictedAccess();
  }
  return *unrestricted_edge_properties_;
}

bool FineGrainedAuthChecker::NeedsFineGrainedAuthChecker() const {
  auto const is_unconfigured = [](auth::PropertyAccessPermissions const &perms) {
    return perms.GetRules().empty() && perms.GetGlobalRules().empty();
  };
  auto const vertex_props_ok =
      HasUnrestrictedAccessToVertexProperties() || is_unconfigured(GetCachedPropertyLabelPermissions());
  auto const edge_props_ok =
      HasUnrestrictedAccessToEdgeTypeProperties() || is_unconfigured(GetCachedPropertyEdgeTypePermissions());
  return !(HasUnrestrictedAccessToVertices() && HasUnrestrictedAccessToEdges() && vertex_props_ok && edge_props_ok);
}

auth::PropertyAccessPermissions const &FineGrainedAuthChecker::GetCachedPropertyLabelPermissions() const {
  if (!cached_property_label_permissions_) {
    cached_property_label_permissions_ = std::visit(memgraph::utils::Overloaded{[this](auto const &user_or_role) {
                                                      return user_or_role.GetPropertyLabelPermissions(db_name_);
                                                    }},
                                                    user_or_role_);
  }
  return *cached_property_label_permissions_;
}

auth::PropertyAccessPermissions const &FineGrainedAuthChecker::GetCachedPropertyEdgeTypePermissions() const {
  if (!cached_property_edge_type_permissions_) {
    cached_property_edge_type_permissions_ = std::visit(memgraph::utils::Overloaded{[this](auto const &user_or_role) {
                                                          return user_or_role.GetPropertyEdgeTypePermissions(db_name_);
                                                        }},
                                                        user_or_role_);
  }
  return *cached_property_edge_type_permissions_;
}

bool FineGrainedAuthChecker::HasPropertyPermission(std::span<storage::LabelId const> labels,
                                                   storage::PropertyId property,
                                                   query::AuthQuery::PropertyPermissionType type) const {
  if (HasUnrestrictedAccessToVertexProperties()) return true;
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return true;
  }
  auto const perm_type = type == query::AuthQuery::PropertyPermissionType::WRITE ? auth::PropertyPermissionType::WRITE
                                                                                 : auth::PropertyPermissionType::READ;
  auto const &permissions = GetCachedPropertyLabelPermissions();
  auto const &prop_name = dba_->PropertyToName(property);
  if (labels.empty()) {
    return permissions.HasGlobal(prop_name, perm_type) == auth::PermissionLevel::GRANT;
  }
  std::vector<std::string> label_names;
  label_names.reserve(labels.size());
  for (auto label : labels) {
    label_names.push_back(dba_->LabelToName(label));
  }
  auto level = permissions.Has(label_names, prop_name, perm_type);
  return level == auth::PermissionLevel::GRANT;
}

bool FineGrainedAuthChecker::HasPropertyPermission(storage::EdgeTypeId const &edge_type, storage::PropertyId property,
                                                   query::AuthQuery::PropertyPermissionType type) const {
  if (HasUnrestrictedAccessToEdgeTypeProperties()) return true;
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return true;
  }
  auto const perm_type = type == query::AuthQuery::PropertyPermissionType::WRITE ? auth::PropertyPermissionType::WRITE
                                                                                 : auth::PropertyPermissionType::READ;
  auto const &permissions = GetCachedPropertyEdgeTypePermissions();
  auto const &edge_type_name = dba_->EdgeTypeToName(edge_type);
  auto const &prop_name = dba_->PropertyToName(property);
  auto level = permissions.Has(std::span{&edge_type_name, 1}, prop_name, perm_type);
  return level == auth::PermissionLevel::GRANT;
}

bool FineGrainedAuthChecker::HasAnyVertexPropertyRule() const {
  auto const &perms = GetCachedPropertyLabelPermissions();
  return !perms.GetRules().empty() || !perms.GetGlobalRules().empty();
}

bool FineGrainedAuthChecker::HasAnyEdgeTypePropertyRule() const {
  auto const &perms = GetCachedPropertyEdgeTypePermissions();
  return !perms.GetRules().empty() || !perms.GetGlobalRules().empty();
}

namespace {
bool AnyReadDenyInLabelPerms(auth::FineGrainedAccessPermissions const &perms) {
  const auto has_read_deny = [](auth::FineGrainedPermission bits) {
    return (static_cast<uint64_t>(bits) & static_cast<uint64_t>(auth::FineGrainedPermission::READ)) != 0U;
  };
  if (const auto &globals = perms.GetGlobalDenies();
      globals && (*globals & static_cast<uint64_t>(auth::FineGrainedPermission::READ)) != 0U) {
    return true;
  }
  return std::ranges::any_of(perms.GetRules(),
                             [&](auth::FineGrainedAccessRule const &rule) { return has_read_deny(rule.denies); });
}

bool AnyReadDenyInPropertyMap(std::string const &prop_name,
                              std::unordered_map<std::string, auth::PropertyPermission> const &props) {
  const auto has_read_deny = [](auth::PropertyPermission const &perm) {
    return (static_cast<uint8_t>(perm.denies) & static_cast<uint8_t>(auth::PropertyPermissionType::READ)) != 0U;
  };
  if (auto it = props.find(prop_name); it != props.end() && has_read_deny(it->second)) return true;
  // "*" wildcard entry with DENY applies to every property, including this one
  if (auto it = props.find("*"); it != props.end() && has_read_deny(it->second)) return true;
  return false;
}

bool AnyReadDenyForProperty(auth::PropertyAccessPermissions const &perms, std::string const &prop_name) {
  if (AnyReadDenyInPropertyMap(prop_name, perms.GetGlobalRules())) return true;
  return std::ranges::any_of(perms.GetRules(), [&](auth::PropertyAccessRule const &rule) {
    return AnyReadDenyInPropertyMap(prop_name, rule.properties);
  });
}
}  // namespace

bool FineGrainedAuthChecker::HasAnyVertexLabelDeny() const {
  return AnyReadDenyInLabelPerms(GetCachedLabelPermissions());
}

bool FineGrainedAuthChecker::HasAnyEdgeTypeDeny() const { return AnyReadDenyInLabelPerms(GetCachedEdgePermissions()); }

bool FineGrainedAuthChecker::HasVertexPropertyDeny(storage::PropertyId property) const {
  return AnyReadDenyForProperty(GetCachedPropertyLabelPermissions(), dba_->PropertyToName(property));
}

bool FineGrainedAuthChecker::HasEdgeTypePropertyDeny(storage::PropertyId property) const {
  return AnyReadDenyForProperty(GetCachedPropertyEdgeTypePermissions(), dba_->PropertyToName(property));
}

void FineGrainedAuthChecker::MakeThreadSafe() const { PopulateCachedPermissions(); }

bool FineGrainedAuthChecker::IsThreadSafe() const { return IsCachedPermissionsPopulated(); }

void FineGrainedAuthChecker::PopulateCachedPermissions() const {
  GetCachedLabelPermissions();
  GetCachedEdgePermissions();
  GetCachedPropertyLabelPermissions();
  GetCachedPropertyEdgeTypePermissions();
  HasUnrestrictedAccessToVertices();
  HasUnrestrictedAccessToEdges();
  HasUnrestrictedAccessToVertexProperties();
  HasUnrestrictedAccessToEdgeTypeProperties();
}

bool FineGrainedAuthChecker::IsCachedPermissionsPopulated() const {
  return cached_label_permissions_.has_value() && cached_edge_permissions_.has_value() &&
         cached_property_label_permissions_.has_value() && cached_property_edge_type_permissions_.has_value() &&
         unrestricted_vertices_.has_value() && unrestricted_edges_.has_value() &&
         unrestricted_vertex_properties_.has_value() && unrestricted_edge_properties_.has_value();
}
#endif
}  // namespace memgraph::glue
