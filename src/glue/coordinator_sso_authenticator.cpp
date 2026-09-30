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

#include "glue/coordinator_sso_authenticator.hpp"

#ifdef MG_ENTERPRISE

#include <utility>

#include <spdlog/spdlog.h>

#include "auth/models.hpp"
#include "utils/join_vector.hpp"

namespace memgraph::glue {

CoordinatorSSOAuthenticator::CoordinatorSSOAuthenticator(ModuleRunner module_runner,
                                                         RoleMaskProvider role_mask_provider)
    : module_runner_(std::move(module_runner)), role_mask_provider_(std::move(role_mask_provider)) {}

std::expected<CoordinatorSSOAuthenticator::AuthResult, SSORejection> CoordinatorSSOAuthenticator::Authenticate(
    std::string const &scheme, std::string const &identity_provider_response) const {
  auto identity = module_runner_(scheme, identity_provider_response);
  // Invalid token / module failure / malformed response / missing license -> reject.
  if (!identity) {
    return std::unexpected{SSORejection::kModuleFailed};
  }
  if (identity->roles.empty()) {
    return std::unexpected{SSORejection::kNoRolesReturned};
  }

  std::vector<uint64_t> role_masks;
  role_masks.reserve(identity->roles.size());
  for (auto const &role_name : identity->roles) {
    auto const mask = role_mask_provider_(role_name);
    // Every role returned by the module must exist in the coordinator's committed role set; a single missing role
    // rejects the whole authentication (a multi-role response succeeds only when all roles exist). The name goes to the
    // log, not to the client, so a rejected login can't be used to enumerate the coordinator's role set.
    if (!mask) {
      spdlog::warn(
          "Rejecting coordinator SSO login for scheme '{}': the module returned role '{}', which is not in the "
          "coordinator's role set.",
          scheme,
          role_name);
      return std::unexpected{SSORejection::kUnknownRole};
    }
    role_masks.push_back(*mask);
  }

  // The effective mask is the union of the matched roles' masks.
  auto const effective_mask = auth::CoordinatorEffectiveMask(role_masks);
  // A session that would hold neither COORDINATOR_READ nor COORDINATOR_WRITE cannot run any coordinator query, so
  // reject the login rather than admitting a session that is denied everything.
  if (!auth::CoordinatorMaskSatisfies(effective_mask, auth::Permission::COORDINATOR_READ)) {
    spdlog::warn(
        "Rejecting coordinator SSO login for scheme '{}': role(s) {} exist but grant no coordinator privilege. Grant "
        "COORDINATOR_READ or COORDINATOR_WRITE to one of them.",
        scheme,
        utils::JoinVector(identity->roles, ", "));
    return std::unexpected{SSORejection::kRoleWithoutPrivilege};
  }
  return AuthResult{
      .effective_mask = effective_mask, .roles = std::move(identity->roles), .username = std::move(identity->username)};
}

}  // namespace memgraph::glue

#endif
