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

#include "auth/models.hpp"

namespace memgraph::glue {

CoordinatorSSOAuthenticator::CoordinatorSSOAuthenticator(ModuleRunner module_runner,
                                                         RoleMaskProvider role_mask_provider)
    : module_runner_(std::move(module_runner)), role_mask_provider_(std::move(role_mask_provider)) {}

std::optional<uint64_t> CoordinatorSSOAuthenticator::Authenticate(std::string const &scheme,
                                                                  std::string const &identity_provider_response) const {
  auto const role_names = module_runner_(scheme, identity_provider_response);
  // Invalid token / module failure / malformed response / no roles returned -> reject.
  if (!role_names || role_names->empty()) {
    return std::nullopt;
  }

  std::vector<uint64_t> role_masks;
  role_masks.reserve(role_names->size());
  for (auto const &role_name : *role_names) {
    auto const mask = role_mask_provider_(role_name);
    // Every role returned by the module must exist in the coordinator's committed role set; a single missing role
    // rejects the whole authentication (a multi-role response succeeds only when all roles exist).
    if (!mask) {
      return std::nullopt;
    }
    role_masks.push_back(*mask);
  }

  // The effective mask is the union of the matched roles' masks.
  return auth::CoordinatorEffectiveMask(role_masks);
}

}  // namespace memgraph::glue

#endif
