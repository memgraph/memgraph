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

#ifdef MG_ENTERPRISE

#include <cstdint>
#include <expected>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "auth/auth.hpp"

namespace memgraph::glue {

// Why a coordinator SSO login was rejected. Kept distinct because the remedy differs per case and they are not
// interchangeable during a rollout: a module/token failure is fixed on the identity-provider side, while the three
// role-related cases are fixed on the coordinator (map the group to a role, create the role, grant it a privilege).
enum class SSORejection : uint8_t {
  // Module error, invalid/expired token, malformed module response, or a missing enterprise license.
  kModuleFailed,
  // The module authenticated the identity but reported no roles for it.
  kNoRolesReturned,
  // A role the module reported is not in the coordinator's committed role set.
  kUnknownRole,
  // The reported roles all exist but their union grants neither COORDINATOR_READ nor COORDINATOR_WRITE, so the session
  // could not run a single coordinator query. Typically a group mapped to a role that was never granted anything.
  kRoleWithoutPrivilege,
};

// Coordinator SSO authenticator: a deep module that authenticates an SSO connection to a coordinator against the
// coordinator's committed role set and computes the session's effective coordinator privilege mask.
//
// It reuses the existing auth-module subprocess machinery to run the identity-provider module (injected as the module
// runner), but -- unlike the data-instance path -- validates the returned roles against the coordinator's
// Raft-replicated role set (injected as the role/mask provider) rather than the auth kvstore.
//
// Shaped as a testable unit: both dependencies are injected, so the accept/reject decision and the resulting effective
// mask can be exercised in isolation with a fake module runner and a fake role/mask provider.
class CoordinatorSSOAuthenticator {
 public:
  // Runs the SSO module for `scheme` with the identity-provider `response` and returns the identity it reports on
  // success. Returns nullopt on any module/authentication/parse failure (including a missing enterprise license).
  using ModuleRunner =
      std::function<std::optional<auth::SSOIdentity>(std::string const &scheme, std::string const &response)>;

  // Returns the coordinator role's privilege mask if `role_name` exists in the committed role set, nullopt otherwise.
  using RoleMaskProvider = std::function<std::optional<uint64_t>(std::string const &role_name)>;

  // Outcome of a successful authentication: the session's effective privilege mask, the role names the session
  // authenticated with (carried so SHOW CURRENT ROLE can report them), and the principal the module authenticated
  // (carried so the session can record who ran a query; empty if the module reported no username).
  struct AuthResult {
    uint64_t effective_mask;
    std::vector<std::string> roles;
    std::string username;
  };

  CoordinatorSSOAuthenticator(ModuleRunner module_runner, RoleMaskProvider role_mask_provider);

  // Authenticates the SSO connection. On success returns the session's effective privilege mask (the union of the
  // matched roles' masks) together with the matched role names and the principal; on rejection returns which of the
  // SSORejection cases applied, so the caller can tell the operator what to fix.
  std::expected<AuthResult, SSORejection> Authenticate(std::string const &scheme,
                                                       std::string const &identity_provider_response) const;

 private:
  ModuleRunner module_runner_;
  RoleMaskProvider role_mask_provider_;
};

}  // namespace memgraph::glue

#endif
