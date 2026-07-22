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
#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace memgraph::glue {

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
  // Runs the SSO module for `scheme` with the identity-provider `response` and returns the role names it reports on
  // success. Returns nullopt on any module/authentication/parse failure (including a missing enterprise license).
  using ModuleRunner =
      std::function<std::optional<std::vector<std::string>>(std::string const &scheme, std::string const &response)>;

  // Returns the coordinator role's privilege mask if `role_name` exists in the committed role set, nullopt otherwise.
  using RoleMaskProvider = std::function<std::optional<uint64_t>(std::string const &role_name)>;

  // Outcome of a successful authentication: the session's effective privilege mask and the role names the session
  // authenticated with (carried so SHOW CURRENT ROLE can report them).
  struct AuthResult {
    uint64_t effective_mask;
    std::vector<std::string> roles;
  };

  CoordinatorSSOAuthenticator(ModuleRunner module_runner, RoleMaskProvider role_mask_provider);

  // Authenticates the SSO connection. On success returns the session's effective privilege mask (the union of the
  // matched roles' masks) together with the matched role names, or nullopt on rejection. Rejection cases: the module
  // fails / returns an invalid token, the module returns no roles, or any returned role does not exist in the committed
  // role set or role exists but without any privilege
  std::optional<AuthResult> Authenticate(std::string const &scheme,
                                         std::string const &identity_provider_response) const;

 private:
  ModuleRunner module_runner_;
  RoleMaskProvider role_mask_provider_;
};

}  // namespace memgraph::glue

#endif
