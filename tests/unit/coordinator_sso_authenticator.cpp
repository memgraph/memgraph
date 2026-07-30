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

#include <gtest/gtest.h>

#ifdef MG_ENTERPRISE

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "auth/models.hpp"
#include "glue/coordinator_sso_authenticator.hpp"

using memgraph::glue::CoordinatorSSOAuthenticator;
namespace auth = memgraph::auth;

namespace {
constexpr uint64_t kRead = static_cast<uint64_t>(auth::Permission::COORDINATOR_READ);
constexpr uint64_t kWrite = static_cast<uint64_t>(auth::Permission::COORDINATOR_WRITE);

// A module runner that always returns the given (possibly empty / absent) role set, ignoring scheme + response.
CoordinatorSSOAuthenticator::ModuleRunner ModuleReturning(std::optional<std::vector<std::string>> roles,
                                                          std::string username = "alice") {
  std::optional<auth::SSOIdentity> identity;
  if (roles) {
    identity = auth::SSOIdentity{.username = std::move(username), .roles = std::move(*roles)};
  }
  return [identity = std::move(identity)](std::string const & /*scheme*/, std::string const & /*response*/) {
    return identity;
  };
}

// A role/mask provider backed by a fixed name->mask map (a role absent from the map "doesn't exist").
CoordinatorSSOAuthenticator::RoleMaskProvider RolesFrom(std::map<std::string, uint64_t> masks) {
  return [masks = std::move(masks)](std::string const &name) -> std::optional<uint64_t> {
    auto const it = masks.find(name);
    if (it == masks.end()) {
      return std::nullopt;
    }
    return it->second;
  };
}
}  // namespace

TEST(CoordinatorSSOAuthenticator, SingleRoleSuccessYieldsItsMask) {
  CoordinatorSSOAuthenticator authenticator{ModuleReturning(std::vector<std::string>{"reader"}),
                                            RolesFrom({{"reader", kRead}})};
  auto const result = authenticator.Authenticate("oidc", "token");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->effective_mask, kRead);
  EXPECT_EQ(result->roles, (std::vector<std::string>{"reader"}));
}

TEST(CoordinatorSSOAuthenticator, PrincipalIsCarriedThroughToTheResult) {
  // The module's username is what attributes a control-plane query to a person: a coordinator session has no
  // QueryUserOrRole to derive it from, so losing it here leaves the session unattributed.
  CoordinatorSSOAuthenticator authenticator{ModuleReturning(std::vector<std::string>{"reader"}, "alice@example.com"),
                                            RolesFrom({{"reader", kRead}})};
  auto const result = authenticator.Authenticate("oidc", "token");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->username, "alice@example.com");
}

TEST(CoordinatorSSOAuthenticator, MissingPrincipalStillAuthenticates) {
  // A module that reports no username authorizes normally (the coordinator authorizes by role); the session is just
  // left unattributed rather than rejected.
  CoordinatorSSOAuthenticator authenticator{ModuleReturning(std::vector<std::string>{"reader"}, ""),
                                            RolesFrom({{"reader", kRead}})};
  auto const result = authenticator.Authenticate("oidc", "token");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->effective_mask, kRead);
  EXPECT_TRUE(result->username.empty());
}

TEST(CoordinatorSSOAuthenticator, MultiRoleYieldsUnionOfMasks) {
  CoordinatorSSOAuthenticator authenticator{ModuleReturning(std::vector<std::string>{"reader", "writer"}),
                                            RolesFrom({{"reader", kRead}, {"writer", kWrite}})};
  auto const result = authenticator.Authenticate("saml", "token");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->effective_mask, kRead | kWrite);
  EXPECT_EQ(result->roles, (std::vector<std::string>{"reader", "writer"}));
}

TEST(CoordinatorSSOAuthenticator, InvalidTokenRejected) {
  // The module runner returns nullopt (bad token / module failure / malformed response).
  CoordinatorSSOAuthenticator authenticator{ModuleReturning(std::nullopt), RolesFrom({{"reader", kRead}})};
  EXPECT_FALSE(authenticator.Authenticate("oidc", "bad").has_value());
}

TEST(CoordinatorSSOAuthenticator, EmptyRoleSetRejected) {
  CoordinatorSSOAuthenticator authenticator{ModuleReturning(std::vector<std::string>{}),
                                            RolesFrom({{"reader", kRead}})};
  EXPECT_FALSE(authenticator.Authenticate("oidc", "token").has_value());
}

TEST(CoordinatorSSOAuthenticator, MissingRoleRejectsWholeLogin) {
  // "ghost" is not in the committed role set -> the multi-role login is rejected even though "reader" exists.
  CoordinatorSSOAuthenticator authenticator{ModuleReturning(std::vector<std::string>{"reader", "ghost"}),
                                            RolesFrom({{"reader", kRead}})};
  EXPECT_FALSE(authenticator.Authenticate("kerberos", "token").has_value());
}

TEST(CoordinatorSSOAuthenticator, BareRoleRejected) {
  // A role that exists but carries no grant would yield a zero (all-denying) mask; since such a session could not run
  // any coordinator query, the login is rejected outright.
  CoordinatorSSOAuthenticator authenticator{ModuleReturning(std::vector<std::string>{"bare"}),
                                            RolesFrom({{"bare", 0}})};
  EXPECT_FALSE(authenticator.Authenticate("kerberos", "token").has_value());
}

TEST(CoordinatorSSOAuthenticator, MultiRoleAllBareRejected) {
  // Even across multiple roles, if the union grants neither COORDINATOR_READ nor COORDINATOR_WRITE the login fails.
  CoordinatorSSOAuthenticator authenticator{ModuleReturning(std::vector<std::string>{"bare1", "bare2"}),
                                            RolesFrom({{"bare1", 0}, {"bare2", 0}})};
  EXPECT_FALSE(authenticator.Authenticate("oidc", "token").has_value());
}

TEST(CoordinatorSSOAuthenticator, WriteRoleSatisfiesReadRequirement) {
  CoordinatorSSOAuthenticator authenticator{ModuleReturning(std::vector<std::string>{"writer"}),
                                            RolesFrom({{"writer", kWrite}})};
  auto const result = authenticator.Authenticate("oidc", "token");
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(auth::CoordinatorMaskSatisfies(result->effective_mask, auth::Permission::COORDINATOR_READ));
  EXPECT_TRUE(auth::CoordinatorMaskSatisfies(result->effective_mask, auth::Permission::COORDINATOR_WRITE));
}

#endif
