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
CoordinatorSSOAuthenticator::ModuleRunner ModuleReturning(std::optional<std::vector<std::string>> roles) {
  return [roles = std::move(roles)](std::string const & /*scheme*/, std::string const & /*response*/) { return roles; };
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
  auto const mask = authenticator.Authenticate("oidc", "token");
  ASSERT_TRUE(mask.has_value());
  EXPECT_EQ(*mask, kRead);
}

TEST(CoordinatorSSOAuthenticator, MultiRoleYieldsUnionOfMasks) {
  CoordinatorSSOAuthenticator authenticator{ModuleReturning(std::vector<std::string>{"reader", "writer"}),
                                            RolesFrom({{"reader", kRead}, {"writer", kWrite}})};
  auto const mask = authenticator.Authenticate("saml", "token");
  ASSERT_TRUE(mask.has_value());
  EXPECT_EQ(*mask, kRead | kWrite);
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

TEST(CoordinatorSSOAuthenticator, BareRoleAcceptedWithZeroMask) {
  // A role that exists but carries no grant authenticates with an all-denying (zero) effective mask.
  CoordinatorSSOAuthenticator authenticator{ModuleReturning(std::vector<std::string>{"bare"}),
                                            RolesFrom({{"bare", 0}})};
  auto const mask = authenticator.Authenticate("kerberos", "token");
  ASSERT_TRUE(mask.has_value());
  EXPECT_EQ(*mask, 0U);
}

TEST(CoordinatorSSOAuthenticator, WriteRoleSatisfiesReadRequirement) {
  CoordinatorSSOAuthenticator authenticator{ModuleReturning(std::vector<std::string>{"writer"}),
                                            RolesFrom({{"writer", kWrite}})};
  auto const mask = authenticator.Authenticate("oidc", "token");
  ASSERT_TRUE(mask.has_value());
  EXPECT_TRUE(auth::CoordinatorMaskSatisfies(*mask, auth::Permission::COORDINATOR_READ));
  EXPECT_TRUE(auth::CoordinatorMaskSatisfies(*mask, auth::Permission::COORDINATOR_WRITE));
}

#endif
