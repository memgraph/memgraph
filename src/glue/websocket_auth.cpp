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

#include "glue/websocket_auth.hpp"

#include <string>

#include "dbms/constants.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::glue {

bool SafeAuth::Authenticate(const std::string &username, const std::string &password) const {
  user_or_role_ = auth::Authenticate(*auth_, username, password);
  return user_or_role_.has_value();
}

bool SafeAuth::HasWebsocketPermission() const {
  auto locked_auth = auth_->ReadLock();
  // Update if cache invalidated
  auto const epoch = locked_auth->CurrentEpoch();
  if (epoch != auth_epoch_ && user_or_role_) {
    bool success = true;
    std::visit(utils::Overloaded{[&](auth::User &user) {
                                   auto tmp = locked_auth->GetUser(user.username());
                                   if (tmp) {
                                     user = std::move(*tmp);
                                   } else {
                                     success = false;
                                   }
                                 },
                                 [&](auth::Roles &roles) {
                                   // Iterate through all roles and update each one
                                   auth::Roles updated_roles;
                                   for (const auto &role : roles) {
                                     auto tmp = locked_auth->GetRole(role.rolename());
                                     if (tmp) {
                                       updated_roles.AddRole(*tmp);
                                     } else {
                                       success = false;
                                       break;
                                     }
                                   }
                                   if (success) {
                                     roles = std::move(updated_roles);
                                   }
                                 }},
               *user_or_role_);
    // Missing user/role; delete from cache
    if (!success) user_or_role_.reset();
  }
  auth_epoch_ = epoch;  // after the refresh: a throw above leaves it stale, so the next call retries
  // Check permissions
  if (user_or_role_) {
    return std::visit(utils::Overloaded{[&](auto &user_or_role) {
                        // Main could be deleted, so we will connect without a db; this is only used by websocket on
                        // connect, so not critical (we will not be able to access any db)
                        const auto &db_name =
#ifdef MG_ENTERPRISE
                            user_or_role.GetMain();
#else
                            dbms::kDefaultDB;
#endif
                        return user_or_role.GetPermissions(db_name).Has(auth::Permission::WEBSOCKET) ==
                               auth::PermissionLevel::GRANT;
                      }},
                      *user_or_role_);
  }
  // NOTE: websocket authenticates only if there is a user, so no need to check if access controlled
  return false;
}

bool SafeAuth::AccessControlled() const { return auth_->ReadLock()->AccessControlled(); }

}  // namespace memgraph::glue
