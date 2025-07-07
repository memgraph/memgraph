// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "communication/websocket/auth.hpp"

#include <string>
#include "utils/variant_helpers.hpp"

namespace memgraph::communication::websocket {

bool SafeAuth::Authenticate(const std::string &username, const std::string &password) const {
  user_or_role_ = auth_->Lock()->Authenticate(username, password);
  return user_or_role_.has_value();
}

bool SafeAuth::HasPermission(const auth::Permission permission) const {
  auto locked_auth = auth_->ReadLock();
  // Update if cache invalidated
  if (!locked_auth->UpToDate(auth_epoch_) && user_or_role_) {
    bool success = true;
    std::visit(utils::Overloaded{[&](auth::User &user) {
                                   auto tmp = locked_auth->GetUser(user.username());
                                   if (!tmp) success = false;
                                   user = std::move(*tmp);
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
  // Check permissions
  if (user_or_role_) {
    return std::visit(utils::Overloaded{[&](auto &user_or_role) {
                        // Main could be deleted, so we will connect without a db; this is only used by websocket on
                        // connect, so not critical (we will not be able to access any db)
                        const auto &db_name = user_or_role.GetMain();
                        return user_or_role.GetPermissions(db_name).Has(permission) == auth::PermissionLevel::GRANT;
                      }},
                      *user_or_role_);
  }
  // NOTE: websocket authenticates only if there is a user, so no need to check if access controlled
  return false;
}

bool SafeAuth::AccessControlled() const { return auth_->ReadLock()->AccessControlled(); }
}  // namespace memgraph::communication::websocket
