// Copyright 2024 Memgraph Ltd.
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
  // TODO
  // auth_->UpToDate()...
  if (user_or_role_) {
    return std::visit(
        utils::Overloaded{
            [&](auth::User &user) { return user.GetPermissions().Has(permission) == auth::PermissionLevel::GRANT; },
            [&](auth::Role &role) { return role.permissions().Has(permission) == auth::PermissionLevel::GRANT; }},
        *user_or_role_);
  }
  return false;
}

bool SafeAuth::AccessControlled() const { return auth_->ReadLock()->AccessControlled(); }
}  // namespace memgraph::communication::websocket
