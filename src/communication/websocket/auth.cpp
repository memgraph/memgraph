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

namespace memgraph::communication::websocket {

bool SafeAuth::Authenticate(const std::string &username, const std::string &password) const {
  return auth_->Lock()->Authenticate(username, password).has_value();
}

bool SafeAuth::HasUserPermission(const std::string &username, const auth::Permission permission) const {
  if (const auto user = auth_->ReadLock()->GetUser(username); user) {
    return user->GetPermissions().Has(permission) == auth::PermissionLevel::GRANT;
  }
  return false;
}

bool SafeAuth::AccessControlled() const { return auth_->ReadLock()->AccessControlled(); }
}  // namespace memgraph::communication::websocket
