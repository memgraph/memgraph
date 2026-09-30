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

#include <optional>
#include <string>

#include "auth/auth.hpp"
#include "communication/websocket/auth.hpp"

namespace memgraph::glue {

// Answers a websocket session's questions out of the user store, caching the authenticated party
// and refreshing it when the store's epoch moves on.
class SafeAuth : public communication::websocket::AuthenticationInterface {
 public:
  explicit SafeAuth(auth::SynchedAuth *auth) : auth_{auth} {}

  bool Authenticate(const std::string &username, const std::string &password) const override;

  bool HasWebsocketPermission() const override;

  bool AccessControlled() const override;

 private:
  auth::SynchedAuth *auth_;
  mutable std::optional<auth::UserOrRole> user_or_role_;
  mutable auth::Auth::Epoch auth_epoch_{};
};

}  // namespace memgraph::glue
