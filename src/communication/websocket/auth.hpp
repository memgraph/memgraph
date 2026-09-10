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

#include <string>

namespace memgraph::communication::websocket {

// What a websocket session needs of whoever holds the users, stated so that the session does not
// have to name the permission vocabulary it is asking about.
class AuthenticationInterface {
 public:
  virtual bool Authenticate(const std::string &username, const std::string &password) const = 0;

  // Whether the authenticated party may use the websocket at all, which is the only question a
  // session asks.
  virtual bool HasWebsocketPermission() const = 0;

  virtual bool AccessControlled() const = 0;

  virtual ~AuthenticationInterface() = default;
};

}  // namespace memgraph::communication::websocket
