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

#pragma once

#include <string>

#include "auth/auth.hpp"

namespace memgraph::communication::websocket {

class AuthenticationInterface {
 public:
  virtual bool Authenticate(const std::string &username, const std::string &password) const = 0;

  virtual bool HasUserPermission(const std::string &username, auth::Permission permission) const = 0;

  virtual bool HasAnyUsers() const = 0;
};

class SafeAuth : public AuthenticationInterface {
 public:
  explicit SafeAuth(auth::SynchedAuth *auth) : auth_{auth} {}

  bool Authenticate(const std::string &username, const std::string &password) const override;

  bool HasUserPermission(const std::string &username, auth::Permission permission) const override;

  bool HasAnyUsers() const override;

 private:
  auth::SynchedAuth *auth_;
};
}  // namespace memgraph::communication::websocket
