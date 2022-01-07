// Copyright 2021 Memgraph Ltd.
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

namespace communication::websocket {

bool SafeAuth::Authenticate(const std::string &username, const std::string &password) {
  std::optional<auth::User> maybe_user;
  {
    // TODO change lock to readlock, auth now creates user...
    auto locked_auth = auth_->Lock();
    maybe_user = locked_auth->Authenticate(username, password);
  }
  return maybe_user.has_value();
}
}  // namespace communication::websocket