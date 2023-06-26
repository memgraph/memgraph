// Copyright 2023 Memgraph Ltd.
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

#if 0 /* Disabled for now; we moved back to a single auth source */

#include <filesystem>
#include <memory>
#include <optional>
#include <unordered_map>

#include "auth/auth.hpp"
#include "global.hpp"
#include "glue/auth_checker.hpp"
#include "glue/auth_handler.hpp"
#include "utils/result.hpp"
#include "utils/rw_lock.hpp"
#include "utils/sync_ptr.hpp"

#include "handler.hpp"

namespace memgraph::dbms {

struct AuthContext {
  explicit AuthContext(const std::filesystem::path &data_directory, const std::string &ah_flag = "")
      : auth(data_directory / "auth"), auth_handler(&auth, ah_flag), auth_checker(&auth) {}

  utils::Synchronized<auth::Auth, utils::WritePrioritizedRWLock> auth;
  glue::AuthQueryHandler auth_handler;
  glue::AuthChecker auth_checker;
};

struct AuthConfig {
  std::filesystem::path storage_dir;
  std::string ah_flag;
};

class AuthContextHandler : public Handler<AuthContext, AuthConfig> {
 public:
  using HandlerT = Handler<AuthContext, AuthConfig>;

  typename HandlerT::NewResult New(const std::string &name, const std::filesystem::path &data_directory,
                                   const std::string &ah_flag = "") {
    // Check if compatible with the existing auth
    if (std::any_of(cbegin(), cend(),
                    [&](const auto &elem) { return elem.second.config().storage_dir == data_directory; })) {
      // LOG
      return NewError::EXISTS;
    }
    return HandlerT::New(name, std::forward_as_tuple(data_directory, ah_flag),
                         std::forward_as_tuple(data_directory, ah_flag));
  }
};

}  // namespace memgraph::dbms

#endif
