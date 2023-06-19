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

namespace memgraph::dbms {

/**
 * @brief Multi-tenancy handler of authentication context.
 */
class AuthHandler {
 public:
  using ContextT = auth::Auth;
  using LockT = utils::WritePrioritizedRWLock;

  struct AuthContext {
    explicit AuthContext(const std::filesystem::path &data_directory, const std::string &ah_flags = "")
        : auth(data_directory / "auth"), auth_handler(&auth, ah_flags), auth_checker(&auth) {}

    utils::Synchronized<ContextT, LockT> auth;
    glue::AuthQueryHandler auth_handler;
    glue::AuthChecker auth_checker;
  };

  using NewResult = utils::BasicResult<NewError, std::shared_ptr<AuthContext>>;

  AuthHandler() = default;

  /**
   * @brief Create a new authentication context associated to the "name" database.
   *
   * @param name name of the database
   * @param data_directory underlying RocksDB directory
   * @param config glue::AuthHandler setup flags
   * @return NewResult pointer to context on success, error on failure
   */
  NewResult New(std::string_view name, const std::filesystem::path &data_directory, const std::string &ah_flags) {
    auto [itr, success] = auth_.emplace(std::piecewise_construct, std::forward_as_tuple(name),
                                        std::forward_as_tuple(0, data_directory, ah_flags));
    if (success) {
      return itr->second.ptr_;
    }
    return NewError::EXISTS;
  }

  /**
   * @brief Get pointer to the authentication context associated with "name" database.
   *
   * @param name name of the database
   * @return std::optional<std::shared_ptr<AuthContext>> empty on error
   */
  std::optional<std::shared_ptr<AuthContext>> Get(const std::string &name) {
    if (auto search = auth_.find(name); search != auth_.end()) {
      return search->second.ptr_;
    }
    return {};
  }

  /**
   * @brief Delete the authentication context associated with the "name" database
   *
   * @param name name of the database
   * @return true on success
   */
  bool Delete(const std::string &name) {
    if (auto itr = auth_.find(name); itr != auth_.end()) {
      auth_.erase(itr);
      return true;
    }
    return false;
  }

 private:
  std::unordered_map<std::string, utils::SyncPtr<AuthContext, int>> auth_;  //!< map to all active interpreters
};

}  // namespace memgraph::dbms
