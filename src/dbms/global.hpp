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

#include <concepts>
#include <cstdint>
#include <string>

#include "utils/exceptions.hpp"

namespace memgraph::dbms {

enum class DeleteError : uint8_t {
  DEFAULT_DB,
  USING,
  NON_EXISTENT,
  FAIL,
  DISK_FAIL,
};

enum class NewError : uint8_t {
  NO_CONFIGS,
  EXISTS,
  DEFUNCT,
  GENERIC,
};

enum class SetForResult : uint8_t {
  SUCCESS,
  ALREADY_SET,
  FAIL,
};

/**
 * UnknownSession Exception
 *
 * Used to indicate that an unknown session was used.
 */
class UnknownSessionException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
};

/**
 * UnknownDatabase Exception
 *
 * Used to indicate that an unknown database was used.
 */
class UnknownDatabaseException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
};

/**
 * @brief Session interface used by the DBMS to handle the the active sessions.
 * @todo Try to remove this dependency from SessionContextHandler. OnDelete could be removed, as it only does an assert.
 * OnChange could be removed if SetFor returned the pointer and the called then handled the OnChange execution.
 * However, the interface is very useful to  decouple the interpreter's query execution and the sessions themselves.
 */
class SessionInterface {
 public:
  SessionInterface() = default;
  virtual ~SessionInterface() = default;

  SessionInterface(const SessionInterface &) = default;
  SessionInterface &operator=(const SessionInterface &) = default;
  SessionInterface(SessionInterface &&) noexcept = default;
  SessionInterface &operator=(SessionInterface &&) noexcept = default;

  /**
   * @brief Return the unique string identifying the session.
   *
   * @return std::string
   */
  virtual std::string UUID() const = 0;

  /**
   * @brief Return the currently active database.
   *
   * @return std::string
   */
  virtual std::string GetDatabaseName() const = 0;

#ifdef MG_ENTERPRISE
  /**
   * @brief Gets called on database change.
   *
   * @return SetForResult enum (SUCCESS, ALREADY_SET or FAIL)
   */
  virtual dbms::SetForResult OnChange(const std::string &) = 0;

  /**
   * @brief Callback that gets called on database delete (drop).
   *
   * @return true on success
   */
  virtual bool OnDelete(const std::string &) = 0;
#endif
};

}  // namespace memgraph::dbms
