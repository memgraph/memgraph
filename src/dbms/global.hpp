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
 * UnknownUser Exception
 *
 * Used to indicate that an unknown user was used.
 */
class UnknownUser : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
};

/**
 * UnknownSession Exception
 *
 * Used to indicate that an unknown session was used.
 */
class UnknownSession : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
};

/**
 * UnknownDatabase Exception
 *
 * Used to indicate that an unknown database was used.
 */
class UnknownDatabase : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
};

#ifdef MG_ENTERPRISE
/**
 * @brief Session interface used by the DBMS to handle the the active sessions.
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
  virtual std::string GetID() const = 0;

  /**
   * @brief Gets called on database change.
   *
   * @return true on success
   */
  virtual dbms::SetForResult OnChange(const std::string &) = 0;

  /**
   * @brief Gets called on database delete (drop).
   *
   * @return true on success
   */
  virtual bool OnDelete(const std::string &) = 0;
};
#endif

}  // namespace memgraph::dbms
