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

#include <cstdint>

#include "utils/exceptions.hpp"

namespace memgraph::dbms {

#ifdef MG_ENTERPRISE
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

enum class RenameError : uint8_t {
  DEFAULT_DB,
  NON_EXISTENT,
  ALREADY_EXISTS,
  USING,
  FAIL,
  SAME_NAME,
};

enum class SuspendError : uint8_t {
  DEFAULT_DB,
  NON_EXISTENT,
  ALREADY_SUSPENDED,
  USING,
  FAIL,
};

enum class ResumeError : uint8_t {
  NON_EXISTENT,
  NOT_SUSPENDED,
  FAIL,
};

enum class RestartError : uint8_t {
  DEFAULT_DB,
  NON_EXISTENT,
  SUSPENDED,
  USING,
  FAIL,
};
#endif

/**
 * UnknownSession Exception
 *
 * Used to indicate that an unknown session was used.
 */
class UnknownSessionException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(UnknownSessionException)
};

/**
 * UnknownDatabase Exception
 *
 * Used to indicate that an unknown database was used.
 */
class UnknownDatabaseException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(UnknownDatabaseException)
};

/**
 * DatabaseInColdStorage Exception
 *
 * Used to indicate that a database is suspended (in cold storage) and must be
 * resumed before it can be used. Derives from UnknownDatabaseException so that
 * existing handlers which catch UnknownDatabaseException keep working.
 */
class DatabaseInColdStorageException : public UnknownDatabaseException {
 public:
  using UnknownDatabaseException::UnknownDatabaseException;
  SPECIALIZE_GET_EXCEPTION_NAME(DatabaseInColdStorageException)
};

}  // namespace memgraph::dbms
