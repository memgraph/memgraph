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

}  // namespace memgraph::dbms
