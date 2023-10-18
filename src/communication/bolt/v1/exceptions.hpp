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

/// @file

#pragma once

#include <fmt/format.h>

#include "utils/exceptions.hpp"

namespace memgraph::communication::bolt {

/**
 * Used to indicate something is wrong with the client but the transaction is
 * kept open for a potential retry.
 *
 * The most common use case for throwing this error is if something is wrong
 * with the query. Perhaps a simple syntax error that can be fixed and query
 * retried.
 */
class ClientError : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(ClientError)
};

/**
 * All exceptions that are sent to the client consist of two parts. The first
 * part is `code` it specifies the type of exception that was raised. The second
 * part is `message` which is a pretty error message that can be displayed to a
 * human.
 *
 * The first part is more interesting for machine processing. It should have the
 * following format: `Memgraph.[Classification].[Category].[Title]` It is
 * defined here: https://neo4j.com/docs/status-codes/current/
 *
 * The first part of the `code` is always Memgraph, it indicates the database.
 * The second part is `classification`. The `classification` part is specified
 * by Neo to always be one of "ClientError", "ClientNotification",
 * "TransientError" or "DatabaseError". We won't use "ClientNotification".
 *
 * The `category` and `title` parts can be freely specified.
 */
class VerboseError : public utils::BasicException {
 public:
  enum class Classification {
    // Client and Database errors mean that the client shouldn't retry the
    // query.
    CLIENT_ERROR,
    DATABASE_ERROR,
    // Transient error means that the client should retry the query.
    TRANSIENT_ERROR,
  };

  template <class... Args>
  VerboseError(Classification classification, const std::string &category, const std::string &title,
               const std::string &format, Args &&...args)
      : BasicException(format, std::forward<Args>(args)...),
        code_(fmt::format("Memgraph.{}.{}.{}", ClassificationToString(classification), category, title)) {}

  const std::string &code() const noexcept { return code_; }
  SPECIALIZE_GET_EXCEPTION_NAME(VerboseError)

 private:
  std::string ClassificationToString(Classification classification) {
    switch (classification) {
      case Classification::CLIENT_ERROR:
        return "ClientError";
      case Classification::DATABASE_ERROR:
        return "DatabaseError";
      case Classification::TRANSIENT_ERROR:
        return "TransientError";
    }
  }

  std::string code_;
};

}  // namespace memgraph::communication::bolt
