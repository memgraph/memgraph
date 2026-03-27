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

#include <fmt/core.h>
#include <fmt/ranges.h>

#include <span>
#include <string>

#include "utils/exceptions.hpp"

namespace memgraph::rpc {

inline auto RpcTimeoutMsg(std::span<const std::string> replica_names, bool transaction_aborted) -> std::string {
  return fmt::format(
      "Main reached an RPC timeout while replicating to [{}]. {} One possible "
      "reason for this error is that the replica is down and in that case make sure to recover "
      "it. If all of your replicas "
      "are up and running normally, then please try setting a smaller parameter value for "
      "'deltas_batch_progress_size' using 'SET COORDINATOR SETTING' query on the coordinator.",
      fmt::join(replica_names, ", "),
      transaction_aborted
          ? "The transaction has been aborted on all instances."
          : "The transaction has been committed on the main and will eventually be replicated to replicas that "
            "didn't accept the transaction.");
}

/// Exception that is thrown whenever a RPC call fails.
/// This exception inherits `std::exception` directly because
/// `utils::BasicException` is used for transient errors that should be reported
/// to the user and `utils::StacktraceException` is used for fatal errors.
/// This exception always requires explicit handling.
class RpcFailedException : public utils::BasicException {
 public:
  explicit RpcFailedException(std::string_view const msg) : utils::BasicException(msg) {}

  SPECIALIZE_GET_EXCEPTION_NAME(RpcFailedException);
};

class RpcFailedToConnectException final : public RpcFailedException {
 public:
  RpcFailedToConnectException() : RpcFailedException("Failed to establish socket connection") {}

  SPECIALIZE_GET_EXCEPTION_NAME(RpcFailedToConnectException);
};

class RpcTimeoutException final : public RpcFailedException {
 public:
  RpcTimeoutException() : RpcFailedException("Timeout occurred during RPC calls") {}

  SPECIALIZE_GET_EXCEPTION_NAME(RpcTimeoutException);
};

class UnsupportedRpcVersionException final : public RpcFailedException {
 public:
  UnsupportedRpcVersionException()
      : RpcFailedException(
            "Couldn't communicate with the cluster! RPC protocol version not supported. "
            "Please contact your database administrator.") {}

  SPECIALIZE_GET_EXCEPTION_NAME(UnsupportedRpcVersionException);
};

class GenericRpcFailedException final : public RpcFailedException {
 public:
  GenericRpcFailedException()
      : RpcFailedException(
            "Couldn't communicate with the cluster! Please contact your "
            "database administrator.") {}

  SPECIALIZE_GET_EXCEPTION_NAME(GenericRpcFailedException);
};

class SlkRpcFailedException final : public RpcFailedException {
 public:
  SlkRpcFailedException()
      : RpcFailedException(
            "Received malformed message from cluster. Please raise an issue on Memgraph GitHub issues.") {}

  SPECIALIZE_GET_EXCEPTION_NAME(SlkRpcFailedException);
};

class FailedToGetRpcStreamException final : public RpcFailedException {
 public:
  FailedToGetRpcStreamException() : RpcFailedException("Failed to get RPC stream by try-locking.") {}

  SPECIALIZE_GET_EXCEPTION_NAME(FailedToGetRpcStreamException);
};

}  // namespace memgraph::rpc
