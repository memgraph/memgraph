// Copyright 2025 Memgraph Ltd.
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

#include "io/network/endpoint.hpp"
#include "utils/exceptions.hpp"

namespace memgraph::rpc {

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
