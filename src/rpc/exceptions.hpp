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
  RpcFailedException(std::string_view msg) : utils::BasicException(msg) {}
  SPECIALIZE_GET_EXCEPTION_NAME(RpcFailedException);
};

class VersionMismatchRpcFailedException : public RpcFailedException {
 public:
  VersionMismatchRpcFailedException()
      : RpcFailedException(
            "Couldn't communicate with the cluster! There was a version mismatch. "
            "Please contact your database administrator.") {}

  SPECIALIZE_GET_EXCEPTION_NAME(VersionMismatchRpcFailedException);
};

class GenericRpcFailedException : public RpcFailedException {
 public:
  GenericRpcFailedException()
      : RpcFailedException(
            "Couldn't communicate with the cluster! Please contact your "
            "database administrator.") {}

  SPECIALIZE_GET_EXCEPTION_NAME(GenericRpcFailedException);
};

}  // namespace memgraph::rpc
