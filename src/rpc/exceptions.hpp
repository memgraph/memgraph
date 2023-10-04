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
class RpcFailedException final : public utils::BasicException {
 public:
  RpcFailedException(const io::network::Endpoint &endpoint)
      : utils::BasicException::BasicException(
            "Couldn't communicate with the cluster! Please contact your "
            "database administrator."),
        endpoint_(endpoint) {}

  /// Returns the endpoint associated with the error.
  const io::network::Endpoint &endpoint() const { return endpoint_; }
  std::string name() const override { return "RpcFailedException"; }

 private:
  io::network::Endpoint endpoint_;
};
}  // namespace memgraph::rpc
