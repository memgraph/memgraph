// Copyright 2022 Memgraph Ltd.
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

#include <random>

#include "io/address.hpp"
#include "io/local_transport/local_transport.hpp"
#include "io/local_transport/local_transport_handle.hpp"
#include "io/transport.hpp"

namespace memgraph::io::local_transport {

class LocalSystem {
  std::shared_ptr<LocalTransportHandle> local_transport_handle_ = std::make_shared<LocalTransportHandle>();

 public:
  Io<LocalTransport> Register(Address address) {
    LocalTransport local_transport(local_transport_handle_, address);
    return Io{local_transport, address};
  }

  void ShutDown() { local_transport_handle_->ShutDown(); }
};

}  // namespace memgraph::io::local_transport
