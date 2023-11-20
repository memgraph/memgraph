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

#include <optional>
#include <string>

#include "io/network/endpoint.hpp"

namespace memgraph::io::network {

/// Resolves hostname to ip, if already an ip, just returns it
std::string ResolveHostname(const std::string &hostname);

/// Gets hostname
std::optional<std::string> GetHostname();

// Try to establish a connection to a remote host
bool CanEstablishConnection(const Endpoint &endpoint);

}  // namespace memgraph::io::network
