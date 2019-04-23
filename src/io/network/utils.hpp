#pragma once

#include <optional>
#include <string>

#include "io/network/endpoint.hpp"

namespace io::network {

/// Resolves hostname to ip, if already an ip, just returns it
std::string ResolveHostname(std::string hostname);

/// Gets hostname
std::optional<std::string> GetHostname();

// Try to establish a connection to a remote host
bool CanEstablishConnection(const Endpoint &endpoint);

}  // namespace io::network
