#pragma once

#include <experimental/optional>
#include <string>

#include "io/network/endpoint.hpp"

namespace utils {

/// Resolves hostname to ip, if already an ip, just returns it
std::string ResolveHostname(std::string hostname);

/// Gets hostname
std::experimental::optional<std::string> GetHostname();

// Try to establish a connection to a remote host
bool CanEstablishConnection(const io::network::Endpoint &endpoint);

}  // namespace utils
