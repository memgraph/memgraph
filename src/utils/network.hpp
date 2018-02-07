#pragma once

#include <experimental/optional>
#include <string>

namespace utils {

/// Resolves hostname to ip, if already an ip, just returns it
std::string ResolveHostname(std::string hostname);

/// Gets hostname
std::experimental::optional<std::string> GetHostname();

};  // namespace utils
