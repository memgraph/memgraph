#pragma once

#include <string>

namespace utils {

/// Resolves hostname to ip, if already an ip, just returns it
std::string ResolveHostname(std::string hostname);

};  // namespace utils
