/**
 * @file
 */
#pragma once

#include <experimental/optional>
#include <string>

namespace utils {

/**
 * Converts a mangled name to a human-readable name using abi::__cxa_demangle.
 * Returns nullopt if the conversion failed.
 */
std::experimental::optional<std::string> Demangle(const char *mangled_name);

}  // namespace utils
