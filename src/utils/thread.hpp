/// @file
#pragma once

#include <string>

namespace utils {

constexpr size_t GetMaxThreadNameSize() { return 16; }

/// This function sets the thread name of the calling thread.
/// Beware, the name length limit is 16 characters!
void ThreadSetName(const std::string &name);

};  // namespace utils
