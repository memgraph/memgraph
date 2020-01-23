/// @file
#pragma once

#include <string>

namespace utils {

/// This function sets the thread name of the calling thread.
/// Beware, the name length limit is 16 characters!
void ThreadSetName(const std::string &name);

};  // namespace utils
