#pragma once

#include <sys/prctl.h>

#include <thread>

#include <glog/logging.h>

namespace utils {

/**
 * This function sets the thread name of the calling thread.
 * Beware, the name length limit is 16 characters!
 */
inline void ThreadSetName(const std::string &name) {
  CHECK(name.size() <= 16) << "Thread name '" << name << "'too long";
  LOG_IF(WARNING, prctl(PR_SET_NAME, name.c_str()) != 0)
      << "Couldn't set thread name: " << name << "!";
}

};  // namespace utils
