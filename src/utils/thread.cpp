#include "utils/thread.hpp"

#include <sys/prctl.h>

#include <glog/logging.h>

namespace utils {

void ThreadSetName(const std::string &name) {
  CHECK(name.size() <= 16) << "Thread name '" << name << "'too long";
  LOG_IF(WARNING, prctl(PR_SET_NAME, name.c_str()) != 0)
      << "Couldn't set thread name: " << name << "!";
}

}  // namespace utils
