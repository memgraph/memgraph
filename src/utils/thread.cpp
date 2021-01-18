#include "utils/thread.hpp"

#include <sys/prctl.h>

#include "utils/logging.hpp"

namespace utils {

void ThreadSetName(const std::string &name) {
  MG_ASSERT(name.size() <= 16, "Thread name '{}' is too long", name);

  if (prctl(PR_SET_NAME, name.c_str()) != 0) {
    spdlog::warn("Couldn't set thread name: {}!", name);
  }
}

}  // namespace utils
