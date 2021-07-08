#include "utils/thread.hpp"

#include <sys/prctl.h>

#include "utils/logging.hpp"

namespace utils {

void ThreadSetName(const std::string &name) {
  constexpr auto max_name_length = GetMaxThreadNameSize();
  MG_ASSERT(name.size() <= max_name_length, "Thread name '{}' is too long", max_name_length);

  if (prctl(PR_SET_NAME, name.c_str()) != 0) {
    spdlog::warn("Couldn't set thread name: {}!", name);
  }
}

}  // namespace utils
