#pragma once

#include <cstdint>
#include <string>

#include "utils/settings.hpp"

namespace utils {

struct License {
  std::string organization_name;
  int64_t valid_until;
};

bool IsValidLicense(utils::Settings *settings);

}  // namespace utils
