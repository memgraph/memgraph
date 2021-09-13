#pragma once

#include <cstdint>
#include <string>

#include "utils/settings.hpp"

namespace utils::license {

struct License {
  std::string organization_name;
  int64_t valid_until;
};

bool IsValidLicense(utils::Settings &settings);
std::optional<License> Decode(std::string_view license_key);
std::string Encode(const License &license);

}  // namespace utils::license
