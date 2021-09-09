#pragma once

#include <cstdint>
#include <string>

namespace utils {

struct License {
  std::string organization_name;
  int64_t valid_until;
};

bool IsValidLicense(const std::string &license_key, const std::string &organization_name);

}  // namespace utils
