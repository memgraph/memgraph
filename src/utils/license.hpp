#pragma once

#include <cstdint>
#include <string>

#include "utils/scheduler.hpp"
#include "utils/settings.hpp"

namespace utils::license {

struct License {
  std::string organization_name;
  int64_t valid_until;
};

void EnableTesting();
void CheckEnvLicense();

bool IsValidLicense();
void StartFastLicenseChecker();
void StopFastLicenseChecker();
bool IsValidLicenseFast();

std::optional<License> Decode(std::string_view license_key);
std::string Encode(const License &license);

}  // namespace utils::license
