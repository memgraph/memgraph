#pragma once

#include <cstdint>
#include <string>

#include "utils/result.hpp"
#include "utils/scheduler.hpp"
#include "utils/settings.hpp"

namespace utils::license {

struct License {
  std::string organization_name;
  int64_t valid_until;
  int64_t memory_limit;
};

constexpr const auto *kEnterpriseLicenseSettingKey = "enterprise.license";
constexpr const auto *kOrganizationNameSettingKey = "organization.name";

void RegisterLicenseSettings();

void EnableTesting();
void CheckEnvLicense();

enum class LicenseCheckError : uint8_t { INVALID_LICENSE_KEY_STRING, INVALID_ORGANIZATION_NAME, EXPIRED_LICENSE };

std::string LicenseCheckErrorToString(LicenseCheckError error, std::string_view feature);

using LicenseCheckResult = utils::BasicResult<LicenseCheckError, void>;

LicenseCheckResult IsValidLicense();
void StartBackgroundLicenseChecker();
void StopBackgroundLicenseChecker();
bool IsValidLicenseFast();

std::optional<License> Decode(std::string_view license_key);
std::string Encode(const License &license);

}  // namespace utils::license
