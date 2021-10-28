// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

  bool operator==(const License &) const = default;
};

constexpr std::string_view kEnterpriseLicenseSettingKey = "enterprise.license";
constexpr std::string_view kOrganizationNameSettingKey = "organization.name";

enum class LicenseCheckError : uint8_t { INVALID_LICENSE_KEY_STRING, INVALID_ORGANIZATION_NAME, EXPIRED_LICENSE };

std::string LicenseCheckErrorToString(LicenseCheckError error, std::string_view feature);

using LicenseCheckResult = utils::BasicResult<LicenseCheckError, void>;

struct LicenseChecker {
 public:
  explicit LicenseChecker() = default;
  ~LicenseChecker();

  LicenseChecker(const LicenseChecker &) = delete;
  LicenseChecker operator=(const LicenseChecker &) = delete;
  LicenseChecker(LicenseChecker &&) = delete;
  LicenseChecker operator=(LicenseChecker &&) = delete;

  void CheckEnvLicense();
  void SetLicenseInfoOverride(std::string license_key, std::string organization_name);
  void EnableTesting();
  LicenseCheckResult IsValidLicense(const utils::Settings &settings) const;
  bool IsValidLicenseFast() const;
  void StartBackgroundLicenseChecker(const utils::Settings &settings);

 private:
  std::pair<std::string, std::string> GetLicenseInfo(const utils::Settings &settings) const;
  void RevalidateLicense(const utils::Settings &settings);
  void RevalidateLicense(const std::string &license_key, const std::string &organization_name);

  std::optional<std::pair<std::string, std::string>> license_info_override_;
  bool enterprise_enabled_{false};
  std::atomic<bool> is_valid_{false};
  utils::Scheduler scheduler_;

  friend void RegisterLicenseSettings(LicenseChecker &license_checker, utils::Settings &settings);
};

void RegisterLicenseSettings(LicenseChecker &license_checker, utils::Settings &settings);

std::optional<License> Decode(std::string_view license_key);
std::string Encode(const License &license);

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
extern LicenseChecker global_license_checker;
}  // namespace utils::license
