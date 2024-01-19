// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "license/license.hpp"

#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <functional>
#include <optional>
#include <unordered_map>

#include "slk/serialization.hpp"
#include "utils/base64.hpp"
#include "utils/cast.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/settings.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::license {

namespace {
inline constexpr std::string_view license_key_prefix = "mglk-";

std::optional<License> GetLicense(const std::string &license_key) {
  if (license_key.empty()) {
    return std::nullopt;
  }

  static utils::Synchronized<std::pair<std::string, License>, utils::SpinLock> cached_license;
  {
    auto cache_locked = cached_license.Lock();
    const auto &[cached_key, license] = *cache_locked;
    if (cached_key == license_key) {
      return license;
    }
  }
  auto license = Decode(license_key);
  if (license) {
    auto cache_locked = cached_license.Lock();
    *cache_locked = std::make_pair(license_key, *license);
  }
  return license;
}

LicenseCheckResult IsValidLicenseInternal(const License &license, const std::string &organization_name) {
  if (license.organization_name != organization_name) {
    return LicenseCheckError::INVALID_ORGANIZATION_NAME;
  }

  const auto now =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

  if (license.valid_until != 0 && now > license.valid_until) {
    return LicenseCheckError::EXPIRED_LICENSE;
  }

  return {};
}
}  // namespace

std::string LicenseTypeToString(const LicenseType license_type) {
  switch (license_type) {
    case LicenseType::ENTERPRISE: {
      return "enterprise";
    }
    case LicenseType::OEM: {
      return "oem";
    }
  }
}

void RegisterLicenseSettings(LicenseChecker &license_checker, utils::Settings &settings) {
  settings.RegisterSetting(std::string{kEnterpriseLicenseSettingKey}, "",
                           [&] { license_checker.RevalidateLicense(settings); });
  settings.RegisterSetting(std::string{kOrganizationNameSettingKey}, "",
                           [&] { license_checker.RevalidateLicense(settings); });
}

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
LicenseChecker global_license_checker;

LicenseChecker::~LicenseChecker() { Finalize(); }

std::pair<std::string, std::string> LicenseChecker::ExtractLicenseInfo(const utils::Settings &settings) const {
  if (license_info_override_) {
    spdlog::warn("Ignoring license info stored in the settings because a different source was specified.");
    return *license_info_override_;
  }

  auto license_key = settings.GetValue(std::string{kEnterpriseLicenseSettingKey});
  MG_ASSERT(license_key, "License key is missing from the settings");

  auto organization_name = settings.GetValue(std::string{kOrganizationNameSettingKey});
  MG_ASSERT(organization_name, "Organization name is missing from the settings");
  return std::make_pair(std::move(*license_key), std::move(*organization_name));
}

void LicenseChecker::RevalidateLicense(const utils::Settings &settings) {
  const auto license_info = ExtractLicenseInfo(settings);
  RevalidateLicense(license_info.first, license_info.second);
}

void LicenseChecker::RevalidateLicense(const std::string &license_key, const std::string &organization_name) {
  static utils::Synchronized<std::optional<int64_t>, utils::SpinLock> previous_memory_limit;
  const auto set_memory_limit = [](const auto memory_limit) {
    auto locked_previous_memory_limit_ptr = previous_memory_limit.Lock();
    auto &locked_previous_memory_limit = *locked_previous_memory_limit_ptr;
    if (!locked_previous_memory_limit || *locked_previous_memory_limit != memory_limit) {
      utils::total_memory_tracker.SetHardLimit(memory_limit);
      locked_previous_memory_limit = memory_limit;
    }
  };

  if (enterprise_enabled_) [[unlikely]] {
    is_valid_.store(true, std::memory_order_relaxed);
    set_memory_limit(0);
    return;
  }

  auto locked_previous_license_info_ptr = previous_license_info_.Lock();
  auto &locked_previous_license_info = *locked_previous_license_info_ptr;
  const bool same_license_info = locked_previous_license_info &&
                                 locked_previous_license_info->license_key == license_key &&
                                 locked_previous_license_info->organization_name == organization_name;
  // If we already know it's invalid skip the check
  if (same_license_info && !locked_previous_license_info->is_valid) {
    return;
  }

  locked_previous_license_info.emplace(license_key, organization_name);

  auto maybe_license = GetLicense(locked_previous_license_info->license_key);
  if (!maybe_license) {
    spdlog::warn(LicenseCheckErrorToString(LicenseCheckError::INVALID_LICENSE_KEY_STRING, "Enterprise features"));
    is_valid_.store(false, std::memory_order_relaxed);
    locked_previous_license_info->is_valid = false;
    set_memory_limit(0);
    return;
  }

  const auto license_check_result =
      IsValidLicenseInternal(*maybe_license, locked_previous_license_info->organization_name);

  if (license_check_result.HasError()) {
    spdlog::warn(LicenseCheckErrorToString(license_check_result.GetError(), "Enterprise features"));
    is_valid_.store(false, std::memory_order_relaxed);
    locked_previous_license_info->is_valid = false;
    license_type_ = maybe_license->type;
    set_memory_limit(0);
    return;
  }

  if (!same_license_info) {
    license_type_ = maybe_license->type;
    if (license_type_ == LicenseType::ENTERPRISE) {
      spdlog::info("Enterprise license is active.");
    } else {
      spdlog::info("OEM license is active.");
    }
    is_valid_.store(true, std::memory_order_relaxed);
    locked_previous_license_info->is_valid = true;
    set_memory_limit(maybe_license->memory_limit);
    locked_previous_license_info->license = std::move(*maybe_license);
  }
}

void LicenseChecker::EnableTesting(const LicenseType license_type) {
  enterprise_enabled_ = true;
  is_valid_.store(true, std::memory_order_relaxed);
  license_type_ = license_type;
  spdlog::info("The license type {} is set for testing.", LicenseTypeToString(license_type));
}

void LicenseChecker::CheckEnvLicense() {
  const char *license_key = std::getenv("MEMGRAPH_ENTERPRISE_LICENSE");
  if (!license_key) {
    return;
  }

  const char *organization_name = std::getenv("MEMGRAPH_ORGANIZATION_NAME");
  if (!organization_name) {
    return;
  }

  spdlog::warn("Using license info from environment variables");
  license_info_override_.emplace(license_key, organization_name);
  RevalidateLicense(license_key, organization_name);
}

void LicenseChecker::SetLicenseInfoOverride(std::string license_key, std::string organization_name) {
  spdlog::warn("Using license info overrides");
  license_info_override_.emplace(std::move(license_key), std::move(organization_name));
  RevalidateLicense(license_info_override_->first, license_info_override_->second);
}

std::string LicenseCheckErrorToString(LicenseCheckError error, const std::string_view feature) {
  switch (error) {
    case LicenseCheckError::INVALID_LICENSE_KEY_STRING:
      return fmt::format(
          "Invalid license key string. To use {} please set it to a valid string using "
          "the following query:\n"
          "SET DATABASE SETTING \"enterprise.license\" TO \"your-license-key\"",
          feature);
    case LicenseCheckError::INVALID_ORGANIZATION_NAME:
      return fmt::format(
          "The organization name contained in the license key is not the same as the one defined in the settings. To "
          "use {} please set the organization name to a valid string using the following query:\n"
          "SET DATABASE SETTING \"organization.name\" TO \"your-organization-name\"",
          feature);
    case LicenseCheckError::EXPIRED_LICENSE:
      return fmt::format(
          "Your license key has expired. To use {} please renew your license and set the updated license key using the "
          "following query:\n"
          "SET DATABASE SETTING \"enterprise.license\" TO \"your-license-key\"",
          feature);
    case LicenseCheckError::NOT_ENTERPRISE_LICENSE:
      return fmt::format("Your license has an invalid type. To use {} you need to have an enterprise license. \n",
                         feature);
  }
}

LicenseCheckResult LicenseChecker::IsEnterpriseValid(const utils::Settings &settings) const {
  if (enterprise_enabled_) [[unlikely]] {
    return {};
  }

  const auto license_info = ExtractLicenseInfo(settings);

  const auto maybe_license = GetLicense(license_info.first);
  if (!maybe_license) {
    return LicenseCheckError::INVALID_LICENSE_KEY_STRING;
  }
  if (maybe_license->type != LicenseType::ENTERPRISE) {
    return LicenseCheckError::NOT_ENTERPRISE_LICENSE;
  }

  return IsValidLicenseInternal(*maybe_license, license_info.second);
}

void LicenseChecker::StartBackgroundLicenseChecker(const utils::Settings &settings) {
  RevalidateLicense(settings);
  scheduler_.Run("licensechecker", std::chrono::minutes{5}, [&, this] { RevalidateLicense(settings); });
}

utils::Synchronized<std::optional<LicenseInfo>, utils::SpinLock> &LicenseChecker::GetLicenseInfo() {
  return previous_license_info_;
}

bool LicenseChecker::IsEnterpriseValidFast() const {
  return license_type_ == LicenseType::ENTERPRISE && is_valid_.load(std::memory_order_relaxed);
}

std::string Encode(const License &license) {
  std::vector<uint8_t> buffer;
  slk::Builder builder([&buffer](const uint8_t *data, size_t size, bool /*have_more*/) {
    for (size_t i = 0; i < size; ++i) {
      buffer.push_back(data[i]);
    }
  });

  slk::Save(license.organization_name, &builder);
  slk::Save(license.valid_until, &builder);
  slk::Save(license.memory_limit, &builder);
  slk::Save(utils::UnderlyingCast(license.type), &builder);
  builder.Finalize();

  return std::string{license_key_prefix} + utils::base64_encode(buffer.data(), buffer.size());
}

std::optional<License> Decode(std::string_view license_key) {
  if (!license_key.starts_with(license_key_prefix)) {
    return std::nullopt;
  }

  license_key.remove_prefix(license_key_prefix.size());

  const auto decoded = std::invoke([license_key]() -> std::optional<std::string> {
    try {
      return utils::base64_decode(license_key);
    } catch (const std::runtime_error & /*exception*/) {
      return std::nullopt;
    }
  });

  if (!decoded) {
    return std::nullopt;
  }

  try {
    slk::Reader reader(std::bit_cast<uint8_t *>(decoded->c_str()), decoded->size());
    std::string organization_name;
    slk::Load(&organization_name, &reader);
    int64_t valid_until{0};
    slk::Load(&valid_until, &reader);
    int64_t memory_limit{0};
    slk::Load(&memory_limit, &reader);
    std::underlying_type_t<LicenseType> license_type{0};
    slk::Load(&license_type, &reader);
    return {License{organization_name, valid_until, memory_limit, LicenseType(license_type)}};
  } catch (const slk::SlkReaderException &e) {
    return std::nullopt;
  }
}

}  // namespace memgraph::license
