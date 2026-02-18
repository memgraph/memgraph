// Copyright 2026 Memgraph Ltd.
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

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <limits>
#include <optional>
#include <type_traits>
#include <vector>

#include "slk/serialization.hpp"
#include "utils/base64.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/settings.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::license {

namespace {
inline constexpr std::string_view license_key_prefix = "mglk-";

std::optional<License> GetLicense(std::string_view license_key) {
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

LicenseCheckResult IsValidLicenseInternal(const License &license, std::string_view organization_name) {
  if (license.organization_name != organization_name) {
    return std::unexpected{LicenseCheckError::INVALID_ORGANIZATION_NAME};
  }

  const auto now =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

  if (license.valid_until != 0 && now > license.valid_until) {
    return std::unexpected{LicenseCheckError::EXPIRED_LICENSE};
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
  // Validate that the license key is well-formed and not already expired.
  // Org name mismatch is caught later in RevalidateLicense; the user gets a warning there.
  auto validate_license_key = [](std::string_view new_value) -> utils::Settings::ValidatorResult {
    if (new_value.empty()) return {};
    const auto maybe_license = Decode(new_value);
    if (!maybe_license) {
      return std::unexpected<std::string>{"Invalid license key: could not be decoded."};
    }
    const auto now =
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    if (maybe_license->valid_until != 0 && now > maybe_license->valid_until) {
      return std::unexpected<std::string>{"Invalid license key: the license has already expired."};
    }
    return {};
  };

  settings.RegisterSetting(
      std::string{kEnterpriseLicenseSettingKey},
      "",
      [&] { license_checker.RevalidateLicense(settings); },
      validate_license_key);
  settings.RegisterSetting(
      std::string{kOrganizationNameSettingKey}, "", [&] { license_checker.RevalidateLicense(settings); });
}

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
LicenseChecker global_license_checker;

LicenseChecker::~LicenseChecker() { Finalize(); }

void LicenseChecker::RevalidateLicense(utils::Settings &settings) {
  spdlog::trace("License revalidation started");

  static utils::Synchronized<std::optional<int64_t>, utils::SpinLock> previous_memory_limit;
  const auto set_memory_limit = [](const int64_t memory_limit) {
    auto locked = previous_memory_limit.Lock();
    if (!*locked || **locked != memory_limit) {
      utils::total_memory_tracker.SetHardLimit(memory_limit);
      *locked = memory_limit;
    }
  };

  if (enterprise_enabled_) [[unlikely]] {
    is_valid_.store(true, std::memory_order_release);
    set_memory_limit(0);
    return;
  }

  // Collect candidates from all three sources: DB (priority 0), ENV (1), CLI (2).
  struct Candidate {
    License license;
    std::string key;
    std::string org;
    int priority;
  };

  std::vector<Candidate> valid_candidates;

  auto try_add = [&](std::string_view key, std::string_view org, int priority, std::string_view source_name) {
    if (key.empty() && org.empty()) return;
    if (key.empty() || org.empty()) {
      spdlog::warn("[{}] Both license key and organization name are required.", source_name);
      return;
    }
    const auto maybe_license = GetLicense(key);
    if (!maybe_license) {
      spdlog::warn("[{}] {}",
                   source_name,
                   LicenseCheckErrorToString(LicenseCheckError::INVALID_LICENSE_KEY_STRING, "Enterprise features"));
      return;
    }
    const auto check = IsValidLicenseInternal(*maybe_license, org);
    if (!check) {
      spdlog::warn("[{}] {}", source_name, LicenseCheckErrorToString(check.error(), "Enterprise features"));
      return;
    }
    valid_candidates.push_back({*maybe_license, std::string{key}, std::string{org}, priority});
  };

  auto db_key = settings.GetValue(std::string{kEnterpriseLicenseSettingKey}).value_or("");
  auto db_org = settings.GetValue(std::string{kOrganizationNameSettingKey}).value_or("");
  try_add(db_key, db_org, 0, "DB");

  if (env_license_info_) try_add(env_license_info_->first, env_license_info_->second, 1, "ENV");
  if (cli_license_info_) try_add(cli_license_info_->first, cli_license_info_->second, 2, "CLI");

  if (valid_candidates.empty()) {
    auto locked = previous_license_info_.Lock();
    if (*locked) {
      spdlog::warn("No valid license found. Running in community mode.");
      locked->reset();
    }
    is_valid_.store(false, std::memory_order_relaxed);
    set_memory_limit(0);
    return;
  }

  // Select the winner: furthest expiry wins; source priority (CLI > ENV > DB) breaks ties.
  // valid_until == 0 means the license never expires, treated as INT64_MAX.
  const auto expiry_of = [](const Candidate &c) -> int64_t {
    return c.license.valid_until == 0 ? std::numeric_limits<int64_t>::max() : c.license.valid_until;
  };
  const auto &winner = *std::ranges::max_element(valid_candidates, [&](const Candidate &a, const Candidate &b) {
    if (expiry_of(a) != expiry_of(b)) return expiry_of(a) < expiry_of(b);
    return a.priority < b.priority;
  });

  // Persist winner to Settings so it survives restarts where CLI/ENV are absent.
  settings.SetValueForce(std::string{kEnterpriseLicenseSettingKey}, winner.key);
  settings.SetValueForce(std::string{kOrganizationNameSettingKey}, winner.org);

  // Log and update stored state only when the winner changes.
  {
    auto locked = previous_license_info_.Lock();
    const bool changed = !*locked || (*locked)->license_key != winner.key || (*locked)->organization_name != winner.org;
    if (changed) {
      spdlog::info("{} license is active.", LicenseTypeToString(winner.license.type));
      locked->emplace(winner.key, winner.org);
      (*locked)->is_valid = true;
      (*locked)->license = winner.license;
      set_memory_limit(winner.license.memory_limit);
    }
  }

  // Write license_type_ before the release store so the acquire load in IsEnterpriseValidFast()
  // establishes a happens-before and reads the correct type.
  license_type_ = winner.license.type;
  is_valid_.store(true, std::memory_order_release);
}

void LicenseChecker::EnableTesting(const LicenseType license_type) {
  enterprise_enabled_ = true;
  license_type_ = license_type;
  is_valid_.store(true, std::memory_order_release);
  spdlog::info("The license type {} is set for testing.", LicenseTypeToString(license_type));
}

void LicenseChecker::DisableTesting() {
  enterprise_enabled_ = false;
  is_valid_.store(false, std::memory_order_relaxed);
  spdlog::info("The license is disabled for testing.");
}

void LicenseChecker::CheckEnvLicense(utils::Settings &settings) {
  const char *license_key = std::getenv("MEMGRAPH_ENTERPRISE_LICENSE");
  const char *organization_name = std::getenv("MEMGRAPH_ORGANIZATION_NAME");
  if (!license_key || !organization_name) {
    return;
  }
  spdlog::warn("License info found in environment variables.");
  env_license_info_.emplace(license_key, organization_name);
  RevalidateLicense(settings);
}

void LicenseChecker::SetCliLicense(std::string license_key, std::string organization_name, utils::Settings &settings) {
  spdlog::warn("License info found in command-line flags.");
  cli_license_info_.emplace(std::move(license_key), std::move(organization_name));
  RevalidateLicense(settings);
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
  auto license_key = settings.GetValue(std::string{kEnterpriseLicenseSettingKey}).value_or("");
  auto organization_name = settings.GetValue(std::string{kOrganizationNameSettingKey}).value_or("");
  return IsEnterpriseValid(license_key, organization_name);
}

LicenseCheckResult LicenseChecker::IsEnterpriseValid(std::string_view license_key,
                                                     std::string_view organization_name) const {
  if (enterprise_enabled_) [[unlikely]] {
    return {};
  }
  const auto maybe_license = GetLicense(license_key);
  if (!maybe_license) {
    return std::unexpected{LicenseCheckError::INVALID_LICENSE_KEY_STRING};
  }
  if (maybe_license->type != LicenseType::ENTERPRISE) {
    return std::unexpected{LicenseCheckError::NOT_ENTERPRISE_LICENSE};
  }

  return IsValidLicenseInternal(*maybe_license, organization_name);
}

LicenseCheckResult LicenseChecker::IsEnterpriseValid() const {
  if (enterprise_enabled_) [[unlikely]] {
    return {};
  }
  auto locked_previous_license_info_ptr = previous_license_info_.Lock();
  const auto &license_info = *locked_previous_license_info_ptr;
  if (!license_info) {
    return std::unexpected{LicenseCheckError::NOT_ENTERPRISE_LICENSE};
  }
  return IsEnterpriseValid(license_info->license_key, license_info->organization_name);
}

void LicenseChecker::StartBackgroundLicenseChecker(std::weak_ptr<utils::Settings> settings) {
  auto locked_settings = settings.lock();
  MG_ASSERT(locked_settings, "Settings are not available");
  RevalidateLicense(*locked_settings);
  scheduler_.SetInterval(std::chrono::minutes{5});
  scheduler_.Run("licensechecker", [&, this] {
    if (auto locked_settings = settings.lock()) {
      RevalidateLicense(*locked_settings);
    }
  });
}

utils::Synchronized<std::optional<LicenseInfo>, utils::SpinLock> &LicenseChecker::GetLicenseInfo() {
  return previous_license_info_;
}

DetailedLicenseInfo LicenseChecker::GetDetailedLicenseInfo() {
  DetailedLicenseInfo info;

  auto locked_previous_license_info_ptr = previous_license_info_.Lock();
  auto &locked_previous_license_info = *locked_previous_license_info_ptr;

  if (!locked_previous_license_info) {
    info.is_valid = false;
    info.status = "You have not provided any license!";
    return info;
  }

  info.license_key = locked_previous_license_info->license_key;
  info.organization_name = locked_previous_license_info->organization_name;

  const auto maybe_license = GetLicense(locked_previous_license_info->license_key);
  if (!maybe_license) {
    if (info.license_key.empty() && info.organization_name.empty()) {
      info.status = "You have not provided any license!";
    } else {
      info.status = "Invalid license key string!";
    }
    info.is_valid = false;
    return info;
  }

  info.memory_limit = maybe_license->memory_limit;
  info.license_type = LicenseTypeToString(maybe_license->type);

  // convert the epoch of validity to date string
  const int64_t valid_until = maybe_license->valid_until;
  if (valid_until != 0) {
    const auto time = static_cast<std::time_t>(valid_until);
    std::tm *tm = std::gmtime(&time);
    std::array<char, 30> buffer;
    if (tm != nullptr && std::strftime(buffer.data(), buffer.size(), "%Y-%m-%d", tm) > 0) {
      info.valid_until = std::string(buffer.data());
    } else {
      info.valid_until = "error";
    }
  } else {
    info.valid_until = "FOREVER";
  }

  // Use the same validation logic as enterprise feature checks to ensure is_valid is consistent.
  // IsEnterpriseValid(key, org) validates: decodeable key + ENTERPRISE type + org name match + not expired.
  // NOTE: this overload does not re-acquire previous_license_info_ lock, so no deadlock.
  const auto license_check_result = IsEnterpriseValid(info.license_key, info.organization_name);
  if (!license_check_result) {
    info.is_valid = false;
    info.status = LicenseCheckErrorToString(license_check_result.error(), "Memgraph Enterprise");
    return info;
  }

  info.is_valid = true;
  info.status = "You are running a valid Memgraph Enterprise License.";
  return info;
}

bool LicenseChecker::IsEnterpriseValidFast() const {
  // Acquire synchronizes with release stores in RevalidateLicense/EnableTesting.
  // This ensures we see the license_type_ write that precedes those release stores,
  // avoiding a data race on the non-atomic license_type_.
  return is_valid_.load(std::memory_order_acquire) && license_type_ == LicenseType::ENTERPRISE;
}

std::string Encode(const License &license) {
  std::vector<uint8_t> buffer;
  slk::Builder builder(
      [&buffer](const uint8_t *data, size_t size, bool /*have_more*/) -> slk::BuilderWriteFunction::result_type {
        for (size_t i = 0; i < size; ++i) {
          buffer.push_back(data[i]);
        }
        return {};
      });

  slk::Save(license.organization_name, &builder);
  slk::Save(license.valid_until, &builder);
  slk::Save(license.memory_limit, &builder);
  slk::Save(license.type, &builder);
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
