#include "utils/license.hpp"

#include <atomic>
#include <charconv>
#include <chrono>
#include <functional>
#include <optional>
#include <unordered_map>

#include "slk/serialization.hpp"
#include "utils/base64.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

namespace utils::license {

namespace {
const std::string_view license_key_prefix = "mglk-";

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
bool enterprise_enabled{false};
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> is_valid{false};
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
utils::Scheduler scheduler;

std::optional<License> GetLicense(const std::string &license_key) {
  static utils::Synchronized<std::unordered_map<std::string, License>, utils::WritePrioritizedRWLock> cache;

  {
    auto cache_locked = cache.ReadLock();
    auto it = cache_locked->find(license_key);
    if (it != cache_locked->end()) {
      return it->second;
    }
  }
  auto license = Decode(license_key);
  if (license) {
    auto cache_locked = cache.Lock();
    cache_locked->insert({license_key, *license});
  }
  return license;
}

LicenseCheckResult IsValidLicenseInternal(const License &license, const std::string &organization_name) {
  if (license.organization_name != organization_name) {
    return LicenseCheckError::INVALID_ORGANIZATION_NAME;
  }

  const auto now =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

  if (now > license.valid_until) {
    return LicenseCheckError::EXPIRED_LICENSE;
  }

  return {};
}
}  // namespace

void RegisterLicenseSettings() {
  auto &settings = utils::Settings::GetInstance();
  settings.RegisterSetting(kEnterpriseLicenseSettingKey, "");
  settings.RegisterSetting(kOrganizationNameSettingKey, "");
}

void EnableTesting() { enterprise_enabled = true; }

void CheckEnvLicense() {
  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  const char *license_key = std::getenv("MEMGRAPH_ENTERPRISE_LICENSE");
  if (!license_key) {
    return;
  }

  const auto maybe_license = GetLicense(license_key);
  if (!maybe_license) {
    return;
  }

  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  const char *organization_name = std::getenv("MEMGRAPH_ORGANIZATION_NAME");
  if (!organization_name) {
    return;
  }

  enterprise_enabled = !IsValidLicenseInternal(*maybe_license, organization_name).HasError();
}

std::string LicenseCheckErrorToString(LicenseCheckError error, const std::string_view feature) {
  switch (error) {
    case LicenseCheckError::INVALID_LICENSE_KEY_STRING:
      return fmt::format(
          "Invalid license key string set. To use {} please set it to a valid string using "
          "the following query:\n"
          "SET DATABASE SETTING \"enterprise_license\" TO \"your-license-key\"",
          feature);
    case LicenseCheckError::INVALID_ORGANIZATION_NAME:
      return fmt::format(
          "The organization name contained in the license key is not the same as the one defined in the settings. To "
          "use {} please set the organization name to a valid string using the following query:\n"
          "SET DATABASE SETTING \"organization_name\" TO \"organization-name\"",
          feature);
    case LicenseCheckError::EXPIRED_LICENSE:
      return fmt::format(
          "Your license key has expired. To use {} please renew your license and set the update license key using the "
          "following query:\n"
          "SET DATABASE SETTING \"license_key\" TO \"your-license-key\"",
          feature);
  }
}

LicenseCheckResult IsValidLicense() {
  if (enterprise_enabled) [[unlikely]] {
    return {};
  }

  const auto &settings = utils::Settings::GetInstance();
  const auto license_key = settings.GetValue(kEnterpriseLicenseSettingKey);
  MG_ASSERT(license_key, "License key is missing from the settings");

  const auto maybe_license = GetLicense(*license_key);
  if (!maybe_license) {
    return LicenseCheckError::INVALID_LICENSE_KEY_STRING;
  }

  const auto organization_name = settings.GetValue(kOrganizationNameSettingKey);
  MG_ASSERT(organization_name, "Organization name is missing from the settings");

  return IsValidLicenseInternal(*maybe_license, *organization_name);
}

void StartBackgroundLicenseChecker() {
  scheduler.Run("licensechecker", std::chrono::milliseconds{10}, [] {
    const auto set_memory_limit = [](const auto memory_limit) {
      static std::optional<int64_t> previous_memory_limit;
      if (!previous_memory_limit || *previous_memory_limit != memory_limit) {
        utils::total_memory_tracker.SetHardLimit(memory_limit);
        previous_memory_limit = memory_limit;
      }
    };

    if (enterprise_enabled) [[unlikely]] {
      is_valid.store(true, std::memory_order_relaxed);
      set_memory_limit(0);
      return;
    }

    static std::optional<std::pair<std::string, std::string>> previous_license_info;
    const auto &settings = utils::Settings::GetInstance();
    auto license_key = settings.GetValue(kEnterpriseLicenseSettingKey);
    MG_ASSERT(license_key, "License key is missing from the settings");

    auto organization_name = settings.GetValue(kOrganizationNameSettingKey);
    MG_ASSERT(organization_name, "Organization name is missing from the settings");

    if (previous_license_info && previous_license_info->first == license_key &&
        previous_license_info->second == organization_name) {
      return;
    }

    previous_license_info.emplace(std::move(*license_key), std::move(*organization_name));

    const auto maybe_license = GetLicense(previous_license_info->first);
    if (!maybe_license) {
      spdlog::warn(LicenseCheckErrorToString(LicenseCheckError::INVALID_LICENSE_KEY_STRING, "Enterprise features"));
      set_memory_limit(0);
      return;
    }

    const auto license_check_result = IsValidLicenseInternal(*maybe_license, previous_license_info->second);

    if (license_check_result.HasError()) {
      spdlog::warn(LicenseCheckErrorToString(license_check_result.GetError(), "Enterprise features"));
      is_valid.store(false, std::memory_order_relaxed);
      return;
    }

    is_valid.store(true, std::memory_order_relaxed);
    set_memory_limit(maybe_license->memory_limit);
  });
}

void StopBackgroundLicenseChecker() { scheduler.Stop(); }

bool IsValidLicenseFast() { return is_valid.load(std::memory_order_relaxed); }

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
  builder.Finalize();

  return std::string{license_key_prefix} + base64_encode(buffer.data(), buffer.size());
}

std::optional<License> Decode(std::string_view license_key) {
  if (!license_key.starts_with(license_key_prefix)) {
    return std::nullopt;
  }

  license_key.remove_prefix(license_key_prefix.size());

  const auto decoded = std::invoke([license_key]() -> std::optional<std::string> {
    try {
      return base64_decode(license_key);
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
    return License{.organization_name = organization_name, .valid_until = valid_until, .memory_limit = memory_limit};
  } catch (const slk::SlkReaderException &e) {
    return std::nullopt;
  }
}

}  // namespace utils::license
