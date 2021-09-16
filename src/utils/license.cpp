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

bool IsValidLicenseInternal(const std::string &license_key, const std::string &organization_name) {
  static utils::Synchronized<std::unordered_map<std::string, License>, utils::WritePrioritizedRWLock> cache;

  auto license = std::invoke([&]() -> std::optional<License> {
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
  });

  auto now =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

  return license && license->organization_name == organization_name && now < license->valid_until;
}
}  // namespace

void EnableTesting() { enterprise_enabled = true; }

void CheckEnvLicense() {
  const char *license_key = std::getenv("MEMGRAPH_ENTERPRISE_LICENSE");
  if (!license_key) {
    return;
  }
  const char *organization_name = std::getenv("MEMGRAPH_ORGANIZATION_NAME");
  if (!organization_name) {
    return;
  }

  enterprise_enabled = IsValidLicenseInternal(license_key, organization_name);
}

// TODO(antonio2368): Return more information (what was wrong with the license if the check fails)
bool IsValidLicense() {
  if (enterprise_enabled) [[unlikely]] {
    return true;
  }

  const auto &settings = utils::Settings::GetInstance();
  const auto license_key = settings.GetValueFor("enterprise.license");
  if (!license_key) {
    return false;
  }

  const auto organization_name = settings.GetValueFor("organization.name");
  if (!organization_name) {
    return false;
  }

  return IsValidLicenseInternal(*license_key, *organization_name);
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
  builder.Finalize();

  return std::string{license_key_prefix} + base64_encode(buffer.data(), buffer.size());
}

void StartFastLicenseChecker() {
  scheduler.Run("licensechecker", std::chrono::milliseconds{10},
                [] { is_valid.store(IsValidLicense(), std::memory_order_relaxed); });
}

void StopFastLicenseChecker() { scheduler.Stop(); }

bool IsValidLicenseFast() { return is_valid.load(std::memory_order_relaxed); }

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
    return License{.organization_name = organization_name, .valid_until = valid_until};
  } catch (const slk::SlkReaderException &e) {
    return std::nullopt;
  }
}

}  // namespace utils::license
