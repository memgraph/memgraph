#include "utils/license.hpp"

#include <charconv>
#include <chrono>
#include <functional>
#include <optional>
#include <unordered_map>

#include "utils/logging.hpp"
#include "utils/synchronized.hpp"

namespace utils {

namespace {
utils::Synchronized<std::unordered_map<std::string, License>> cache;

std::optional<License> DecodeKey(std::string_view license_key) {
  if (!license_key.starts_with("lk-")) {
    return std::nullopt;
  }

  license_key.remove_prefix(3);
  auto organization_name_end = license_key.find('-');

  if (organization_name_end == std::string_view::npos) {
    return std::nullopt;
  }

  std::string organization_name{license_key.substr(0, organization_name_end)};

  license_key.remove_prefix(organization_name_end + 1);
  if (license_key.empty()) {
    return std::nullopt;
  }

  int64_t value{};
  if (const auto [p, ec] = std::from_chars(license_key.data(), license_key.data() + license_key.size(), value);
      ec != std::errc() || p != license_key.data() + license_key.size()) {
    return std::nullopt;
  }

  spdlog::critical(value);

  return License{.organization_name = std::move(organization_name), .valid_until = value};
}

}  // namespace

bool IsValidLicense(utils::Settings *settings) {
  const auto license_key = settings->GetValueFor("enterprise.license");
  MG_ASSERT(license_key);
  const auto organization_name = settings->GetValueFor("organization.name");
  MG_ASSERT(organization_name);

  auto license = std::invoke([&]() -> std::optional<License> {
    {
      auto cache_locked = cache.Lock();
      auto it = cache_locked->find(*license_key);
      if (it != cache_locked->end()) {
        return it->second;
      }
    }
    auto license = DecodeKey(*license_key);
    if (license) {
      auto cache_locked = cache.Lock();
      cache_locked->insert({*license_key, *license});
    }
    return license;
  });

  auto now =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

  return license && license->organization_name == organization_name && now < license->valid_until;
}

}  // namespace utils
