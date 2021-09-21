#include <filesystem>

#include <gmock/gmock-generated-matchers.h>
#include <gtest/gtest.h>

#include "utils/settings.hpp"

class SettingsTest : public ::testing::Test {
 public:
  void SetUp() override { utils::Settings::GetInstance().Initialize(settings_directory); }

  void TearDown() override {
    utils::Settings::GetInstance().Finalize();
    std::filesystem::remove_all(test_directory);
  }

 protected:
  const std::filesystem::path test_directory{"MG_tests_unit_utils_settings"};
  const std::filesystem::path settings_directory{test_directory / "settings"};
};

TEST_F(SettingsTest, GetSetRegisteredSetting) {
  const std::string setting_name{"name"};
  const std::string setting_value{"value"};
  const std::string default_value{"default"};

  auto &settings = utils::Settings::GetInstance();
  settings.RegisterSetting(setting_name, default_value);

  const auto check_value = [&](const auto &expected_value) {
    auto maybe_value = settings.GetValue(setting_name);
    ASSERT_TRUE(maybe_value) << "Failed to access registered setting";
    ASSERT_EQ(maybe_value, expected_value);
  };

  check_value(default_value);
  ASSERT_TRUE(settings.SetValue(setting_name, setting_value)) << "Failed to modify registered setting";
  check_value(setting_value);
}

TEST_F(SettingsTest, GetSetUnregisteredSetting) {
  auto &settings = utils::Settings::GetInstance();
  ASSERT_FALSE(settings.GetValue("Somesetting")) << "Accessed unregistered setting";
  ASSERT_FALSE(settings.SetValue("Somesetting", "Somevalue")) << "Modified unregistered setting";
}

TEST_F(SettingsTest, Initialization) {
  auto &settings = utils::Settings::GetInstance();

  const auto assert_no_failures = [&] {
    ASSERT_NO_FATAL_FAILURE(settings.GetValue("setting"));
    ASSERT_NO_FATAL_FAILURE(settings.SetValue("setting", "value"));
    ASSERT_NO_FATAL_FAILURE(settings.AllSettings());
  };

  assert_no_failures();

  settings.Finalize();
  ASSERT_DEATH(settings.GetValue("setting"), ".*");
  ASSERT_DEATH(settings.SetValue("setting", "value"), ".*");
  ASSERT_DEATH(settings.AllSettings(), ".*");

  settings.Initialize(settings_directory);
  assert_no_failures();
}

namespace {
std::vector<std::pair<std::string, std::string>> GenerateSettings(const size_t amount) {
  std::vector<std::pair<std::string, std::string>> result;
  result.reserve(amount);

  for (size_t i = 0; i < amount; ++i) {
    result.emplace_back(fmt::format("setting{}", i), fmt::format("value{}", i));
  }

  return result;
}
}  // namespace

TEST_F(SettingsTest, AllSettings) {
  const auto generated_settings = GenerateSettings(100);

  auto &settings = utils::Settings::GetInstance();
  for (const auto &[setting_name, setting_value] : generated_settings) {
    settings.RegisterSetting(setting_name, setting_value);
  }
  ASSERT_THAT(settings.AllSettings(), testing::UnorderedElementsAreArray(generated_settings));
}

TEST_F(SettingsTest, Persistance) {
  auto generated_settings = GenerateSettings(100);

  auto &settings = utils::Settings::GetInstance();

  for (const auto &[setting_name, setting_value] : generated_settings) {
    settings.RegisterSetting(setting_name, setting_value);
  }

  ASSERT_THAT(settings.AllSettings(), testing::UnorderedElementsAreArray(generated_settings));

  // reinitialize to other directory and then back to the first
  settings.Initialize(test_directory / "other_settings");
  ASSERT_TRUE(settings.AllSettings().empty());

  settings.Initialize(settings_directory);
  ASSERT_THAT(settings.AllSettings(), testing::UnorderedElementsAreArray(generated_settings));

  for (size_t i = 0; i < generated_settings.size(); ++i) {
    auto &[setting_name, setting_value] = generated_settings[i];
    setting_value = fmt::format("new_value{}", i);
    settings.SetValue(setting_name, setting_value);
  }

  ASSERT_THAT(settings.AllSettings(), testing::UnorderedElementsAreArray(generated_settings));

  // reinitialize to other directory and then back to the first
  settings.Initialize(test_directory / "other_settings");
  ASSERT_TRUE(settings.AllSettings().empty());

  settings.Initialize(settings_directory);
  ASSERT_THAT(settings.AllSettings(), testing::UnorderedElementsAreArray(generated_settings));
}
