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

#include <filesystem>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "utils/settings.hpp"

class SettingsTest : public ::testing::Test {
 public:
  void TearDown() override { std::filesystem::remove_all(test_directory); }

 protected:
  const std::filesystem::path test_directory{"MG_tests_unit_utils_settings"};
  const std::filesystem::path settings_directory{test_directory / "settings"};
  static void DummyCallback() {}
};

namespace {
void CheckSettingValue(const memgraph::utils::Settings &settings, const std::string &setting_name,
                       const std::string &expected_value) {
  auto maybe_value = settings.GetValue(setting_name);
  ASSERT_TRUE(maybe_value) << "Failed to access registered setting";
  ASSERT_EQ(maybe_value, expected_value);
}
}  // namespace

TEST_F(SettingsTest, RegisterSetting) {
  const std::string setting_name{"name"};
  const std::string default_value{"value"};

  {
    memgraph::utils::Settings settings;

    settings.Initialize(settings_directory);
    settings.RegisterSetting(setting_name, default_value, DummyCallback);
    CheckSettingValue(settings, setting_name, default_value);
  }
  {
    memgraph::utils::Settings settings;
    settings.Initialize(settings_directory);
    // registering the same object shouldn't change its value
    settings.RegisterSetting(setting_name, fmt::format("{}-modified", default_value), DummyCallback);
    CheckSettingValue(settings, setting_name, default_value);
  }
}

TEST_F(SettingsTest, RegisterSettingCallback) {
  const std::string setting_name{"name"};
  const std::string default_value{"value"};

  memgraph::utils::Settings settings;
  settings.Initialize(settings_directory);

  size_t callback_counter{0};
  const auto callback = [&]() { ++callback_counter; };

  size_t setting_change_counter{0};
  const auto assert_equal_counters = [&] { ASSERT_EQ(callback_counter, setting_change_counter); };

  settings.RegisterSetting(setting_name, default_value, callback);
  assert_equal_counters();

  ASSERT_TRUE(settings.SetValue(setting_name, default_value));
  ++setting_change_counter;
  assert_equal_counters();

  ASSERT_TRUE(settings.SetValue(setting_name, fmt::format("{}-modified", default_value)));
  ++setting_change_counter;
  assert_equal_counters();
}

TEST_F(SettingsTest, GetSetRegisteredSetting) {
  const std::string setting_name{"name"};
  const std::string setting_value{"value"};
  const std::string default_value{"default"};

  memgraph::utils::Settings settings;
  settings.Initialize(settings_directory);
  settings.RegisterSetting(setting_name, default_value, DummyCallback);

  CheckSettingValue(settings, setting_name, default_value);
  ASSERT_TRUE(settings.SetValue(setting_name, setting_value)) << "Failed to modify registered setting";
  CheckSettingValue(settings, setting_name, setting_value);
}

TEST_F(SettingsTest, GetSetUnregisteredSetting) {
  memgraph::utils::Settings settings;
  settings.Initialize(settings_directory);
  ASSERT_FALSE(settings.GetValue("Somesetting")) << "Accessed unregistered setting";
  ASSERT_FALSE(settings.SetValue("Somesetting", "Somevalue")) << "Modified unregistered setting";
}

TEST_F(SettingsTest, Initialization) {
  memgraph::utils::Settings settings;
  settings.Initialize(settings_directory);
  ASSERT_NO_FATAL_FAILURE(settings.GetValue("setting"));
  ASSERT_NO_FATAL_FAILURE(settings.SetValue("setting", "value"));
  ASSERT_NO_FATAL_FAILURE(settings.AllSettings());
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

  memgraph::utils::Settings settings;
  settings.Initialize(settings_directory);
  for (const auto &[setting_name, setting_value] : generated_settings) {
    settings.RegisterSetting(setting_name, setting_value, DummyCallback);
  }
  ASSERT_THAT(settings.AllSettings(), testing::UnorderedElementsAreArray(generated_settings));
}

TEST_F(SettingsTest, Persistance) {
  auto generated_settings = GenerateSettings(100);

  memgraph::utils::Settings settings;
  settings.Initialize(settings_directory);

  for (const auto &[setting_name, setting_value] : generated_settings) {
    settings.RegisterSetting(setting_name, setting_value, DummyCallback);
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
