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

#include <filesystem>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kvstore/kvstore.hpp"
#include "utils/exceptions.hpp"
#include "utils/settings.hpp"

using memgraph::utils::Settings;
using Persistence = Settings::Persistence;

class SettingsTest : public ::testing::Test {
 public:
  void TearDown() override { std::filesystem::remove_all(test_directory); }

 protected:
  const std::filesystem::path test_directory{"MG_tests_unit_utils_settings"};
  const std::filesystem::path settings_directory{test_directory / "settings"};

  static void DummyCallback() {}

  // Writes straight to the store, the way an older version persisted every setting.
  void SeedStore(const std::string &name, const std::string &value) {
    memgraph::kvstore::KVStore store(settings_directory);
    ASSERT_TRUE(store.Put(name, value));
  }

  std::optional<std::string> ReadStore(const std::string &name) {
    memgraph::kvstore::KVStore store(settings_directory);
    return store.Get(name);
  }
};

namespace {
void CheckSettingValue(const Settings &settings, const std::string &setting_name, const std::string &expected_value) {
  auto maybe_value = settings.GetValue(setting_name);
  ASSERT_TRUE(maybe_value) << "Failed to access registered setting";
  ASSERT_EQ(maybe_value, expected_value);
}
}  // namespace

TEST_F(SettingsTest, RegisterSetting) {
  const std::string setting_name{"name"};
  const std::string default_value{"value"};

  {
    Settings settings(settings_directory);
    settings.RegisterSetting(setting_name, default_value, Persistence::kRuntimeOnly, DummyCallback);
    CheckSettingValue(settings, setting_name, default_value);
  }
  {
    Settings settings(settings_directory);
    // a run-time only setting always starts from its default
    settings.RegisterSetting(
        setting_name, fmt::format("{}-modified", default_value), Persistence::kRuntimeOnly, DummyCallback);
    CheckSettingValue(settings, setting_name, fmt::format("{}-modified", default_value));
  }
}

TEST_F(SettingsTest, RegisterSettingCallback) {
  const std::string setting_name{"name"};
  const std::string default_value{"value"};

  Settings settings(settings_directory);

  size_t callback_counter{0};
  const auto callback = [&]() { ++callback_counter; };

  size_t setting_change_counter{0};
  const auto assert_equal_counters = [&] { ASSERT_EQ(callback_counter, setting_change_counter); };

  settings.RegisterSetting(setting_name, default_value, Persistence::kRuntimeOnly, callback);
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

  Settings settings(settings_directory);
  settings.RegisterSetting(setting_name, default_value, Persistence::kRuntimeOnly, DummyCallback);

  CheckSettingValue(settings, setting_name, default_value);
  ASSERT_TRUE(settings.SetValue(setting_name, setting_value)) << "Failed to modify registered setting";
  CheckSettingValue(settings, setting_name, setting_value);
}

TEST_F(SettingsTest, GetSetUnregisteredSetting) {
  Settings settings(settings_directory);
  ASSERT_FALSE(settings.GetValue("Somesetting")) << "Accessed unregistered setting";
  ASSERT_FALSE(settings.SetValue("Somesetting", "Somevalue")) << "Modified unregistered setting";
}

TEST_F(SettingsTest, SetValueValidation) {
  Settings settings(settings_directory);
  settings.RegisterSetting(
      "name", "ok", Persistence::kRuntimeOnly, DummyCallback, [](std::string_view in) -> Settings::ValidatorResult {
        if (in == "ok") return {};
        return std::unexpected{"only ok"};
      });
  ASSERT_THROW(settings.SetValue("name", "bad"), memgraph::utils::BasicException);
  CheckSettingValue(settings, "name", "ok");
}

TEST_F(SettingsTest, Initialization) {
  Settings settings(settings_directory);
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

  Settings settings(settings_directory);
  for (const auto &[setting_name, setting_value] : generated_settings) {
    settings.RegisterSetting(setting_name, setting_value, Persistence::kRuntimeOnly, DummyCallback);
  }
  ASSERT_THAT(settings.AllSettings(), testing::UnorderedElementsAreArray(generated_settings));
}

TEST_F(SettingsTest, PersistedSurvivesReopen) {
  auto generated_settings = GenerateSettings(100);
  {
    Settings settings(settings_directory);
    for (const auto &[setting_name, setting_value] : generated_settings) {
      settings.RegisterSetting(setting_name, setting_value, Persistence::kPersisted, DummyCallback);
    }
    ASSERT_THAT(settings.AllSettings(), testing::UnorderedElementsAreArray(generated_settings));
  }
  {
    // another directory sees nothing
    Settings settings(test_directory / "other_settings");
    ASSERT_TRUE(settings.AllSettings().empty());
  }
  {
    Settings settings(settings_directory);
    // the stored value wins over the default passed at registration
    for (const auto &[setting_name, setting_value] : generated_settings) {
      settings.RegisterSetting(setting_name, "ignored-default", Persistence::kPersisted, DummyCallback);
    }
    ASSERT_THAT(settings.AllSettings(), testing::UnorderedElementsAreArray(generated_settings));

    for (size_t i = 0; i < generated_settings.size(); ++i) {
      auto &[setting_name, setting_value] = generated_settings[i];
      setting_value = fmt::format("new_value{}", i);
      settings.SetValue(setting_name, setting_value);
    }
    ASSERT_THAT(settings.AllSettings(), testing::UnorderedElementsAreArray(generated_settings));
  }
  {
    Settings settings(settings_directory);
    for (const auto &[setting_name, setting_value] : generated_settings) {
      settings.RegisterSetting(setting_name, "ignored-default", Persistence::kPersisted, DummyCallback);
    }
    ASSERT_THAT(settings.AllSettings(), testing::UnorderedElementsAreArray(generated_settings));
  }
}

TEST_F(SettingsTest, RuntimeOnlyNeverWritesTheStore) {
  {
    Settings settings(settings_directory);
    settings.RegisterSetting("name", "default", Persistence::kRuntimeOnly, DummyCallback);
    ASSERT_TRUE(settings.SetValue("name", "changed"));
    settings.SetValueForce("name", "forced");
    CheckSettingValue(settings, "name", "forced");
    ASSERT_FALSE(settings.StoredValue("name"));
  }
  ASSERT_FALSE(ReadStore("name"));
  {
    Settings settings(settings_directory);
    settings.RegisterSetting("name", "default", Persistence::kRuntimeOnly, DummyCallback);
    CheckSettingValue(settings, "name", "default");
  }
}

TEST_F(SettingsTest, RuntimeOnlyDeletesLeftover) {
  SeedStore("name", "from-older-version");
  Settings settings(settings_directory);
  settings.RegisterSetting("name", "default", Persistence::kRuntimeOnly, DummyCallback);
  CheckSettingValue(settings, "name", "default");
  ASSERT_FALSE(settings.StoredValue("name"));
}

TEST_F(SettingsTest, DeprecatedRestoreExposesLeftoverWithoutApplyingIt) {
  SeedStore("name", "from-older-version");
  {
    Settings settings(settings_directory);
    settings.RegisterSetting("name", "default", Persistence::kDeprecatedRestore, DummyCallback);
    // the caller decides whether the leftover applies
    CheckSettingValue(settings, "name", "default");
    ASSERT_EQ(settings.StoredValue("name"), "from-older-version");

    // run-time changes no longer reach the store
    ASSERT_TRUE(settings.SetValue("name", "changed"));
    settings.SetValueForce("name", "forced");
    ASSERT_EQ(settings.StoredValue("name"), "from-older-version");
  }
  // the leftover is kept across reopens until it is dropped
  ASSERT_EQ(ReadStore("name"), "from-older-version");
  {
    Settings settings(settings_directory);
    settings.RegisterSetting("name", "default", Persistence::kDeprecatedRestore, DummyCallback);
    settings.DropStoredValue("name");
    ASSERT_FALSE(settings.StoredValue("name"));
  }
  ASSERT_FALSE(ReadStore("name"));
}

TEST_F(SettingsTest, DeprecatedRestoreWithoutLeftover) {
  Settings settings(settings_directory);
  settings.RegisterSetting("name", "default", Persistence::kDeprecatedRestore, DummyCallback);
  CheckSettingValue(settings, "name", "default");
  ASSERT_FALSE(settings.StoredValue("name"));
  ASSERT_NO_FATAL_FAILURE(settings.DropStoredValue("name"));
}

TEST_F(SettingsTest, PersistedAndRuntimeOnlyShareOneStore) {
  {
    Settings settings(settings_directory);
    settings.RegisterSetting("license", "", Persistence::kPersisted, DummyCallback);
    settings.RegisterSetting("runtime", "default", Persistence::kRuntimeOnly, DummyCallback);
    ASSERT_TRUE(settings.SetValue("license", "key"));
    ASSERT_TRUE(settings.SetValue("runtime", "changed"));
  }
  {
    Settings settings(settings_directory);
    settings.RegisterSetting("license", "", Persistence::kPersisted, DummyCallback);
    settings.RegisterSetting("runtime", "default", Persistence::kRuntimeOnly, DummyCallback);
    CheckSettingValue(settings, "license", "key");
    CheckSettingValue(settings, "runtime", "default");
  }
}
