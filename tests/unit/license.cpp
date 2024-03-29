// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gtest/gtest.h>
#include <cstdint>

#include "license/license.hpp"
#include "utils/settings.hpp"

class LicenseTest : public ::testing::Test {
 public:
  void SetUp() override {
    settings.emplace();
    settings->Initialize(settings_directory);

    license_checker.emplace();
    memgraph::license::RegisterLicenseSettings(*license_checker, *settings);

    license_checker->StartBackgroundLicenseChecker(*settings);
  }

  void TearDown() override { std::filesystem::remove_all(test_directory); }

 protected:
  const std::filesystem::path test_directory{"MG_tests_unit_utils_license"};
  const std::filesystem::path settings_directory{test_directory / "settings"};

  void CheckLicenseValidity(const bool expected_valid) {
    ASSERT_EQ(!license_checker->IsEnterpriseValid(*settings).HasError(), expected_valid);
    ASSERT_EQ(license_checker->IsEnterpriseValidFast(), expected_valid);
  }

  std::optional<memgraph::utils::Settings> settings;
  std::optional<memgraph::license::LicenseChecker> license_checker;
};

TEST_F(LicenseTest, EncodeDecode) {
  const std::array licenses = {
      memgraph::license::License{"Organization", 1, 2, memgraph::license::LicenseType::OEM},
      memgraph::license::License{"", -1, 0, memgraph::license::LicenseType::ENTERPRISE},
      memgraph::license::License{"Some very long name for the organization Ltd", -999, -9999,
                                 memgraph::license::LicenseType::ENTERPRISE},
  };

  for (const auto &license : licenses) {
    const auto result = memgraph::license::Encode(license);
    auto maybe_license = memgraph::license::Decode(result);
    ASSERT_TRUE(maybe_license);
    ASSERT_EQ(*maybe_license, license);
  }
}

TEST_F(LicenseTest, TestingFlag) {
  CheckLicenseValidity(false);

  license_checker->EnableTesting();
  CheckLicenseValidity(true);

  SCOPED_TRACE("EnableTesting shouldn't be affected by settings change");
  settings->SetValue("enterprise.license", "");
  CheckLicenseValidity(true);
}

TEST_F(LicenseTest, LicenseOrganizationName) {
  const std::string organization_name{"Memgraph"};
  memgraph::license::License license{organization_name, 0, 0, memgraph::license::LicenseType::ENTERPRISE};

  settings->SetValue("enterprise.license", memgraph::license::Encode(license));
  settings->SetValue("organization.name", organization_name);
  CheckLicenseValidity(true);

  settings->SetValue("organization.name", fmt::format("{}modified", organization_name));
  CheckLicenseValidity(false);

  settings->SetValue("organization.name", organization_name);
  CheckLicenseValidity(true);
}

TEST_F(LicenseTest, Expiration) {
  const std::string organization_name{"Memgraph"};

  {
    const auto now =
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch());
    const auto delta = std::chrono::seconds(1);
    const auto valid_until = now + delta;
    memgraph::license::License license{organization_name, valid_until.count(), 0,
                                       memgraph::license::LicenseType::ENTERPRISE};

    settings->SetValue("enterprise.license", memgraph::license::Encode(license));
    settings->SetValue("organization.name", organization_name);
    CheckLicenseValidity(true);

    std::this_thread::sleep_for(delta + std::chrono::seconds(1));
    ASSERT_TRUE(license_checker->IsEnterpriseValid(*settings).HasError());
    // We can't check fast checker because it has unknown refresh rate
  }
  {
    SCOPED_TRACE("License with valid_until = 0 is always valid");
    memgraph::license::License license{organization_name, 0, 0, memgraph::license::LicenseType::ENTERPRISE};
    settings->SetValue("enterprise.license", memgraph::license::Encode(license));
    settings->SetValue("organization.name", organization_name);
    CheckLicenseValidity(true);
  }
}

TEST_F(LicenseTest, LicenseInfoOverride) {
  CheckLicenseValidity(false);

  const std::string organization_name{"Memgraph"};
  memgraph::license::License license{organization_name, 0, 0, memgraph::license::LicenseType::ENTERPRISE};
  const std::string license_key = memgraph::license::Encode(license);

  {
    SCOPED_TRACE("Checker should use overrides instead of info from the settings");
    license_checker->SetLicenseInfoOverride(license_key, organization_name);
    CheckLicenseValidity(true);
  }
  {
    SCOPED_TRACE("License info override shouldn't be affected by settings change");
    settings->SetValue("enterprise.license", "INVALID");
    CheckLicenseValidity(true);
  }
  {
    SCOPED_TRACE("Override with invalid key");
    license_checker->SetLicenseInfoOverride("INVALID", organization_name);
    CheckLicenseValidity(false);
    settings->SetValue("enterprise.license", license_key);
    settings->SetValue("organization.name", organization_name);
    CheckLicenseValidity(false);
  }
}

TEST_F(LicenseTest, LicenseType) {
  CheckLicenseValidity(false);
  const std::string organization_name{"Memgraph"};
  {
    memgraph::license::License license_entr{organization_name, 0, 0, memgraph::license::LicenseType::ENTERPRISE};
    const std::string license_key = memgraph::license::Encode(license_entr);
    license_checker->SetLicenseInfoOverride(license_key, organization_name);
    CheckLicenseValidity(true);
  }
  {
    memgraph::license::License license_oem{organization_name, 0, 0, memgraph::license::LicenseType::OEM};
    const std::string license_key = memgraph::license::Encode(license_oem);
    license_checker->SetLicenseInfoOverride(license_key, organization_name);
    CheckLicenseValidity(false);
  }
  {
    memgraph::license::License license_oem{organization_name, 0, 0, memgraph::license::LicenseType::OEM};
    const std::string license_key = memgraph::license::Encode(license_oem);
    license_checker->SetLicenseInfoOverride(license_key, organization_name);
    CheckLicenseValidity(false);
  }
  {
    memgraph::license::License license_oem{organization_name, std::numeric_limits<int64_t>::min(),
                                           std::numeric_limits<int64_t>::max(), memgraph::license::LicenseType::OEM};
    const std::string license_key = memgraph::license::Encode(license_oem);
    license_checker->SetLicenseInfoOverride(license_key, organization_name);
    CheckLicenseValidity(false);
  }
  {
    memgraph::license::License license_oem{organization_name, std::numeric_limits<int64_t>::max(),
                                           std::numeric_limits<int64_t>::min(), memgraph::license::LicenseType::OEM};
    const std::string license_key = memgraph::license::Encode(license_oem);
    license_checker->SetLicenseInfoOverride(license_key, organization_name);
    CheckLicenseValidity(false);
  }
}
