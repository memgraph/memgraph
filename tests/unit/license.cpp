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

#include <gtest/gtest.h>
#include <cstdint>
#include <memory>

#include "license/license.hpp"
#include "utils/settings.hpp"

class LicenseTest : public ::testing::Test {
 public:
  void SetUp() override {
    settings = std::make_shared<memgraph::utils::Settings>(settings_directory);

    license_checker.emplace();
    memgraph::license::RegisterLicenseSettings(*license_checker, *settings);

    license_checker->StartBackgroundLicenseChecker(settings);
  }

  void TearDown() override { std::filesystem::remove_all(test_directory); }

 protected:
  const std::filesystem::path test_directory{"MG_tests_unit_utils_license"};
  const std::filesystem::path settings_directory{test_directory / "settings"};

  void CheckLicenseValidity(const bool expected_valid) {
    ASSERT_EQ(license_checker->IsEnterpriseValid(*settings).has_value(), expected_valid);
    ASSERT_EQ(license_checker->IsEnterpriseValidFast(), expected_valid);
  }

  std::shared_ptr<memgraph::utils::Settings> settings;
  std::optional<memgraph::license::LicenseChecker> license_checker;
};

TEST_F(LicenseTest, EncodeDecode) {
  const std::array licenses = {
      memgraph::license::License{"Organization", 1, 2, memgraph::license::LicenseType::OEM},
      memgraph::license::License{"", -1, 0, memgraph::license::LicenseType::ENTERPRISE},
      memgraph::license::License{
          "Some very long name for the organization Ltd", -999, -9999, memgraph::license::LicenseType::ENTERPRISE},
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
    memgraph::license::License license{
        organization_name, valid_until.count(), 0, memgraph::license::LicenseType::ENTERPRISE};

    settings->SetValue("enterprise.license", memgraph::license::Encode(license));
    settings->SetValue("organization.name", organization_name);
    CheckLicenseValidity(true);

    std::this_thread::sleep_for(delta + std::chrono::seconds(1));
    ASSERT_FALSE(license_checker->IsEnterpriseValid(*settings).has_value());
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

TEST_F(LicenseTest, CliLicenseCheckSettings) {
  const std::string organization_name{"Memgraph"};
  memgraph::license::License license{organization_name, 0, 0, memgraph::license::LicenseType::ENTERPRISE};
  const std::string license_key = memgraph::license::Encode(license);
  SCOPED_TRACE("Checker should use overrides instead of info from the settings");
  license_checker->SetCliLicense(license_key, organization_name, *settings);
  CheckLicenseValidity(true);
  ASSERT_EQ(settings->GetValue("enterprise.license").value(), license_key);
  ASSERT_EQ(settings->GetValue("organization.name").value(), "Memgraph");
}

TEST_F(LicenseTest, CliLicense) {
  CheckLicenseValidity(false);

  const std::string organization_name{"Memgraph"};
  memgraph::license::License license{organization_name, 0, 0, memgraph::license::LicenseType::ENTERPRISE};
  const std::string license_key = memgraph::license::Encode(license);

  {
    SCOPED_TRACE("Checker should use CLI info instead of info from the settings");
    license_checker->SetCliLicense(license_key, organization_name, *settings);
    CheckLicenseValidity(true);
  }
}

TEST_F(LicenseTest, LicenseType) {
  CheckLicenseValidity(false);
  const std::string organization_name{"Memgraph"};
  {
    memgraph::license::License license_entr{organization_name, 0, 0, memgraph::license::LicenseType::ENTERPRISE};
    const std::string license_key = memgraph::license::Encode(license_entr);
    license_checker->SetCliLicense(license_key, organization_name, *settings);
    CheckLicenseValidity(true);
  }
  {
    memgraph::license::License license_oem{organization_name, 0, 0, memgraph::license::LicenseType::OEM};
    const std::string license_key = memgraph::license::Encode(license_oem);
    license_checker->SetCliLicense(license_key, organization_name, *settings);
    CheckLicenseValidity(false);
  }
  {
    memgraph::license::License license_oem{organization_name, 0, 0, memgraph::license::LicenseType::OEM};
    const std::string license_key = memgraph::license::Encode(license_oem);
    license_checker->SetCliLicense(license_key, organization_name, *settings);
    CheckLicenseValidity(false);
  }
  {
    memgraph::license::License license_oem{organization_name,
                                           std::numeric_limits<int64_t>::min(),
                                           std::numeric_limits<int64_t>::max(),
                                           memgraph::license::LicenseType::OEM};
    const std::string license_key = memgraph::license::Encode(license_oem);
    license_checker->SetCliLicense(license_key, organization_name, *settings);
    CheckLicenseValidity(false);
  }
  {
    memgraph::license::License license_oem{organization_name,
                                           std::numeric_limits<int64_t>::max(),
                                           std::numeric_limits<int64_t>::min(),
                                           memgraph::license::LicenseType::OEM};
    const std::string license_key = memgraph::license::Encode(license_oem);
    license_checker->SetCliLicense(license_key, organization_name, *settings);
    CheckLicenseValidity(false);
  }
}

TEST_F(LicenseTest, DetailedLicenseInfo_Enterprise) {
  const std::string organization_name{"Memgraph"};
  memgraph::license::License license_entr{organization_name, 0, 0, memgraph::license::LicenseType::ENTERPRISE};
  const std::string license_key = memgraph::license::Encode(license_entr);
  license_checker->SetCliLicense(license_key, organization_name, *settings);
  auto detailed_license_info = license_checker->GetDetailedLicenseInfo();
  ASSERT_EQ(detailed_license_info.organization_name, "Memgraph");
  ASSERT_TRUE(detailed_license_info.is_valid);
  ASSERT_EQ(detailed_license_info.license_type, "enterprise");
  ASSERT_EQ(detailed_license_info.valid_until, "FOREVER");
  ASSERT_EQ(detailed_license_info.memory_limit, 0);
  ASSERT_EQ(detailed_license_info.status, "You are running a valid Memgraph Enterprise License.");
}

TEST_F(LicenseTest, DetailedLicenseInfo_OemExpired) {
  // An expired OEM key produces no valid candidate; previous_license_info_ is reset.
  // GetDetailedLicenseInfo returns is_valid=false with no license details.
  const std::string organization_name{"Memgraph"};
  memgraph::license::License license_oem{organization_name, 1, 6, memgraph::license::LicenseType::OEM};
  const std::string license_key = memgraph::license::Encode(license_oem);
  license_checker->SetCliLicense(license_key, organization_name, *settings);
  auto detailed_license_info = license_checker->GetDetailedLicenseInfo();
  ASSERT_FALSE(detailed_license_info.is_valid);
}

TEST_F(LicenseTest, DetailedLicenseInfo_OemNonExpiredIsInvalid) {
  SCOPED_TRACE("Valid non-expired OEM license must report is_valid=false (not enterprise type)");
  const std::string organization_name{"Memgraph"};
  memgraph::license::License license_oem{organization_name, 0, 0, memgraph::license::LicenseType::OEM};
  const std::string license_key = memgraph::license::Encode(license_oem);
  license_checker->SetCliLicense(license_key, organization_name, *settings);
  auto detailed_license_info = license_checker->GetDetailedLicenseInfo();
  ASSERT_FALSE(detailed_license_info.is_valid);
  ASSERT_EQ(detailed_license_info.license_type, "oem");
}

TEST_F(LicenseTest, DetailedLicenseInfo_MismatchedOrgName) {
  SCOPED_TRACE("Enterprise key with mismatched organization name must report is_valid=false");
  // Org-name mismatch means no valid candidate; previous_license_info_ is reset.
  const std::string organization_name{"Memgraph"};
  memgraph::license::License license{organization_name, 0, 0, memgraph::license::LicenseType::ENTERPRISE};
  const std::string license_key = memgraph::license::Encode(license);
  license_checker->SetCliLicense(license_key, fmt::format("{}wrong", organization_name), *settings);
  auto detailed_license_info = license_checker->GetDetailedLicenseInfo();
  ASSERT_FALSE(detailed_license_info.is_valid);
}

// ===========================================================================
// Multi-source selection
// ===========================================================================

// DB only valid, no CLI set → DB wins.
TEST_F(LicenseTest, MultiSource_DbOnly) {
  const std::string org{"Memgraph"};
  memgraph::license::License lic{org, 0, 0, memgraph::license::LicenseType::ENTERPRISE};
  settings->SetValue("enterprise.license", memgraph::license::Encode(lic));
  settings->SetValue("organization.name", org);
  CheckLicenseValidity(true);
}

// CLI and DB carry the same key (same expiry) → CLI wins on source priority.
TEST_F(LicenseTest, MultiSource_CliWinsTiebreak) {
  const std::string org{"Memgraph"};
  memgraph::license::License lic{org, 0, 0, memgraph::license::LicenseType::ENTERPRISE};
  const std::string key = memgraph::license::Encode(lic);
  // Seed DB first.
  settings->SetValue("enterprise.license", key);
  settings->SetValue("organization.name", org);
  // Set CLI with the identical key; CLI should win (priority 2 > 0).
  license_checker->SetCliLicense(key, org, *settings);
  CheckLicenseValidity(true);
  // Settings must reflect the CLI winner written back by SetValueForce.
  ASSERT_EQ(settings->GetValue("enterprise.license").value(), key);
}

// DB has later expiry (forever) than CLI (finite) → DB wins despite lower source priority.
TEST_F(LicenseTest, MultiSource_DbWinsOnExpiry) {
  const std::string org{"Memgraph"};
  const auto now =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

  // CLI key: expires in 10 seconds.
  memgraph::license::License cli_lic{org, now + 10, 0, memgraph::license::LicenseType::ENTERPRISE};
  license_checker->SetCliLicense(memgraph::license::Encode(cli_lic), org, *settings);

  // DB key: valid_until=0 (forever), treated as INT64_MAX in the comparison.
  memgraph::license::License db_lic{org, 0, 0, memgraph::license::LicenseType::ENTERPRISE};
  settings->SetValue("enterprise.license", memgraph::license::Encode(db_lic));
  settings->SetValue("organization.name", org);

  CheckLicenseValidity(true);
  // The DB key (forever) must be the winner written back to settings.
  ASSERT_EQ(settings->GetValue("enterprise.license").value(), memgraph::license::Encode(db_lic));
}

// CLI has later expiry (forever) than DB (finite) → CLI wins.
TEST_F(LicenseTest, MultiSource_CliWinsOnExpiry) {
  const std::string org{"Memgraph"};
  const auto now =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

  // DB key: expires in 10 seconds.
  memgraph::license::License db_lic{org, now + 10, 0, memgraph::license::LicenseType::ENTERPRISE};
  settings->SetValue("enterprise.license", memgraph::license::Encode(db_lic));
  settings->SetValue("organization.name", org);

  // CLI key: valid forever (0).
  memgraph::license::License cli_lic{org, 0, 0, memgraph::license::LicenseType::ENTERPRISE};
  const std::string cli_key = memgraph::license::Encode(cli_lic);
  license_checker->SetCliLicense(cli_key, org, *settings);

  CheckLicenseValidity(true);
  // CLI (forever) must be the winner persisted to settings.
  ASSERT_EQ(settings->GetValue("enterprise.license").value(), cli_key);
}

// valid_until=0 (forever) beats any finite expiry regardless of source.
TEST_F(LicenseTest, MultiSource_ForeverBeatsFinite) {
  const std::string org{"Memgraph"};
  const auto now =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

  // DB: large finite expiry.
  memgraph::license::License db_lic{org, now + 10000, 0, memgraph::license::LicenseType::ENTERPRISE};
  settings->SetValue("enterprise.license", memgraph::license::Encode(db_lic));
  settings->SetValue("organization.name", org);

  // CLI: forever (0) — must win even though DB timestamp is numerically large.
  memgraph::license::License cli_lic{org, 0, 0, memgraph::license::LicenseType::ENTERPRISE};
  const std::string cli_key = memgraph::license::Encode(cli_lic);
  license_checker->SetCliLicense(cli_key, org, *settings);

  CheckLicenseValidity(true);
  ASSERT_EQ(settings->GetValue("enterprise.license").value(), cli_key);
}

// CLI key has mismatched org name, DB has a fully valid key → DB wins, enterprise active.
TEST_F(LicenseTest, MultiSource_DbWinsWhenCliMismatch) {
  const std::string org{"Memgraph"};

  // DB: correct key + org.
  memgraph::license::License db_lic{org, 0, 0, memgraph::license::LicenseType::ENTERPRISE};
  settings->SetValue("enterprise.license", memgraph::license::Encode(db_lic));
  settings->SetValue("organization.name", org);

  // CLI: same key but presented with the wrong org name → fails validation.
  license_checker->SetCliLicense(memgraph::license::Encode(db_lic), fmt::format("{}wrong", org), *settings);

  CheckLicenseValidity(true);
}

// ===========================================================================
// Validator behavior on SET DATABASE SETTING
// ===========================================================================

// Malformed / garbled key → validator throws immediately; write is rejected.
TEST_F(LicenseTest, Validator_MalformedKeyThrows) {
  EXPECT_THROW(settings->SetValue("enterprise.license", "not-a-valid-key"), memgraph::utils::BasicException);
  CheckLicenseValidity(false);
}

// Already-expired key → validator throws; write is rejected.
TEST_F(LicenseTest, Validator_ExpiredKeyThrows) {
  const std::string org{"Memgraph"};
  // valid_until=1 is epoch 1970-01-01, always expired.
  memgraph::license::License expired{org, 1, 0, memgraph::license::LicenseType::ENTERPRISE};
  EXPECT_THROW(settings->SetValue("enterprise.license", memgraph::license::Encode(expired)),
               memgraph::utils::BasicException);
  CheckLicenseValidity(false);
}

// Valid key with a mismatched org name → write succeeds (validator only checks key format/expiry).
// The org-name mismatch is caught later in RevalidateLicense, not at write time.
TEST_F(LicenseTest, Validator_OrgMismatchWriteSucceeds) {
  const std::string org{"Memgraph"};
  memgraph::license::License lic{org, 0, 0, memgraph::license::LicenseType::ENTERPRISE};
  ASSERT_TRUE(settings->SetValue("enterprise.license", memgraph::license::Encode(lic)));
  settings->SetValue("organization.name", fmt::format("{}wrong", org));
  // Mismatch detected at revalidation, not at write time → invalid.
  CheckLicenseValidity(false);
}

// Empty string → clearing is always allowed; no exception thrown.
TEST_F(LicenseTest, Validator_EmptyStringAllowed) {
  const std::string org{"Memgraph"};
  memgraph::license::License lic{org, 0, 0, memgraph::license::LicenseType::ENTERPRISE};
  settings->SetValue("enterprise.license", memgraph::license::Encode(lic));
  settings->SetValue("organization.name", org);
  CheckLicenseValidity(true);

  ASSERT_TRUE(settings->SetValue("enterprise.license", ""));
  CheckLicenseValidity(false);
}

// ===========================================================================
// Live update while running
// ===========================================================================

// CLI is active; DB is updated with a longer-lived key → DB becomes winner.
TEST_F(LicenseTest, LiveUpdate_DbLongerExpiry) {
  const std::string org{"Memgraph"};
  const auto now =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

  // Start: CLI key with a finite expiry.
  memgraph::license::License cli_lic{org, now + 10, 0, memgraph::license::LicenseType::ENTERPRISE};
  license_checker->SetCliLicense(memgraph::license::Encode(cli_lic), org, *settings);
  CheckLicenseValidity(true);

  // Update DB with a forever key (longer-lived) → DB becomes the new winner.
  memgraph::license::License db_lic{org, 0, 0, memgraph::license::LicenseType::ENTERPRISE};
  const std::string db_key = memgraph::license::Encode(db_lic);
  settings->SetValue("enterprise.license", db_key);
  settings->SetValue("organization.name", org);
  CheckLicenseValidity(true);
  // The forever key must now be the winner persisted to settings.
  ASSERT_EQ(settings->GetValue("enterprise.license").value(), db_key);
}

// CLI is active; DB is updated with a shorter-lived key → CLI stays winner.
// SetValueForce re-persists CLI's key back to settings after revalidation.
TEST_F(LicenseTest, LiveUpdate_CliLongerExpiry) {
  const std::string org{"Memgraph"};
  const auto now =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

  // Start: CLI key with forever validity.
  memgraph::license::License cli_lic{org, 0, 0, memgraph::license::LicenseType::ENTERPRISE};
  const std::string cli_key = memgraph::license::Encode(cli_lic);
  license_checker->SetCliLicense(cli_key, org, *settings);
  CheckLicenseValidity(true);

  // DB key with a short finite expiry.
  memgraph::license::License db_lic{org, now + 10, 0, memgraph::license::LicenseType::ENTERPRISE};
  settings->SetValue("enterprise.license", memgraph::license::Encode(db_lic));
  settings->SetValue("organization.name", org);
  // CLI (forever) still wins; SetValueForce re-persists CLI key back to settings.
  CheckLicenseValidity(true);
  ASSERT_EQ(settings->GetValue("enterprise.license").value(), cli_key);
}

// ===========================================================================
// Community mode transitions
// ===========================================================================

// All sources fail → is_valid_ goes false, license info cleared.
TEST_F(LicenseTest, CommunityMode_AllSourcesFail) {
  const std::string org{"Memgraph"};
  memgraph::license::License lic{org, 0, 0, memgraph::license::LicenseType::ENTERPRISE};
  settings->SetValue("enterprise.license", memgraph::license::Encode(lic));
  settings->SetValue("organization.name", org);
  CheckLicenseValidity(true);

  // Break the only source: org name mismatch → no valid candidate.
  settings->SetValue("organization.name", fmt::format("{}wrong", org));
  CheckLicenseValidity(false);
  auto info = license_checker->GetDetailedLicenseInfo();
  ASSERT_FALSE(info.is_valid);
}

// Transition: valid license → org name changed to mismatch → community mode.
TEST_F(LicenseTest, CommunityMode_OrgNameChangedToMismatch) {
  const std::string org{"Memgraph"};
  memgraph::license::License lic{org, 0, 0, memgraph::license::LicenseType::ENTERPRISE};
  settings->SetValue("enterprise.license", memgraph::license::Encode(lic));
  settings->SetValue("organization.name", org);
  CheckLicenseValidity(true);

  settings->SetValue("organization.name", "SomeOtherOrg");
  ASSERT_FALSE(license_checker->IsEnterpriseValid(*settings).has_value());
  ASSERT_FALSE(license_checker->IsEnterpriseValidFast());
}
