#include <gtest/gtest.h>

#include "utils/license.hpp"
#include "utils/settings.hpp"

class LicenseTest : public ::testing::Test {
 public:
  void SetUp() override {
    utils::global_settings.Initialize(settings_directory);
    utils::license::RegisterLicenseSettings();
    utils::license::StartBackgroundLicenseChecker();
  }

  void TearDown() override {
    utils::global_settings.Finalize();
    std::filesystem::remove_all(test_directory);
    utils::license::StopBackgroundLicenseChecker();
  }

 protected:
  const std::filesystem::path test_directory{"MG_tests_unit_utils_settings"};
  const std::filesystem::path settings_directory{test_directory / "settings"};
};

TEST_F(LicenseTest, EncodeDecode) {
  const std::array licenses = {
      utils::license::License{"Organization", 1, 2},
      utils::license::License{"", -1, 0},
      utils::license::License{"Some very long name for the organization Ltd", -999, -9999},
  };

  for (const auto &license : licenses) {
    const auto result = utils::license::Encode(license);
    auto maybe_license = utils::license::Decode(result);
    ASSERT_TRUE(maybe_license);
    ASSERT_EQ(*maybe_license, license);
  }
}

TEST_F(LicenseTest, TestingFlag) {
  ASSERT_FALSE(utils::license::IsValidLicenseFast());
  ASSERT_TRUE(utils::license::IsValidLicense().HasError());

  utils::license::EnableTesting();
  ASSERT_EQ(utils::license::IsValidLicenseFast(), true);
  ASSERT_FALSE(utils::license::IsValidLicense().HasError());

  utils::license::DisableTesting();
  ASSERT_EQ(utils::license::IsValidLicenseFast(), false);
  ASSERT_TRUE(utils::license::IsValidLicense().HasError());
}

TEST_F(LicenseTest, LicenseOrganizationName) {
  const std::string organization_name{"Memgraph"};
  utils::license::License license{.organization_name = organization_name, .valid_until = 0, .memory_limit = 0};

  utils::global_settings.SetValue("enterprise.license", utils::license::Encode(license));
  utils::global_settings.SetValue("organization.name", organization_name);
  ASSERT_FALSE(utils::license::IsValidLicense().HasError());
  ASSERT_EQ(utils::license::IsValidLicenseFast(), true);

  utils::global_settings.SetValue("organization.name", fmt::format("{}modified", organization_name));
  ASSERT_TRUE(utils::license::IsValidLicense().HasError());
  ASSERT_EQ(utils::license::IsValidLicenseFast(), false);
}
