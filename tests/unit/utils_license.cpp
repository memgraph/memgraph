#include <gtest/gtest.h>

#include "utils/license.hpp"

class LicenseTest : public ::testing::Test {
 public:
  void SetUp() override {
    utils::Settings::GetInstance().Initialize(settings_directory);
    utils::license::RegisterLicenseSettings();
    utils::license::StartBackgroundLicenseChecker();
  }

  void TearDown() override {
    utils::Settings::GetInstance().Finalize();
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
  std::this_thread::sleep_for(std::chrono::seconds(1));
  ASSERT_TRUE(utils::license::IsValidLicenseFast());
  ASSERT_FALSE(utils::license::IsValidLicense().HasError());

  utils::license::DisableTesting();
  std::this_thread::sleep_for(std::chrono::seconds(1));
  ASSERT_FALSE(utils::license::IsValidLicenseFast());
  ASSERT_TRUE(utils::license::IsValidLicense().HasError());
}
