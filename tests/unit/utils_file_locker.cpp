#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <utils/file_locker.hpp>

using namespace std::chrono_literals;

class FileLockerTest : public ::testing::Test {
 protected:
  std::filesystem::path testing_directory{
      std::filesystem::temp_directory_path() /
      "MG_test_unit_utils_file_lcoker"};

  void SetUp() override {
    Clear();
    auto save_path = std::filesystem::current_path();
    std::filesystem::create_directory(testing_directory);
    std::filesystem::current_path(testing_directory);

    for (auto i = 0; i < 100; ++i) {
      std::ofstream file(fmt::format("{}", i));
    }

    std::filesystem::current_path(save_path);
  }

  void TearDown() override { Clear(); }

 private:
  void Clear() {
    if (!std::filesystem::exists(testing_directory)) return;
    std::filesystem::remove_all(testing_directory);
  }
};

TEST_F(FileLockerTest, DeleteWhileLocking) {
  utils::FileLockerManager manager;
  auto t1 = std::thread([&]() {
    auto locker = manager.AddLocker();
    {
      auto acc = locker.Access();
      std::this_thread::sleep_for(100ms);
    }
  });
  const auto file = testing_directory / "1";
  auto t2 = std::thread([&]() {
    std::this_thread::sleep_for(50ms);
    manager.DeleteFile(file);
    ASSERT_TRUE(std::filesystem::exists(file));
  });

  t1.join();
  t2.join();
  ASSERT_FALSE(std::filesystem::exists(file));
}
