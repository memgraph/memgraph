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
  utils::FileRetainer file_retainer;
  auto t1 = std::thread([&]() {
    auto locker = file_retainer.AddLocker();
    {
      auto acc = locker.Access();
      std::this_thread::sleep_for(100ms);
    }
  });
  const auto file = testing_directory / "1";
  auto t2 = std::thread([&]() {
    std::this_thread::sleep_for(50ms);
    file_retainer.DeleteFile(file);
    ASSERT_TRUE(std::filesystem::exists(file));
  });

  t1.join();
  t2.join();
  ASSERT_FALSE(std::filesystem::exists(file));
}

TEST_F(FileLockerTest, DeleteWhileInLocker) {
  utils::FileRetainer file_retainer;
  const auto file = testing_directory / "1";
  auto t1 = std::thread([&]() {
    auto locker = file_retainer.AddLocker();
    {
      auto acc = locker.Access();
      acc.AddFile(file);
    }
    std::this_thread::sleep_for(100ms);
  });

  auto t2 = std::thread([&]() {
    std::this_thread::sleep_for(50ms);
    file_retainer.DeleteFile(file);
    ASSERT_TRUE(std::filesystem::exists(file));
  });

  t1.join();
  t2.join();
  ASSERT_FALSE(std::filesystem::exists(file));
}

TEST_F(FileLockerTest, MultipleLockers) {
  utils::FileRetainer file_retainer;
  const auto file1 = testing_directory / "1";
  const auto file2 = testing_directory / "2";
  const auto common_file = testing_directory / "3";

  auto t1 = std::thread([&]() {
    auto locker = file_retainer.AddLocker();
    {
      auto acc = locker.Access();
      acc.AddFile(file1);
      acc.AddFile(common_file);
    }
  });

  auto t2 = std::thread([&]() {
    auto locker = file_retainer.AddLocker();
    {
      auto acc = locker.Access();
      acc.AddFile(file2);
      acc.AddFile(common_file);
    }
    std::this_thread::sleep_for(200ms);
  });

  auto t3 = std::thread([&]() {
    std::this_thread::sleep_for(50ms);
    file_retainer.DeleteFile(file1);
    file_retainer.DeleteFile(file2);
    file_retainer.DeleteFile(common_file);
    ASSERT_FALSE(std::filesystem::exists(file1));
    ASSERT_TRUE(std::filesystem::exists(file2));
    ASSERT_TRUE(std::filesystem::exists(common_file));
  });

  t1.join();
  t2.join();
  t3.join();
  ASSERT_FALSE(std::filesystem::exists(file1));
  ASSERT_FALSE(std::filesystem::exists(file2));
  ASSERT_FALSE(std::filesystem::exists(common_file));
}
