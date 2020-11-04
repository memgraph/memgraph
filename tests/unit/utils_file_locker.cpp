#include <chrono>
#include <filesystem>
#include <fstream>
#include <random>
#include <thread>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <utils/file_locker.hpp>

using namespace std::chrono_literals;

class FileLockerTest : public ::testing::Test {
 protected:
  std::filesystem::path testing_directory{
      std::filesystem::temp_directory_path() /
      "MG_test_unit_utils_file_locker"};

  void SetUp() override { Clear(); }

  void TearDown() override { Clear(); }

  void CreateFiles(const size_t files_number) {
    const auto save_path = std::filesystem::current_path();
    std::filesystem::create_directory(testing_directory);
    std::filesystem::current_path(testing_directory);

    for (auto i = 1; i <= files_number; ++i) {
      std::ofstream file(fmt::format("{}", i));
    }

    std::filesystem::current_path(save_path);
  }

 private:
  void Clear() {
    if (!std::filesystem::exists(testing_directory)) return;
    std::filesystem::remove_all(testing_directory);
  }
};

TEST_F(FileLockerTest, DeleteWhileLocking) {
  CreateFiles(1);
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
  CreateFiles(1);
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
  CreateFiles(3);
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

TEST_F(FileLockerTest, MultipleLockersAndDeleters) {
  constexpr size_t files_number = 2000;

  CreateFiles(files_number);
  // setup random number generator
  std::random_device r;

  std::default_random_engine engine(r());
  std::uniform_int_distribution<int> random_short_wait(1, 10);
  std::uniform_int_distribution<int> random_wait(1, 100);
  std::uniform_int_distribution<int> file_distribution(0, files_number - 1);

  const auto sleep_for = [&](int milliseconds) {
    std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds));
  };

  const auto random_file = [&]() {
    return testing_directory / fmt::format("{}", file_distribution(engine));
  };

  utils::FileRetainer file_retainer;

  constexpr size_t thread_num = 8;
  constexpr size_t file_access_num = 800;
  constexpr size_t file_delete_num = 1000;

  std::vector<std::thread> accessor_threads;
  accessor_threads.reserve(thread_num);
  for (auto i = 0; i < thread_num - 1; ++i) {
    accessor_threads.emplace_back([&]() {
      sleep_for(random_wait(engine));

      std::vector<std::filesystem::path> locked_files;
      auto locker = file_retainer.AddLocker();
      {
        auto acc = locker.Access();
        for (auto i = 0; i < file_access_num; ++i) {
          auto file = random_file();
          if (acc.AddFile(file)) {
            ASSERT_TRUE(std::filesystem::exists(file));
            locked_files.emplace_back(std::move(file));
          } else {
            ASSERT_FALSE(std::filesystem::exists(file));
          }
          sleep_for(random_short_wait(engine));
        }
      }
      sleep_for(random_wait(engine));
      for (const auto &file : locked_files) {
        ASSERT_TRUE(std::filesystem::exists(file));
      }
    });
  }

  std::vector<std::filesystem::path> deleted_files;
  auto deleter = std::thread([&]() {
    sleep_for(random_short_wait(engine));
    for (auto i = 0; i < file_delete_num; ++i) {
      auto file = random_file();
      if (std::filesystem::exists(file)) {
        file_retainer.DeleteFile(file);
        deleted_files.emplace_back(std::move(file));
      }
      sleep_for(random_short_wait(engine));
    }
  });

  for (auto &thread : accessor_threads) {
    thread.join();
  }

  deleter.join();

  for (const auto &file : deleted_files) {
    ASSERT_FALSE(std::filesystem::exists(file));
  }
}
