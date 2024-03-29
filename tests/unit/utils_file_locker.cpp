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
  std::filesystem::path testing_directory{std::filesystem::temp_directory_path() / "MG_test_unit_utils_file_locker"};

  void SetUp() override { Clear(); }

  void TearDown() override { Clear(); }

  void CreateFiles(const size_t files_number) {
    const auto save_path = std::filesystem::current_path();
    std::filesystem::create_directory(testing_directory);
    std::filesystem::current_path(testing_directory);

    for (auto i = 1; i <= files_number; ++i) {
      std::ofstream(fmt::format("{}", i));
    }

    std::filesystem::current_path(save_path);
  }

 private:
  void Clear() {
    if (!std::filesystem::exists(testing_directory)) return;
    std::filesystem::remove_all(testing_directory);
  }
};

// Test are parameterized based on the type of path used for locking and
// deleting. We test all of the combinations for absolute/relative paths for
// locking path and absolute/relative paths for deleting
// Parameter is represented by tuple (lock_absolute, delete_absolute).
class FileLockerParameterizedTest : public FileLockerTest,
                                    public ::testing::WithParamInterface<std::tuple<bool, bool>> {};

TEST_P(FileLockerParameterizedTest, DeleteWhileLocking) {
  CreateFiles(1);
  memgraph::utils::FileRetainer file_retainer;
  const auto save_path = std::filesystem::current_path();
  std::filesystem::current_path(testing_directory);
  const auto file = std::filesystem::path("1");
  const auto file_absolute = std::filesystem::absolute(file);
  const auto [lock_absolute, delete_absolute] = GetParam();
  {
    auto locker = file_retainer.AddLocker();
    {
      auto acc = locker.Access();
      file_retainer.DeleteFile(delete_absolute ? file_absolute : file);
      ASSERT_TRUE(std::filesystem::exists(file));
    }
  }
  ASSERT_FALSE(std::filesystem::exists(file));

  std::filesystem::current_path(save_path);
}

TEST_P(FileLockerParameterizedTest, DeleteWhileInLocker) {
  CreateFiles(1);
  memgraph::utils::FileRetainer file_retainer;
  const auto save_path = std::filesystem::current_path();
  std::filesystem::current_path(testing_directory);
  const auto file = std::filesystem::path("1");
  const auto file_absolute = std::filesystem::absolute(file);
  const auto [lock_absolute, delete_absolute] = GetParam();
  {
    auto locker = file_retainer.AddLocker();
    {
      auto acc = locker.Access();
      const auto lock_success = acc.AddPath(lock_absolute ? file_absolute : file);
      ASSERT_FALSE(lock_success.HasError());
    }

    file_retainer.DeleteFile(delete_absolute ? file_absolute : file);
    ASSERT_TRUE(std::filesystem::exists(file));
  }

  ASSERT_FALSE(std::filesystem::exists(file));
  std::filesystem::current_path(save_path);
}

TEST_P(FileLockerParameterizedTest, DirectoryLock) {
  memgraph::utils::FileRetainer file_retainer;
  // For this test we create the following file structure
  // testing_directory
  //     1
  //     additional
  //        2
  // We check 2 cases:
  //  - locking the subdirectory "additional", only "2" should be preserved
  //  - locking the directory testing_directory, all of the files shold be
  //    preserved
  ASSERT_TRUE(std::filesystem::create_directory(testing_directory));
  const auto save_path = std::filesystem::current_path();
  std::filesystem::current_path(testing_directory);

  // Create additional directory inside the testing directory with a single file
  const auto additional_directory = std::filesystem::path("additional");
  ASSERT_TRUE(std::filesystem::create_directory(additional_directory));

  const auto nested_file = std::filesystem::path(fmt::format("{}/2", additional_directory.string()));
  const auto nested_file_absolute = std::filesystem::absolute(nested_file);

  const auto file = std::filesystem::path("1");
  const auto file_absolute = std::filesystem::absolute(file);
  const auto directory_lock_test = [&](const bool lock_nested_directory) {
    const auto directory_to_lock = lock_nested_directory ? additional_directory : testing_directory;
    const auto [lock_absolute, delete_absolute] = GetParam();
    std::ofstream(file.string());
    std::ofstream(nested_file.string());
    {
      auto locker = file_retainer.AddLocker();
      {
        auto acc = locker.Access();
        const auto lock_success =
            acc.AddPath(lock_absolute ? std::filesystem::absolute(directory_to_lock) : directory_to_lock);
        ASSERT_FALSE(lock_success.HasError());
      }

      file_retainer.DeleteFile(delete_absolute ? file_absolute : file);
      ASSERT_NE(std::filesystem::exists(file), lock_nested_directory);
      file_retainer.DeleteFile(delete_absolute ? nested_file_absolute : nested_file);
      ASSERT_TRUE(std::filesystem::exists(nested_file));
    }
    ASSERT_FALSE(std::filesystem::exists(file));
    ASSERT_FALSE(std::filesystem::exists(nested_file));
  };

  directory_lock_test(true);
  directory_lock_test(false);

  std::filesystem::current_path(save_path);
}

TEST_P(FileLockerParameterizedTest, RemovePath) {
  memgraph::utils::FileRetainer file_retainer;
  ASSERT_TRUE(std::filesystem::create_directory(testing_directory));
  const auto save_path = std::filesystem::current_path();
  std::filesystem::current_path(testing_directory);
  const auto file = std::filesystem::path("1");
  const auto file_absolute = std::filesystem::absolute(file);
  auto remove_path_test = [&](const bool delete_explicitly_file) {
    const auto [lock_absolute, delete_absolute] = GetParam();
    // Create the file
    std::ofstream(file.string());
    auto locker = file_retainer.AddLocker();
    {
      auto acc = locker.Access();
      const auto lock_success = acc.AddPath(lock_absolute ? file_absolute : file);
      ASSERT_FALSE(lock_success.HasError());
    }

    file_retainer.DeleteFile(delete_absolute ? file_absolute : file);
    ASSERT_TRUE(std::filesystem::exists(file));

    {
      auto acc = locker.Access();
      // If absolute was sent to AddPath method, use relative now
      // to test those combinations.
      acc.RemovePath(lock_absolute ? file : file_absolute);
    }
    if (delete_explicitly_file) {
      file_retainer.DeleteFile(delete_absolute ? file_absolute : file);
    } else {
      file_retainer.CleanQueue();
    }
    ASSERT_FALSE(std::filesystem::exists(file));
  };

  remove_path_test(true);
  remove_path_test(false);
  std::filesystem::current_path(save_path);
}

INSTANTIATE_TEST_CASE_P(FileLockerPathVariantTests, FileLockerParameterizedTest,
                        ::testing::Values(std::make_tuple(false, false), std::make_tuple(false, true),
                                          std::make_tuple(true, false), std::make_tuple(true, true)));

TEST_F(FileLockerTest, MultipleLockers) {
  CreateFiles(3);
  memgraph::utils::FileRetainer file_retainer;
  const auto file1 = testing_directory / "1";
  const auto file2 = testing_directory / "2";
  const auto common_file = testing_directory / "3";

  auto t1 = std::thread([&]() {
    auto locker = file_retainer.AddLocker();
    {
      auto acc = locker.Access();
      const auto lock_success1 = acc.AddPath(file1);
      ASSERT_FALSE(lock_success1.HasError());
      const auto lock_success2 = acc.AddPath(common_file);
      ASSERT_FALSE(lock_success2.HasError());
    }
    std::this_thread::sleep_for(200ms);
  });

  auto t2 = std::thread([&]() {
    auto locker = file_retainer.AddLocker();
    {
      auto acc = locker.Access();
      const auto lock_success1 = acc.AddPath(file2);
      ASSERT_FALSE(lock_success1.HasError());
      const auto lock_success2 = acc.AddPath(common_file);
      ASSERT_FALSE(lock_success2.HasError());
    }
    std::this_thread::sleep_for(200ms);
  });

  auto t3 = std::thread([&]() {
    std::this_thread::sleep_for(50ms);
    file_retainer.DeleteFile(file1);
    file_retainer.DeleteFile(file2);
    file_retainer.DeleteFile(common_file);
    ASSERT_TRUE(std::filesystem::exists(file1));
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
  static constexpr size_t files_number = 2000;

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

  const auto random_file = [&]() { return testing_directory / fmt::format("{}", file_distribution(engine)); };

  memgraph::utils::FileRetainer file_retainer;

  static constexpr size_t thread_num = 8;
  static constexpr size_t file_access_num = 800;
  static constexpr size_t file_delete_num = 1000;

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
          const auto res = acc.AddPath(file);
          if (!res.HasError()) {
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
