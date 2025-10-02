// Copyright 2025 Memgraph Ltd.
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
#include <atomic>
#include <chrono>
#include <future>
#include <latch>
#include <mutex>
#include <thread>
#include <vector>

#include "nlohmann/json.hpp"
#include "utils/safe_string.hpp"

using namespace memgraph::utils;

class SafeStringTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(SafeStringTest, DefaultConstructor) {
  SafeString str;
  EXPECT_EQ(str.str(), "");
}

TEST_F(SafeStringTest, StringConstructor) {
  std::string input = "test string";
  SafeString str(input);
  EXPECT_EQ(str.str(), "test string");
}

TEST_F(SafeStringTest, StringViewConstructor) {
  std::string_view input = "test string view";
  SafeString str(input);
  EXPECT_EQ(str.str(), "test string view");
}

TEST_F(SafeStringTest, CStringConstructor) {
  const char *input = "test c string";
  SafeString str(input);
  EXPECT_EQ(str.str(), "test c string");
}

TEST_F(SafeStringTest, CopyConstructor) {
  SafeString original("original");
  SafeString copy(original);
  EXPECT_EQ(copy.str(), "original");
  EXPECT_EQ(original.str(), "original");
}

TEST_F(SafeStringTest, MoveConstructor) {
  SafeString original("original");
  SafeString moved(std::move(original));
  EXPECT_EQ(moved.str(), "original");
  // After move, original should still be accessible (due to implementation)
  EXPECT_EQ(original.str(), "original");
}

TEST_F(SafeStringTest, CopyAssignment) {
  SafeString str1("first");
  SafeString str2("second");

  str1 = str2;
  EXPECT_EQ(str1.str(), "second");
  EXPECT_EQ(str2.str(), "second");
}

TEST_F(SafeStringTest, CopyAssignmentSelf) {
  SafeString str("test");
  str = str;  // Self-assignment
  EXPECT_EQ(str.str(), "test");
}

TEST_F(SafeStringTest, MoveAssignment) {
  SafeString str1("first");
  SafeString str2("second");

  str1 = std::move(str2);
  EXPECT_EQ(str1.str(), "second");
}

TEST_F(SafeStringTest, MoveAssignmentSelf) {
  SafeString str("test");
  str = std::move(str);  // Self-move assignment
  EXPECT_EQ(str.str(), "test");
}

TEST_F(SafeStringTest, StringAssignment) {
  SafeString str("old");
  str = std::string("new");
  EXPECT_EQ(str.str(), "new");
}

TEST_F(SafeStringTest, StringViewAssignment) {
  SafeString str("old");
  str = std::string_view("new");
  EXPECT_EQ(str.str(), "new");
}

TEST_F(SafeStringTest, StrMethod) {
  SafeString str("test content");
  std::string result = str.str();
  EXPECT_EQ(result, "test content");
}

TEST_F(SafeStringTest, MoveMethod) {
  SafeString str("test content");
  std::string result = str.move();
  EXPECT_EQ(result, "test content");
}

TEST_F(SafeStringTest, StrViewMethod) {
  SafeString str("test content");
  auto wrapper = str.str_view();
  const std::string &view = *wrapper;
  EXPECT_EQ(view, "test content");
}

TEST_F(SafeStringTest, EqualityOperator) {
  SafeString str1("test");
  SafeString str2("test");
  SafeString str3("different");

  EXPECT_TRUE(str1 == str2);
  EXPECT_FALSE(str1 == str3);
  EXPECT_FALSE(str2 == str3);
}

TEST_F(SafeStringTest, EqualityOperatorSelf) {
  SafeString str("test");
  EXPECT_TRUE(str == str);
}

TEST_F(SafeStringTest, MultipleAssignments) {
  SafeString str("initial");
  EXPECT_EQ(str.str(), "initial");

  str = "second";
  EXPECT_EQ(str.str(), "second");

  str = std::string("third");
  EXPECT_EQ(str.str(), "third");

  str = std::string_view("fourth");
  EXPECT_EQ(str.str(), "fourth");
}

TEST_F(SafeStringTest, ConstSafeWrapper) {
  SafeString str("test");
  auto wrapper = str.str_view();

  // Test that the wrapper provides access to the underlying string
  const std::string &view = *wrapper;
  EXPECT_EQ(view, "test");

  // Test that the wrapper can be used multiple times
  EXPECT_EQ((*wrapper).size(), 4);

  // Test that the wrapper can be used multiple times
  EXPECT_EQ((*wrapper).size(), 4);
}

TEST_F(SafeStringTest, ThreadSafetyBasic) {
  SafeString str("initial");
  std::vector<std::thread> threads;

  // Multiple threads reading
  for (int i = 0; i < 10; ++i) {
    threads.emplace_back([&str]() {
      for (int j = 0; j < 100; ++j) {
        EXPECT_EQ(str.str(), "initial");
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }
}

TEST_F(SafeStringTest, ThreadSafetyReadWrite) {
  SafeString str("initial");
  std::vector<std::future<void>> futures;

  // Multiple threads writing
  for (int i = 0; i < 5; ++i) {
    futures.push_back(
        std::async(std::launch::async, [&str, i]() { str = std::string("thread_") + std::to_string(i); }));
  }

  // Multiple threads reading
  for (int i = 0; i < 5; ++i) {
    futures.push_back(std::async(std::launch::async, [&str]() {
      for (int j = 0; j < 100; ++j) {
        std::string result = str.str();
        EXPECT_FALSE(result.empty());
      }
    }));
  }

  for (auto &future : futures) {
    future.wait();
  }
}

TEST_F(SafeStringTest, ThreadSafetyConcurrentAccess) {
  SafeString str("start");
  const int num_threads = 10;
  const int operations_per_thread = 1000;

  auto worker = [&str](int thread_id) {
    for (int i = 0; i < operations_per_thread; ++i) {
      if (i % 2 == 0) {
        // Read operation
        std::string result = str.str();
        EXPECT_FALSE(result.empty());
      } else {
        // Write operation
        str = std::string("thread_") + std::to_string(thread_id) + "_op_" + std::to_string(i);
      }
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back(worker, i);
  }

  for (auto &thread : threads) {
    thread.join();
  }
}

TEST_F(SafeStringTest, ThreadSafetyStrViewConcurrent) {
  SafeString str("initial value");
  const int num_threads = 20;
  std::atomic<int> read_count{0};
  std::atomic<int> write_count{0};

  auto reader_worker = [&str, &read_count]() {
    for (int i = 0; i < 100; ++i) {
      auto wrapper = str.str_view();
      const std::string &view = *wrapper;
      EXPECT_FALSE(view.empty());
      EXPECT_GT(view.length(), 0);
      read_count.fetch_add(1, std::memory_order_relaxed);

      // Use the wrapper multiple times to test lock behavior
      EXPECT_EQ(view.length(), (*wrapper).length());
      EXPECT_FALSE((*wrapper).empty());
    }
  };

  auto writer_worker = [&str, &write_count](int thread_id) {
    for (int i = 0; i < 50; ++i) {
      str = std::string("writer_") + std::to_string(thread_id) + "_iteration_" + std::to_string(i);
      write_count.fetch_add(1, std::memory_order_relaxed);

      // Verify the write took effect
      auto wrapper = str.str_view();
      const std::string &view = *wrapper;
      EXPECT_TRUE(view.find("writer_") == 0);
    }
  };

  std::vector<std::thread> threads;

  // Start multiple reader threads
  for (int i = 0; i < num_threads / 2; ++i) {
    threads.emplace_back(reader_worker);
  }

  // Start multiple writer threads
  for (int i = 0; i < num_threads / 2; ++i) {
    threads.emplace_back(writer_worker, i);
  }

  for (auto &thread : threads) {
    thread.join();
  }

  // Verify that both reading and writing occurred
  EXPECT_GT(read_count.load(), 0);
  EXPECT_GT(write_count.load(), 0);
  EXPECT_GT(read_count.load() + write_count.load(), num_threads);
}

TEST_F(SafeStringTest, ThreadSafetyStrViewMixedOperations) {
  SafeString str("base");
  const int num_threads = 15;
  std::atomic<bool> stop_flag{false};
  std::vector<std::string> captured_values;
  std::mutex captured_mutex;

  auto mixed_worker = [&str, &stop_flag, &captured_values, &captured_mutex](int thread_id) {
    while (!stop_flag.load(std::memory_order_relaxed)) {
      if (thread_id % 3 == 0) {
        // Write operation
        str = std::string("thread_") + std::to_string(thread_id) + "_" + std::to_string(rand());
      } else if (thread_id % 3 == 1) {
        // Read operation using str()
        std::string result = str.str();
        EXPECT_FALSE(result.empty());
      } else {
        // Read operation using str_view()
        auto wrapper = str.str_view();
        const std::string &view = *wrapper;
        EXPECT_FALSE(view.empty());

        // Capture some values for verification
        if (rand() % 100 == 0) {  // 1% chance to capture
          std::lock_guard<std::mutex> lock(captured_mutex);
          captured_values.push_back(view);
        }
      }

      // Small delay to increase contention
      std::this_thread::yield();
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back(mixed_worker, i);
  }

  // Let the threads run for a bit
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Signal threads to stop
  stop_flag.store(true, std::memory_order_relaxed);

  for (auto &thread : threads) {
    thread.join();
  }

  // Verify that we captured some values and they're all valid
  EXPECT_FALSE(captured_values.empty());
  for (const auto &value : captured_values) {
    EXPECT_FALSE(value.empty());
    EXPECT_TRUE(value.find("thread_") == 0 || value == "base");
  }
}

TEST_F(SafeStringTest, ThreadSafetyStrViewConsistency) {
  SafeString str("initial");
  const int num_readers = 10;
  const int num_writers = 5;
  std::atomic<int> successful_reads{0};
  std::atomic<int> successful_writes{0};
  std::vector<std::string> read_values;
  std::mutex read_mutex;

  std::latch latch(num_readers + num_writers);

  auto reader = [&str, &successful_reads, &read_values, &read_mutex, &latch]() {
    latch.arrive_and_wait();
    for (int i = 0; i < 100; ++i) {
      auto wrapper = str.str_view();
      const std::string &view = *wrapper;

      // Verify the view is consistent
      EXPECT_EQ(view.length(), (*wrapper).length());
      EXPECT_EQ(view, (*wrapper));

      // Store the value for later verification
      {
        std::lock_guard<std::mutex> lock(read_mutex);
        read_values.push_back(view);
      }

      successful_reads.fetch_add(1, std::memory_order_relaxed);

      // Small delay to increase contention
      std::this_thread::yield();
    }
  };

  auto writer = [&str, &successful_writes, &latch](int thread_id) {
    latch.arrive_and_wait();
    for (int i = 0; i < 20; ++i) {
      std::string new_value = "writer_" + std::to_string(thread_id) + "_value_" + std::to_string(i);
      str = std::move(new_value);

      successful_writes.fetch_add(1, std::memory_order_relaxed);

      // Small delay to increase contention
      std::this_thread::yield();
    }
  };

  std::vector<std::jthread> threads;

  // Start reader threads
  for (int i = 0; i < num_readers; ++i) {
    threads.emplace_back(reader);
  }

  // Start writer threads
  for (int i = 0; i < num_writers; ++i) {
    threads.emplace_back(writer, i);
  }

  latch.wait();
  threads.clear();

  // Verify that operations completed successfully
  EXPECT_GT(successful_reads.load(), 0);
  EXPECT_GT(successful_writes.load(), 0);
  EXPECT_FALSE(read_values.empty());

  // Verify that all read values are valid
  for (const auto &value : read_values) {
    EXPECT_FALSE(value.empty());
    // Values should either be "initial" or start with "writer_"
    EXPECT_TRUE(value == "initial" || value.find("writer_") == 0);
  }
}

TEST_F(SafeStringTest, ThreadSafetyStrViewWrapperLifetime) {
  SafeString str("test");
  std::atomic<int> successful_operations{0};
  std::vector<std::string> captured_values;
  std::mutex captured_mutex;

  auto worker = [&str, &successful_operations, &captured_values, &captured_mutex]() {
    for (int i = 0; i < 50; ++i) {
      // Create a wrapper and use it multiple times
      auto wrapper = str.str_view();
      const std::string &view1 = *wrapper;

      // Simulate some work that might cause the string to change
      std::this_thread::yield();

      // Use the wrapper again - should still be valid
      const std::string &view2 = *wrapper;

      // Both views should be consistent
      EXPECT_EQ(view1, view2);
      EXPECT_EQ(view1.length(), view2.length());

      // Capture the value for verification
      {
        std::lock_guard<std::mutex> lock(captured_mutex);
        captured_values.push_back(view1);
      }

      successful_operations.fetch_add(1, std::memory_order_relaxed);

      // Small delay to increase contention
      std::this_thread::yield();
    }
  };

  std::vector<std::thread> threads;
  const int num_threads = 8;

  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back(worker);
  }

  for (auto &thread : threads) {
    thread.join();
  }

  // Verify that operations completed successfully
  EXPECT_GT(successful_operations.load(), 0);
  EXPECT_FALSE(captured_values.empty());

  // Verify that all captured values are valid
  for (const auto &value : captured_values) {
    EXPECT_FALSE(value.empty());
    EXPECT_EQ(value, "test");
  }
}

TEST_F(SafeStringTest, JsonSerialization) {
  SafeString str("test json");
  nlohmann::json json_data;

  // Test to_json
  to_json(json_data, str);
  EXPECT_EQ(json_data, "test json");

  // Test from_json
  SafeString new_str;
  from_json(json_data, new_str);
  EXPECT_EQ(new_str.str(), "test json");
}

TEST_F(SafeStringTest, JsonSerializationEmpty) {
  SafeString str("");
  nlohmann::json json_data;

  to_json(json_data, str);
  EXPECT_EQ(json_data, "");

  SafeString new_str;
  from_json(json_data, new_str);
  EXPECT_EQ(new_str.str(), "");
}

TEST_F(SafeStringTest, JsonSerializationSpecialChars) {
  SafeString str("test\n\t\"json\"");
  nlohmann::json json_data;

  to_json(json_data, str);
  EXPECT_EQ(json_data, "test\n\t\"json\"");

  SafeString new_str;
  from_json(json_data, new_str);
  EXPECT_EQ(new_str.str(), "test\n\t\"json\"");
}

TEST_F(SafeStringTest, ConstCorrectness) {
  const SafeString const_str("const test");

  // Should be able to call const methods
  std::string result = const_str.str();
  EXPECT_EQ(result, "const test");

  auto wrapper = const_str.str_view();
  const std::string &view = *wrapper;
  EXPECT_EQ(view, "const test");
}

TEST_F(SafeStringTest, ExceptionSafety) {
  SafeString str("test");

  // Test that operations don't throw exceptions under normal circumstances
  EXPECT_NO_THROW(str = "new value");
  EXPECT_NO_THROW(str.str());
  EXPECT_NO_THROW(str.move());
  EXPECT_NO_THROW(str.str_view());
}

TEST_F(SafeStringTest, PerformanceStressTest) {
  SafeString str("initial");
  const int num_operations = 10000;

  auto start = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < num_operations; ++i) {
    str = std::string("value_") + std::to_string(i);
    std::string result = str.str();
    EXPECT_EQ(result, "value_" + std::to_string(i));
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  // This should complete reasonably quickly (less than 1 second)
  EXPECT_LT(duration.count(), 1000);
}

TEST_F(SafeStringTest, MemoryConsistency) {
  SafeString str("test");

  // Multiple reads should return consistent values
  std::string val1 = str.str();
  std::string val2 = str.str();
  std::string val3 = str.str();

  EXPECT_EQ(val1, val2);
  EXPECT_EQ(val2, val3);
  EXPECT_EQ(val1, "test");
}

TEST_F(SafeStringTest, AssignmentChain) {
  SafeString str1("first");
  SafeString str2("second");
  SafeString str3("third");

  // Test assignment chaining
  str1 = str2 = str3;

  EXPECT_EQ(str1.str(), "third");
  EXPECT_EQ(str2.str(), "third");
  EXPECT_EQ(str3.str(), "third");
}

TEST_F(SafeStringTest, StringViewConsistency) {
  SafeString str("test string");
  std::string_view sv = "test string";

  // Test that string_view constructor and assignment work consistently
  SafeString str2(sv);
  EXPECT_EQ(str2.str(), str.str());

  str = sv;
  EXPECT_EQ(str.str(), "test string");
}
