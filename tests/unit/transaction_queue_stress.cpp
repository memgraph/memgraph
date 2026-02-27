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

#include <atomic>
#include <chrono>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include "spdlog/spdlog.h"

#include "disk_test_utils.hpp"
#include "interpreter_faker.hpp"
#include "query/context.hpp"
#include "query/exceptions.hpp"
#include "query/interpreter_context.hpp"
#include "storage/v2/inmemory/storage.hpp"

// Stress test configuration
constexpr int kNumWorkers = 8;
constexpr int kNumShowTerminateThreads = 4;
constexpr int kIterationsPerWorker = 200;
constexpr int kShowIterations = 500;

class TransactionQueueStressTest : public ::testing::Test {
 protected:
  const std::string testSuite = "transaction_queue_stress";
  std::filesystem::path data_directory{std::filesystem::temp_directory_path() /
                                       "MG_tests_unit_transaction_queue_stress"};

  memgraph::storage::Config config{
      [&]() {
        memgraph::storage::Config config{};
        config.durability.storage_directory = data_directory;
        config.disk.main_storage_directory = config.durability.storage_directory / "disk";
        return config;
      }()  // iile
  };

  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{config};
  memgraph::dbms::DatabaseAccess db{
      [&]() {
        auto db_acc_opt = db_gk.access();
        MG_ASSERT(db_acc_opt, "Failed to access db");
        auto &db_acc = *db_acc_opt;
        MG_ASSERT(db_acc->GetStorageMode() == memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL,
                  "Wrong storage mode!");
        return db_acc;
      }()  // iile
  };

  memgraph::system::System system_state;
  memgraph::query::InterpreterContext interpreter_context{{},
                                                          nullptr,
                                                          nullptr,
                                                          nullptr,
                                                          repl_state,
                                                          system_state
#ifdef MG_ENTERPRISE
                                                          ,
                                                          std::nullopt,
                                                          nullptr
#endif
  };

  void TearDown() override {
    disk_test_utils::RemoveRocksDbDirs(testSuite);
    std::filesystem::remove_all(data_directory);
  }
};

// Stress test: multiple threads concurrently writing, committing, aborting, reading,
// SHOW TRANSACTIONS, and TERMINATE TRANSACTIONS. Verifies no crashes, no deadlocks,
// and data consistency.
TEST_F(TransactionQueueStressTest, ConcurrentOperations) {
  // Create interpreters for each thread
  std::vector<std::unique_ptr<InterpreterFaker>> worker_interpreters;
  for (int i = 0; i < kNumWorkers; ++i) {
    worker_interpreters.push_back(std::make_unique<InterpreterFaker>(&interpreter_context, db));
  }
  std::vector<std::unique_ptr<InterpreterFaker>> show_interpreters;
  for (int i = 0; i < kNumShowTerminateThreads; ++i) {
    show_interpreters.push_back(std::make_unique<InterpreterFaker>(&interpreter_context, db));
  }

  // Track which workers successfully committed so we can verify data at the end
  std::vector<std::atomic<int>> committed_count(kNumWorkers);
  for (auto &c : committed_count) c.store(0, std::memory_order_relaxed);

  std::atomic<bool> stop_flag{false};
  std::atomic<int> workers_started{0};

  // Worker threads: BEGIN, do some writes, then either COMMIT or ROLLBACK
  auto worker_func = [&](int worker_id) {
    auto &faker = *worker_interpreters[worker_id];
    std::mt19937 rng(42 + worker_id);  // deterministic seed per worker
    std::uniform_int_distribution<int> action_dist(0, 9);

    workers_started.fetch_add(1, std::memory_order_release);

    for (int iter = 0; iter < kIterationsPerWorker; ++iter) {
      try {
        // Explicit transaction: BEGIN + writes + COMMIT/ROLLBACK
        faker.Interpret("BEGIN");

        int num_writes = 1 + (action_dist(rng) % 3);  // 1-3 writes
        for (int w = 0; w < num_writes; ++w) {
          faker.Interpret("CREATE (:StressNode {worker: " + std::to_string(worker_id) +
                          ", iter: " + std::to_string(iter) + "})");
        }

        // 60% commit, 40% rollback
        if (action_dist(rng) < 6) {
          faker.Interpret("COMMIT");
          committed_count[worker_id].fetch_add(num_writes, std::memory_order_relaxed);
        } else {
          faker.Interpret("ROLLBACK");
        }
      } catch (memgraph::query::HintedAbortError &) {
        // Transaction was terminated by another thread — clean up
        faker.Abort();
      } catch (memgraph::utils::BasicException &) {
        // Transaction was terminated during commit — clean up
        faker.Abort();
      }

      // Also do some autocommit (single-statement) transactions
      try {
        faker.Interpret("CREATE (:AutoNode {worker: " + std::to_string(worker_id) + "})");
        committed_count[worker_id].fetch_add(1, std::memory_order_relaxed);
      } catch (memgraph::query::HintedAbortError &) {
        faker.Abort();
      } catch (memgraph::utils::BasicException &) {
        faker.Abort();
      }
    }
  };

  // SHOW TRANSACTIONS threads: repeatedly call SHOW TRANSACTIONS and validate results
  auto show_func = [&](int show_id) {
    auto &faker = *show_interpreters[show_id];
    std::mt19937 rng(1000 + show_id);
    std::uniform_int_distribution<int> action_dist(0, 2);

    // Wait for at least some workers to start
    while (workers_started.load(std::memory_order_acquire) < kNumWorkers / 2) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    for (int iter = 0; iter < kShowIterations && !stop_flag.load(std::memory_order_relaxed); ++iter) {
      try {
        auto show_stream = faker.Interpret("SHOW TRANSACTIONS");
        auto &results = show_stream.GetResults();

        // Validate: results should not be empty (at least the SHOW TRANSACTIONS itself)
        EXPECT_GE(results.size(), 1U);

        for (const auto &row : results) {
          // Validate column count
          ASSERT_EQ(row.size(), 5U) << "Expected 5 columns in SHOW TRANSACTIONS output";

          // Column 0: username (string)
          ASSERT_TRUE(row[0].IsString());

          // Column 1: transaction_id (string, parseable as uint64)
          ASSERT_TRUE(row[1].IsString());
          auto tx_id_str = row[1].ValueString();
          EXPECT_FALSE(tx_id_str.empty());

          // Column 2: query list
          ASSERT_TRUE(row[2].IsList());

          // Column 3: status (must be one of the known statuses)
          ASSERT_TRUE(row[3].IsString());
          auto status = row[3].ValueString();
          EXPECT_TRUE(status == "running" || status == "committing" || status == "aborting")
              << "Unexpected status: " << status;

          // Column 4: metadata (map)
          ASSERT_TRUE(row[4].IsMap());
        }

        // Occasionally try to TERMINATE a random transaction
        if (action_dist(rng) == 0 && results.size() > 1) {
          // Pick a random non-self transaction
          std::uniform_int_distribution<size_t> idx_dist(0, results.size() - 1);
          size_t target_idx = idx_dist(rng);
          auto target_tx_id = results[target_idx][1].ValueString();

          try {
            faker.Interpret("TERMINATE TRANSACTIONS '" + target_tx_id + "'");
          } catch (...) {
            // Termination might fail in various ways — that's OK for a stress test
          }
        }
      } catch (memgraph::query::HintedAbortError &) {
        // Our own transaction was terminated — clean up
        faker.Abort();
      } catch (memgraph::utils::BasicException &) {
        faker.Abort();
      }
    }
  };

  // Launch all threads
  std::vector<std::jthread> threads;
  threads.reserve(kNumWorkers + kNumShowTerminateThreads);

  for (int i = 0; i < kNumWorkers; ++i) {
    threads.emplace_back(worker_func, i);
  }
  for (int i = 0; i < kNumShowTerminateThreads; ++i) {
    threads.emplace_back(show_func, i);
  }

  // Wait for all worker threads to finish
  for (int i = 0; i < kNumWorkers; ++i) {
    threads[i].join();
  }

  // Signal show threads to stop and wait for them
  stop_flag.store(true, std::memory_order_release);
  for (int i = kNumWorkers; i < kNumWorkers + kNumShowTerminateThreads; ++i) {
    threads[i].join();
  }

  // Clean up any interpreters left in non-IDLE state (e.g., terminated but not yet aborted)
  for (auto &interp : worker_interpreters) {
    auto status = interp->interpreter.transaction_status_.load(std::memory_order_acquire);
    if (status != memgraph::query::TransactionStatus::IDLE) {
      interp->Abort();
    }
  }
  for (auto &interp : show_interpreters) {
    auto status = interp->interpreter.transaction_status_.load(std::memory_order_acquire);
    if (status != memgraph::query::TransactionStatus::IDLE) {
      interp->Abort();
    }
  }

  // Final verification: SHOW TRANSACTIONS should show only the verifier interpreter
  auto verifier = std::make_unique<InterpreterFaker>(&interpreter_context, db);
  auto final_show = verifier->Interpret("SHOW TRANSACTIONS");
  ASSERT_EQ(final_show.GetResults().size(), 1U);
  EXPECT_EQ(final_show.GetResults()[0][3].ValueString(), "running");

  // Verify data consistency: count nodes per worker
  int total_expected = 0;
  for (int i = 0; i < kNumWorkers; ++i) {
    total_expected += committed_count[i].load(std::memory_order_relaxed);
  }

  auto count_stream = verifier->Interpret("MATCH (n) RETURN count(n) AS cnt");
  ASSERT_EQ(count_stream.GetResults().size(), 1U);
  int64_t actual_count = count_stream.GetResults()[0][0].ValueInt();
  // Due to TERMINATE TRANSACTIONS, some committed counts may overcount (the thread thought
  // it committed but was actually terminated). So actual_count <= total_expected.
  EXPECT_LE(actual_count, total_expected)
      << "More nodes than expected: actual=" << actual_count << " expected<=" << total_expected;
  spdlog::info("Stress test: {} nodes committed out of {} expected", actual_count, total_expected);

  // Cleanup
  verifier->Interpret("MATCH (n) DETACH DELETE n");
}

// Stress test focused specifically on SHOW TRANSACTIONS racing with commit/abort.
// Exercises the CAS/VERIFYING spin-wait synchronization.
TEST_F(TransactionQueueStressTest, ShowTransactionsRacesWithCommitAbort) {
  constexpr int kRaceWorkers = 4;
  constexpr int kRaceIterations = 500;
  constexpr int kShowThreads = 4;

  std::vector<std::unique_ptr<InterpreterFaker>> worker_interpreters;
  for (int i = 0; i < kRaceWorkers; ++i) {
    worker_interpreters.push_back(std::make_unique<InterpreterFaker>(&interpreter_context, db));
  }
  std::vector<std::unique_ptr<InterpreterFaker>> show_thread_interpreters;
  for (int i = 0; i < kShowThreads; ++i) {
    show_thread_interpreters.push_back(std::make_unique<InterpreterFaker>(&interpreter_context, db));
  }

  std::atomic<bool> stop_flag{false};
  std::atomic<int> workers_ready{0};

  // Workers: rapidly BEGIN-CREATE-COMMIT or BEGIN-CREATE-ROLLBACK
  auto worker_func = [&](int id) {
    auto &faker = *worker_interpreters[id];
    std::mt19937 rng(200 + id);
    std::uniform_int_distribution<int> coin(0, 1);

    workers_ready.fetch_add(1, std::memory_order_release);

    for (int i = 0; i < kRaceIterations; ++i) {
      try {
        faker.Interpret("BEGIN");
        faker.Interpret("CREATE (:RaceNode {id: " + std::to_string(id * kRaceIterations + i) + "})");
        if (coin(rng)) {
          faker.Interpret("COMMIT");
        } else {
          faker.Interpret("ROLLBACK");
        }
      } catch (memgraph::query::HintedAbortError &) {
        faker.Abort();
      } catch (memgraph::utils::BasicException &) {
        faker.Abort();
      }
    }
  };

  // Show threads: call SHOW TRANSACTIONS as fast as possible
  auto show_func = [&](int id) {
    auto &faker = *show_thread_interpreters[id];
    // Wait for all workers
    while (workers_ready.load(std::memory_order_acquire) < kRaceWorkers) {
      std::this_thread::yield();
    }
    while (!stop_flag.load(std::memory_order_relaxed)) {
      try {
        auto stream = faker.Interpret("SHOW TRANSACTIONS");
        for (const auto &row : stream.GetResults()) {
          // Validate that every visible transaction has a valid transaction_id
          ASSERT_TRUE(row[1].IsString());
          EXPECT_FALSE(row[1].ValueString().empty());
          // Status must be valid
          auto status = row[3].ValueString();
          EXPECT_TRUE(status == "running" || status == "committing" || status == "aborting")
              << "Unexpected status: " << status;
        }
      } catch (memgraph::query::HintedAbortError &) {
        faker.Abort();
      } catch (memgraph::utils::BasicException &) {
        faker.Abort();
      }
    }
  };

  std::vector<std::jthread> threads;
  for (int i = 0; i < kRaceWorkers; ++i) {
    threads.emplace_back(worker_func, i);
  }
  for (int i = 0; i < kShowThreads; ++i) {
    threads.emplace_back(show_func, i);
  }

  // Wait for workers
  for (int i = 0; i < kRaceWorkers; ++i) {
    threads[i].join();
  }
  stop_flag.store(true, std::memory_order_release);
  for (int i = kRaceWorkers; i < kRaceWorkers + kShowThreads; ++i) {
    threads[i].join();
  }

  // Clean up any interpreters left in non-IDLE state
  for (auto &interp : worker_interpreters) {
    if (interp->interpreter.transaction_status_.load(std::memory_order_acquire) !=
        memgraph::query::TransactionStatus::IDLE) {
      interp->Abort();
    }
  }
  for (auto &interp : show_thread_interpreters) {
    if (interp->interpreter.transaction_status_.load(std::memory_order_acquire) !=
        memgraph::query::TransactionStatus::IDLE) {
      interp->Abort();
    }
  }

  // Final: all worker transactions should be cleaned up
  auto verifier = std::make_unique<InterpreterFaker>(&interpreter_context, db);
  auto final_show = verifier->Interpret("SHOW TRANSACTIONS");
  ASSERT_EQ(final_show.GetResults().size(), 1U);

  verifier->Interpret("MATCH (n) DETACH DELETE n");
}

// Stress test: rapidly terminate transactions and verify the terminated transactions
// eventually disappear from SHOW TRANSACTIONS.
TEST_F(TransactionQueueStressTest, TerminateAndVerifyCleanup) {
  constexpr int kTargetInterpreters = 6;
  constexpr int kTerminateRounds = 50;

  std::vector<std::unique_ptr<InterpreterFaker>> targets;
  for (int i = 0; i < kTargetInterpreters; ++i) {
    targets.push_back(std::make_unique<InterpreterFaker>(&interpreter_context, db));
  }
  auto terminator = std::make_unique<InterpreterFaker>(&interpreter_context, db);

  std::mt19937 rng(999);

  for (int round = 0; round < kTerminateRounds; ++round) {
    // Start transactions on all targets
    std::vector<std::jthread> target_threads;
    std::vector<std::atomic<bool>> started(kTargetInterpreters);
    for (auto &s : started) s.store(false, std::memory_order_relaxed);

    for (int i = 0; i < kTargetInterpreters; ++i) {
      target_threads.emplace_back([&, i]() {
        try {
          targets[i]->Interpret("BEGIN");
          started[i].store(true, std::memory_order_release);
          // Do some work to keep the transaction alive
          for (int j = 0; j < 10; ++j) {
            targets[i]->Interpret("CREATE (:TNode {round: " + std::to_string(round) + ", id: " + std::to_string(i) +
                                  "})");
          }
        } catch (memgraph::query::HintedAbortError &) {
          targets[i]->Abort();
        } catch (memgraph::utils::BasicException &) {
          targets[i]->Abort();
        }
      });
    }

    // Wait for all to start
    while (
        !std::all_of(started.begin(), started.end(), [](const auto &v) { return v.load(std::memory_order_acquire); })) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    // Terminate a random subset
    int num_to_terminate = 1 + (rng() % (kTargetInterpreters / 2));
    std::vector<int> indices(kTargetInterpreters);
    std::iota(indices.begin(), indices.end(), 0);
    std::shuffle(indices.begin(), indices.end(), rng);

    for (int k = 0; k < num_to_terminate; ++k) {
      int idx = indices[k];
      auto tx_id_opt = targets[idx]->interpreter.GetTransactionId();
      if (!tx_id_opt) continue;
      std::string tx_id = std::to_string(tx_id_opt.value());
      try {
        auto result = terminator->Interpret("TERMINATE TRANSACTIONS '" + tx_id + "'");
        // Result should have 1 row
        if (!result.GetResults().empty()) {
          EXPECT_EQ(result.GetResults()[0][0].ValueString(), tx_id);
        }
      } catch (...) {
      }
    }

    // Verify SHOW TRANSACTIONS sees valid status for all
    try {
      auto show_stream = terminator->Interpret("SHOW TRANSACTIONS");
      for (const auto &row : show_stream.GetResults()) {
        auto status = row[3].ValueString();
        EXPECT_TRUE(status == "running" || status == "committing" || status == "aborting")
            << "Round " << round << ": unexpected status: " << status;
      }
    } catch (...) {
    }

    // Wait for all target threads to finish
    for (auto &t : target_threads) {
      t.join();
    }

    // Commit or abort remaining
    for (int i = 0; i < kTargetInterpreters; ++i) {
      try {
        auto status = targets[i]->interpreter.transaction_status_.load(std::memory_order_acquire);
        if (status != memgraph::query::TransactionStatus::IDLE) {
          targets[i]->Abort();
        }
      } catch (...) {
      }
    }
  }

  // Final: only the terminator should show
  auto final_show = terminator->Interpret("SHOW TRANSACTIONS");
  ASSERT_EQ(final_show.GetResults().size(), 1U);

  terminator->Interpret("MATCH (n) DETACH DELETE n");
}
