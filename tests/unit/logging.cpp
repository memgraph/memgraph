#include <atomic>
#include "gtest/gtest.h"
#include "logging/logger.hpp"
#include "logging/logs/async_log.hpp"
#include "logging/logs/sync_log.hpp"
#include "logging/streams/stdout.hpp"

class Testout : public Log::Stream {
 public:
  void emit(const Log::Record& record) override {
    counter_.fetch_add(1);
    sequence_.emplace_back(counter_.load());
    texts_.insert(record.text());
  }
  std::atomic<int> counter_;
  std::vector<int> sequence_;
  std::set<std::string> texts_;
};

TEST(LoggingTest, SyncLogger) {
  Log::uptr log = std::make_unique<SyncLog>();
  auto test_stream = std::make_unique<Testout>();
  auto check = test_stream.get();
  log->pipe(std::move(test_stream));
  auto logger = log->logger("sync_logger");

  logger.info("{}", "info");
  logger.warn("{}", "warn");
  logger.error("{}", "error");
  logger.trace("{}", "trace");
  logger.debug("{}", "debug");

#ifdef NDEBUG
  ASSERT_EQ(check->counter_, 3);
  ASSERT_EQ(check->texts_.size(), 3);
#else
  ASSERT_EQ(check->counter_, 5);
  ASSERT_EQ(check->texts_.size(), 5);
#endif

  ASSERT_EQ(check->sequence_[2], 3);
}

TEST(LoggingTest, AysncLogger) {
  Log::uptr log = std::make_unique<AsyncLog>();
  auto test_stream = std::make_unique<Testout>();
  auto check = test_stream.get();
  log->pipe(std::move(test_stream));
  auto logger = log->logger("async_logger");

  logger.info("{}", "info");
  logger.warn("{}", "warn");
  logger.error("{}", "error");
  logger.trace("{}", "trace");
  logger.debug("{}", "debug");

  using namespace std::chrono;
  std::this_thread::sleep_for(1s);

#ifdef NDEBUG
  ASSERT_EQ(check->counter_, 3);
  ASSERT_EQ(check->sequence_.size(), 3);
  ASSERT_EQ(check->texts_.size(), 3);
#else
  ASSERT_EQ(check->counter_, 5);
  ASSERT_EQ(check->sequence_.size(), 5);
  ASSERT_EQ(check->texts_.size(), 5);
#endif
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
