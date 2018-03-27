#include <thread>
#include <vector>

#include <glog/logging.h>

#include "transactions/engine_single_node.hpp"
#include "utils/timer.hpp"

void Benchmark(int64_t num_threads, int64_t num_transactions) {
  LOG(INFO) << "Testing with " << num_threads << " threads and "
            << num_transactions << " transactions per thread...";

  tx::SingleNodeEngine engine;
  std::vector<std::thread> threads;
  utils::Timer timer;
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([num_transactions, &engine]() {
      for (int j = 0; j < num_transactions; ++j) {
        auto *tx = engine.Begin();
        engine.Commit(*tx);
      }
    });
  }
  for (auto &t : threads) t.join();

  int64_t tx_count = engine.GlobalGcSnapshot().front() - 1;
  CHECK(tx_count == num_threads * num_transactions)
      << "Got a bad number of transactions: " << tx_count;

  auto tps = (double)(tx_count) / timer.Elapsed().count();
  LOG(INFO) << "Result (millions of transactions per second) " << tps / 1000000;
}

int main(int, char **argv) {
  google::InitGoogleLogging(argv[0]);
  for (int thread_count : {1, 2, 4, 8, 16}) {
    Benchmark(thread_count, 100000);
  }
  return 0;
}
