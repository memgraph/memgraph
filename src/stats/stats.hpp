/// @file

#pragma once

#include <thread>

#include "data_structures/concurrent/push_queue.hpp"
#include "io/network/endpoint.hpp"
#include "stats/stats_rpc_messages.hpp"

namespace stats {

// TODO (buda): documentation + tests
class StatsClient {
 public:
  StatsClient();

  // To be clear.
  StatsClient(const StatsClient &other) = delete;
  StatsClient(StatsClient &&other) = delete;
  StatsClient &operator=(const StatsClient &) = delete;
  StatsClient &operator=(StatsClient &&) = delete;

  void Log(StatsReq req);

  void Start(const io::network::Endpoint &endpoint);

  ~StatsClient();

 private:
  ConcurrentPushQueue<StatsReq> queue_;
  std::atomic<bool> is_running_{false};
  std::thread dispatch_thread_;
};

}  // namespace stats

// TODO (buda): ON/OFF compile OR runtime parameter?

void InitStatsLogging();
void LogStat(const std::string &metric_path, double value,
             const std::vector<std::pair<std::string, std::string>> &tags = {});
