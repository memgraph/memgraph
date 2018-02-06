/// @file

#pragma once

#include <thread>

#include "data_structures/concurrent/push_queue.hpp"
#include "io/network/endpoint.hpp"
#include "stats/stats_rpc_messages.hpp"

DECLARE_string(statsd_address);
DECLARE_int32(statsd_port);
DECLARE_int32(statsd_flush_interval);

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
  void Stop();

 private:
  ConcurrentPushQueue<StatsReq> queue_;
  std::atomic<bool> is_running_{false};
  std::thread dispatch_thread_;
};

}  // namespace stats

// TODO (buda): ON/OFF compile OR runtime parameter?

void InitStatsLogging();
void StopStatsLogging();
void LogStat(const std::string &metric_path, double value,
             const std::vector<std::pair<std::string, std::string>> &tags = {});
