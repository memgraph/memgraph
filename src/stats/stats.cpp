#include "stats/stats.hpp"

#include "glog/logging.h"

#include "communication/rpc/client.hpp"
#include "data_structures/concurrent/push_queue.hpp"
#include "stats/metrics.hpp"
#include "stats/stats_rpc_messages.hpp"
#include "utils/thread.hpp"

DEFINE_HIDDEN_string(statsd_address, "", "Stats server IP address");
DEFINE_HIDDEN_int32(statsd_port, 2500, "Stats server port");
DEFINE_HIDDEN_int32(statsd_flush_interval, 500,
                    "Stats flush interval (in milliseconds)");

namespace stats {

std::string statsd_prefix = "";
std::thread stats_dispatch_thread;
std::thread counter_refresh_thread;
std::atomic<bool> stats_running{false};
ConcurrentPushQueue<StatsReq> stats_queue;

void RefreshMetrics() {
  LOG(INFO) << "Metrics flush thread started";
  utils::ThreadSetName("Stats refresh");
  while (stats_running) {
    auto &metrics = AccessMetrics();
    for (auto &kv : metrics) {
      auto value = kv.second->Flush();
      if (value) {
        LogStat(kv.first, *value);
      }
    }
    ReleaseMetrics();
    // TODO(mtomic): hardcoded sleep time
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  LOG(INFO) << "Metrics flush thread stopped";
}

void StatsDispatchMain(const io::network::Endpoint &endpoint) {
  // TODO(mtomic): we probably want to batch based on request size and MTU
  const int MAX_BATCH_SIZE = 100;

  LOG(INFO) << "Stats dispatcher thread started";
  utils::ThreadSetName("Stats dispatcher");

  communication::rpc::Client client(endpoint);

  BatchStatsReq batch_request;
  batch_request.requests.reserve(MAX_BATCH_SIZE);

  while (stats_running) {
    auto last = stats_queue.begin();
    size_t sent = 0, total = 0;

    auto flush_batch = [&] {
      if (client.Call<BatchStatsRpc>(batch_request)) {
        sent += batch_request.requests.size();
      }
      total += batch_request.requests.size();
      batch_request.requests.clear();
    };

    for (auto it = last; it != stats_queue.end(); it++) {
      batch_request.requests.emplace_back(std::move(*it));
      if (batch_request.requests.size() == MAX_BATCH_SIZE) {
        flush_batch();
      }
    }

    if (!batch_request.requests.empty()) {
      flush_batch();
    }

    VLOG(30) << fmt::format("Sent {} out of {} events from queue.", sent,
                            total);
    last.delete_tail();
    std::this_thread::sleep_for(
        std::chrono::milliseconds(FLAGS_statsd_flush_interval));
  }
}

void LogStat(const std::string &metric_path, double value,
             const std::vector<std::pair<std::string, std::string>> &tags) {
  if (stats_running) {
    stats_queue.push(statsd_prefix + metric_path, tags, value);
  }
}

void InitStatsLogging(std::string prefix) {
  if (!prefix.empty()) {
    statsd_prefix = prefix + ".";
  }
  if (FLAGS_statsd_address != "") {
    stats_running = true;
    stats_dispatch_thread = std::thread(
        StatsDispatchMain, io::network::Endpoint{FLAGS_statsd_address,
                                                 (uint16_t)FLAGS_statsd_port});
    counter_refresh_thread = std::thread(RefreshMetrics);
  }
}

void StopStatsLogging() {
  if (stats_running) {
    stats_running = false;
    stats_dispatch_thread.join();
    counter_refresh_thread.join();
  }
}

}  // namespace stats
