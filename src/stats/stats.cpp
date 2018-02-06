#include "glog/logging.h"

#include "communication/rpc/client.hpp"
#include "stats/stats.hpp"

DEFINE_HIDDEN_string(statsd_address, "", "Stats server IP address");
DEFINE_HIDDEN_int32(statsd_port, 2500, "Stats server port");
DEFINE_HIDDEN_int32(statsd_flush_interval, 500,
                    "Stats flush interval (in milliseconds)");

namespace stats {

const std::string kStatsServiceName = "stats";

StatsClient::StatsClient() {}

void StatsClient::Log(StatsReq req) {
  if (is_running_) {
    queue_.push(req);
  }
}

void StatsClient::Start(const io::network::Endpoint &endpoint) {
  // TODO(mtomic): we probably want to batch based on request size and MTU
  const int MAX_BATCH_SIZE = 100;

  dispatch_thread_ = std::thread([this, endpoint]() {
    CHECK(!is_running_) << "Stats logging already initialized!";
    LOG(INFO) << "Stats dispatcher thread started";

    communication::rpc::Client client(endpoint, kStatsServiceName);

    BatchStatsReq batch_request;
    batch_request.requests.reserve(MAX_BATCH_SIZE);

    is_running_ = true;
    while (is_running_) {
      auto last = queue_.begin();
      size_t sent = 0, total = 0;

      auto flush_batch = [&] {
        if (auto rep = client.Call<BatchStatsRpc>(batch_request)) {
          sent += batch_request.requests.size();
        }
        total += batch_request.requests.size();
        batch_request.requests.clear();
      };

      for (auto it = last; it != queue_.end(); it++) {
        batch_request.requests.emplace_back(std::move(*it));
        if (batch_request.requests.size() == MAX_BATCH_SIZE) {
          flush_batch();
        }
      }

      if (!batch_request.requests.empty()) {
        flush_batch();
      }

      VLOG(10) << fmt::format("Sent {} out of {} events from queue.", sent,
                              total);
      last.delete_tail();
      std::this_thread::sleep_for(
          std::chrono::milliseconds(FLAGS_statsd_flush_interval));
    }
  });
}

void StatsClient::Stop() {
  if (is_running_) {
    is_running_ = false;
    dispatch_thread_.join();
  }
}

StatsClient client;

}  // namespace stats

void InitStatsLogging() {
  if (FLAGS_statsd_address != "") {
    stats::client.Start({FLAGS_statsd_address, (uint16_t)FLAGS_statsd_port});
  }
}

void StopStatsLogging() { stats::client.Stop(); }

void LogStat(const std::string &metric_path, double value,
             const std::vector<std::pair<std::string, std::string>> &tags) {
  stats::client.Log({metric_path, tags, value});
}
