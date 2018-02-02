#include "glog/logging.h"

#include "communication/rpc/client.hpp"
#include "stats/stats.hpp"

DEFINE_HIDDEN_string(statsd_address, "", "Stats server IP address.");
DEFINE_HIDDEN_int32(statsd_port, 2500, "Stats server port.");

namespace stats {

const std::string kStatsServiceName = "stats";

StatsClient::StatsClient() {}

void StatsClient::Log(StatsReq req) {
  if (is_running_) {
    queue_.push(req);
  }
}

void StatsClient::Start(const io::network::Endpoint &endpoint) {
  dispatch_thread_ = std::thread([this, endpoint]() {
    CHECK(!is_running_) << "Stats logging already initialized!";
    LOG(INFO) << "Stats dispatcher thread started";

    communication::rpc::Client client(endpoint, kStatsServiceName);

    is_running_ = true;
    while (is_running_) {
      auto last = queue_.begin();
      size_t sent = 0, total = 0;
      for (auto it = last; it != queue_.end(); it++) {
        // TODO (buda): batch messages
        // TODO (buda): set reasonable timeout
        if (auto rep = client.Call<StatsRpc>(*it)) {
          ++sent;
        }
        ++total;
      }
      LOG(INFO) << fmt::format("Sent {} out of {} events from queue.", sent,
                               total);
      last.delete_tail();
      // TODO (buda): parametrize sleep time
      std::this_thread::sleep_for(std::chrono::microseconds(500));
    }
  });
}

StatsClient::~StatsClient() {
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

void LogStat(const std::string &metric_path, double value,
             const std::vector<std::pair<std::string, std::string>> &tags) {
  stats::client.Log({metric_path, tags, value});
}
