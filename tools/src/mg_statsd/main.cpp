#include "gflags/gflags.h"

#include "communication/rpc/server.hpp"
#include "io/network/socket.hpp"
#include "stats/stats.hpp"
#include "stats/stats_rpc_messages.hpp"
#include "utils/flag_validation.hpp"

DEFINE_string(interface, "0.0.0.0",
              "Communication interface on which to listen.");
DEFINE_VALIDATED_int32(port, 2500, "Communication port on which to listen.",
                       FLAG_IN_RANGE(0, std::numeric_limits<uint16_t>::max()));

DEFINE_string(graphite_address, "", "Graphite address.");
DEFINE_int32(graphite_port, 0, "Graphite port.");
DEFINE_string(prefix, "", "Prefix for all collected stats");

std::string GraphiteFormat(const stats::StatsReq &req) {
  std::stringstream sstr;
  if (!FLAGS_prefix.empty()) {
    sstr << FLAGS_prefix << "." << req.metric_path;
  } else {
    sstr << req.metric_path;
  }
  for (const auto &tag : req.tags) {
    sstr << ";" << tag.first << "=" << tag.second;
  }
  sstr << " " << req.value << " " << req.timestamp << "\n";
  LOG(INFO) << sstr.str();
  return sstr.str();
}

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  communication::rpc::Server server({FLAGS_interface, (uint16_t)FLAGS_port});

  io::network::Socket graphite_socket;

  CHECK(graphite_socket.Connect(
      {FLAGS_graphite_address, (uint16_t)FLAGS_graphite_port}))
      << "Failed to connect to Graphite";
  graphite_socket.SetKeepAlive();

  server.Register<stats::StatsRpc>([&](const stats::StatsReq &req) {
    LOG(INFO) << "StatsRpc::Received";
    std::string data = GraphiteFormat(req);
    graphite_socket.Write(data);
    return std::make_unique<stats::StatsRes>();
  });

  server.Register<stats::BatchStatsRpc>([&](const stats::BatchStatsReq &req) {
    // TODO(mtomic): batching?
    LOG(INFO) << fmt::format("BatchStatsRpc::Received: {}",
                             req.requests.size());
    for (size_t i = 0; i < req.requests.size(); ++i) {
      std::string data = GraphiteFormat(req.requests[i]);
      graphite_socket.Write(data, i + 1 < req.requests.size());
    }
    return std::make_unique<stats::BatchStatsRes>();
  });

  std::this_thread::sleep_until(std::chrono::system_clock::time_point::max());

  return 0;
}
