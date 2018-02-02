#include "gflags/gflags.h"

#include "communication/rpc/server.hpp"
#include "stats/stats.hpp"

DEFINE_string(address, "", "address");
DEFINE_int32(port, 2500, "port");

std::string GraphiteFormat(const stats::StatsReq &req) {
  std::stringstream sstr;
  sstr << req.metric_path;
  for (const auto &tag : req.tags) {
    sstr << ";" << tag.first << "=" << tag.second;
  }
  sstr << " " << req.value << " " << req.timestamp << "\n";
  return sstr.str();
}

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  communication::rpc::System system({FLAGS_address, (uint16_t)FLAGS_port});
  communication::rpc::Server server(system, "stats");

  server.Register<stats::StatsRpc>([](const stats::StatsReq &req) {
    // TODO(mtomic): actually send to graphite
    LOG(INFO) << fmt::format("Got message: {}", GraphiteFormat(req));
    return std::make_unique<stats::StatsRes>();
  });

  std::this_thread::sleep_until(std::chrono::system_clock::time_point::max());

  return 0;
}
