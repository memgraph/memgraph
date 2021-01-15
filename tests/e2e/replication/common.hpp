#include <chrono>
#include <random>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "io/network/endpoint.hpp"
#include "mgclient.hpp"
#include "utils/string.hpp"

DEFINE_string(database_endpoints,
              "127.0.0.1:7687,127.0.0.1:7688,127.0.0.1:7689",
              "An array of database endspoints. Each endpoint is separated by "
              "comma. Within each endpoint, colon separates host and port. Use "
              "IPv4 addresses as hosts. First endpoint represents main "
              "replication instance.");
DEFINE_string(username, "", "Database username.");
DEFINE_string(password, "", "Database password.");
DEFINE_bool(use_ssl, false, "Use SSL connection.");
DEFINE_int32(nodes, 1000, "Number of nodes in DB.");
DEFINE_int32(edges, 5000, "Number of edges in DB.");
DEFINE_double(reads_duration_limit, 10.0,
              "How long should the client perform reads (seconds)");

namespace mg::e2e::replication {

auto ParseDatabaseEndpoints(const std::string &database_endpoints_str) {
  const auto db_endpoints_strs = utils::Split(database_endpoints_str, ",");
  std::vector<io::network::Endpoint> database_endpoints;
  for (const auto &db_endpoint_str : db_endpoints_strs) {
    const auto maybe_host_port =
        io::network::Endpoint::ParseSocketOrIpAddress(db_endpoint_str, 7687);
    CHECK(maybe_host_port);
    database_endpoints.emplace_back(
        io::network::Endpoint(maybe_host_port->first, maybe_host_port->second));
  }
  return database_endpoints;
}

auto Connect(const io::network::Endpoint &database_endpoint) {
  mg::Client::Params params;
  params.host = database_endpoint.address;
  params.port = database_endpoint.port;
  params.use_ssl = FLAGS_use_ssl;
  auto client = mg::Client::Connect(params);
  if (!client) {
    LOG(FATAL) << "Failed to connect!";
  }
  return client;
}

class IntGenerator {
 public:
  IntGenerator(const std::string &purpose, int start, int end)
      : seed_(std::chrono::high_resolution_clock::now()
                  .time_since_epoch()
                  .count()),
        rng_(seed_),
        dist_(start, end) {
    LOG(INFO) << purpose << " int generator seed: " << seed_;
  }

  int Next() { return dist_(rng_); }

 private:
  uint64_t seed_;
  std::mt19937 rng_;
  std::uniform_int_distribution<int> dist_;
};

}  // namespace mg::e2e::replication
