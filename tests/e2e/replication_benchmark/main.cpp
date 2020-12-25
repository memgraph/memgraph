#include <chrono>
#include <random>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "mgclient.hpp"

DEFINE_string(host, "127.0.0.1", "Server host");
DEFINE_int32(port, 7687, "Server port");
DEFINE_string(username, "", "Database username");
DEFINE_string(password, "", "Database password");
DEFINE_bool(use_ssl, false, "Use SSL connection");
DEFINE_int32(nodes, 1000, "Number of nodes in DB");
DEFINE_int32(edges, 5000, "Number of edges in DB");

int main(int argc, char **argv) {
  google::SetUsageMessage("Memgraph E2E Replication Benchmark Test");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  mg::Client::Init();

  {
    mg::Client::Params params;
    params.host = FLAGS_host;
    params.port = static_cast<uint16_t>(FLAGS_port);
    params.use_ssl = FLAGS_use_ssl;
    auto client = mg::Client::Connect(params);
    if (!client) {
      LOG(FATAL) << "Failed to connect!";
    }
    client->Execute("MATCH (n) DETACH DELETE n;");
    client->DiscardAll();
    client->Execute("CREATE INDEX ON :Node(id);");
    client->DiscardAll();

    // TODO(gitbuda): Add more threads in a configurable way and measure writes.
    for (int i = 0; i < FLAGS_nodes; ++i) {
      client->Execute("CREATE (:Node {id:" + std::to_string(i) + "});");
      client->DiscardAll();
    }
    auto seed =
        std::chrono::high_resolution_clock::now().time_since_epoch().count();
    std::mt19937 rng(seed);
    std::uniform_int_distribution<int> dist(0, FLAGS_nodes - 1);
    for (int i = 0; i < FLAGS_edges; ++i) {
      int a = dist(rng), b = dist(rng);
      client->Execute("MATCH (n {id:" + std::to_string(a) + "})," +
                      "      (m {id:" + std::to_string(b) + "}) " +
                      "CREATE (n)-[:Edge]->(m);");
      client->DiscardAll();
    }

    // TODO(gitbuda): Add more threads in a configurable way and measure reads.
  }

  mg::Client::Finalize();

  return 0;
}
