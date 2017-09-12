// TODO: work in progress.
#include <array>
#include <chrono>
#include <fstream>
#include <iostream>
#include <queue>
#include <random>
#include <sstream>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <json/json.hpp>

#include "common.hpp"
#include "communication/bolt/client.hpp"
#include "communication/bolt/v1/decoder/decoded_value.hpp"
#include "io/network/network_endpoint.hpp"
#include "io/network/socket.hpp"
#include "threading/sync/spinlock.hpp"
#include "utils/algorithm.hpp"
#include "utils/algorithm.hpp"
#include "utils/assert.hpp"
#include "utils/timer.hpp"

using SocketT = io::network::Socket;
using EndpointT = io::network::NetworkEndpoint;
using Client = communication::bolt::Client<SocketT>;
using communication::bolt::DecodedValue;
using communication::bolt::DecodedVertex;
using communication::bolt::DecodedEdge;

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_string(port, "7687", "Server port");
DEFINE_int32(num_workers, 1, "Number of workers");
DEFINE_string(output, "", "Output file");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_int32(duration, 30, "Number of seconds to execute benchmark");

const int MAX_RETRIES = 30;
const int NUM_BUCKETS = 100;

struct VertexAndEdges {
  DecodedVertex vertex;
  std::vector<DecodedEdge> edges;
  std::vector<DecodedVertex> vertices;
};

std::pair<VertexAndEdges, int> DetachDeleteVertex(Client &client,
                                                  const std::string &label,
                                                  int64_t id) {
  auto records =
      ExecuteNTimesTillSuccess(
          client, "MATCH (n :" + label + " {id : $id})-[e]-(m) RETURN n, e, m",
          std::map<std::string, DecodedValue>{{"id", id}}, MAX_RETRIES)
          .records;

  if (records.size() == 0U) return {{}, 1};

  ExecuteNTimesTillSuccess(
      client, "MATCH (n :" + label + " {id : $id})-[]-(m) DETACH DELETE n",
      std::map<std::string, DecodedValue>{{"id", id}}, MAX_RETRIES);

  std::vector<DecodedEdge> edges;
  edges.reserve(records.size());
  for (const auto &record : records) {
    edges.push_back(record[1].ValueEdge());
  }

  std::vector<DecodedVertex> vertices;
  vertices.reserve(records.size());
  for (const auto &record : records) {
    vertices.push_back(record[2].ValueVertex());
  }

  return {{records[0][0].ValueVertex(), edges, vertices}, 2};
}

int ReturnVertexAndEdges(Client &client, const VertexAndEdges &vertex_and_edges,
                         const std::string &independent_label) {
  int num_queries = 0;
  {
    std::stringstream os;
    os << "CREATE (n :";
    PrintIterable(os, vertex_and_edges.vertex.labels, ":");
    os << " {";
    PrintIterable(os, vertex_and_edges.vertex.properties, ", ",
                  [&](auto &stream, const auto &pair) {
                    if (pair.second.type() == DecodedValue::Type::String) {
                      stream << pair.first << ": \"" << pair.second << "\"";
                    } else {
                      stream << pair.first << ": " << pair.second;
                    }
                  });
    os << "})";
    ExecuteNTimesTillSuccess(client, os.str(), {}, MAX_RETRIES);
    ++num_queries;
  }

  for (int i = 0; i < static_cast<int>(vertex_and_edges.vertices.size()); ++i) {
    std::stringstream os;
    os << "MATCH (n :" << independent_label
       << " {id: " << vertex_and_edges.vertex.properties.at("id") << "}) ";
    os << "MATCH (m :" << independent_label
       << " {id: " << vertex_and_edges.vertices[i].properties.at("id") << "}) ";
    const auto &edge = vertex_and_edges.edges[i];
    os << "CREATE (n)";
    if (edge.to == vertex_and_edges.vertex.id) {
      os << "<-";
    } else {
      os << "-";
    }
    os << "[:" << edge.type << " {";
    PrintIterable(os, edge.properties, ", ",
                  [&](auto &stream, const auto &pair) {
                    if (pair.second.type() == DecodedValue::Type::String) {
                      stream << pair.first << ": \"" << pair.second << "\"";
                    } else {
                      stream << pair.first << ": " << pair.second;
                    }
                  });
    os << "}]";
    if (edge.from == vertex_and_edges.vertex.id) {
      os << "->";
    } else {
      os << "-";
    }
    os << "(m)";
    os << " RETURN n.id";
    auto ret = ExecuteNTimesTillSuccess(client, os.str(), {}, MAX_RETRIES);
    auto x = ret.metadata["plan_execution_time"];
    auto y = ret.metadata["planning_time"];
    if (x.type() == DecodedValue::Type::Double) {
      LOG_EVERY_N(INFO, 5000) << "exec " << x.ValueDouble() << " planning "
                              << y.ValueDouble();
      CHECK(ret.records.size() == 1U) << "Graph in invalid state";
    }
    ++num_queries;
  }
  return num_queries;
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  nlohmann::json config;
  std::cin >> config;
  const auto &queries = config["queries"];
  const double read_probability = config["read_probability"];
  const int64_t num_independent_nodes = config["num_independent_nodes"];
  const std::string independent_label = config["independent_label"];
  const int64_t num_nodes = config["num_nodes"];

  utils::Timer timer;
  std::vector<std::thread> threads;
  std::atomic<int64_t> executed_queries{0};
  std::atomic<bool> keep_running{true};

  for (int i = 0; i < FLAGS_num_workers; ++i) {
    threads.emplace_back(
        [&](int thread_id) {
          // Initialise client.
          SocketT socket;
          EndpointT endpoint;
          try {
            endpoint = EndpointT(FLAGS_address, FLAGS_port);
          } catch (const io::network::NetworkEndpointException &e) {
            LOG(FATAL) << "Invalid address or port: " << FLAGS_address << ":"
                       << FLAGS_port;
          }
          if (!socket.Connect(endpoint)) {
            LOG(FATAL) << "Could not connect to: " << FLAGS_address << ":"
                       << FLAGS_port;
          }
          Client client(std::move(socket), FLAGS_username, FLAGS_password);

          std::mt19937 random_gen(thread_id);
          int64_t to_remove =
              num_independent_nodes / FLAGS_num_workers * thread_id + 1;
          int64_t last_to_remove =
              to_remove + num_independent_nodes / FLAGS_num_workers;
          bool remove = true;
          int64_t num_shifts = 0;
          std::vector<VertexAndEdges> removed;

          while (keep_running) {
            std::uniform_real_distribution<> real_dist(0.0, 1.0);

            // Read query.
            if (real_dist(random_gen) < read_probability) {
              std::uniform_int_distribution<> read_query_dist(
                  0, static_cast<int>(queries.size()) - 1);
              const auto &query = queries[read_query_dist(random_gen)];
              std::map<std::string, DecodedValue> params;
              for (const auto &param : query["params"]) {
                std::uniform_int_distribution<int64_t> param_value_dist(
                    param["low"], param["high"]);
                params[param["name"]] = param_value_dist(random_gen);
              }
              ExecuteNTimesTillSuccess(client, query["query"], params,
                                       MAX_RETRIES);
              ++executed_queries;
            } else {
              if (!remove) {
                executed_queries += ReturnVertexAndEdges(client, removed.back(),
                                                         independent_label);
                removed.pop_back();
                if (removed.empty()) {
                  remove = true;
                }
              } else {
                auto ret =
                    DetachDeleteVertex(client, independent_label, to_remove);
                ++to_remove;
                executed_queries += ret.second;
                if (ret.second > 1) {
                  removed.push_back(std::move(ret.first));
                }
                if (to_remove == last_to_remove) {
                  for (auto &x : removed) {
                    x.vertex.properties["id"].ValueInt() += num_nodes;
                  }
                  remove = false;
                  ++num_shifts;
                  to_remove =
                      num_independent_nodes / FLAGS_num_workers * thread_id +
                      1 + num_shifts * num_nodes;
                  last_to_remove =
                      to_remove + num_independent_nodes / FLAGS_num_workers;
                }
              }
            }
          }

          client.Close();
        },
        i);
  }

  // Open stream for writing stats.
  std::streambuf *buf;
  std::ofstream f;
  if (FLAGS_output != "") {
    f.open(FLAGS_output);
    buf = f.rdbuf();
  } else {
    buf = std::cout.rdbuf();
  }
  std::ostream out(buf);

  while (timer.Elapsed().count() < FLAGS_duration) {
    using namespace std::chrono_literals;
    out << "{ \"num_executed_queries\": " << executed_queries << ", "
        << "\"elapsed_time\": " << timer.Elapsed().count() << "}" << std::endl;
    out.flush();
    std::this_thread::sleep_for(1s);
  }
  keep_running = false;

  for (int i = 0; i < FLAGS_num_workers; ++i) {
    threads[i].join();
  }

  return 0;
}
