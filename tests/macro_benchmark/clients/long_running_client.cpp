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

#include "bolt_client.hpp"
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

struct VertexAndEdges {
  DecodedVertex vertex;
  std::vector<DecodedEdge> edges;
  std::vector<DecodedVertex> vertices;
};

std::pair<VertexAndEdges, int> DetachDeleteVertex(BoltClient &client,
                                                  const std::string &label,
                                                  int64_t id) {
  auto vertex_record =
      ExecuteNTimesTillSuccess(
          client, "MATCH (n :" + label + " {id : $id}) RETURN n",
          std::map<std::string, DecodedValue>{{"id", id}}, MAX_RETRIES)
          .records;
  CHECK(vertex_record.size() == 1U) << "id : " << id << " "
                                    << vertex_record.size();

  auto records =
      ExecuteNTimesTillSuccess(
          client, "MATCH (n :" + label + " {id : $id})-[e]-(m) RETURN n, e, m",
          std::map<std::string, DecodedValue>{{"id", id}}, MAX_RETRIES)
          .records;

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

  return {{vertex_record[0][0].ValueVertex(), edges, vertices}, 3};
}

int ReturnVertexAndEdges(BoltClient &client,
                         const VertexAndEdges &vertex_and_edges,
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
    }
    CHECK(ret.records.size() == 1U)
        << "Graph in invalid state "
        << vertex_and_edges.vertex.properties.at("id");
    ++num_queries;
  }
  return num_queries;
}

int64_t NumNodes(BoltClient &client, const std::string &label) {
  auto result = ExecuteNTimesTillSuccess(
      client, "MATCH (n :" + label + ") RETURN COUNT(n) as cnt", {},
      MAX_RETRIES);
  return result.records[0][0].ValueInt();
}

std::vector<int64_t> Neighbours(BoltClient &client, const std::string &label,
                                int64_t id) {
  auto result = ExecuteNTimesTillSuccess(
      client, "MATCH (n :" + label + " {id: " + std::to_string(id) +
                  "})-[e]-(m) RETURN m.id",
      {}, MAX_RETRIES);
  std::vector<int64_t> ret;
  for (const auto &record : result.records) {
    ret.push_back(record[0].ValueInt());
  }
  return ret;
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  nlohmann::json config;
  std::cin >> config;
  const auto &queries = config["queries"];
  const double read_probability = config["read_probability"];
  const std::string independent_label = config["independent_label"];
  std::vector<int64_t> independent_nodes_ids;

  BoltClient client(FLAGS_address, FLAGS_port, FLAGS_username, FLAGS_password);
  const int64_t num_nodes = NumNodes(client, independent_label);
  {
    std::vector<int64_t> ids;
    std::unordered_set<int64_t> independent;
    for (int64_t i = 1; i <= num_nodes; ++i) {
      ids.push_back(i);
      independent.insert(i);
    }
    {
      std::mt19937 mt;
      std::shuffle(ids.begin(), ids.end(), mt);
    }

    for (auto i : ids) {
      if (independent.find(i) == independent.end()) continue;
      independent.erase(i);
      std::vector<int64_t> neighbour_ids =
          Neighbours(client, independent_label, i);
      independent_nodes_ids.push_back(i);
      for (auto j : neighbour_ids) {
        independent.erase(j);
      }
    }
  }

  utils::Timer timer;
  std::vector<std::thread> threads;
  std::atomic<int64_t> executed_queries{0};
  std::atomic<bool> keep_running{true};

  LOG(INFO) << "nodes " << num_nodes << " independent "
            << independent_nodes_ids.size();

  int64_t next_to_assign = 0;
  for (int i = 0; i < FLAGS_num_workers; ++i) {
    int64_t size = independent_nodes_ids.size();
    int64_t next_next_to_assign = next_to_assign + size / FLAGS_num_workers +
                                  (i < size % FLAGS_num_workers);
    std::vector<int64_t> to_remove(
        independent_nodes_ids.begin() + next_to_assign,
        independent_nodes_ids.begin() + next_next_to_assign);
    LOG(INFO) << next_to_assign << " " << next_next_to_assign;
    next_to_assign = next_next_to_assign;

    threads.emplace_back(
        [&](int thread_id, std::vector<int64_t> to_remove) {
          BoltClient client(FLAGS_address, FLAGS_port, FLAGS_username,
                            FLAGS_password);

          std::mt19937 random_gen(thread_id);
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
              if (real_dist(random_gen) <
                  static_cast<double>(removed.size()) /
                      (removed.size() + to_remove.size())) {
                CHECK(removed.size());
                std::uniform_int_distribution<> int_dist(0, removed.size() - 1);
                std::swap(removed.back(), removed[int_dist(random_gen)]);
                executed_queries += ReturnVertexAndEdges(client, removed.back(),
                                                         independent_label);
                to_remove.push_back(
                    removed.back().vertex.properties["id"].ValueInt());
                removed.pop_back();
              } else {
                CHECK(to_remove.size());
                std::uniform_int_distribution<> int_dist(0,
                                                         to_remove.size() - 1);
                std::swap(to_remove.back(), to_remove[int_dist(random_gen)]);
                auto ret = DetachDeleteVertex(client, independent_label,
                                              to_remove.back());
                removed.push_back(ret.first);
                to_remove.pop_back();
                executed_queries += ret.second;
              }
            }
          }

          client.Close();
        },
        i, std::move(to_remove));
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
