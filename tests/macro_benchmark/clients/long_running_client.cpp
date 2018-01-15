// TODO: work in progress.
#include <array>
#include <chrono>
#include <fstream>
#include <iostream>
#include <queue>
#include <random>
#include <sstream>
#include <unordered_map>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <json/json.hpp>

#include "bolt_client.hpp"
#include "common.hpp"
#include "communication/bolt/client.hpp"
#include "communication/bolt/v1/decoder/decoded_value.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/socket.hpp"
#include "threading/sync/spinlock.hpp"
#include "utils/algorithm.hpp"
#include "utils/network.hpp"
#include "utils/timer.hpp"

using communication::bolt::DecodedEdge;
using communication::bolt::DecodedValue;
using communication::bolt::DecodedVertex;

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 7687, "Server port");
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

std::atomic<int64_t> executed_queries;

class Session {
 public:
  Session(const nlohmann::json &config, const std::string &address,
          uint16_t port, const std::string &username,
          const std::string &password)
      : config_(config), client_(address, port, username, password) {}

 private:
  const nlohmann::json &config_;
  BoltClient client_;
  std::unordered_map<std::string,
                     std::vector<std::map<std::string, DecodedValue>>>
      stats_;
  SpinLock lock_;

  auto Execute(const std::string &query,
               const std::map<std::string, DecodedValue> &params,
               const std::string &query_name = "") {
    utils::Timer timer;
    auto result = ExecuteNTimesTillSuccess(client_, query, params, MAX_RETRIES);
    ++executed_queries;
    auto wall_time = timer.Elapsed();
    auto metadata = result.metadata;
    metadata["wall_time"] = wall_time.count();
    {
      std::unique_lock<SpinLock> guard(lock_);
      if (query_name != "") {
        stats_[query_name].push_back(std::move(metadata));
      } else {
        stats_[query].push_back(std::move(metadata));
      }
    }
    return result;
  }

  auto MatchVertex(const std::string &label, int64_t id) {
    return Execute(fmt::format("MATCH (n :{} {{id : $id}}) RETURN n", label),
                   {{"id", id}});
  }

  auto MatchNeighbours(const std::string &label, int64_t id) {
    return Execute(
        fmt::format("MATCH (n :{} {{id : $id}})-[e]-(m) RETURN n, e, m", label),
        {{"id", id}});
  }

  auto DetachDeleteVertex(const std::string &label, int64_t id) {
    return Execute(
        fmt::format("MATCH (n :{} {{id : $id}}) DETACH DELETE n", label),
        {{"id", id}});
  }

  auto CreateVertex(const DecodedVertex &vertex) {
    std::stringstream os;
    os << "CREATE (n :";
    utils::PrintIterable(os, vertex.labels, ":");
    os << " {";
    utils::PrintIterable(
        os, vertex.properties, ", ", [&](auto &stream, const auto &pair) {
          if (pair.second.type() == DecodedValue::Type::String) {
            stream << pair.first << ": \"" << pair.second << "\"";
          } else {
            stream << pair.first << ": " << pair.second;
          }
        });
    os << "})";
    return Execute(os.str(), {}, "CREATE (n :labels... {...})");
  }

  auto CreateEdge(const DecodedVertex &from, const std::string &from_label,
                  int64_t from_id, const std::string &to_label, int64_t to_id,
                  const DecodedEdge &edge) {
    std::stringstream os;
    os << fmt::format("MATCH (n :{} {{id : {}}}) ", from_label, from_id);
    os << fmt::format("MATCH (m :{} {{id : {}}}) ", to_label, to_id);
    os << "CREATE (n)";
    if (edge.to == from.id) {
      os << "<-";
    } else {
      os << "-";
    }
    os << "[:" << edge.type << " {";
    utils::PrintIterable(
        os, edge.properties, ", ", [&](auto &stream, const auto &pair) {
          if (pair.second.type() == DecodedValue::Type::String) {
            stream << pair.first << ": \"" << pair.second << "\"";
          } else {
            stream << pair.first << ": " << pair.second;
          }
        });
    os << "}]";
    if (edge.from == from.id) {
      os << "->";
    } else {
      os << "-";
    }
    os << "(m) ";
    os << "RETURN n.id";
    auto ret = Execute(os.str(), {},
                       "MATCH (n :label {id: ...}) MATCH (m :label {id: ...}) "
                       "CREATE (n)-[:type ...]-(m)");
    CHECK(ret.records.size() == 1U)
        << "from_id: " << from_id << " "
        << "to_id: " << to_id << " "
        << "ret.records.size(): " << ret.records.size();
    return ret;
  }

  VertexAndEdges RetrieveAndDeleteVertex(const std::string &label, int64_t id) {
    auto vertex_record = MatchVertex(label, id).records;

    CHECK(vertex_record.size() == 1U)
        << "id: " << id << " "
        << "vertex_record.size(): " << vertex_record.size();

    auto records = MatchNeighbours(label, id).records;

    DetachDeleteVertex(label, id);

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

    return {vertex_record[0][0].ValueVertex(), edges, vertices};
  }

  void ReturnVertexAndEdges(const VertexAndEdges &vertex_and_edges,
                            const std::string &label) {
    int num_queries = 0;
    CreateVertex(vertex_and_edges.vertex);
    ++num_queries;

    for (int i = 0; i < static_cast<int>(vertex_and_edges.vertices.size());
         ++i) {
      auto records =
          CreateEdge(
              vertex_and_edges.vertex, label,
              vertex_and_edges.vertex.properties.at("id").ValueInt(), label,
              vertex_and_edges.vertices[i].properties.at("id").ValueInt(),
              vertex_and_edges.edges[i])
              .records;
      CHECK(records.size() == 1U)
          << "Graph in invalid state "
          << vertex_and_edges.vertex.properties.at("id");
      ++num_queries;
    }
  }

 public:
  void Run(int id, std::vector<int64_t> to_remove,
           std::atomic<bool> &keep_running) {
    std::mt19937 rg(id);
    std::vector<VertexAndEdges> removed;

    const auto &queries = config_["queries"];
    const double read_probability = config_["read_probability"];
    const std::string independent_label = config_["independent_label"];

    while (keep_running) {
      std::uniform_real_distribution<> real_dist(0.0, 1.0);

      // Read query.
      if (real_dist(rg) < read_probability) {
        CHECK(queries.size())
            << "Specify at least one read query or set read_probability to 0";
        std::uniform_int_distribution<> read_query_dist(0, queries.size() - 1);
        const auto &query = queries[read_query_dist(rg)];
        std::map<std::string, DecodedValue> params;
        for (const auto &param : query["params"]) {
          std::uniform_int_distribution<int64_t> param_value_dist(
              param["low"], param["high"]);
          params[param["name"]] = param_value_dist(rg);
        }
        Execute(query["query"], params);
      } else {
        auto remove_random = [&](auto &v) {
          CHECK(v.size());
          std::uniform_int_distribution<> int_dist(0, v.size() - 1);
          std::swap(v.back(), v[int_dist(rg)]);
          auto ret = v.back();
          v.pop_back();
          return ret;
        };
        if (real_dist(rg) < static_cast<double>(removed.size()) /
                                (removed.size() + to_remove.size())) {
          auto vertices_and_edges = remove_random(removed);
          ReturnVertexAndEdges(vertices_and_edges, independent_label);
          to_remove.push_back(
              vertices_and_edges.vertex.properties["id"].ValueInt());
        } else {
          auto node_id = remove_random(to_remove);
          auto ret = RetrieveAndDeleteVertex(independent_label, node_id);
          removed.push_back(ret);
        }
      }
    }
  }

  auto ConsumeStats() {
    std::unique_lock<SpinLock> guard(lock_);
    auto stats = stats_;
    stats_.clear();
    return stats;
  }
};

int64_t NumNodes(BoltClient &client, const std::string &label) {
  auto result = ExecuteNTimesTillSuccess(
      client, "MATCH (n :" + label + ") RETURN COUNT(n) as cnt", {},
      MAX_RETRIES);
  return result.records[0][0].ValueInt();
}

std::vector<int64_t> Neighbours(BoltClient &client, const std::string &label,
                                int64_t id) {
  auto result = ExecuteNTimesTillSuccess(client,
                                         "MATCH (n :" + label +
                                             " {id: " + std::to_string(id) +
                                             "})-[e]-(m) RETURN m.id",
                                         {}, MAX_RETRIES);
  std::vector<int64_t> ret;
  for (const auto &record : result.records) {
    ret.push_back(record[0].ValueInt());
  }
  return ret;
}

std::vector<int64_t> IndependentSet(BoltClient &client,
                                    const std::string &label) {
  const int64_t num_nodes = NumNodes(client, label);
  std::vector<int64_t> independent_nodes_ids;
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
    std::vector<int64_t> neighbour_ids = Neighbours(client, label, i);
    independent_nodes_ids.push_back(i);
    for (auto j : neighbour_ids) {
      independent.erase(j);
    }
  }
  LOG(INFO) << "Number of nodes nodes: " << num_nodes << "\n"
            << "Number of independent nodes: " << independent_nodes_ids.size();

  return independent_nodes_ids;
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  nlohmann::json config;
  std::cin >> config;
  const std::string independent_label = config["independent_label"];

  auto independent_nodes_ids = [&] {
    BoltClient client(utils::ResolveHostname(FLAGS_address), FLAGS_port,
                      FLAGS_username, FLAGS_password);
    return IndependentSet(client, independent_label);
  }();

  utils::Timer timer;
  std::vector<std::thread> threads;
  std::atomic<bool> keep_running{true};

  int64_t next_to_assign = 0;
  std::vector<std::unique_ptr<Session>> sessions;
  sessions.reserve(FLAGS_num_workers);

  for (int i = 0; i < FLAGS_num_workers; ++i) {
    int64_t size = independent_nodes_ids.size();
    int64_t next_next_to_assign = next_to_assign + size / FLAGS_num_workers +
                                  (i < size % FLAGS_num_workers);

    sessions.push_back(std::make_unique<Session>(
        config, FLAGS_address, FLAGS_port, FLAGS_username, FLAGS_password));

    std::vector<int64_t> to_remove(
        independent_nodes_ids.begin() + next_to_assign,
        independent_nodes_ids.begin() + next_next_to_assign);
    LOG(INFO) << next_to_assign << " " << next_next_to_assign;
    next_to_assign = next_next_to_assign;

    threads.emplace_back(
        [&](int thread_id, const std::vector<int64_t> to_remove) {
          sessions[thread_id]->Run(thread_id, std::move(to_remove),
                                   keep_running);
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
    std::unordered_map<std::string, std::map<std::string, DecodedValue>>
        aggregated_stats;

    using namespace std::chrono_literals;
    std::unordered_map<std::string,
                       std::vector<std::map<std::string, DecodedValue>>>
        stats;
    for (const auto &session : sessions) {
      auto session_stats = session->ConsumeStats();
      for (const auto &session_query_stats : session_stats) {
        auto &query_stats = stats[session_query_stats.first];
        query_stats.insert(query_stats.end(),
                           session_query_stats.second.begin(),
                           session_query_stats.second.end());
      }
    }

    // TODO: Here we combine pure values, json and DecodedValue which is a
    // little bit chaotic. Think about refactoring this part to only use json
    // and write DecodedValue to json converter.
    const std::vector<std::string> fields = {
        "wall_time",
        "parsing_time",
        "planning_time",
        "plan_execution_time",
    };
    for (const auto &query_stats : stats) {
      std::map<std::string, double> new_aggregated_query_stats;
      for (const auto &stats : query_stats.second) {
        for (const auto &field : fields) {
          auto it = stats.find(field);
          if (it != stats.end()) {
            new_aggregated_query_stats[field] += it->second.ValueDouble();
          }
        }
      }
      int64_t new_count = query_stats.second.size();

      auto &aggregated_query_stats = aggregated_stats[query_stats.first];
      aggregated_query_stats.insert({"count", DecodedValue(0)});
      auto old_count = aggregated_query_stats["count"].ValueInt();
      aggregated_query_stats["count"].ValueInt() += new_count;
      for (const auto &stat : new_aggregated_query_stats) {
        auto it = aggregated_query_stats.insert({stat.first, DecodedValue(0.0)})
                      .first;
        it->second =
            (it->second.ValueDouble() * old_count + stat.second * new_count) /
            (old_count + new_count);
      }
    }

    out << "{\"num_executed_queries\": " << executed_queries << ", "
        << "\"elapsed_time\": " << timer.Elapsed().count()
        << ", \"queries\": [";
    utils::PrintIterable(
        out, aggregated_stats, ", ", [](auto &stream, const auto &x) {
          stream << "{\"query\": " << nlohmann::json(x.first)
                 << ", \"stats\": ";
          PrintJsonDecodedValue(stream, DecodedValue(x.second));
          stream << "}";
        });
    out << "]}" << std::endl;
    out.flush();
    std::this_thread::sleep_for(1s);
  }
  keep_running = false;

  for (int i = 0; i < FLAGS_num_workers; ++i) {
    threads[i].join();
  }

  return 0;
}
