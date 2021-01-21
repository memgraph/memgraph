#include <fstream>
#include <random>
#include <set>
#include <thread>

#include <fmt/format.h>
#include <gflags/gflags.h>

#include "communication/bolt/ha_client.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/utils.hpp"
#include "utils/exceptions.hpp"
#include "utils/string.hpp"
#include "utils/timer.hpp"

using EndpointT = io::network::Endpoint;
using ClientContextT = communication::ClientContext;
using ClientT = communication::bolt::HAClient;
using ValueT = communication::bolt::Value;
using QueryDataT = communication::bolt::QueryData;
using ExceptionT = communication::bolt::ClientQueryException;

DEFINE_string(endpoints, "127.0.0.1:7687,127.0.0.1:7688,127.0.0.1:7689",
              "Cluster server endpoints (host:port, separated by comma).");
DEFINE_int32(cluster_size, 3, "Size of the raft cluster.");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");

DEFINE_int32(vertex_count, 0,
             "The average number of vertices in the graph per worker");
DEFINE_int32(edge_count, 0,
             "The average number of edges in the graph per worker");
DEFINE_int32(prop_count, 5, "The max number of properties on a node");
DEFINE_uint64(max_queries, 1U << 30U, "Maximum number of queries to execute");
DEFINE_int32(max_time, 1, "Maximum execution time in minutes");
DEFINE_int32(verify, 0, "Interval (seconds) between checking local info");
DEFINE_int32(worker_count, 1,
             "The number of workers that operate on the graph independently");
DEFINE_bool(global_queries, true,
            "If queries that modifiy globally should be executed sometimes");

DEFINE_string(stats_file, "", "File into which to write statistics.");

std::vector<EndpointT> GetEndpoints() {
  std::vector<io::network::Endpoint> ret;
  for (const auto &endpoint : utils::Split(FLAGS_endpoints, ",")) {
    auto split = utils::Split(utils::Trim(endpoint), ":");
    MG_ASSERT(split.size() == 2, "Invalid endpoint!");
    ret.emplace_back(
        io::network::ResolveHostname(std::string(utils::Trim(split[0]))),
        static_cast<uint16_t>(std::stoi(std::string(utils::Trim(split[1])))));
  }
  return ret;
}

/**
 * Encapsulates a Graph and a Bolt session and provides CRUD op functions.
 * Also defines a run-loop for a generic exectutor, and a graph state
 * verification function.
 */
class GraphSession {
 public:
  explicit GraphSession(int id)
      : id_(id),
        indexed_label_(fmt::format("indexed_label{}", id)),
        generator_{std::random_device{}()} {
    for (int i = 0; i < FLAGS_prop_count; ++i) {
      auto label = fmt::format("label{}", i);
      labels_.insert(label);
      labels_vertices_.insert({label, {}});
    }

    std::vector<EndpointT> endpoints = GetEndpoints();

    uint64_t retries = 15;
    std::chrono::milliseconds retry_delay(1000);
    ClientContextT context(FLAGS_use_ssl);
    client_ = std::make_unique<ClientT>(endpoints, &context_, FLAGS_username,
                                        FLAGS_password, retries, retry_delay);
  }

 private:
  uint64_t id_;
  ClientContextT context_{FLAGS_use_ssl};
  std::unique_ptr<ClientT> client_;

  uint64_t vertex_id_{0};
  uint64_t edge_id_{0};

  std::set<uint64_t> vertices_;
  std::set<uint64_t> edges_;

  std::string indexed_label_;
  std::set<std::string> labels_;

  std::map<std::string, std::set<uint64_t>> labels_vertices_;

  uint64_t executed_queries_{0};
  std::map<std::string, uint64_t> query_failures_;

  std::mt19937 generator_;

  utils::Timer timer_;

 private:
  double GetRandom() { return std::generate_canonical<double, 10>(generator_); }

  bool Bernoulli(double p) { return GetRandom() < p; }

  template <typename T>
  T RandomElement(const std::set<T> &data) {
    uint32_t pos = std::floor(GetRandom() * data.size());
    auto it = data.begin();
    std::advance(it, pos);
    return *it;
  }

  void AddQueryFailure(std::string what) {
    auto it = query_failures_.find(what);
    if (it != query_failures_.end()) {
      ++it->second;
    } else {
      query_failures_.insert(std::make_pair(what, 1));
    }
  }

  QueryDataT Execute(std::string query) {
    try {
      SPDLOG_INFO("Runner {} executing query: {}", id_, query);
      executed_queries_ += 1;
      return client_->Execute(query, {});
    } catch (const ExceptionT &e) {
      AddQueryFailure(std::string{e.what()});
      return QueryDataT();
    }
  }

  void CreateVertices(uint64_t vertices_count) {
    if (vertices_count == 0) return;
    auto ret = Execute(fmt::format(
        "UNWIND RANGE({}, {}) AS r CREATE (n:{} {{id: r}}) RETURN count(n)",
        vertex_id_, vertex_id_ + vertices_count - 1, indexed_label_));
    MG_ASSERT(ret.records.size() == 1, "Vertices creation failed!");
    MG_ASSERT(ret.records[0][0].ValueInt() == vertices_count,
              "Created {} vertices instead of {}!",
              ret.records[0][0].ValueInt(), vertices_count);
    for (uint64_t i = 0; i < vertices_count; ++i) {
      vertices_.insert(vertex_id_ + i);
    }
    vertex_id_ += vertices_count;
  }

  void RemoveVertex() {
    auto vertex_id = RandomElement(vertices_);
    auto ret =
        Execute(fmt::format("MATCH (n:{} {{id: {}}}) OPTIONAL MATCH (n)-[r]-() "
                            "DETACH DELETE n RETURN n.id, labels(n), r.id",
                            indexed_label_, vertex_id));
    if (!ret.records.empty()) {
      std::set<uint64_t> processed_vertices;
      for (auto &record : ret.records) {
        // remove vertex but note there could be duplicates
        auto n_id = record[0].ValueInt();
        if (processed_vertices.insert(n_id).second) {
          vertices_.erase(n_id);
          for (auto &label : record[1].ValueList()) {
            if (label.ValueString() == indexed_label_) {
              continue;
            }
            labels_vertices_[label.ValueString()].erase(n_id);
          }
        }
        // remove edge
        auto &edge = record[2];
        if (edge.type() == ValueT::Type::Int) {
          edges_.erase(edge.ValueInt());
        }
      }
    }
  }

  void CreateEdges(uint64_t edges_count) {
    if (edges_count == 0) return;
    auto edges_per_node = (double)edges_count / vertices_.size();
    MG_ASSERT(std::abs(edges_per_node - (int64_t)edges_per_node) < 0.0001,
              "Edges per node not a whole number");

    auto ret = Execute(fmt::format(
        "MATCH (a:{0}) WITH a "
        "UNWIND range(0, {1}) AS i WITH a, tointeger(rand() * {2}) AS id "
        "MATCH (b:{0} {{id: id}}) WITH a, b "
        "CREATE (a)-[e:EdgeType {{id: counter(\"edge\", {3})}}]->(b) "
        "RETURN count(e)",
        indexed_label_, (int64_t)edges_per_node - 1, vertices_.size(),
        edge_id_));

    MG_ASSERT(ret.records.size() == 1, "Failed to create edges");
    uint64_t count = ret.records[0][0].ValueInt();
    for (uint64_t i = 0; i < count; ++i) {
      edges_.insert(edge_id_ + i);
    }
    edge_id_ += count;
  }

  void CreateEdge() {
    auto ret = Execute(
        fmt::format("MATCH (from:{} {{id: {}}}), (to:{} {{id: {}}}) "
                    "CREATE (from)-[e:EdgeType {{id: "
                    "counter(\"edge\", {})}}]->(to) RETURN e.id",
                    indexed_label_, RandomElement(vertices_), indexed_label_,
                    RandomElement(vertices_), edge_id_));
    if (!ret.records.empty()) {
      edges_.insert(ret.records[0][0].ValueInt());
      edge_id_ += 1;
    }
  }

  void AddLabel() {
    auto vertex_id = RandomElement(vertices_);
    auto label = RandomElement(labels_);
    // add a label on a vertex that didn't have that label
    // yet (we need that for book-keeping)
    auto ret = Execute(fmt::format(
        "MATCH (v:{} {{id: {}}}) WHERE not v:{} SET v:{} RETURN v.id",
        indexed_label_, vertex_id, label, label));
    if (!ret.records.empty()) {
      labels_vertices_[label].insert(vertex_id);
    }
  }

  void UpdateGlobalVertices() {
    uint64_t vertex_id = *vertices_.rbegin();
    uint64_t lo = std::floor(GetRandom() * vertex_id);
    uint64_t hi = std::floor(lo + vertex_id * 0.01);
    uint64_t num = std::floor(GetRandom() * (1U << 30U));
    Execute(
        fmt::format("MATCH (n) WHERE n.id > {} AND n.id < {} SET n.value = {}",
                    lo, hi, num));
  }

  void UpdateGlobalEdges() {
    uint64_t vertex_id = *vertices_.rbegin();
    uint64_t lo = std::floor(GetRandom() * vertex_id);
    uint64_t hi = std::floor(lo + vertex_id * 0.01);
    uint64_t num = std::floor(GetRandom() * (1U << 30U));
    Execute(fmt::format(
        "MATCH ()-[e]->() WHERE e.id > {} AND e.id < {} SET e.value = {}", lo,
        hi, num));
  }

  /** Checks if the local info corresponds to DB state */
  void VerifyGraph() {
    // helper lambda for count verification
    auto test_count = [this](std::string query, int64_t count,
                             std::string message) {
      auto ret = Execute(query);
      if (ret.records.empty()) {
        throw utils::BasicException("Couldn't execute count!");
      }
      if (ret.records[0][0].ValueInt() != count) {
        throw utils::BasicException(
            fmt::format(message, id_, count, ret.records[0][0].ValueInt()));
      }
    };

    test_count(fmt::format("MATCH (n:{}) RETURN count(n)", indexed_label_),
               vertices_.size(), "Runner {} expected {} vertices, found {}!");
    test_count(
        fmt::format("MATCH (:{0})-[r]->(:{0}) RETURN count(r)", indexed_label_),
        edges_.size(), "Runner {} expected {} edges, found {}!");

    for (auto &item : labels_vertices_) {
      test_count(
          fmt::format("MATCH (n:{}:{}) RETURN count(n)", indexed_label_,
                      item.first),
          item.second.size(),
          fmt::format(
              "Runner {{}} expected {{}} vertices with label '{}', found {{}}!",
              item.first));
    }

    // generate report
    std::ostringstream report;
    report << fmt::format("Runner {} graph verification success:", id_)
           << std::endl
           << fmt::format("\tExecuted {} queries in {:.2f} seconds",
                          executed_queries_, timer_.Elapsed().count())
           << std::endl
           << fmt::format("\tGraph has {} vertices and {} edges",
                          vertices_.size(), edges_.size())
           << std::endl;
    for (auto &label : labels_) {
      report << fmt::format("\tVertices with label '{}': {}", label,
                            labels_vertices_[label].size())
             << std::endl;
    }
    if (!query_failures_.empty()) {
      report << "\tQuery failed (reason: count)" << std::endl;
      for (auto &item : query_failures_) {
        report << fmt::format("\t\t'{}': {}", item.first, item.second)
               << std::endl;
      }
    }
    spdlog::info(report.str());
  }

 public:
  void Run() {
    // initial vertex creation
    CreateVertices(FLAGS_vertex_count);

    // initial edge creation
    CreateEdges(FLAGS_edge_count);

    if (FLAGS_verify > 0) VerifyGraph();
    double last_verify = timer_.Elapsed().count();

    // run rest
    while (executed_queries_ < FLAGS_max_queries &&
           timer_.Elapsed().count() / 60.0 < FLAGS_max_time) {
      if (FLAGS_verify > 0 &&
          timer_.Elapsed().count() - last_verify > FLAGS_verify) {
        VerifyGraph();
        last_verify = timer_.Elapsed().count();
      }

      double ratio_e = (double)edges_.size() / (double)FLAGS_edge_count;
      double ratio_v = (double)vertices_.size() / (double)FLAGS_vertex_count;

      // try to edit vertices globally
      if (FLAGS_global_queries) {
        if (Bernoulli(0.01)) {
          UpdateGlobalVertices();
        }

        // try to edit edges globally
        if (Bernoulli(0.01)) {
          UpdateGlobalEdges();
        }
      }

      // if we're missing edges (due to vertex detach delete), add some!
      if (Bernoulli(ratio_e < 0.9)) {
        CreateEdge();
        continue;
      }

      // if we are near vertex balance, we can also do updates
      // instad of update / deletes
      if (std::fabs(1.0 - ratio_v) < 0.5 && Bernoulli(0.5)) {
        AddLabel();
        continue;
      }

      if (Bernoulli(ratio_v / 2.0)) {
        RemoveVertex();
      } else {
        CreateVertices(1);
      }
    }
  }

  uint64_t GetExecutedQueries() { return executed_queries_; }

  uint64_t GetFailedQueries() {
    uint64_t failed = 0;
    for (const auto &item : query_failures_) {
      failed += item.second;
    }
    return failed;
  }
};

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  MG_ASSERT(FLAGS_vertex_count > 0, "Vertex count must be greater than 0!");
  MG_ASSERT(FLAGS_edge_count > 0, "Edge count must be greater than 0!");

  communication::SSLInit sslInit;

  spdlog::info("Starting Memgraph HA normal operation long running test");

  try {
    std::vector<EndpointT> endpoints = GetEndpoints();

    // create client
    uint64_t retries = 15;
    std::chrono::milliseconds retry_delay(1000);
    ClientContextT context(FLAGS_use_ssl);
    ClientT client(endpoints, &context, FLAGS_username, FLAGS_password, retries,
                   retry_delay);

    // cleanup and create indexes
    client.Execute("MATCH (n) DETACH DELETE n", {});
    for (int i = 0; i < FLAGS_worker_count; ++i) {
      client.Execute(fmt::format("CREATE INDEX ON :indexed_label{}(id)", i),
                     {});
    }
  } catch (const communication::bolt::ClientFatalException &e) {
    spdlog::warn("Unable to find cluster leader");
    return 1;
  } catch (const communication::bolt::ClientQueryException &e) {
    spdlog::warn(
        "Transient error while executing query. (eg. mistyped query, etc.)\n{}",
        e.what());
    return 1;
  } catch (const utils::BasicException &e) {
    spdlog::warn("Error while executing query\n{}", e.what());
    return 1;
  }

  // sessions
  std::vector<GraphSession> sessions;
  sessions.reserve(FLAGS_worker_count);
  for (int i = 0; i < FLAGS_worker_count; ++i) sessions.emplace_back(i);

  // workers
  std::vector<std::thread> threads;
  threads.reserve(FLAGS_worker_count);
  for (int i = 0; i < FLAGS_worker_count; ++i)
    threads.emplace_back([&, i]() { sessions[i].Run(); });

  for (int i = 0; i < FLAGS_worker_count; ++i) threads[i].join();

  if (!FLAGS_stats_file.empty()) {
    uint64_t executed = 0;
    uint64_t failed = 0;
    for (int i = 0; i < FLAGS_worker_count; ++i) {
      executed += sessions[i].GetExecutedQueries();
      failed += sessions[i].GetFailedQueries();
    }
    std::ofstream stream(FLAGS_stats_file);
    stream << executed << std::endl << failed << std::endl;
    spdlog::info("Written statistics to file: {}", FLAGS_stats_file);
  }

  spdlog::info("All query runners done");

  return 0;
}
