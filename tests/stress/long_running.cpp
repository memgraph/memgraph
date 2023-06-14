// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <atomic>
#include <fstream>
#include <random>
#include <set>
#include <thread>

#include <fmt/format.h>
#include <gflags/gflags.h>

#include "communication/bolt/client.hpp"
#include "io/network/endpoint.hpp"
#include "utils/exceptions.hpp"
#include "utils/timer.hpp"

using EndpointT = memgraph::io::network::Endpoint;
using ClientContextT = memgraph::communication::ClientContext;
using ClientT = memgraph::communication::bolt::Client;
using ValueT = memgraph::communication::bolt::Value;
using QueryDataT = memgraph::communication::bolt::QueryData;
using ExceptionT = memgraph::communication::bolt::ClientQueryException;

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 7687, "Server port");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");

DEFINE_int32(vertex_count, 0, "The average number of vertices in the graph per worker");
DEFINE_int32(edge_count, 0, "The average number of edges in the graph per worker");
DEFINE_int32(prop_count, 5, "The max number of properties on a node");
DEFINE_uint64(max_queries, 1 << 30, "Maximum number of queries to execute");
DEFINE_int32(max_time, 1, "Maximum execution time in minutes");
DEFINE_int32(verify, 0, "Interval (seconds) between checking local info");
DEFINE_int32(worker_count, 1, "The number of workers that operate on the graph independently");
DEFINE_bool(global_queries, true, "If queries that modify globally should be executed sometimes");

DEFINE_string(stats_file, "", "File into which to write statistics.");

/**
 * Encapsulates a Graph and a Bolt session and provides CRUD op functions.
 * Also defines a run-loop for a generic executor, and a graph state
 * verification function.
 */
class GraphSession {
 public:
  GraphSession(int id, std::atomic<uint64_t> *init_complete)
      : id_(id),
        init_complete_(init_complete),
        indexed_label_(fmt::format("indexed_label{}", id)),
        generator_{std::random_device{}()} {
    for (int i = 0; i < FLAGS_prop_count; ++i) {
      auto label = fmt::format("label{}", i);
      labels_.insert(label);
      labels_vertices_.insert({label, {}});
    }

    EndpointT endpoint(FLAGS_address, FLAGS_port);
    client_ = std::make_unique<ClientT>(context_);
    client_->Connect(endpoint, FLAGS_username, FLAGS_password);
  }

 private:
  uint64_t id_;
  std::atomic<uint64_t> *init_complete_;
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

  memgraph::utils::Timer timer_;

 private:
  double GetRandom() { return std::generate_canonical<double, 10>(generator_); }

  bool Bernoulli(double p) { return GetRandom() < p; }

  uint64_t RandomElement(std::set<uint64_t> &data) {
    uint64_t min = *data.begin(), max = *data.rbegin();
    uint64_t val = std::floor(GetRandom() * (max - min) + min);
    auto it = data.lower_bound(val);
    return *it;
  }

  std::string RandomElement(std::set<std::string> &data) {
    uint64_t pos = std::floor(GetRandom() * data.size());
    auto it = data.begin();
    std::advance(it, pos);
    return *it;
  }

  void AddQueryFailure(const std::string &what) {
    auto it = query_failures_.find(what);
    if (it != query_failures_.end()) {
      ++it->second;
    } else {
      query_failures_.insert(std::make_pair(what, 1));
    }
  }

  QueryDataT ExecuteWithoutCatch(const std::string &query) {
    SPDLOG_INFO("Runner {} executing query: {}", id_, query);
    executed_queries_ += 1;
    return client_->Execute(query, {});
  }

  QueryDataT Execute(const std::string &query) {
    try {
      return ExecuteWithoutCatch(query);
    } catch (const ExceptionT &e) {
      AddQueryFailure(e.what());
      return QueryDataT();
    }
  }

  void CreateVertices(uint64_t vertices_count) {
    if (vertices_count == 0) return;
    QueryDataT ret;
    try {
      ret = ExecuteWithoutCatch(fmt::format("UNWIND RANGE({}, {}) AS r CREATE (n:{} {{id: r}}) RETURN count(n)",
                                            vertex_id_, vertex_id_ + vertices_count - 1, indexed_label_));
    } catch (const ExceptionT &e) {
      LOG_FATAL("Runner {} vertices creation failed because of: {}", id_, e.what());
    }
    MG_ASSERT(ret.records.size(), "Runner {} the vertices creation query should have returned a row!", id_);

    MG_ASSERT(ret.records[0][0].ValueInt() == vertices_count, "Runner {} created {} vertices instead of {}!", id_,
              ret.records[0][0].ValueInt(), vertices_count);
    for (uint64_t i = 0; i < vertices_count; ++i) {
      MG_ASSERT(vertices_.insert(vertex_id_ + i).second, "Runner {} vertex with ID {} shouldn't exist!", id_,
                vertex_id_ + i);
    }
    vertex_id_ += vertices_count;
  }

  void RemoveVertex() {
    auto vertex_id = RandomElement(vertices_);
    auto ret =
        Execute(fmt::format("MATCH (n:{} {{id: {}}}) OPTIONAL MATCH (n)-[r]-() "
                            "WITH n, n.id as n_id, labels(n) as labels_n, collect(r.id) as r_ids "
                            "DETACH DELETE n RETURN n_id, labels_n, r_ids",
                            indexed_label_, vertex_id));
    if (ret.records.size() > 0) {
      std::set<uint64_t> processed_vertices;
      auto &record = ret.records[0];
      // remove vertex but note there could be duplicates
      auto n_id = record[0].ValueInt();
      if (processed_vertices.insert(n_id).second) {
        MG_ASSERT(vertices_.erase(n_id), "Runner {} vertex with ID {} should exist!", id_, n_id);
        for (auto &label : record[1].ValueList()) {
          if (label.ValueString() == indexed_label_) {
            continue;
          }
          labels_vertices_[label.ValueString()].erase(n_id);
        }
      }
      // remove edge
      auto &edges = record[2];
      for (auto &edge : edges.ValueList()) {
        if (edge.type() == ValueT::Type::Int) {
          MG_ASSERT(edges_.erase(edge.ValueInt()), "Runner {} edge with ID {} should exist!", id_, edge.ValueInt());
        }
      }
    }
  }

  void CreateEdges(uint64_t edges_count) {
    if (edges_count == 0) return;
    auto edges_per_node = (double)edges_count / vertices_.size();
    MG_ASSERT(std::abs(edges_per_node - (int64_t)edges_per_node) < 0.0001,
              "Runner {} edges per node not a whole number!", id_);

    QueryDataT ret;
    try {
      ret =
          ExecuteWithoutCatch(fmt::format("MATCH (a:{0}) WITH a "
                                          "UNWIND range(0, {1}) AS i WITH a, tointeger(rand() * {2}) AS id "
                                          "MATCH (b:{0} {{id: id}}) WITH a, b "
                                          "CREATE (a)-[e:EdgeType {{id: counter(\"edge\", {3})}}]->(b) "
                                          "RETURN count(e)",
                                          indexed_label_, (int64_t)edges_per_node - 1, vertices_.size(), edge_id_));
    } catch (const ExceptionT &e) {
      LOG_FATAL("Runner {} edges creation failed because of: {}", id_, e.what());
    }
    MG_ASSERT(ret.records.size(), "Runner {} the edges creation query should have returned a row!", id_);

    uint64_t count = ret.records[0][0].ValueInt();
    for (uint64_t i = 0; i < count; ++i) {
      MG_ASSERT(edges_.insert(edge_id_ + i).second, "Runner {} edge with ID {} shouldn't exist!", id_, edge_id_ + i);
    }
    edge_id_ += count;
  }

  void CreateEdge() {
    auto ret = Execute(
        fmt::format("MATCH (from:{} {{id: {}}}), (to:{} {{id: {}}}) "
                    "CREATE (from)-[e:EdgeType {{id: {}}}]->(to) RETURN 1",
                    indexed_label_, RandomElement(vertices_), indexed_label_, RandomElement(vertices_), edge_id_));
    if (ret.records.size() > 0) {
      MG_ASSERT(edges_.insert(edge_id_).second, "Runner {} edge with ID {} shouldn't exist!", id_, edge_id_);
      ++edge_id_;
    }
  }

  void AddLabel() {
    auto vertex_id = RandomElement(vertices_);
    auto label = RandomElement(labels_);
    // add a label on a vertex that didn't have that label
    // yet (we need that for book-keeping)
    auto ret = Execute(fmt::format("MATCH (v:{} {{id: {}}}) WHERE not v:{} SET v:{} RETURN 1", indexed_label_,
                                   vertex_id, label, label));
    if (ret.records.size() > 0) {
      labels_vertices_[label].insert(vertex_id);
    }
  }

  void UpdateGlobalVertices() {
    uint64_t vertex_id = *vertices_.rbegin();
    uint64_t lo = std::floor(GetRandom() * vertex_id);
    uint64_t hi = std::floor(lo + vertex_id * 0.01);
    uint64_t num = std::floor(GetRandom() * (1 << 30));
    Execute(fmt::format("MATCH (n) WHERE n.id > {} AND n.id < {} SET n.value = {}", lo, hi, num));
  }

  void UpdateGlobalEdges() {
    uint64_t vertex_id = *vertices_.rbegin();
    uint64_t lo = std::floor(GetRandom() * vertex_id);
    uint64_t hi = std::floor(lo + vertex_id * 0.01);
    uint64_t num = std::floor(GetRandom() * (1 << 30));
    Execute(fmt::format("MATCH ()-[e]->() WHERE e.id > {} AND e.id < {} SET e.value = {}", lo, hi, num));
  }

  void CheckGraphProjection() {
    uint64_t vertex_id = *vertices_.rbegin();
    uint64_t lo = std::floor(GetRandom() * vertex_id);
    uint64_t hi = std::floor(lo + vertex_id * 0.01);

    Execute(fmt::format(
        "MATCH p=()-[e]->() WHERE e.id > {} AND e.id < {} WITH project(p) as graph WITH graph.nodes as nodes "
        "UNWIND nodes as n RETURN n.x "
        "as x ORDER BY x DESC",
        lo, hi));
    Execute(fmt::format(
        "MATCH p=()-[e]->() WHERE e.id > {} AND e.id < {} WITH project(p) as graph WITH graph.edges as edges "
        "UNWIND edges as e RETURN e.prop as y ORDER BY y DESC",
        lo, hi));
  }

  /** Checks if the local info corresponds to DB state */
  void VerifyGraph() {
    // helper lambda for set verification
    auto test_set = [this](const auto &query, const auto &container, const auto &what) {
      QueryDataT ret;
      try {
        ret = ExecuteWithoutCatch(query);
      } catch (const ExceptionT &e) {
        LOG_FATAL("Runner {} couldn't execute {} ID retrieval because of: {}", id_, what, e.what());
      }
      MG_ASSERT(ret.records.size() == container.size(), "Runner {} expected {} {}, found {}!", id_, container.size(),
                what, ret.records.size());
      for (size_t i = 0; i < ret.records.size(); ++i) {
        MG_ASSERT(ret.records[i].size() == 1, "Runner {} received an invalid ID row for {}!", id_, what);
        auto id = ret.records[i][0].ValueInt();
        MG_ASSERT(container.find(id) != container.end(),
                  "Runner {} couldn't find ID {} for {}! Examined {} items out "
                  "of {} items!",
                  id_, id, what, i, ret.records.size());
      }
    };

    test_set(fmt::format("MATCH (n:{}) RETURN n.id", indexed_label_), vertices_, "vertices");
    test_set(fmt::format("MATCH (:{0})-[r]->(:{0}) RETURN r.id", indexed_label_), edges_, "edges");

    for (auto &item : labels_vertices_) {
      test_set(fmt::format("MATCH (n:{}:{}) RETURN n.id", indexed_label_, item.first), item.second,
               fmt::format("vertices with label '{}'", item.first));
    }

    // generate report
    std::ostringstream report;
    report << fmt::format("Runner {} graph verification success:", id_) << std::endl
           << fmt::format("\tExecuted {} queries in {:.2f} seconds", executed_queries_, timer_.Elapsed().count())
           << std::endl
           << fmt::format("\tGraph has {} vertices and {} edges", vertices_.size(), edges_.size()) << std::endl;
    for (auto &label : labels_) {
      report << fmt::format("\tVertices with label '{}': {}", label, labels_vertices_[label].size()) << std::endl;
    }
    if (query_failures_.size() > 0) {
      report << "\tQuery failed (reason: count)" << std::endl;
      for (auto &item : query_failures_) {
        report << fmt::format("\t\t'{}': {}", item.first, item.second) << std::endl;
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

    VerifyGraph();
    double last_verify = timer_.Elapsed().count();

    // notify that we completed our initialization
    init_complete_->fetch_add(-1, std::memory_order_acq_rel);

    // run rest
    while (executed_queries_ < FLAGS_max_queries && timer_.Elapsed().count() / 60.0 < FLAGS_max_time) {
      if (FLAGS_verify > 0 && timer_.Elapsed().count() - last_verify > FLAGS_verify) {
        VerifyGraph();
        last_verify = timer_.Elapsed().count();
      }

      double ratio_e = (double)edges_.size() / (double)FLAGS_edge_count;
      double ratio_v = (double)vertices_.size() / (double)FLAGS_vertex_count;

      // try to edit vertices globally if all workers completed their
      // initialization
      if (FLAGS_global_queries && init_complete_->load(std::memory_order_acquire) == 0) {
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
      CheckGraphProjection();
    }

    // final verification
    VerifyGraph();
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

  memgraph::communication::SSLInit sslInit;

  MG_ASSERT(FLAGS_vertex_count > 0, "Vertex count must be greater than 0!");
  MG_ASSERT(FLAGS_edge_count > 0, "Edge count must be greater than 0!");

  spdlog::info("Starting Memgraph long running test");

  // create client
  EndpointT endpoint(FLAGS_address, FLAGS_port);
  ClientContextT context(FLAGS_use_ssl);
  ClientT client(context);
  client.Connect(endpoint, FLAGS_username, FLAGS_password);

  // cleanup and create indexes
  client.Execute("MATCH (n) DETACH DELETE n", {});
  for (int i = 0; i < FLAGS_worker_count; ++i) {
    client.Execute(fmt::format("CREATE INDEX ON :indexed_label{}", i), {});
    client.Execute(fmt::format("CREATE INDEX ON :indexed_label{}(id)", i), {});
  }

  // close client
  client.Close();

  // sessions
  std::vector<GraphSession> sessions;
  std::atomic<uint64_t> init_complete(FLAGS_worker_count);
  sessions.reserve(FLAGS_worker_count);
  for (int i = 0; i < FLAGS_worker_count; ++i) {
    sessions.emplace_back(i, &init_complete);
  }

  // workers
  std::vector<std::thread> threads;
  for (int i = 0; i < FLAGS_worker_count; ++i) {
    threads.push_back(std::thread([&, i]() { sessions[i].Run(); }));
  }

  for (int i = 0; i < FLAGS_worker_count; ++i) {
    threads[i].join();
  }

  if (FLAGS_stats_file != "") {
    uint64_t executed = 0, failed = 0;
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
