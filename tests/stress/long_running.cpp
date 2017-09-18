#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/bolt/client.hpp"
#include "io/network/network_endpoint.hpp"
#include "io/network/socket.hpp"
#include "utils/exceptions.hpp"
#include "utils/timer.hpp"

using SocketT = io::network::Socket;
using EndpointT = io::network::NetworkEndpoint;
using ClientT = communication::bolt::Client<SocketT>;
using DecodedValueT = communication::bolt::DecodedValue;
using QueryDataT = communication::bolt::QueryData;
using ExceptionT = communication::bolt::ClientQueryException;

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_string(port, "7687", "Server port");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");

DEFINE_int32(vertex_count, 0, "The average number of vertices in the graph");
DEFINE_int32(edge_count, 0, "The average number of edges in the graph");
DEFINE_int32(prop_count, 5, "The max number of properties on a node");
DEFINE_uint64(max_queries, 1 << 30, "Maximum number of queries to execute");
DEFINE_int32(max_time, 1, "Maximum execution time in minutes");
DEFINE_int32(verify, 0, "Interval (seconds) between checking local info");
DEFINE_int32(worker_count, 1,
             "The number of workers that operate on the graph independently");
DEFINE_bool(global_queries, false,
            "If queries that modifiy globally should be executed sometimes");

/**
 * Encapsulates a Graph and a Bolt session and provides CRUD op functions.
 * Also defines a run-loop for a generic exectutor, and a graph state
 * verification function.
 */
class GraphSession {
 public:
  GraphSession(int id)
      : id_(id),
        indexed_label_(fmt::format("indexed_label{}", id)),
        generator_{std::random_device{}()} {
    for (int i = 0; i < FLAGS_prop_count; ++i) {
      auto label = fmt::format("label{}", i);
      labels_.insert(label);
      labels_vertices_.insert({label, {}});
    }

    EndpointT endpoint(FLAGS_address, FLAGS_port);
    SocketT socket;

    if (!socket.Connect(endpoint)) {
      throw utils::BasicException("Couldn't connect to server!");
    }

    client_ = std::make_unique<ClientT>(std::move(socket), FLAGS_username,
                                        FLAGS_password);
  }

 private:
  uint64_t id_;
  std::unique_ptr<ClientT> client_;

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
  T RandomElement(std::set<T> &data) {
    uint64_t pos = std::floor(GetRandom() * data.size());
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
      DLOG(INFO) << "Runner " << id_ << " executing query: " << query;
      executed_queries_ += 1;
      return client_->Execute(query, {});
    } catch (const ExceptionT &e) {
      AddQueryFailure(std::string{e.what()});
      return QueryDataT();
    }
  }

  void CreateVertices(uint64_t vertices_count) {
    if (vertices_count == 0) return;
    auto ret =
        Execute(fmt::format("UNWIND RANGE(1, {}) AS r CREATE (n:{} {{id: "
                            "counter(\"vertex{}\")}}) RETURN min(n.id)",
                            vertices_count, indexed_label_, id_));
    permanent_assert(ret.records.size() == 1, "Vertices creation failed!");
    uint64_t min_id = ret.records[0][0].ValueInt();
    for (uint64_t i = 0; i < vertices_count; ++i) {
      vertices_.insert(min_id + i);
    }
  }

  void RemoveVertex() {
    auto vertex_id = RandomElement(vertices_);
    auto ret =
        Execute(fmt::format("MATCH (n:{} {{id: {}}}) OPTIONAL MATCH (n)-[r]-() "
                            "DETACH DELETE n RETURN n.id, labels(n), r.id",
                            indexed_label_, vertex_id));
    if (ret.records.size() > 0) {
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
        if (edge.type() == DecodedValueT::Type::Int) {
          edges_.erase(edge.ValueInt());
        }
      }
    }
  }

  void CreateEdges(uint64_t edges_count) {
    if (edges_count == 0) return;
    double probability =
        (double)edges_count / (double)(vertices_.size() * vertices_.size());
    auto ret = Execute(fmt::format(
        "MATCH (a:{0}) WITH a MATCH (b:{0}) WITH a, b WHERE rand() < {1} "
        "CREATE (a)-[e:EdgeType {{id: counter(\"edge{2}\")}}]->(b) RETURN "
        "min(e.id), count(e)",
        indexed_label_, probability, id_));
    if (ret.records.size() > 0) {
      uint64_t min_id = ret.records[0][0].ValueInt();
      uint64_t count = ret.records[0][1].ValueInt();
      for (uint64_t i = 0; i < count; ++i) {
        edges_.insert(min_id + i);
      }
    }
  }

  void CreateEdge() {
    auto ret =
        Execute(fmt::format("MATCH (from:{} {{id: {}}}), (to:{} {{id: {}}}) "
                            "CREATE (from)-[e:EdgeType {{id: "
                            "counter(\"edge{}\")}}]->(to) RETURN e.id",
                            indexed_label_, RandomElement(vertices_),
                            indexed_label_, RandomElement(vertices_), id_));
    if (ret.records.size() > 0) {
      edges_.insert(ret.records[0][0].ValueInt());
    }
  }

  void RemoveEdge() {
    auto edge_id = RandomElement(edges_);
    auto ret = Execute(
        fmt::format("MATCH (:{})-[e {{id: {}}}]->(:{}) DELETE e RETURN e.id",
                    indexed_label_, edge_id, indexed_label_));
    if (ret.records.size() > 0) {
      edges_.erase(edge_id);
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
    if (ret.records.size() > 0) {
      labels_vertices_[label].insert(vertex_id);
    }
  }

  void UpdateGlobalVertices() {
    uint64_t vertex_id = *vertices_.rbegin();
    uint64_t lo = std::floor(GetRandom() * vertex_id);
    uint64_t hi = std::floor(lo + vertex_id * 0.01);
    uint64_t num = std::floor(GetRandom() * (1 << 30));
    Execute(
        fmt::format("MATCH (n) WHERE n.id > {} AND n.id < {} SET n.value = {}",
                    lo, hi, num));
  }

  void UpdateGlobalEdges() {
    uint64_t vertex_id = *vertices_.rbegin();
    uint64_t lo = std::floor(GetRandom() * vertex_id);
    uint64_t hi = std::floor(lo + vertex_id * 0.01);
    uint64_t num = std::floor(GetRandom() * (1 << 30));
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
      if (ret.records.size() == 0) {
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
    report << std::endl
           << fmt::format("Runner {} graph verification success:", id_)
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
    if (query_failures_.size() > 0) {
      report << "\tQuery failed (reason: count)" << std::endl;
      for (auto &item : query_failures_) {
        report << fmt::format("\t\t'{}': {}", item.first, item.second)
               << std::endl;
      }
    }
    LOG(INFO) << report.str();
  }

 public:
  void Run() {
    uint64_t vertex_count = FLAGS_vertex_count / FLAGS_worker_count;
    uint64_t edge_count = FLAGS_edge_count / FLAGS_worker_count;

    // initial vertex creation
    CreateVertices(vertex_count);

    // initial edge creation
    CreateEdges(edge_count);

    double last_verify = timer_.Elapsed().count();

    // run rest
    while (executed_queries_ < FLAGS_max_queries &&
           timer_.Elapsed().count() / 60.0 < FLAGS_max_time) {
      if (FLAGS_verify > 0 &&
          timer_.Elapsed().count() - last_verify > FLAGS_verify) {
        VerifyGraph();
        last_verify = timer_.Elapsed().count();
      }

      double ratio_e = (double)edges_.size() / (double)edge_count;
      double ratio_v = (double)vertices_.size() / (double)vertex_count;

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

      // prefer adding/removing edges whenever there is an edge
      // disbalance and there is enough vertices
      if (ratio_v > 0.5 && std::fabs(1.0 - ratio_e) > 0.2) {
        if (Bernoulli(ratio_e / 2.0)) {
          RemoveEdge();
        } else {
          CreateEdge();
        }
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
};

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  permanent_assert(FLAGS_vertex_count > 0,
                   "Vertex count must be greater than 0!");
  permanent_assert(FLAGS_edge_count > 0, "Edge count must be greater than 0!");

  LOG(INFO) << "Starting Memgraph long running test";

  // create client
  EndpointT endpoint(FLAGS_address, FLAGS_port);
  SocketT socket;
  if (!socket.Connect(endpoint)) {
    throw utils::BasicException("Couldn't connect to server!");
  }
  ClientT client(std::move(socket), FLAGS_username, FLAGS_password);

  // cleanup and create indexes
  client.Execute("MATCH (n) DETACH DELETE n", {});
  for (int i = 0; i < FLAGS_worker_count; ++i) {
    client.Execute(fmt::format("CREATE INDEX ON :indexed_label{}(id)", i), {});
    client.Execute(fmt::format("RETURN counterSet(\"vertex{}\", 0)", i), {});
    client.Execute(fmt::format("RETURN counterSet(\"edge{}\", 0)", i), {});
  }

  // close client
  client.Close();

  // workers
  std::vector<std::thread> threads;

  for (int i = 0; i < FLAGS_worker_count; ++i) {
    threads.push_back(std::thread([&, i]() {
      GraphSession session(i);
      session.Run();
    }));
  }

  for (int i = 0; i < FLAGS_worker_count; ++i) {
    threads[i].join();
  }

  LOG(INFO) << "All query runners done";

  return 0;
}
