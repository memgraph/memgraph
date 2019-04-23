// Macros from query_common.hpp break enum declaration in distributed_ops.hpp
// (because of the SHOW_STREAMS macro), so we must be careful with the order of
// includes.
#include "query/plan/distributed_ops.hpp"

#include "bfs_common.hpp"
#include "distributed_common.hpp"

using namespace query;
using namespace query::plan;

class DistributedDb : public Database {
 public:
  DistributedDb() : cluster_(3, "DistributedBfsTest") {}

  std::unique_ptr<database::GraphDbAccessor> Access() override {
    return cluster_.master()->Access();
  }

  void AdvanceCommand(tx::TransactionId tx_id) override {
    cluster_.AdvanceCommand(tx_id);
  }

  std::unique_ptr<LogicalOperator> MakeBfsOperator(
      Symbol source_sym, Symbol sink_sym, Symbol edge_sym,
      EdgeAtom::Direction direction,
      const std::vector<storage::EdgeType> &edge_types,
      const std::shared_ptr<LogicalOperator> &input, bool existing_node,
      Expression *lower_bound, Expression *upper_bound,
      const ExpansionLambda &filter_lambda) override {
    return std::make_unique<DistributedExpandBfs>(
        input, source_sym, sink_sym, edge_sym, direction, edge_types,
        existing_node, lower_bound, upper_bound, filter_lambda);
  }

  std::pair<std::vector<storage::VertexAddress>,
            std::vector<storage::EdgeAddress>>
  BuildGraph(
      database::GraphDbAccessor *dba, const std::vector<int> &vertex_locations,
      const std::vector<std::tuple<int, int, std::string>> &edges) override {
    std::vector<storage::VertexAddress> vertex_addr;
    std::vector<storage::EdgeAddress> edge_addr;

    for (size_t id = 0; id < vertex_locations.size(); ++id) {
      if (vertex_locations[id] == 0) {
        auto vertex = dba->InsertVertex();
        vertex.PropsSet(dba->Property("id"), (int64_t)id);
        vertex_addr.push_back(vertex.GlobalAddress());
      } else {
        auto vertex = database::InsertVertexIntoRemote(
            dba, vertex_locations[id], {}, {{dba->Property("id"), (int64_t)id}},
            std::nullopt);
        vertex_addr.push_back(vertex.GlobalAddress());
      }
    }

    for (auto e : edges) {
      int u, v;
      std::string type;
      std::tie(u, v, type) = e;
      VertexAccessor from(vertex_addr[u], *dba);
      VertexAccessor to(vertex_addr[v], *dba);
      auto edge = dba->InsertEdge(from, to, dba->EdgeType(type));
      edge.PropsSet(dba->Property("from"), u);
      edge.PropsSet(dba->Property("to"), v);
      edge_addr.push_back(edge.GlobalAddress());
    }

    return std::make_pair(vertex_addr, edge_addr);
  }

 private:
  Cluster cluster_;
};

class DistributedBfsTest
    : public ::testing::TestWithParam<
          std::tuple<int, int, EdgeAtom::Direction, std::vector<std::string>,
                     bool, FilterLambdaType>> {
 public:
  static void SetUpTestCase() { db_ = std::make_unique<DistributedDb>(); }
  static void TearDownTestCase() { db_ = nullptr; }

 protected:
  static std::unique_ptr<DistributedDb> db_;
};

std::unique_ptr<DistributedDb> DistributedBfsTest::db_{nullptr};

TEST_P(DistributedBfsTest, All) {
  int lower_bound;
  int upper_bound;
  EdgeAtom::Direction direction;
  std::vector<std::string> edge_types;
  bool known_sink;
  FilterLambdaType filter_lambda_type;
  std::tie(lower_bound, upper_bound, direction, edge_types, known_sink,
           filter_lambda_type) = GetParam();
  BfsTest(db_.get(), lower_bound, upper_bound, direction, edge_types,
          known_sink, filter_lambda_type);
}

INSTANTIATE_TEST_CASE_P(
    DirectionAndExpansionDepth, DistributedBfsTest,
    testing::Combine(testing::Range(-1, kVertexCount),
                     testing::Range(-1, kVertexCount),
                     testing::Values(EdgeAtom::Direction::OUT,
                                     EdgeAtom::Direction::IN,
                                     EdgeAtom::Direction::BOTH),
                     testing::Values(std::vector<std::string>{}),
                     testing::Bool(), testing::Values(FilterLambdaType::NONE)));

INSTANTIATE_TEST_CASE_P(
    EdgeType, DistributedBfsTest,
    testing::Combine(testing::Values(-1), testing::Values(-1),
                     testing::Values(EdgeAtom::Direction::OUT,
                                     EdgeAtom::Direction::IN,
                                     EdgeAtom::Direction::BOTH),
                     testing::Values(std::vector<std::string>{},
                                     std::vector<std::string>{"a"},
                                     std::vector<std::string>{"b"},
                                     std::vector<std::string>{"a", "b"}),
                     testing::Bool(), testing::Values(FilterLambdaType::NONE)));

INSTANTIATE_TEST_CASE_P(
    FilterLambda, DistributedBfsTest,
    testing::Combine(
        testing::Values(-1), testing::Values(-1),
        testing::Values(EdgeAtom::Direction::OUT, EdgeAtom::Direction::IN,
                        EdgeAtom::Direction::BOTH),
        testing::Values(std::vector<std::string>{}), testing::Bool(),
        testing::Values(FilterLambdaType::NONE, FilterLambdaType::USE_FRAME,
                        FilterLambdaType::USE_CTX, FilterLambdaType::ERROR)));
