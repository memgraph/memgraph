#include "bfs_common.hpp"

using namespace query;
using namespace query::plan;

class SingleNodeDb : public Database {
 public:
  SingleNodeDb() : db_() {}

  std::unique_ptr<database::GraphDbAccessor> Access() override {
    return db_.Access();
  }

  void AdvanceCommand(tx::TransactionId tx_id) override {
    auto dba = db_.Access(tx_id);
    dba->AdvanceCommand();
  }

  std::unique_ptr<LogicalOperator> MakeBfsOperator(
      Symbol source_sym, Symbol sink_sym, Symbol edge_sym,
      EdgeAtom::Direction direction,
      const std::vector<storage::EdgeType> &edge_types,
      const std::shared_ptr<LogicalOperator> &input, bool existing_node,
      Expression *lower_bound, Expression *upper_bound,
      const ExpansionLambda &filter_lambda) override {
    return std::make_unique<ExpandVariable>(
        sink_sym, edge_sym, EdgeAtom::Type::BREADTH_FIRST, direction,
        edge_types, false, lower_bound, upper_bound, input, source_sym,
        existing_node, filter_lambda, std::experimental::nullopt,
        std::experimental::nullopt, GraphView::OLD);
  }

  std::pair<std::vector<storage::VertexAddress>,
            std::vector<storage::EdgeAddress>>
  BuildGraph(
      database::GraphDbAccessor *dba, const std::vector<int> &vertex_locations,
      const std::vector<std::tuple<int, int, std::string>> &edges) override {
    std::vector<storage::VertexAddress> vertex_addr;
    std::vector<storage::EdgeAddress> edge_addr;

    for (size_t id = 0; id < vertex_locations.size(); ++id) {
      auto vertex = dba->InsertVertex();
      vertex.PropsSet(dba->Property("id"), (int64_t)id);
      vertex_addr.push_back(vertex.GlobalAddress());
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

 protected:
  database::SingleNode db_;
};

class SingleNodeBfsTest
    : public ::testing::TestWithParam<
          std::tuple<int, int, EdgeAtom::Direction, std::vector<std::string>,
                     bool, FilterLambdaType>> {
 public:
  static void SetUpTestCase() { db_ = std::make_unique<SingleNodeDb>(); }
  static void TearDownTestCase() { db_ = nullptr; }

 protected:
  static std::unique_ptr<SingleNodeDb> db_;
};

TEST_P(SingleNodeBfsTest, All) {
  int lower_bound;
  int upper_bound;
  EdgeAtom::Direction direction;
  std::vector<std::string> edge_types;
  bool known_sink;
  FilterLambdaType filter_lambda_type;
  std::tie(lower_bound, upper_bound, direction, edge_types, known_sink,
           filter_lambda_type) = GetParam();
  BfsTest(db_.get(), lower_bound, upper_bound, direction,edge_types,
          known_sink, filter_lambda_type);
}

std::unique_ptr<SingleNodeDb> SingleNodeBfsTest::db_{nullptr};

INSTANTIATE_TEST_CASE_P(
    DirectionAndExpansionDepth, SingleNodeBfsTest,
    testing::Combine(testing::Range(-1, kVertexCount),
                     testing::Range(-1, kVertexCount),
                     testing::Values(EdgeAtom::Direction::OUT,
                                     EdgeAtom::Direction::IN,
                                     EdgeAtom::Direction::BOTH),
                     testing::Values(std::vector<std::string>{}),
                     testing::Bool(), testing::Values(FilterLambdaType::NONE)));

INSTANTIATE_TEST_CASE_P(
    EdgeType, SingleNodeBfsTest,
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
    FilterLambda, SingleNodeBfsTest,
    testing::Combine(
        testing::Values(-1), testing::Values(-1),
        testing::Values(EdgeAtom::Direction::OUT, EdgeAtom::Direction::IN,
                        EdgeAtom::Direction::BOTH),
        testing::Values(std::vector<std::string>{}), testing::Bool(),
        testing::Values(FilterLambdaType::NONE, FilterLambdaType::USE_FRAME,
                        FilterLambdaType::USE_FRAME_NULL,
                        FilterLambdaType::USE_CTX, FilterLambdaType::ERROR)));
