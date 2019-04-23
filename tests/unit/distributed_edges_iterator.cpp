#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "distributed_common.hpp"
#include "storage/distributed/edges_iterator.hpp"

class EdgesIterableTest : public DistributedGraphDbTest {
 public:
  EdgesIterableTest() : DistributedGraphDbTest("edges") {}

  void SetUp() override {
    DistributedGraphDbTest::SetUp();
    v.emplace(InsertVertex(master()));
    w1_v1_out.emplace(InsertVertex(worker(1)));
    w1_v2_out.emplace(InsertVertex(worker(1)));
    w1_v3_out.emplace(InsertVertex(worker(1)));
    w1_v1_in.emplace(InsertVertex(worker(1)));
    w1_v2_in.emplace(InsertVertex(worker(1)));
    w1_v3_in.emplace(InsertVertex(worker(1)));
    w1_e1_out.emplace(InsertEdge(*v, *w1_v1_out, type1));
    w1_e1_in.emplace(InsertEdge(*w1_v1_in, *v, type1));
    w1_e2_out.emplace(InsertEdge(*v, *w1_v2_out, type1));
    w1_e2_in.emplace(InsertEdge(*w1_v2_in, *v, type1));
    w1_e3_out.emplace(InsertEdge(*v, *w1_v3_out, type2));
    w1_e3_in.emplace(InsertEdge(*w1_v3_in, *v, type2));
    w2_v1_out.emplace(InsertVertex(worker(2)));
    w2_v2_out.emplace(InsertVertex(worker(2)));
    w2_v3_out.emplace(InsertVertex(worker(2)));
    w2_v1_in.emplace(InsertVertex(worker(2)));
    w2_v2_in.emplace(InsertVertex(worker(2)));
    w2_v3_in.emplace(InsertVertex(worker(2)));
    w2_e1_out.emplace(InsertEdge(*v, *w2_v1_out, type1));
    w2_e1_in.emplace(InsertEdge(*w2_v1_in, *v, type1));
    w2_e2_out.emplace(InsertEdge(*v, *w2_v2_out, type1));
    w2_e2_in.emplace(InsertEdge(*w2_v2_in, *v, type1));
    w2_e3_out.emplace(InsertEdge(*v, *w2_v3_out, type2));
    w2_e3_in.emplace(InsertEdge(*w2_v3_in, *v, type2));
  }

  // master
  std::optional<storage::VertexAddress> v;

  // worker 1 vertices
  std::optional<storage::VertexAddress> w1_v1_out;
  std::optional<storage::VertexAddress> w1_v2_out;
  std::optional<storage::VertexAddress> w1_v3_out;
  std::optional<storage::VertexAddress> w1_v1_in;
  std::optional<storage::VertexAddress> w1_v2_in;
  std::optional<storage::VertexAddress> w1_v3_in;

  // worker 1 edges
  std::optional<storage::EdgeAddress> w1_e1_out;
  std::optional<storage::EdgeAddress> w1_e2_out;
  std::optional<storage::EdgeAddress> w1_e3_out;
  std::optional<storage::EdgeAddress> w1_e1_in;
  std::optional<storage::EdgeAddress> w1_e2_in;
  std::optional<storage::EdgeAddress> w1_e3_in;

  // worker 2 vertices
  std::optional<storage::VertexAddress> w2_v1_out;
  std::optional<storage::VertexAddress> w2_v2_out;
  std::optional<storage::VertexAddress> w2_v3_out;
  std::optional<storage::VertexAddress> w2_v1_in;
  std::optional<storage::VertexAddress> w2_v2_in;
  std::optional<storage::VertexAddress> w2_v3_in;

  // worker 2 edges
  std::optional<storage::EdgeAddress> w2_e1_out;
  std::optional<storage::EdgeAddress> w2_e2_out;
  std::optional<storage::EdgeAddress> w2_e3_out;
  std::optional<storage::EdgeAddress> w2_e1_in;
  std::optional<storage::EdgeAddress> w2_e2_in;
  std::optional<storage::EdgeAddress> w2_e3_in;

  // types
  std::string type1{"type1"};
  std::string type2{"type2"};
};

TEST_F(EdgesIterableTest, OutEdges) {
  auto dba = master().Access();
  VertexAccessor va(*v, *dba);
  auto iterable = va.out();
  auto i = iterable.begin();
  EXPECT_EQ(i->GlobalAddress(), *w1_e1_out);
  ++i;
  EXPECT_EQ(i->GlobalAddress(), *w1_e2_out);
  ++i;
  EXPECT_EQ(i->GlobalAddress(), *w1_e3_out);
  ++i;
  EXPECT_EQ(i->GlobalAddress(), *w2_e1_out);
  ++i;
  EXPECT_EQ(i->GlobalAddress(), *w2_e2_out);
  ++i;
  EXPECT_EQ(i->GlobalAddress(), *w2_e3_out);
  ++i;
  EXPECT_EQ(iterable.end(), i);
}

TEST_F(EdgesIterableTest, InEdges) {
  auto dba = master().Access();
  VertexAccessor va(*v, *dba);
  auto iterable = va.in();
  auto i = iterable.begin();
  EXPECT_EQ(i->GlobalAddress(), *w1_e1_in);
  ++i;
  EXPECT_EQ(i->GlobalAddress(), *w1_e2_in);
  ++i;
  EXPECT_EQ(i->GlobalAddress(), *w1_e3_in);
  ++i;
  EXPECT_EQ(i->GlobalAddress(), *w2_e1_in);
  ++i;
  EXPECT_EQ(i->GlobalAddress(), *w2_e2_in);
  ++i;
  EXPECT_EQ(i->GlobalAddress(), *w2_e3_in);
  ++i;
  EXPECT_EQ(iterable.end(), i);
}

TEST_F(EdgesIterableTest, InEdgesDestAddrFilter) {
  auto dba = master().Access();
  VertexAccessor va(*v, *dba);
  {
    VertexAccessor va_dest(*w1_v2_in, *dba);
    auto iterable = va.in(va_dest);
    auto i = iterable.begin();
    EXPECT_EQ(i->GlobalAddress(), *w1_e2_in);
    ++i;
    EXPECT_EQ(iterable.end(), i);
  }
  {
    VertexAccessor va_dest(*w2_v1_in, *dba);
    auto iterable = va.in(va_dest);
    auto i = iterable.begin();
    EXPECT_EQ(i->GlobalAddress(), *w2_e1_in);
    ++i;
    EXPECT_EQ(iterable.end(), i);
  }
}

TEST_F(EdgesIterableTest, OutEdgesDestAddrFilter) {
  auto dba = master().Access();
  VertexAccessor va(*v, *dba);
  {
    VertexAccessor va_dest(*w1_v2_out, *dba);
    auto iterable = va.out(va_dest);
    auto i = iterable.begin();
    EXPECT_EQ(i->GlobalAddress(), *w1_e2_out);
    ++i;
    EXPECT_EQ(iterable.end(), i);
  }
  {
    VertexAccessor va_dest(*w2_v1_out, *dba);
    auto iterable = va.out(va_dest);
    auto i = iterable.begin();
    EXPECT_EQ(i->GlobalAddress(), *w2_e1_out);
    ++i;
    EXPECT_EQ(iterable.end(), i);
  }
}

TEST_F(EdgesIterableTest, InEdgesEdgeTypeFilter) {
  auto dba = master().Access();
  VertexAccessor va(*v, *dba);
  std::vector<storage::EdgeType> edge_types{dba->EdgeType(type2)};
  auto iterable = va.in(&edge_types);
  auto i = iterable.begin();
  EXPECT_EQ(i->GlobalAddress(), *w1_e3_in);
  ++i;
  EXPECT_EQ(i->GlobalAddress(), *w2_e3_in);
  ++i;
  EXPECT_EQ(iterable.end(), i);
}

TEST_F(EdgesIterableTest, OutEdgesEdgeTypeFilter) {
  auto dba = master().Access();
  VertexAccessor va(*v, *dba);
  std::vector<storage::EdgeType> edge_types{dba->EdgeType(type2)};
  auto iterable = va.out(&edge_types);
  auto i = iterable.begin();
  EXPECT_EQ(i->GlobalAddress(), *w1_e3_out);
  ++i;
  EXPECT_EQ(i->GlobalAddress(), *w2_e3_out);
  ++i;
  EXPECT_EQ(iterable.end(), i);
}

TEST_F(EdgesIterableTest, InEdgesEdgeTypeAndDestFilter) {
  auto dba = master().Access();
  VertexAccessor va(*v, *dba);
  VertexAccessor va_dest(*w1_v1_in, *dba);
  std::vector<storage::EdgeType> edge_types{dba->EdgeType(type1)};
  auto iterable = va.in(va_dest, &edge_types);
  auto i = iterable.begin();
  EXPECT_EQ(i->GlobalAddress(), *w1_e1_in);
  ++i;
  EXPECT_EQ(iterable.end(), i);
}

TEST_F(EdgesIterableTest, OutEdgesEdgeTypeAndDestFilter) {
  auto dba = master().Access();
  VertexAccessor va(*v, *dba);
  VertexAccessor va_dest(*w1_v1_out, *dba);
  std::vector<storage::EdgeType> edge_types{dba->EdgeType(type1)};
  auto iterable = va.out(va_dest, &edge_types);
  auto i = iterable.begin();
  EXPECT_EQ(i->GlobalAddress(), *w1_e1_out);
  ++i;
  EXPECT_EQ(iterable.end(), i);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::InitGoogleLogging(argv[0]);
  return RUN_ALL_TESTS();
}
