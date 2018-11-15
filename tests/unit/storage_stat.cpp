#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "database/single_node/graph_db.hpp"
#include "database/single_node/graph_db_accessor.hpp"

class StatTest : public ::testing::Test {
 public:
  database::GraphDb db_;
};

#define COMPARE(stat, vc, ec, ad)   \
  EXPECT_EQ(stat.vertex_count, vc); \
  EXPECT_EQ(stat.edge_count, ec);   \
  EXPECT_EQ(stat.avg_degree, ad);

TEST_F(StatTest, CountTest1) {
  auto &stat = db_.GetStat();
  COMPARE(stat, 0, 0, 0);

  auto dba = db_.Access();
  dba->InsertVertex();
  dba->InsertVertex();
  dba->InsertVertex();

  COMPARE(stat, 0, 0, 0);
  db_.RefreshStat();
  COMPARE(stat, 3, 0, 0);
}

TEST_F(StatTest, CountTest2) {
  auto &stat = db_.GetStat();
  COMPARE(stat, 0, 0, 0);

  auto dba = db_.Access();
  auto type = dba->EdgeType("edge");
  auto v1 = dba->InsertVertex();
  auto v2 = dba->InsertVertex();
  auto v3 = dba->InsertVertex();
  auto v4 = dba->InsertVertex();
  auto e1 = dba->InsertEdge(v1, v2, type);
  auto e2 = dba->InsertEdge(v2, v2, type);
  auto e3 = dba->InsertEdge(v3, v2, type);
  auto e4 = dba->InsertEdge(v4, v2, type);
  auto e5 = dba->InsertEdge(v1, v3, type);

  COMPARE(stat, 0, 0, 0);
  db_.RefreshStat();
  COMPARE(stat, 4, 5, 2.5);

  dba->Commit();

  auto dba1 = db_.Access();
  auto v22 = dba1->FindVertex(v2.gid(), true);
  dba1->DetachRemoveVertex(v22);

  db_.RefreshStat();
  COMPARE(stat, 4, 5, 2.5);

  dba1->Commit();
  db_.CollectGarbage();
  db_.RefreshStat();
  COMPARE(stat, 3, 1, 2.0 / 3);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::InitGoogleLogging(argv[0]);
  return RUN_ALL_TESTS();
}
