#include <memory>

#include <gtest/gtest.h>

#include "database/single_node/graph_db.hpp"
#include "database/single_node/graph_db_accessor.hpp"
#include "storage/common/types.hpp"
#include "storage/single_node/indexes/label_property_index.hpp"

TEST(GraphDbTest, GarbageCollectIndices) {
  database::Config config;
  config.gc_cycle_sec = -1;
  database::GraphDb graph_db{config};
  std::unique_ptr<database::GraphDbAccessor> dba = graph_db.Access();

  auto commit = [&] {
    dba->Commit();
    dba = graph_db.Access();
  };
  auto label = dba->Label("label");
  auto property = dba->Property("property");
  dba->BuildIndex(label, property);
  commit();

  auto vertex = dba->InsertVertex();
  vertex.add_label(label);
  vertex.PropsSet(property, 42);
  commit();

  EXPECT_EQ(dba->VerticesCount(label, property), 1);
  auto vertex_transferred = dba->Transfer(vertex);
  dba->RemoveVertex(vertex_transferred.value());
  EXPECT_EQ(dba->VerticesCount(label, property), 1);
  commit();
  EXPECT_EQ(dba->VerticesCount(label, property), 1);
  graph_db.CollectGarbage();
  EXPECT_EQ(dba->VerticesCount(label, property), 0);
}
