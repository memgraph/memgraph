#include <memory>

#include "gtest/gtest.h"

#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "database/indexes/label_property_index.hpp"
#include "storage/types.hpp"

TEST(GraphDbTest, GarbageCollectIndices) {
  database::Config config;
  config.gc_cycle_sec = -1;
  database::SingleNode graph_db{config};
  std::unique_ptr<database::GraphDbAccessor> dba =
      std::make_unique<database::GraphDbAccessor>(graph_db);

  auto commit = [&] {
    dba->Commit();
    dba = std::make_unique<database::GraphDbAccessor>(graph_db);
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
