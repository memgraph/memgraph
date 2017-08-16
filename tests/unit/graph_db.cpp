#include <memory>

#include "gtest/gtest.h"

#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "database/graph_db_datatypes.hpp"
#include "database/indexes/label_property_index.hpp"

class GraphDbTest : public testing::Test {
 protected:
  GraphDb graph_db{"default", fs::path()};
  std::unique_ptr<GraphDbAccessor> dba =
      std::make_unique<GraphDbAccessor>(graph_db);

  void Commit() {
    dba->Commit();
    auto dba2 = std::make_unique<GraphDbAccessor>(graph_db);
    dba.swap(dba2);
  }
};

TEST_F(GraphDbTest, GarbageCollectIndices) {
  auto label = dba->Label("label");
  auto property = dba->Property("property");
  dba->BuildIndex(label, property);
  Commit();

  auto vertex = dba->InsertVertex();
  vertex.add_label(label);
  vertex.PropsSet(property, 42);
  Commit();

  EXPECT_EQ(dba->VerticesCount(label, property), 1);
  auto vertex_transferred = dba->Transfer(vertex);
  dba->RemoveVertex(vertex_transferred.value());
  EXPECT_EQ(dba->VerticesCount(label, property), 1);
  Commit();
  EXPECT_EQ(dba->VerticesCount(label, property), 1);
  graph_db.CollectGarbage();
  EXPECT_EQ(dba->VerticesCount(label, property), 0);

}
