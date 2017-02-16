
#include "gtest/gtest.h"

#include "dbms/dbms.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"

#include "storage/typed_value.hpp"
#include "storage/vertex_accessor.hpp"
#include "storage/edge_accessor.hpp"


TEST(RecordAccessor, PropertySet) {

  Dbms dbms;
  GraphDbAccessor dba = dbms.active();

  auto vertex = dba.insert_vertex();
  auto property = dba.property("PropName");

  vertex.PropsSet(property, 42);
  EXPECT_EQ(vertex.PropsAt(property).Value<int>(), 42);
  EXPECT_TRUE((vertex.PropsAt(dba.property("Other")) == TypedValue::Null).Value<bool>());
}
