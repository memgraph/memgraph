#include <gtest/gtest.h>
#include <sstream>

#include <capnp/message.h>

#include "distributed/serialization.hpp"
#include "mvcc/version_list.hpp"
#include "query/typed_value.hpp"
#include "storage/edge.hpp"
#include "storage/property_value_store.hpp"
#include "storage/types.hpp"
#include "storage/vertex.hpp"
#include "transactions/single_node/engine_single_node.hpp"

using namespace storage;

template <typename TAddress>
TAddress ToGlobal(const TAddress &address, int worker_id) {
  if (address.is_remote()) return address;
  return TAddress{address.local()->gid_, worker_id};
}

#define CHECK_RETURN(condition)     \
  {                                 \
    if (!(condition)) return false; \
  }

bool CheckEdges(const Edges &e1, int w1, const Edges &e2, int w2) {
  CHECK_RETURN(e1.size() == e2.size());
  auto e1_it = e1.begin();
  for (auto e2_it = e2.begin(); e2_it != e2.end(); ++e1_it, ++e2_it) {
    CHECK_RETURN(ToGlobal(e1_it->vertex, w1) == ToGlobal(e2_it->vertex, w2));
    CHECK_RETURN(ToGlobal(e1_it->edge, w1) == ToGlobal(e2_it->edge, w2));
    CHECK_RETURN(e1_it->edge_type == e2_it->edge_type);
  }
  return true;
}

bool CheckProperties(const PropertyValueStore &p1,
                     const PropertyValueStore &p2) {
  CHECK_RETURN(p1.size() == p2.size());
  auto p1_it = p1.begin();
  for (auto p2_it = p2.begin(); p2_it != p2.end(); ++p1_it, ++p2_it) {
    CHECK_RETURN(p1_it->first == p2_it->first);
    auto tv =
        query::TypedValue(p1_it->second) == query::TypedValue(p2_it->second);
    CHECK_RETURN(tv.IsBool());
    CHECK_RETURN(tv.ValueBool());
  }
  return true;
}

bool CheckVertex(const Vertex &v1, int w1, const Vertex &v2, int w2) {
  CHECK_RETURN(CheckEdges(v1.in_, w1, v2.in_, w2));
  CHECK_RETURN(CheckEdges(v1.out_, w1, v2.out_, w2));
  CHECK_RETURN(v1.labels_ == v2.labels_);
  CHECK_RETURN(CheckProperties(v1.properties_, v2.properties_));
  return true;
}

bool CheckEdge(const Edge &e1, int w1, const Edge &e2, int w2) {
  CHECK_RETURN(ToGlobal(e1.from_, w1) == ToGlobal(e2.from_, w2));
  CHECK_RETURN(ToGlobal(e1.to_, w1) == ToGlobal(e2.to_, w2));
  CHECK_RETURN(e1.edge_type_ == e2.edge_type_);
  CHECK_RETURN(CheckProperties(e1.properties_, e2.properties_));
  return true;
}

#undef CHECK_RETURN

#define SAVE_AND_LOAD(type, name, element)                       \
  std::unique_ptr<type> name;                                    \
  {                                                              \
    ::capnp::MallocMessageBuilder message;                       \
    auto builder = message.initRoot<distributed::capnp::type>(); \
    distributed::Save##type(element, &builder, 0);               \
    auto reader = message.getRoot<distributed::capnp::type>();   \
    name = distributed::Load##type(reader);                      \
  }

TEST(DistributedSerialization, Empty) {
  Vertex v;
  int w_id{0};
  SAVE_AND_LOAD(Vertex, v_recovered, v)
  EXPECT_TRUE(CheckVertex(v, w_id, *v_recovered, w_id));
}

#define UPDATE_AND_CHECK(type, x, action)        \
  {                                              \
    SAVE_AND_LOAD(type, before, x)               \
    EXPECT_TRUE(Check##type(x, 0, *before, 0));  \
    action;                                      \
    EXPECT_FALSE(Check##type(x, 0, *before, 0)); \
    SAVE_AND_LOAD(type, after, x)                \
    EXPECT_TRUE(Check##type(x, 0, *after, 0));   \
  }

#define UPDATE_AND_CHECK_V(v, action) UPDATE_AND_CHECK(Vertex, v, action)
#define UPDATE_AND_CHECK_E(e, action) UPDATE_AND_CHECK(Edge, e, action)

TEST(DistributedSerialization, VertexLabels) {
  Vertex v;
  UPDATE_AND_CHECK_V(v, v.labels_.emplace_back(Label(1)));
  UPDATE_AND_CHECK_V(v, v.labels_.emplace_back(Label(2)));
  UPDATE_AND_CHECK_V(v, v.labels_.resize(1));
  UPDATE_AND_CHECK_V(v, v.labels_.clear());
}

TEST(DistributedSerialization, VertexProperties) {
  Vertex v;
  UPDATE_AND_CHECK_V(v, v.properties_.set(Property(1), true));
  UPDATE_AND_CHECK_V(v, v.properties_.set(Property(1), "string"));
  UPDATE_AND_CHECK_V(v, v.properties_.set(Property(2), 42));
  UPDATE_AND_CHECK_V(v, v.properties_.erase(Property(1)));
  UPDATE_AND_CHECK_V(v, v.properties_.clear());
}

class DistributedSerializationMvcc : public ::testing::Test {
 protected:
  tx::EngineSingleNode engine;
  tx::Transaction *tx = engine.Begin();
  mvcc::VersionList<Vertex> v1_vlist{*tx, 0, 0};
  Vertex &v1 = *v1_vlist.Oldest();
  mvcc::VersionList<Vertex> v2_vlist{*tx, 1, 1};
  Vertex &v2 = *v2_vlist.Oldest();
  mvcc::VersionList<Edge> e1_vlist{*tx,
                                   0,
                                   0,
                                   storage::VertexAddress(&v1_vlist),
                                   storage::VertexAddress(&v2_vlist),
                                   EdgeType(0)};
  Edge &e1 = *e1_vlist.Oldest();
  mvcc::VersionList<Edge> e2_vlist{*tx,
                                   1,
                                   1,
                                   storage::VertexAddress(&v2_vlist),
                                   storage::VertexAddress(&v1_vlist),
                                   EdgeType(2)};
  Edge &e2 = *e2_vlist.Oldest();
};

TEST_F(DistributedSerializationMvcc, VertexEdges) {
  UPDATE_AND_CHECK_V(
      v1, v1.out_.emplace(storage::VertexAddress(&v2_vlist),
                          storage::EdgeAddress(&e1_vlist), EdgeType(0)));
  UPDATE_AND_CHECK_V(
      v2, v2.in_.emplace(storage::VertexAddress(&v1_vlist),
                         storage::EdgeAddress(&e1_vlist), EdgeType(0)));
  UPDATE_AND_CHECK_V(
      v1, v1.in_.emplace(storage::VertexAddress(&v2_vlist),
                         storage::EdgeAddress(&e2_vlist), EdgeType(2)));
  UPDATE_AND_CHECK_V(
      v2, v2.out_.emplace(storage::VertexAddress(&v1_vlist),
                          storage::EdgeAddress(&e2_vlist), EdgeType(2)));
}

TEST_F(DistributedSerializationMvcc, EdgeFromAndTo) {
  UPDATE_AND_CHECK_E(e1, e1.from_ = storage::VertexAddress(&v2_vlist));
  UPDATE_AND_CHECK_E(e1, e1.to_ = storage::VertexAddress(&v1_vlist));
}

TEST_F(DistributedSerializationMvcc, EdgeType) {
  UPDATE_AND_CHECK_E(e1, e1.edge_type_ = EdgeType(123));
  UPDATE_AND_CHECK_E(e1, e1.edge_type_ = EdgeType(55));
}

TEST_F(DistributedSerializationMvcc, EdgeProperties) {
  UPDATE_AND_CHECK_E(e1, e1.properties_.set(Property(1), true));
  UPDATE_AND_CHECK_E(e1, e1.properties_.set(Property(1), "string"));
  UPDATE_AND_CHECK_E(e1, e1.properties_.set(Property(2), 42));
  UPDATE_AND_CHECK_E(e1, e1.properties_.erase(Property(1)));
  UPDATE_AND_CHECK_E(e1, e1.properties_.clear());
}

#undef UPDATE_AND_CHECK_E
#undef UPDATE_AND_CHECK_V
#undef UPDATE_AND_CHECK
#undef SAVE_AND_LOAD
