#include <algorithm>
#include <iterator>
#include <list>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "mg_procedure.h"
#include "query/db_accessor.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/view.hpp"
#include "storage_test_utils.hpp"
#include "utils/memory.hpp"

namespace {
struct MgpVertexDeleter {
  void operator()(mgp_vertex *v) {
    if (v != nullptr) {
      mgp_vertex_destroy(v);
    }
  }
};

struct MgpVerticesIteratorDeleter {
  void operator()(mgp_vertices_iterator *it) {
    if (it != nullptr) {
      mgp_vertices_iterator_destroy(it);
    }
  }
};

using MgpVertexPtr = std::unique_ptr<mgp_vertex, MgpVertexDeleter>;
using MgpVerticesIteratorPtr = std::unique_ptr<mgp_vertices_iterator, MgpVerticesIteratorDeleter>;

}  // namespace

struct MgpGraphTest : public ::testing::Test {
  mgp_graph CreateGraph(const storage::View view = storage::View::NEW) {
    // the execution context can be null as it shouldn't be used in these tests
    return mgp_graph{&CreateDbAccessor(storage::IsolationLevel::SNAPSHOT_ISOLATION), view, nullptr};
  }

  query::DbAccessor &CreateDbAccessor(const storage::IsolationLevel isolationLevel) {
    accessors_.push_back(storage.Access(isolationLevel));
    db_accessors_.emplace_back(&accessors_.back());
    return db_accessors_.back();
  }

  storage::Storage storage;
  mgp_memory memory{utils::NewDeleteResource()};

 private:
  std::list<storage::Storage::Accessor> accessors_;
  std::list<query::DbAccessor> db_accessors_;
};

TEST_F(MgpGraphTest, IsMutable) {
  const mgp_graph immutable_graph = CreateGraph(storage::View::OLD);
  EXPECT_FALSE(mgp_graph_is_mutable(&immutable_graph));
  const mgp_graph mutable_graph = CreateGraph(storage::View::NEW);
  EXPECT_TRUE(mgp_graph_is_mutable(&mutable_graph));
}

TEST_F(MgpGraphTest, CreateVertex) {
  mgp_graph graph = CreateGraph();
  auto read_uncommited_accessor = storage.Access(storage::IsolationLevel::READ_UNCOMMITTED);
  EXPECT_EQ(CountVertices(read_uncommited_accessor, storage::View::NEW), 0);
  MgpVertexPtr vertex{mgp_graph_create_vertex(&graph, &memory)};
  EXPECT_NE(vertex, nullptr);
  EXPECT_EQ(CountVertices(read_uncommited_accessor, storage::View::NEW), 1);
  const auto vertex_id = mgp_vertex_get_id(vertex.get());
  EXPECT_TRUE(
      read_uncommited_accessor.FindVertex(storage::Gid::FromInt(vertex_id.as_int), storage::View::NEW).has_value());
}

TEST_F(MgpGraphTest, RemoveVertex) {
  storage::Gid vertex_id{};
  {
    auto accessor = CreateDbAccessor(storage::IsolationLevel::SNAPSHOT_ISOLATION);
    const auto vertex = accessor.InsertVertex();
    vertex_id = vertex.Gid();
    ASSERT_FALSE(accessor.Commit().HasError());
  }
  mgp_graph graph = CreateGraph();
  auto read_uncommited_accessor = storage.Access(storage::IsolationLevel::READ_UNCOMMITTED);
  EXPECT_EQ(CountVertices(read_uncommited_accessor, storage::View::NEW), 1);
  MgpVertexPtr vertex{mgp_graph_get_vertex_by_id(&graph, mgp_vertex_id{vertex_id.AsInt()}, &memory)};
  EXPECT_NE(vertex, nullptr);
  EXPECT_NE(mgp_graph_remove_vertex(&graph, vertex.get()), 0);
  EXPECT_EQ(CountVertices(read_uncommited_accessor, storage::View::NEW), 0);
}

TEST_F(MgpGraphTest, CreateRemoveWithImmutableGraph) {
  storage::Gid vertex_id{};
  {
    auto accessor = CreateDbAccessor(storage::IsolationLevel::SNAPSHOT_ISOLATION);
    const auto vertex = accessor.InsertVertex();
    vertex_id = vertex.Gid();
    ASSERT_FALSE(accessor.Commit().HasError());
  }
  auto read_uncommited_accessor = storage.Access(storage::IsolationLevel::READ_UNCOMMITTED);
  EXPECT_EQ(CountVertices(read_uncommited_accessor, storage::View::NEW), 1);

  mgp_graph immutable_graph = CreateGraph(storage::View::OLD);
  MgpVertexPtr created_vertex{mgp_graph_create_vertex(&immutable_graph, &memory)};
  EXPECT_EQ(created_vertex, nullptr);
  EXPECT_EQ(CountVertices(read_uncommited_accessor, storage::View::NEW), 1);
  MgpVertexPtr vertex_to_remove{
      mgp_graph_get_vertex_by_id(&immutable_graph, mgp_vertex_id{vertex_id.AsInt()}, &memory)};
  ASSERT_NE(vertex_to_remove, nullptr);
  EXPECT_EQ(mgp_graph_remove_vertex(&immutable_graph, vertex_to_remove.get()), 0);
  EXPECT_EQ(CountVertices(read_uncommited_accessor, storage::View::NEW), 1);
}

TEST_F(MgpGraphTest, VerticesIterator) {
  {
    auto accessor = CreateDbAccessor(storage::IsolationLevel::SNAPSHOT_ISOLATION);
    accessor.InsertVertex();
    ASSERT_FALSE(accessor.Commit().HasError());
  }
  auto check_vertices_iterator = [this](const storage::View view) {
    mgp_graph graph = CreateGraph(view);
    MgpVerticesIteratorPtr vertices_iter{mgp_graph_iter_vertices(&graph, &memory)};
    ASSERT_NE(vertices_iter, nullptr);
    EXPECT_NE(mgp_vertices_iterator_get(vertices_iter.get()), nullptr);
    if (view == storage::View::NEW) {
      EXPECT_TRUE(mgp_vertices_iterator_is_mutable(vertices_iter.get()));
      EXPECT_NE(mgp_vertices_iterator_get_mutable(vertices_iter.get()), nullptr);
    } else {
      EXPECT_FALSE(mgp_vertices_iterator_is_mutable(vertices_iter.get()));
      EXPECT_EQ(mgp_vertices_iterator_get_mutable(vertices_iter.get()), nullptr);
    }
  };
  {
    SCOPED_TRACE("View::OLD");
    check_vertices_iterator(storage::View::OLD);
  }
  {
    SCOPED_TRACE("View::NEW");
    check_vertices_iterator(storage::View::NEW);
  }
}
