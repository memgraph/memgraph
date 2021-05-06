#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <limits>

#include "storage/v2/storage.hpp"

using testing::UnorderedElementsAre;

class StorageEdgeTest : public ::testing::TestWithParam<bool> {};

INSTANTIATE_TEST_CASE_P(EdgesWithProperties, StorageEdgeTest, ::testing::Values(true));
INSTANTIATE_TEST_CASE_P(EdgesWithoutProperties, StorageEdgeTest, ::testing::Values(false));

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeCreateFromSmallerCommit) {
  storage::Storage store({.items = {.properties_on_edges = GetParam()}});
  storage::Gid gid_from = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid_to = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertices
  {
    auto acc = store.Access();
    auto vertex_from = acc.CreateVertex();
    auto vertex_to = acc.CreateVertex();
    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();
    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Create edge
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    auto res = acc.CreateEdge(&*vertex_from, &*vertex_to, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 0);
    {
      auto ret = vertex_to->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {other_et}, &*vertex_to)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {et, other_et}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {et, other_et}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_to)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {other_et}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {et, other_et}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {et, other_et}, &*vertex_to)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {other_et}, &*vertex_to)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {other_et}, &*vertex_to)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {et, other_et}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {et, other_et}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {et, other_et}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {et, other_et}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_to)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_to)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {other_et}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {other_et}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {et, other_et}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {et, other_et}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {et, other_et}, &*vertex_to)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {et, other_et}, &*vertex_to)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeCreateFromLargerCommit) {
  storage::Storage store({.items = {.properties_on_edges = GetParam()}});
  storage::Gid gid_from = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid_to = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertices
  {
    auto acc = store.Access();
    auto vertex_to = acc.CreateVertex();
    auto vertex_from = acc.CreateVertex();
    gid_to = vertex_to.Gid();
    gid_from = vertex_from.Gid();
    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Create edge
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    auto res = acc.CreateEdge(&*vertex_from, &*vertex_to, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 0);
    {
      auto ret = vertex_to->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_to)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_to)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_to)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeCreateFromSameCommit) {
  storage::Storage store({.items = {.properties_on_edges = GetParam()}});
  storage::Gid gid_vertex = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertex
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid_vertex = vertex.Gid();
    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Create edge
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto et = acc.NameToEdgeType("et5");

    auto res = acc.CreateEdge(&*vertex, &*vertex, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex);
    ASSERT_EQ(edge.ToVertex(), *vertex);

    // Check edges without filters
    ASSERT_EQ(vertex->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex->InDegree(storage::View::OLD), 0);
    {
      auto ret = vertex->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex->OutDegree(storage::View::OLD), 0);
    {
      auto ret = vertex->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {other_et}, &*vertex)->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {other_et}, &*vertex)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto et = acc.NameToEdgeType("et5");

    // Check edges without filters
    {
      auto ret = vertex->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {other_et}, &*vertex)->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {other_et}, &*vertex)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeCreateFromSmallerAbort) {
  storage::Storage store({.items = {.properties_on_edges = GetParam()}});
  storage::Gid gid_from = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid_to = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertices
  {
    auto acc = store.Access();
    auto vertex_from = acc.CreateVertex();
    auto vertex_to = acc.CreateVertex();
    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();
    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Create edge, but abort the transaction
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    auto res = acc.CreateEdge(&*vertex_from, &*vertex_to, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 0);
    {
      auto ret = vertex_to->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_to)->size(), 0);

    acc.Abort();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Create edge
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    auto res = acc.CreateEdge(&*vertex_from, &*vertex_to, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 0);
    {
      auto ret = vertex_to->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_to)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_to)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_to)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeCreateFromLargerAbort) {
  storage::Storage store({.items = {.properties_on_edges = GetParam()}});
  storage::Gid gid_from = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid_to = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertices
  {
    auto acc = store.Access();
    auto vertex_to = acc.CreateVertex();
    auto vertex_from = acc.CreateVertex();
    gid_to = vertex_to.Gid();
    gid_from = vertex_from.Gid();
    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Create edge, but abort the transaction
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    auto res = acc.CreateEdge(&*vertex_from, &*vertex_to, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 0);
    {
      auto ret = vertex_to->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_to)->size(), 0);

    acc.Abort();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Create edge
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    auto res = acc.CreateEdge(&*vertex_from, &*vertex_to, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 0);
    {
      auto ret = vertex_to->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_to)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_to)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_to)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeCreateFromSameAbort) {
  storage::Storage store({.items = {.properties_on_edges = GetParam()}});
  storage::Gid gid_vertex = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertex
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid_vertex = vertex.Gid();
    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Create edge, but abort the transaction
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto et = acc.NameToEdgeType("et5");

    auto res = acc.CreateEdge(&*vertex, &*vertex, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex);
    ASSERT_EQ(edge.ToVertex(), *vertex);

    // Check edges without filters
    ASSERT_EQ(vertex->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex->InDegree(storage::View::OLD), 0);
    {
      auto ret = vertex->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex->OutDegree(storage::View::OLD), 0);
    {
      auto ret = vertex->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {other_et}, &*vertex)->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {other_et}, &*vertex)->size(), 0);

    acc.Abort();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    // Check edges without filters
    ASSERT_EQ(vertex->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex->OutDegree(storage::View::NEW), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Create edge
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto et = acc.NameToEdgeType("et5");

    auto res = acc.CreateEdge(&*vertex, &*vertex, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex);
    ASSERT_EQ(edge.ToVertex(), *vertex);

    // Check edges without filters
    ASSERT_EQ(vertex->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex->InDegree(storage::View::OLD), 0);
    {
      auto ret = vertex->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex->OutDegree(storage::View::OLD), 0);
    {
      auto ret = vertex->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {other_et}, &*vertex)->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {other_et}, &*vertex)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto et = acc.NameToEdgeType("et5");

    // Check edges without filters
    {
      auto ret = vertex->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {other_et}, &*vertex)->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {other_et}, &*vertex)->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {other_et}, &*vertex)->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {other_et}, &*vertex)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeDeleteFromSmallerCommit) {
  storage::Storage store({.items = {.properties_on_edges = GetParam()}});
  storage::Gid gid_from = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid_to = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertices
  {
    auto acc = store.Access();
    auto vertex_from = acc.CreateVertex();
    auto vertex_to = acc.CreateVertex();
    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();
    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Create edge
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    auto res = acc.CreateEdge(&*vertex_from, &*vertex_to, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 0);
    {
      auto ret = vertex_to->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_to)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_to)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_to)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Delete edge
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    auto edge = vertex_from->OutEdges(storage::View::NEW).GetValue()[0];

    auto res = acc.DeleteEdge(&edge);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_to->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_to)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeDeleteFromLargerCommit) {
  storage::Storage store({.items = {.properties_on_edges = GetParam()}});
  storage::Gid gid_from = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid_to = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertices
  {
    auto acc = store.Access();
    auto vertex_to = acc.CreateVertex();
    auto vertex_from = acc.CreateVertex();
    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();
    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Create edge
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    auto res = acc.CreateEdge(&*vertex_from, &*vertex_to, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 0);
    {
      auto ret = vertex_to->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_to)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_to)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_to)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Delete edge
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    auto edge = vertex_from->OutEdges(storage::View::NEW).GetValue()[0];

    auto res = acc.DeleteEdge(&edge);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_to->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_to)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeDeleteFromSameCommit) {
  storage::Storage store({.items = {.properties_on_edges = GetParam()}});
  storage::Gid gid_vertex = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertex
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid_vertex = vertex.Gid();
    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Create edge
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto et = acc.NameToEdgeType("et5");

    auto res = acc.CreateEdge(&*vertex, &*vertex, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex);
    ASSERT_EQ(edge.ToVertex(), *vertex);

    // Check edges without filters
    ASSERT_EQ(vertex->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex->InDegree(storage::View::OLD), 0);
    {
      auto ret = vertex->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex->OutDegree(storage::View::OLD), 0);
    {
      auto ret = vertex->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {other_et}, &*vertex)->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {other_et}, &*vertex)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto et = acc.NameToEdgeType("et5");

    // Check edges without filters
    {
      auto ret = vertex->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {other_et}, &*vertex)->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {other_et}, &*vertex)->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {other_et}, &*vertex)->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {other_et}, &*vertex)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Delete edge
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto et = acc.NameToEdgeType("et5");

    auto edge = vertex->OutEdges(storage::View::NEW).GetValue()[0];

    auto res = acc.DeleteEdge(&edge);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());

    // Check edges without filters
    {
      auto ret = vertex->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex->InDegree(storage::View::NEW), 0);
    {
      auto ret = vertex->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {other_et}, &*vertex)->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {other_et}, &*vertex)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    // Check edges without filters
    ASSERT_EQ(vertex->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex->OutDegree(storage::View::NEW), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeDeleteFromSmallerAbort) {
  storage::Storage store({.items = {.properties_on_edges = GetParam()}});
  storage::Gid gid_from = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid_to = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertices
  {
    auto acc = store.Access();
    auto vertex_from = acc.CreateVertex();
    auto vertex_to = acc.CreateVertex();
    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();
    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Create edge
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    auto res = acc.CreateEdge(&*vertex_from, &*vertex_to, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 0);
    {
      auto ret = vertex_to->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_to)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_to)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_to)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Delete the edge, but abort the transaction
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    auto edge = vertex_from->OutEdges(storage::View::NEW).GetValue()[0];

    auto res = acc.DeleteEdge(&edge);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_to->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_to)->size(), 0);

    acc.Abort();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_to)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_to)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Delete the edge
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    auto edge = vertex_from->OutEdges(storage::View::NEW).GetValue()[0];

    auto res = acc.DeleteEdge(&edge);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_to->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_to)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeDeleteFromLargerAbort) {
  storage::Storage store({.items = {.properties_on_edges = GetParam()}});
  storage::Gid gid_from = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid_to = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertices
  {
    auto acc = store.Access();
    auto vertex_from = acc.CreateVertex();
    auto vertex_to = acc.CreateVertex();
    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();
    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Create edge
  {
    auto acc = store.Access();
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    auto res = acc.CreateEdge(&*vertex_from, &*vertex_to, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 0);
    {
      auto ret = vertex_to->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_to)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_to)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_to)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Delete the edge, but abort the transaction
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    auto edge = vertex_from->OutEdges(storage::View::NEW).GetValue()[0];

    auto res = acc.DeleteEdge(&edge);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_to->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_to)->size(), 0);

    acc.Abort();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_to)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW, {}, &*vertex_to)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Delete the edge
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    auto edge = vertex_from->OutEdges(storage::View::NEW).GetValue()[0];

    auto res = acc.DeleteEdge(&edge);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_to->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_to)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD, {}, &*vertex_from)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_from)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD, {}, &*vertex_to)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeDeleteFromSameAbort) {
  storage::Storage store({.items = {.properties_on_edges = GetParam()}});
  storage::Gid gid_vertex = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertex
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid_vertex = vertex.Gid();
    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Create edge
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto et = acc.NameToEdgeType("et5");

    auto res = acc.CreateEdge(&*vertex, &*vertex, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex);
    ASSERT_EQ(edge.ToVertex(), *vertex);

    // Check edges without filters
    ASSERT_EQ(vertex->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex->InDegree(storage::View::OLD), 0);
    {
      auto ret = vertex->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex->OutDegree(storage::View::OLD), 0);
    {
      auto ret = vertex->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {other_et}, &*vertex)->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {other_et}, &*vertex)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto et = acc.NameToEdgeType("et5");

    // Check edges without filters
    {
      auto ret = vertex->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {other_et}, &*vertex)->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {other_et}, &*vertex)->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {other_et}, &*vertex)->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {other_et}, &*vertex)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Delete the edge, but abort the transaction
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto et = acc.NameToEdgeType("et5");

    auto edge = vertex->OutEdges(storage::View::NEW).GetValue()[0];

    auto res = acc.DeleteEdge(&edge);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());

    // Check edges without filters
    {
      auto ret = vertex->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex->InDegree(storage::View::NEW), 0);
    {
      auto ret = vertex->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {other_et}, &*vertex)->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {other_et}, &*vertex)->size(), 0);

    acc.Abort();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto et = acc.NameToEdgeType("et5");

    // Check edges without filters
    {
      auto ret = vertex->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {other_et}, &*vertex)->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW, {other_et}, &*vertex)->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {other_et}, &*vertex)->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW, {other_et}, &*vertex)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Delete the edge
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto et = acc.NameToEdgeType("et5");

    auto edge = vertex->OutEdges(storage::View::NEW).GetValue()[0];

    auto res = acc.DeleteEdge(&edge);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());

    // Check edges without filters
    {
      auto ret = vertex->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex->InDegree(storage::View::NEW), 0);
    {
      auto ret = vertex->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex->OutDegree(storage::View::NEW), 0);

    auto other_et = acc.NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->InEdges(storage::View::OLD, {other_et}, &*vertex)->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {}, &*vertex)->size(), 1);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD, {other_et}, &*vertex)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    // Check edges without filters
    ASSERT_EQ(vertex->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex->OutDegree(storage::View::NEW), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, VertexDetachDeleteSingleCommit) {
  storage::Storage store({.items = {.properties_on_edges = GetParam()}});
  storage::Gid gid_from = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid_to = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create dataset
  {
    auto acc = store.Access();
    auto vertex_from = acc.CreateVertex();
    auto vertex_to = acc.CreateVertex();

    auto et = acc.NameToEdgeType("et5");

    auto res = acc.CreateEdge(&vertex_from, &vertex_to, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), vertex_from);
    ASSERT_EQ(edge.ToVertex(), vertex_to);

    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();

    // Check edges
    ASSERT_EQ(vertex_from.InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from.InDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_from.OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from.OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), vertex_from);
      ASSERT_EQ(e.ToVertex(), vertex_to);
    }
    {
      auto ret = vertex_to.InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to.InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), vertex_from);
      ASSERT_EQ(e.ToVertex(), vertex_to);
    }
    ASSERT_EQ(vertex_to.OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to.OutDegree(storage::View::NEW), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Detach delete vertex
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    // Delete must fail
    {
      auto ret = acc.DeleteVertex(&*vertex_from);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), storage::Error::VERTEX_HAS_EDGES);
    }

    // Detach delete vertex
    {
      auto ret = acc.DetachDeleteVertex(&*vertex_from);
      ASSERT_TRUE(ret.HasValue());
      ASSERT_TRUE(*ret);
    }

    // Check edges
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex_from->InDegree(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex_from->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex_from->OutDegree(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex_to->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check dataset
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_FALSE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, VertexDetachDeleteMultipleCommit) {
  storage::Storage store({.items = {.properties_on_edges = GetParam()}});
  storage::Gid gid_vertex1 = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid_vertex2 = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create dataset
  {
    auto acc = store.Access();
    auto vertex1 = acc.CreateVertex();
    auto vertex2 = acc.CreateVertex();

    gid_vertex1 = vertex1.Gid();
    gid_vertex2 = vertex2.Gid();

    auto et1 = acc.NameToEdgeType("et1");
    auto et2 = acc.NameToEdgeType("et2");
    auto et3 = acc.NameToEdgeType("et3");
    auto et4 = acc.NameToEdgeType("et4");

    auto res1 = acc.CreateEdge(&vertex1, &vertex2, et1);
    ASSERT_TRUE(res1.HasValue());
    auto edge1 = res1.GetValue();
    ASSERT_EQ(edge1.EdgeType(), et1);
    ASSERT_EQ(edge1.FromVertex(), vertex1);
    ASSERT_EQ(edge1.ToVertex(), vertex2);

    auto res2 = acc.CreateEdge(&vertex2, &vertex1, et2);
    ASSERT_TRUE(res2.HasValue());
    auto edge2 = res2.GetValue();
    ASSERT_EQ(edge2.EdgeType(), et2);
    ASSERT_EQ(edge2.FromVertex(), vertex2);
    ASSERT_EQ(edge2.ToVertex(), vertex1);

    auto res3 = acc.CreateEdge(&vertex1, &vertex1, et3);
    ASSERT_TRUE(res3.HasValue());
    auto edge3 = res3.GetValue();
    ASSERT_EQ(edge3.EdgeType(), et3);
    ASSERT_EQ(edge3.FromVertex(), vertex1);
    ASSERT_EQ(edge3.ToVertex(), vertex1);

    auto res4 = acc.CreateEdge(&vertex2, &vertex2, et4);
    ASSERT_TRUE(res4.HasValue());
    auto edge4 = res4.GetValue();
    ASSERT_EQ(edge4.EdgeType(), et4);
    ASSERT_EQ(edge4.FromVertex(), vertex2);
    ASSERT_EQ(edge4.ToVertex(), vertex2);

    // Check edges
    {
      auto ret = vertex1.InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1.InDegree(storage::View::NEW), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), vertex2);
        ASSERT_EQ(e.ToVertex(), vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), vertex1);
        ASSERT_EQ(e.ToVertex(), vertex1);
      }
    }
    {
      auto ret = vertex1.OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1.OutDegree(storage::View::NEW), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), vertex1);
        ASSERT_EQ(e.ToVertex(), vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), vertex1);
        ASSERT_EQ(e.ToVertex(), vertex1);
      }
    }
    {
      auto ret = vertex2.InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2.InDegree(storage::View::NEW), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), vertex1);
        ASSERT_EQ(e.ToVertex(), vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), vertex2);
        ASSERT_EQ(e.ToVertex(), vertex2);
      }
    }
    {
      auto ret = vertex2.OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2.OutDegree(storage::View::NEW), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), vertex2);
        ASSERT_EQ(e.ToVertex(), vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), vertex2);
        ASSERT_EQ(e.ToVertex(), vertex2);
      }
    }

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Detach delete vertex
  {
    auto acc = store.Access();
    auto vertex1 = acc.FindVertex(gid_vertex1, storage::View::NEW);
    auto vertex2 = acc.FindVertex(gid_vertex2, storage::View::NEW);
    ASSERT_TRUE(vertex1);
    ASSERT_TRUE(vertex2);

    auto et1 = acc.NameToEdgeType("et1");
    auto et2 = acc.NameToEdgeType("et2");
    auto et3 = acc.NameToEdgeType("et3");
    auto et4 = acc.NameToEdgeType("et4");

    // Delete must fail
    {
      auto ret = acc.DeleteVertex(&*vertex1);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), storage::Error::VERTEX_HAS_EDGES);
    }

    // Detach delete vertex
    {
      auto ret = acc.DetachDeleteVertex(&*vertex1);
      ASSERT_TRUE(ret.HasValue());
      ASSERT_TRUE(*ret);
    }

    // Check edges
    {
      auto ret = vertex1->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1->InDegree(storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    ASSERT_EQ(vertex1->InEdges(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex1->InDegree(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex1->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1->OutDegree(storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    ASSERT_EQ(vertex1->OutEdges(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex1->OutDegree(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex2->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2->InDegree(storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2->OutDegree(storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check dataset
  {
    auto acc = store.Access();
    auto vertex1 = acc.FindVertex(gid_vertex1, storage::View::NEW);
    auto vertex2 = acc.FindVertex(gid_vertex2, storage::View::NEW);
    ASSERT_FALSE(vertex1);
    ASSERT_TRUE(vertex2);

    auto et4 = acc.NameToEdgeType("et4");

    // Check edges
    {
      auto ret = vertex2->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, VertexDetachDeleteSingleAbort) {
  storage::Storage store({.items = {.properties_on_edges = GetParam()}});
  storage::Gid gid_from = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid_to = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create dataset
  {
    auto acc = store.Access();
    auto vertex_from = acc.CreateVertex();
    auto vertex_to = acc.CreateVertex();

    auto et = acc.NameToEdgeType("et5");

    auto res = acc.CreateEdge(&vertex_from, &vertex_to, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), vertex_from);
    ASSERT_EQ(edge.ToVertex(), vertex_to);

    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();

    // Check edges
    ASSERT_EQ(vertex_from.InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from.InDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_from.OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from.OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), vertex_from);
      ASSERT_EQ(e.ToVertex(), vertex_to);
    }
    {
      auto ret = vertex_to.InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to.InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), vertex_from);
      ASSERT_EQ(e.ToVertex(), vertex_to);
    }
    ASSERT_EQ(vertex_to.OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to.OutDegree(storage::View::NEW), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Detach delete vertex, but abort the transaction
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    // Delete must fail
    {
      auto ret = acc.DeleteVertex(&*vertex_from);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), storage::Error::VERTEX_HAS_EDGES);
    }

    // Detach delete vertex
    {
      auto ret = acc.DetachDeleteVertex(&*vertex_from);
      ASSERT_TRUE(ret.HasValue());
      ASSERT_TRUE(*ret);
    }

    // Check edges
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex_from->InDegree(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex_from->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex_from->OutDegree(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex_to->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    acc.Abort();
  }

  // Check dataset
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    // Check edges
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Detach delete vertex
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc.NameToEdgeType("et5");

    // Delete must fail
    {
      auto ret = acc.DeleteVertex(&*vertex_from);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), storage::Error::VERTEX_HAS_EDGES);
    }

    // Detach delete vertex
    {
      auto ret = acc.DetachDeleteVertex(&*vertex_from);
      ASSERT_TRUE(ret.HasValue());
      ASSERT_TRUE(*ret);
    }

    // Check edges
    ASSERT_EQ(vertex_from->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex_from->InDegree(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex_from->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex_from->OutDegree(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex_to->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check dataset
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_FALSE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges
    ASSERT_EQ(vertex_to->InEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->InEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(storage::View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(storage::View::NEW), 0);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, VertexDetachDeleteMultipleAbort) {
  storage::Storage store({.items = {.properties_on_edges = GetParam()}});
  storage::Gid gid_vertex1 = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid_vertex2 = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create dataset
  {
    auto acc = store.Access();
    auto vertex1 = acc.CreateVertex();
    auto vertex2 = acc.CreateVertex();

    gid_vertex1 = vertex1.Gid();
    gid_vertex2 = vertex2.Gid();

    auto et1 = acc.NameToEdgeType("et1");
    auto et2 = acc.NameToEdgeType("et2");
    auto et3 = acc.NameToEdgeType("et3");
    auto et4 = acc.NameToEdgeType("et4");

    auto res1 = acc.CreateEdge(&vertex1, &vertex2, et1);
    ASSERT_TRUE(res1.HasValue());
    auto edge1 = res1.GetValue();
    ASSERT_EQ(edge1.EdgeType(), et1);
    ASSERT_EQ(edge1.FromVertex(), vertex1);
    ASSERT_EQ(edge1.ToVertex(), vertex2);

    auto res2 = acc.CreateEdge(&vertex2, &vertex1, et2);
    ASSERT_TRUE(res2.HasValue());
    auto edge2 = res2.GetValue();
    ASSERT_EQ(edge2.EdgeType(), et2);
    ASSERT_EQ(edge2.FromVertex(), vertex2);
    ASSERT_EQ(edge2.ToVertex(), vertex1);

    auto res3 = acc.CreateEdge(&vertex1, &vertex1, et3);
    ASSERT_TRUE(res3.HasValue());
    auto edge3 = res3.GetValue();
    ASSERT_EQ(edge3.EdgeType(), et3);
    ASSERT_EQ(edge3.FromVertex(), vertex1);
    ASSERT_EQ(edge3.ToVertex(), vertex1);

    auto res4 = acc.CreateEdge(&vertex2, &vertex2, et4);
    ASSERT_TRUE(res4.HasValue());
    auto edge4 = res4.GetValue();
    ASSERT_EQ(edge4.EdgeType(), et4);
    ASSERT_EQ(edge4.FromVertex(), vertex2);
    ASSERT_EQ(edge4.ToVertex(), vertex2);

    // Check edges
    {
      auto ret = vertex1.InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1.InDegree(storage::View::NEW), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), vertex2);
        ASSERT_EQ(e.ToVertex(), vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), vertex1);
        ASSERT_EQ(e.ToVertex(), vertex1);
      }
    }
    {
      auto ret = vertex1.OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1.OutDegree(storage::View::NEW), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), vertex1);
        ASSERT_EQ(e.ToVertex(), vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), vertex1);
        ASSERT_EQ(e.ToVertex(), vertex1);
      }
    }
    {
      auto ret = vertex2.InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2.InDegree(storage::View::NEW), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), vertex1);
        ASSERT_EQ(e.ToVertex(), vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), vertex2);
        ASSERT_EQ(e.ToVertex(), vertex2);
      }
    }
    {
      auto ret = vertex2.OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2.OutDegree(storage::View::NEW), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), vertex2);
        ASSERT_EQ(e.ToVertex(), vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), vertex2);
        ASSERT_EQ(e.ToVertex(), vertex2);
      }
    }

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Detach delete vertex, but abort the transaction
  {
    auto acc = store.Access();
    auto vertex1 = acc.FindVertex(gid_vertex1, storage::View::NEW);
    auto vertex2 = acc.FindVertex(gid_vertex2, storage::View::NEW);
    ASSERT_TRUE(vertex1);
    ASSERT_TRUE(vertex2);

    auto et1 = acc.NameToEdgeType("et1");
    auto et2 = acc.NameToEdgeType("et2");
    auto et3 = acc.NameToEdgeType("et3");
    auto et4 = acc.NameToEdgeType("et4");

    // Delete must fail
    {
      auto ret = acc.DeleteVertex(&*vertex1);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), storage::Error::VERTEX_HAS_EDGES);
    }

    // Detach delete vertex
    {
      auto ret = acc.DetachDeleteVertex(&*vertex1);
      ASSERT_TRUE(ret.HasValue());
      ASSERT_TRUE(*ret);
    }

    // Check edges
    {
      auto ret = vertex1->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1->InDegree(storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    ASSERT_EQ(vertex1->InEdges(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex1->InDegree(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex1->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1->OutDegree(storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    ASSERT_EQ(vertex1->OutEdges(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex1->OutDegree(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex2->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2->InDegree(storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2->OutDegree(storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }

    acc.Abort();
  }

  // Check dataset
  {
    auto acc = store.Access();
    auto vertex1 = acc.FindVertex(gid_vertex1, storage::View::NEW);
    auto vertex2 = acc.FindVertex(gid_vertex2, storage::View::NEW);
    ASSERT_TRUE(vertex1);
    ASSERT_TRUE(vertex2);

    auto et1 = acc.NameToEdgeType("et1");
    auto et2 = acc.NameToEdgeType("et2");
    auto et3 = acc.NameToEdgeType("et3");
    auto et4 = acc.NameToEdgeType("et4");

    // Check edges
    {
      auto ret = vertex1->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1->InDegree(storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    {
      auto ret = vertex1->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1->InDegree(storage::View::NEW), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    {
      auto ret = vertex1->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1->OutDegree(storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    {
      auto ret = vertex1->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1->OutDegree(storage::View::NEW), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    {
      auto ret = vertex2->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2->InDegree(storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2->InDegree(storage::View::NEW), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2->OutDegree(storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2->OutDegree(storage::View::NEW), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Detach delete vertex
  {
    auto acc = store.Access();
    auto vertex1 = acc.FindVertex(gid_vertex1, storage::View::NEW);
    auto vertex2 = acc.FindVertex(gid_vertex2, storage::View::NEW);
    ASSERT_TRUE(vertex1);
    ASSERT_TRUE(vertex2);

    auto et1 = acc.NameToEdgeType("et1");
    auto et2 = acc.NameToEdgeType("et2");
    auto et3 = acc.NameToEdgeType("et3");
    auto et4 = acc.NameToEdgeType("et4");

    // Delete must fail
    {
      auto ret = acc.DeleteVertex(&*vertex1);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), storage::Error::VERTEX_HAS_EDGES);
    }

    // Detach delete vertex
    {
      auto ret = acc.DetachDeleteVertex(&*vertex1);
      ASSERT_TRUE(ret.HasValue());
      ASSERT_TRUE(*ret);
    }

    // Check edges
    {
      auto ret = vertex1->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1->InDegree(storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    ASSERT_EQ(vertex1->InEdges(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex1->InDegree(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex1->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1->OutDegree(storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    ASSERT_EQ(vertex1->OutEdges(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex1->OutDegree(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex2->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2->InDegree(storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2->OutDegree(storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check dataset
  {
    auto acc = store.Access();
    auto vertex1 = acc.FindVertex(gid_vertex1, storage::View::NEW);
    auto vertex2 = acc.FindVertex(gid_vertex2, storage::View::NEW);
    ASSERT_FALSE(vertex1);
    ASSERT_TRUE(vertex2);

    auto et4 = acc.NameToEdgeType("et4");

    // Check edges
    {
      auto ret = vertex2->InEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->InDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->InEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->InDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->OutEdges(storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->OutDegree(storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->OutEdges(storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->OutDegree(storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageWithProperties, EdgePropertyCommit) {
  storage::Storage store({.items = {.properties_on_edges = true}});
  storage::Gid gid = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    auto et = acc.NameToEdgeType("et5");
    auto edge = acc.CreateEdge(&vertex, &vertex, et).GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), vertex);
    ASSERT_EQ(edge.ToVertex(), vertex);

    auto property = acc.NameToProperty("property5");

    ASSERT_TRUE(edge.GetProperty(property, storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(storage::View::NEW)->size(), 0);

    {
      auto old_value = edge.SetProperty(property, storage::PropertyValue("temporary"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_EQ(edge.GetProperty(property, storage::View::NEW)->ValueString(), "temporary");
    {
      auto properties = edge.Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "temporary");
    }

    {
      auto old_value = edge.SetProperty(property, storage::PropertyValue("nandare"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(edge.GetProperty(property, storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto edge = vertex->OutEdges(storage::View::NEW).GetValue()[0];

    auto property = acc.NameToProperty("property5");

    ASSERT_EQ(edge.GetProperty(property, storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(edge.GetProperty(property, storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    auto other_property = acc.NameToProperty("other");

    ASSERT_TRUE(edge.GetProperty(other_property, storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(other_property, storage::View::NEW)->IsNull());

    acc.Abort();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto edge = vertex->OutEdges(storage::View::NEW).GetValue()[0];

    auto property = acc.NameToProperty("property5");

    {
      auto old_value = edge.SetProperty(property, storage::PropertyValue());
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(edge.GetProperty(property, storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_TRUE(edge.GetProperty(property, storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(storage::View::NEW)->size(), 0);

    {
      auto old_value = edge.SetProperty(property, storage::PropertyValue());
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto edge = vertex->OutEdges(storage::View::NEW).GetValue()[0];

    auto property = acc.NameToProperty("property5");

    ASSERT_TRUE(edge.GetProperty(property, storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property, storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(storage::View::OLD)->size(), 0);
    ASSERT_EQ(edge.Properties(storage::View::NEW)->size(), 0);

    auto other_property = acc.NameToProperty("other");

    ASSERT_TRUE(edge.GetProperty(other_property, storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(other_property, storage::View::NEW)->IsNull());

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageWithProperties, EdgePropertyAbort) {
  storage::Storage store({.items = {.properties_on_edges = true}});
  storage::Gid gid = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create the vertex.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    auto et = acc.NameToEdgeType("et5");
    auto edge = acc.CreateEdge(&vertex, &vertex, et).GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), vertex);
    ASSERT_EQ(edge.ToVertex(), vertex);
    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Set property 5 to "nandare", but abort the transaction.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto edge = vertex->OutEdges(storage::View::NEW).GetValue()[0];

    auto property = acc.NameToProperty("property5");

    ASSERT_TRUE(edge.GetProperty(property, storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(storage::View::NEW)->size(), 0);

    {
      auto old_value = edge.SetProperty(property, storage::PropertyValue("temporary"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_EQ(edge.GetProperty(property, storage::View::NEW)->ValueString(), "temporary");
    {
      auto properties = edge.Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "temporary");
    }

    {
      auto old_value = edge.SetProperty(property, storage::PropertyValue("nandare"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(edge.GetProperty(property, storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    acc.Abort();
  }

  // Check that property 5 is null.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto edge = vertex->OutEdges(storage::View::NEW).GetValue()[0];

    auto property = acc.NameToProperty("property5");

    ASSERT_TRUE(edge.GetProperty(property, storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property, storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(storage::View::OLD)->size(), 0);
    ASSERT_EQ(edge.Properties(storage::View::NEW)->size(), 0);

    auto other_property = acc.NameToProperty("other");

    ASSERT_TRUE(edge.GetProperty(other_property, storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(other_property, storage::View::NEW)->IsNull());

    acc.Abort();
  }

  // Set property 5 to "nandare".
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto edge = vertex->OutEdges(storage::View::NEW).GetValue()[0];

    auto property = acc.NameToProperty("property5");

    ASSERT_TRUE(edge.GetProperty(property, storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(storage::View::NEW)->size(), 0);

    {
      auto old_value = edge.SetProperty(property, storage::PropertyValue("temporary"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_EQ(edge.GetProperty(property, storage::View::NEW)->ValueString(), "temporary");
    {
      auto properties = edge.Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "temporary");
    }

    {
      auto old_value = edge.SetProperty(property, storage::PropertyValue("nandare"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(edge.GetProperty(property, storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check that property 5 is "nandare".
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto edge = vertex->OutEdges(storage::View::NEW).GetValue()[0];

    auto property = acc.NameToProperty("property5");

    ASSERT_EQ(edge.GetProperty(property, storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(edge.GetProperty(property, storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    auto other_property = acc.NameToProperty("other");

    ASSERT_TRUE(edge.GetProperty(other_property, storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(other_property, storage::View::NEW)->IsNull());

    acc.Abort();
  }

  // Set property 5 to null, but abort the transaction.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto edge = vertex->OutEdges(storage::View::NEW).GetValue()[0];

    auto property = acc.NameToProperty("property5");

    ASSERT_EQ(edge.GetProperty(property, storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(edge.GetProperty(property, storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    {
      auto old_value = edge.SetProperty(property, storage::PropertyValue());
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(edge.GetProperty(property, storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_TRUE(edge.GetProperty(property, storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(storage::View::NEW)->size(), 0);

    acc.Abort();
  }

  // Check that property 5 is "nandare".
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto edge = vertex->OutEdges(storage::View::NEW).GetValue()[0];

    auto property = acc.NameToProperty("property5");

    ASSERT_EQ(edge.GetProperty(property, storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(edge.GetProperty(property, storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    auto other_property = acc.NameToProperty("other");

    ASSERT_TRUE(edge.GetProperty(other_property, storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(other_property, storage::View::NEW)->IsNull());

    acc.Abort();
  }

  // Set property 5 to null.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto edge = vertex->OutEdges(storage::View::NEW).GetValue()[0];

    auto property = acc.NameToProperty("property5");

    ASSERT_EQ(edge.GetProperty(property, storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(edge.GetProperty(property, storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    {
      auto old_value = edge.SetProperty(property, storage::PropertyValue());
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(edge.GetProperty(property, storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_TRUE(edge.GetProperty(property, storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(storage::View::NEW)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check that property 5 is null.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto edge = vertex->OutEdges(storage::View::NEW).GetValue()[0];

    auto property = acc.NameToProperty("property5");

    ASSERT_TRUE(edge.GetProperty(property, storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property, storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(storage::View::OLD)->size(), 0);
    ASSERT_EQ(edge.Properties(storage::View::NEW)->size(), 0);

    auto other_property = acc.NameToProperty("other");

    ASSERT_TRUE(edge.GetProperty(other_property, storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(other_property, storage::View::NEW)->IsNull());

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageWithProperties, EdgePropertySerializationError) {
  storage::Storage store({.items = {.properties_on_edges = true}});
  storage::Gid gid = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    auto et = acc.NameToEdgeType("et5");
    auto edge = acc.CreateEdge(&vertex, &vertex, et).GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), vertex);
    ASSERT_EQ(edge.ToVertex(), vertex);
    ASSERT_FALSE(acc.Commit().HasError());
  }

  auto acc1 = store.Access();
  auto acc2 = store.Access();

  // Set property 1 to 123 in accessor 1.
  {
    auto vertex = acc1.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto edge = vertex->OutEdges(storage::View::NEW).GetValue()[0];

    auto property1 = acc1.NameToProperty("property1");
    auto property2 = acc1.NameToProperty("property2");

    ASSERT_TRUE(edge.GetProperty(property1, storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property1, storage::View::NEW)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property2, storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property2, storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(storage::View::OLD)->size(), 0);
    ASSERT_EQ(edge.Properties(storage::View::NEW)->size(), 0);

    {
      auto old_value = edge.SetProperty(property1, storage::PropertyValue(123));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_TRUE(edge.GetProperty(property1, storage::View::OLD)->IsNull());
    ASSERT_EQ(edge.GetProperty(property1, storage::View::NEW)->ValueInt(), 123);
    ASSERT_TRUE(edge.GetProperty(property2, storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property2, storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(storage::View::OLD)->size(), 0);
    {
      auto properties = edge.Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property1].ValueInt(), 123);
    }
  }

  // Set property 2 to "nandare" in accessor 2.
  {
    auto vertex = acc2.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto edge = vertex->OutEdges(storage::View::NEW).GetValue()[0];

    auto property1 = acc2.NameToProperty("property1");
    auto property2 = acc2.NameToProperty("property2");

    ASSERT_TRUE(edge.GetProperty(property1, storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property1, storage::View::NEW)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property2, storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property2, storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(storage::View::OLD)->size(), 0);
    ASSERT_EQ(edge.Properties(storage::View::NEW)->size(), 0);

    {
      auto res = edge.SetProperty(property2, storage::PropertyValue("nandare"));
      ASSERT_TRUE(res.HasError());
      ASSERT_EQ(res.GetError(), storage::Error::SERIALIZATION_ERROR);
    }
  }

  // Finalize both accessors.
  ASSERT_FALSE(acc1.Commit().HasError());
  acc2.Abort();

  // Check which properties exist.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto edge = vertex->OutEdges(storage::View::NEW).GetValue()[0];

    auto property1 = acc.NameToProperty("property1");
    auto property2 = acc.NameToProperty("property2");

    ASSERT_EQ(edge.GetProperty(property1, storage::View::OLD)->ValueInt(), 123);
    ASSERT_TRUE(edge.GetProperty(property2, storage::View::OLD)->IsNull());
    {
      auto properties = edge.Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property1].ValueInt(), 123);
    }

    ASSERT_EQ(edge.GetProperty(property1, storage::View::NEW)->ValueInt(), 123);
    ASSERT_TRUE(edge.GetProperty(property2, storage::View::NEW)->IsNull());
    {
      auto properties = edge.Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property1].ValueInt(), 123);
    }

    acc.Abort();
  }
}

TEST(StorageWithProperties, EdgePropertyClear) {
  storage::Storage store({.items = {.properties_on_edges = true}});
  storage::Gid gid;
  auto property1 = store.NameToProperty("property1");
  auto property2 = store.NameToProperty("property2");
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    auto et = acc.NameToEdgeType("et5");
    auto edge = acc.CreateEdge(&vertex, &vertex, et).GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), vertex);
    ASSERT_EQ(edge.ToVertex(), vertex);

    auto old_value = edge.SetProperty(property1, storage::PropertyValue("value"));
    ASSERT_TRUE(old_value.HasValue());
    ASSERT_TRUE(old_value->IsNull());

    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto edge = vertex->OutEdges(storage::View::NEW).GetValue()[0];

    ASSERT_EQ(edge.GetProperty(property1, storage::View::OLD)->ValueString(), "value");
    ASSERT_TRUE(edge.GetProperty(property2, storage::View::OLD)->IsNull());
    ASSERT_THAT(edge.Properties(storage::View::OLD).GetValue(),
                UnorderedElementsAre(std::pair(property1, storage::PropertyValue("value"))));

    {
      auto old_values = edge.ClearProperties();
      ASSERT_TRUE(old_values.HasValue());
      ASSERT_FALSE(old_values->empty());
    }

    ASSERT_TRUE(edge.GetProperty(property1, storage::View::NEW)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property2, storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(storage::View::NEW).GetValue().size(), 0);

    {
      auto old_values = edge.ClearProperties();
      ASSERT_TRUE(old_values.HasValue());
      ASSERT_TRUE(old_values->empty());
    }

    ASSERT_TRUE(edge.GetProperty(property1, storage::View::NEW)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property2, storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(storage::View::NEW).GetValue().size(), 0);

    acc.Abort();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto edge = vertex->OutEdges(storage::View::NEW).GetValue()[0];

    auto old_value = edge.SetProperty(property2, storage::PropertyValue(42));
    ASSERT_TRUE(old_value.HasValue());
    ASSERT_TRUE(old_value->IsNull());

    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto edge = vertex->OutEdges(storage::View::NEW).GetValue()[0];

    ASSERT_EQ(edge.GetProperty(property1, storage::View::OLD)->ValueString(), "value");
    ASSERT_EQ(edge.GetProperty(property2, storage::View::OLD)->ValueInt(), 42);
    ASSERT_THAT(edge.Properties(storage::View::OLD).GetValue(),
                UnorderedElementsAre(std::pair(property1, storage::PropertyValue("value")),
                                     std::pair(property2, storage::PropertyValue(42))));

    {
      auto old_values = edge.ClearProperties();
      ASSERT_TRUE(old_values.HasValue());
      ASSERT_FALSE(old_values->empty());
    }

    ASSERT_TRUE(edge.GetProperty(property1, storage::View::NEW)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property2, storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(storage::View::NEW).GetValue().size(), 0);

    {
      auto old_values = edge.ClearProperties();
      ASSERT_TRUE(old_values.HasValue());
      ASSERT_TRUE(old_values->empty());
    }

    ASSERT_TRUE(edge.GetProperty(property1, storage::View::NEW)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property2, storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(storage::View::NEW).GetValue().size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto edge = vertex->OutEdges(storage::View::NEW).GetValue()[0];

    ASSERT_TRUE(edge.GetProperty(property1, storage::View::NEW)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property2, storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(storage::View::NEW).GetValue().size(), 0);

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageWithoutProperties, EdgePropertyAbort) {
  storage::Storage store({.items = {.properties_on_edges = false}});
  storage::Gid gid = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    auto et = acc.NameToEdgeType("et5");
    auto edge = acc.CreateEdge(&vertex, &vertex, et).GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), vertex);
    ASSERT_EQ(edge.ToVertex(), vertex);
    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto edge = vertex->OutEdges(storage::View::NEW).GetValue()[0];

    auto property = acc.NameToProperty("property5");

    ASSERT_TRUE(edge.GetProperty(property, storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(storage::View::NEW)->size(), 0);

    {
      auto res = edge.SetProperty(property, storage::PropertyValue("temporary"));
      ASSERT_TRUE(res.HasError());
      ASSERT_EQ(res.GetError(), storage::Error::PROPERTIES_DISABLED);
    }

    ASSERT_TRUE(edge.GetProperty(property, storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(storage::View::NEW)->size(), 0);

    {
      auto res = edge.SetProperty(property, storage::PropertyValue("nandare"));
      ASSERT_TRUE(res.HasError());
      ASSERT_EQ(res.GetError(), storage::Error::PROPERTIES_DISABLED);
    }

    ASSERT_TRUE(edge.GetProperty(property, storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(storage::View::NEW)->size(), 0);

    acc.Abort();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto edge = vertex->OutEdges(storage::View::NEW).GetValue()[0];

    auto property = acc.NameToProperty("property5");

    ASSERT_TRUE(edge.GetProperty(property, storage::View::OLD)->IsNull());
    ASSERT_EQ(edge.Properties(storage::View::OLD)->size(), 0);

    ASSERT_TRUE(edge.GetProperty(property, storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(storage::View::NEW)->size(), 0);

    auto other_property = acc.NameToProperty("other");

    ASSERT_TRUE(edge.GetProperty(other_property, storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(other_property, storage::View::NEW)->IsNull());

    acc.Abort();
  }
}

TEST(StorageWithoutProperties, EdgePropertyClear) {
  storage::Storage store({.items = {.properties_on_edges = false}});
  storage::Gid gid;
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    auto et = acc.NameToEdgeType("et5");
    auto edge = acc.CreateEdge(&vertex, &vertex, et).GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), vertex);
    ASSERT_EQ(edge.ToVertex(), vertex);
    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto edge = vertex->OutEdges(storage::View::NEW).GetValue()[0];

    ASSERT_EQ(edge.ClearProperties().GetError(), storage::Error::PROPERTIES_DISABLED);

    acc.Abort();
  }
}

TEST(StorageWithProperties, EdgeNonexistentPropertyAPI) {
  storage::Storage store({.items = {.properties_on_edges = true}});

  auto property = store.NameToProperty("property");

  auto acc = store.Access();
  auto vertex = acc.CreateVertex();
  auto edge = acc.CreateEdge(&vertex, &vertex, acc.NameToEdgeType("edge"));
  ASSERT_TRUE(edge.HasValue());

  // Check state before (OLD view).
  ASSERT_EQ(edge->Properties(storage::View::OLD).GetError(), storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(edge->GetProperty(property, storage::View::OLD).GetError(), storage::Error::NONEXISTENT_OBJECT);

  // Check state before (NEW view).
  ASSERT_EQ(edge->Properties(storage::View::NEW)->size(), 0);
  ASSERT_EQ(*edge->GetProperty(property, storage::View::NEW), storage::PropertyValue());

  // Modify edge.
  ASSERT_TRUE(edge->SetProperty(property, storage::PropertyValue("value"))->IsNull());

  // Check state after (OLD view).
  ASSERT_EQ(edge->Properties(storage::View::OLD).GetError(), storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(edge->GetProperty(property, storage::View::OLD).GetError(), storage::Error::NONEXISTENT_OBJECT);

  // Check state after (NEW view).
  ASSERT_EQ(edge->Properties(storage::View::NEW)->size(), 1);
  ASSERT_EQ(*edge->GetProperty(property, storage::View::NEW), storage::PropertyValue("value"));

  ASSERT_FALSE(acc.Commit().HasError());
}
