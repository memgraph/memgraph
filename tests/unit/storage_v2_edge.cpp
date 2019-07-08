#include <gtest/gtest.h>

#include <limits>

#include "storage/v2/storage.hpp"

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, EdgeCreateFromSmallerCommit) {
  storage::Storage store;
  storage::Gid gid_from =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid_to =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertices
  {
    auto acc = store.Access();
    auto vertex_from = acc.CreateVertex();
    auto vertex_to = acc.CreateVertex();
    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();
    acc.Commit();
  }

  // Create edge
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto res = acc.CreateEdge(&*vertex_from, &*vertex_to, 5);
    ASSERT_TRUE(res.IsReturn());
    auto edge = res.GetReturn();
    ASSERT_EQ(edge.EdgeType(), 5);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    {
      auto ret = vertex_to->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::OLD).GetReturn().size(),
        1);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, EdgeCreateFromLargerCommit) {
  storage::Storage store;
  storage::Gid gid_from =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid_to =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertices
  {
    auto acc = store.Access();
    auto vertex_to = acc.CreateVertex();
    auto vertex_from = acc.CreateVertex();
    gid_to = vertex_to.Gid();
    gid_from = vertex_from.Gid();
    acc.Commit();
  }

  // Create edge
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto res = acc.CreateEdge(&*vertex_from, &*vertex_to, 5);
    ASSERT_TRUE(res.IsReturn());
    auto edge = res.GetReturn();
    ASSERT_EQ(edge.EdgeType(), 5);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    {
      auto ret = vertex_to->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::OLD).GetReturn().size(),
        1);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, EdgeCreateFromSameCommit) {
  storage::Storage store;
  storage::Gid gid_vertex =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertex
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid_vertex = vertex.Gid();
    acc.Commit();
  }

  // Create edge
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto res = acc.CreateEdge(&*vertex, &*vertex, 5);
    ASSERT_TRUE(res.IsReturn());
    auto edge = res.GetReturn();
    ASSERT_EQ(edge.EdgeType(), 5);
    ASSERT_EQ(edge.FromVertex(), *vertex);
    ASSERT_EQ(edge.ToVertex(), *vertex);

    // Check edges without filters
    ASSERT_EQ(vertex->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    {
      auto ret = vertex->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->OutEdges({}, storage::View::OLD).GetReturn().size(), 0);
    {
      auto ret = vertex->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    // Check edges with filters
    ASSERT_EQ(vertex->OutEdges({2}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);
    ASSERT_EQ(vertex->InEdges({2}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    // Check edges without filters
    {
      auto ret = vertex->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    // Check edges with filters
    ASSERT_EQ(vertex->InEdges({2}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->InEdges({2}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex->InEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);
    ASSERT_EQ(vertex->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);
    ASSERT_EQ(vertex->OutEdges({2}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->OutEdges({2}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex->OutEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);
    ASSERT_EQ(vertex->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, EdgeCreateFromSmallerAbort) {
  storage::Storage store;
  storage::Gid gid_from =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid_to =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertices
  {
    auto acc = store.Access();
    auto vertex_from = acc.CreateVertex();
    auto vertex_to = acc.CreateVertex();
    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();
    acc.Commit();
  }

  // Create edge, but abort the transaction
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto res = acc.CreateEdge(&*vertex_from, &*vertex_to, 5);
    ASSERT_TRUE(res.IsReturn());
    auto edge = res.GetReturn();
    ASSERT_EQ(edge.EdgeType(), 5);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    {
      auto ret = vertex_to->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

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
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    acc.Commit();
  }

  // Create edge
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto res = acc.CreateEdge(&*vertex_from, &*vertex_to, 5);
    ASSERT_TRUE(res.IsReturn());
    auto edge = res.GetReturn();
    ASSERT_EQ(edge.EdgeType(), 5);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    {
      auto ret = vertex_to->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::OLD).GetReturn().size(),
        1);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, EdgeCreateFromLargerAbort) {
  storage::Storage store;
  storage::Gid gid_from =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid_to =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertices
  {
    auto acc = store.Access();
    auto vertex_to = acc.CreateVertex();
    auto vertex_from = acc.CreateVertex();
    gid_to = vertex_to.Gid();
    gid_from = vertex_from.Gid();
    acc.Commit();
  }

  // Create edge, but abort the transaction
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto res = acc.CreateEdge(&*vertex_from, &*vertex_to, 5);
    ASSERT_TRUE(res.IsReturn());
    auto edge = res.GetReturn();
    ASSERT_EQ(edge.EdgeType(), 5);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    {
      auto ret = vertex_to->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

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
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    acc.Commit();
  }

  // Create edge
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto res = acc.CreateEdge(&*vertex_from, &*vertex_to, 5);
    ASSERT_TRUE(res.IsReturn());
    auto edge = res.GetReturn();
    ASSERT_EQ(edge.EdgeType(), 5);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    {
      auto ret = vertex_to->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::OLD).GetReturn().size(),
        1);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, EdgeCreateFromSameAbort) {
  storage::Storage store;
  storage::Gid gid_vertex =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertex
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid_vertex = vertex.Gid();
    acc.Commit();
  }

  // Create edge, but abort the transaction
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto res = acc.CreateEdge(&*vertex, &*vertex, 5);
    ASSERT_TRUE(res.IsReturn());
    auto edge = res.GetReturn();
    ASSERT_EQ(edge.EdgeType(), 5);
    ASSERT_EQ(edge.FromVertex(), *vertex);
    ASSERT_EQ(edge.ToVertex(), *vertex);

    // Check edges without filters
    ASSERT_EQ(vertex->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    {
      auto ret = vertex->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->OutEdges({}, storage::View::OLD).GetReturn().size(), 0);
    {
      auto ret = vertex->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    // Check edges with filters
    ASSERT_EQ(vertex->OutEdges({2}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);
    ASSERT_EQ(vertex->InEdges({2}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Abort();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    // Check edges without filters
    ASSERT_EQ(vertex->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->InEdges({}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex->OutEdges({}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->OutEdges({}, storage::View::NEW).GetReturn().size(), 0);

    acc.Commit();
  }

  // Create edge
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto res = acc.CreateEdge(&*vertex, &*vertex, 5);
    ASSERT_TRUE(res.IsReturn());
    auto edge = res.GetReturn();
    ASSERT_EQ(edge.EdgeType(), 5);
    ASSERT_EQ(edge.FromVertex(), *vertex);
    ASSERT_EQ(edge.ToVertex(), *vertex);

    // Check edges without filters
    ASSERT_EQ(vertex->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    {
      auto ret = vertex->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->OutEdges({}, storage::View::OLD).GetReturn().size(), 0);
    {
      auto ret = vertex->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    // Check edges with filters
    ASSERT_EQ(vertex->OutEdges({2}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);
    ASSERT_EQ(vertex->InEdges({2}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    // Check edges without filters
    {
      auto ret = vertex->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    // Check edges with filters
    ASSERT_EQ(vertex->InEdges({2}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->InEdges({2}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex->InEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);
    ASSERT_EQ(vertex->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);
    ASSERT_EQ(vertex->OutEdges({2}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->OutEdges({2}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex->OutEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);
    ASSERT_EQ(vertex->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, EdgeDeleteFromSmallerCommit) {
  storage::Storage store;
  storage::Gid gid_from =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid_to =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertices
  {
    auto acc = store.Access();
    auto vertex_from = acc.CreateVertex();
    auto vertex_to = acc.CreateVertex();
    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();
    acc.Commit();
  }

  // Create edge
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto res = acc.CreateEdge(&*vertex_from, &*vertex_to, 5);
    ASSERT_TRUE(res.IsReturn());
    auto edge = res.GetReturn();
    ASSERT_EQ(edge.EdgeType(), 5);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    {
      auto ret = vertex_to->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::OLD).GetReturn().size(),
        1);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Delete edge
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto [edge_type, other_vertex, edge] =
        vertex_from->OutEdges({}, storage::View::NEW).GetReturn()[0];

    auto res = acc.DeleteEdge(&edge);
    ASSERT_TRUE(res.IsReturn());
    ASSERT_TRUE(res.GetReturn());

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_to->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::OLD).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    acc.Commit();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, EdgeDeleteFromLargerCommit) {
  storage::Storage store;
  storage::Gid gid_from =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid_to =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertices
  {
    auto acc = store.Access();
    auto vertex_to = acc.CreateVertex();
    auto vertex_from = acc.CreateVertex();
    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();
    acc.Commit();
  }

  // Create edge
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto res = acc.CreateEdge(&*vertex_from, &*vertex_to, 5);
    ASSERT_TRUE(res.IsReturn());
    auto edge = res.GetReturn();
    ASSERT_EQ(edge.EdgeType(), 5);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    {
      auto ret = vertex_to->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::OLD).GetReturn().size(),
        1);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Delete edge
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto [edge_type, other_vertex, edge] =
        vertex_from->OutEdges({}, storage::View::NEW).GetReturn()[0];

    auto res = acc.DeleteEdge(&edge);
    ASSERT_TRUE(res.IsReturn());
    ASSERT_TRUE(res.GetReturn());

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_to->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::OLD).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    acc.Commit();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, EdgeDeleteFromSameCommit) {
  storage::Storage store;
  storage::Gid gid_vertex =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertex
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid_vertex = vertex.Gid();
    acc.Commit();
  }

  // Create edge
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto res = acc.CreateEdge(&*vertex, &*vertex, 5);
    ASSERT_TRUE(res.IsReturn());
    auto edge = res.GetReturn();
    ASSERT_EQ(edge.EdgeType(), 5);
    ASSERT_EQ(edge.FromVertex(), *vertex);
    ASSERT_EQ(edge.ToVertex(), *vertex);

    // Check edges without filters
    ASSERT_EQ(vertex->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    {
      auto ret = vertex->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->OutEdges({}, storage::View::OLD).GetReturn().size(), 0);
    {
      auto ret = vertex->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    // Check edges with filters
    ASSERT_EQ(vertex->OutEdges({2}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);
    ASSERT_EQ(vertex->InEdges({2}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    // Check edges without filters
    {
      auto ret = vertex->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    // Check edges with filters
    ASSERT_EQ(vertex->InEdges({2}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->InEdges({2}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex->InEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);
    ASSERT_EQ(vertex->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);
    ASSERT_EQ(vertex->OutEdges({2}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->OutEdges({2}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex->OutEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);
    ASSERT_EQ(vertex->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Delete edge
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto [edge_type, other_vertex, edge] =
        vertex->OutEdges({}, storage::View::NEW).GetReturn()[0];

    auto res = acc.DeleteEdge(&edge);
    ASSERT_TRUE(res.IsReturn());
    ASSERT_TRUE(res.GetReturn());

    // Check edges without filters
    {
      auto ret = vertex->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->InEdges({}, storage::View::NEW).GetReturn().size(), 0);
    {
      auto ret = vertex->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->OutEdges({}, storage::View::NEW).GetReturn().size(), 0);

    // Check edges with filters
    ASSERT_EQ(vertex->InEdges({2}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->InEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);
    ASSERT_EQ(vertex->OutEdges({2}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->OutEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    // Check edges without filters
    ASSERT_EQ(vertex->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->InEdges({}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex->OutEdges({}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->OutEdges({}, storage::View::NEW).GetReturn().size(), 0);

    acc.Commit();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, EdgeDeleteFromSmallerAbort) {
  storage::Storage store;
  storage::Gid gid_from =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid_to =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertices
  {
    auto acc = store.Access();
    auto vertex_from = acc.CreateVertex();
    auto vertex_to = acc.CreateVertex();
    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();
    acc.Commit();
  }

  // Create edge
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto res = acc.CreateEdge(&*vertex_from, &*vertex_to, 5);
    ASSERT_TRUE(res.IsReturn());
    auto edge = res.GetReturn();
    ASSERT_EQ(edge.EdgeType(), 5);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    {
      auto ret = vertex_to->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::OLD).GetReturn().size(),
        1);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Delete the edge, but abort the transaction
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto [edge_type, other_vertex, edge] =
        vertex_from->OutEdges({}, storage::View::NEW).GetReturn()[0];

    auto res = acc.DeleteEdge(&edge);
    ASSERT_TRUE(res.IsReturn());
    ASSERT_TRUE(res.GetReturn());

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_to->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::OLD).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);

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
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::OLD).GetReturn().size(),
        1);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Delete the edge
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto [edge_type, other_vertex, edge] =
        vertex_from->OutEdges({}, storage::View::NEW).GetReturn()[0];

    auto res = acc.DeleteEdge(&edge);
    ASSERT_TRUE(res.IsReturn());
    ASSERT_TRUE(res.GetReturn());

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_to->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::OLD).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    acc.Commit();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, EdgeDeleteFromLargerAbort) {
  storage::Storage store;
  storage::Gid gid_from =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid_to =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertices
  {
    auto acc = store.Access();
    auto vertex_from = acc.CreateVertex();
    auto vertex_to = acc.CreateVertex();
    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();
    acc.Commit();
  }

  // Create edge
  {
    auto acc = store.Access();
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto res = acc.CreateEdge(&*vertex_from, &*vertex_to, 5);
    ASSERT_TRUE(res.IsReturn());
    auto edge = res.GetReturn();
    ASSERT_EQ(edge.EdgeType(), 5);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    {
      auto ret = vertex_to->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::OLD).GetReturn().size(),
        1);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Delete the edge, but abort the transaction
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto [edge_type, other_vertex, edge] =
        vertex_from->OutEdges({}, storage::View::NEW).GetReturn()[0];

    auto res = acc.DeleteEdge(&edge);
    ASSERT_TRUE(res.IsReturn());
    ASSERT_TRUE(res.GetReturn());

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_to->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::OLD).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);

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
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::OLD).GetReturn().size(),
        1);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Delete the edge
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto [edge_type, other_vertex, edge] =
        vertex_from->OutEdges({}, storage::View::NEW).GetReturn()[0];

    auto res = acc.DeleteEdge(&edge);
    ASSERT_TRUE(res.IsReturn());
    ASSERT_TRUE(res.GetReturn());

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_to->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(
        vertex_from->OutEdges({2, 5}, storage::View::OLD).GetReturn().size(),
        1);
    ASSERT_EQ(vertex_to->InEdges({2}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    acc.Commit();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, EdgeDeleteFromSameAbort) {
  storage::Storage store;
  storage::Gid gid_vertex =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertex
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid_vertex = vertex.Gid();
    acc.Commit();
  }

  // Create edge
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto res = acc.CreateEdge(&*vertex, &*vertex, 5);
    ASSERT_TRUE(res.IsReturn());
    auto edge = res.GetReturn();
    ASSERT_EQ(edge.EdgeType(), 5);
    ASSERT_EQ(edge.FromVertex(), *vertex);
    ASSERT_EQ(edge.ToVertex(), *vertex);

    // Check edges without filters
    ASSERT_EQ(vertex->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    {
      auto ret = vertex->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->OutEdges({}, storage::View::OLD).GetReturn().size(), 0);
    {
      auto ret = vertex->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    // Check edges with filters
    ASSERT_EQ(vertex->OutEdges({2}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);
    ASSERT_EQ(vertex->InEdges({2}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    // Check edges without filters
    {
      auto ret = vertex->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    // Check edges with filters
    ASSERT_EQ(vertex->InEdges({2}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->InEdges({2}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex->InEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);
    ASSERT_EQ(vertex->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);
    ASSERT_EQ(vertex->OutEdges({2}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->OutEdges({2}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex->OutEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);
    ASSERT_EQ(vertex->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Delete the edge, but abort the transaction
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto [edge_type, other_vertex, edge] =
        vertex->OutEdges({}, storage::View::NEW).GetReturn()[0];

    auto res = acc.DeleteEdge(&edge);
    ASSERT_TRUE(res.IsReturn());
    ASSERT_TRUE(res.GetReturn());

    // Check edges without filters
    {
      auto ret = vertex->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->InEdges({}, storage::View::NEW).GetReturn().size(), 0);
    {
      auto ret = vertex->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->OutEdges({}, storage::View::NEW).GetReturn().size(), 0);

    // Check edges with filters
    ASSERT_EQ(vertex->InEdges({2}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->InEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);
    ASSERT_EQ(vertex->OutEdges({2}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->OutEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);

    acc.Abort();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    // Check edges without filters
    {
      auto ret = vertex->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    // Check edges with filters
    ASSERT_EQ(vertex->InEdges({2}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->InEdges({2}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex->InEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);
    ASSERT_EQ(vertex->InEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);
    ASSERT_EQ(vertex->OutEdges({2}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->OutEdges({2}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex->OutEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);
    ASSERT_EQ(vertex->OutEdges({2, 5}, storage::View::NEW).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Delete the edge
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto [edge_type, other_vertex, edge] =
        vertex->OutEdges({}, storage::View::NEW).GetReturn()[0];

    auto res = acc.DeleteEdge(&edge);
    ASSERT_TRUE(res.IsReturn());
    ASSERT_TRUE(res.GetReturn());

    // Check edges without filters
    {
      auto ret = vertex->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->InEdges({}, storage::View::NEW).GetReturn().size(), 0);
    {
      auto ret = vertex->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->OutEdges({}, storage::View::NEW).GetReturn().size(), 0);

    // Check edges with filters
    ASSERT_EQ(vertex->InEdges({2}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->InEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);
    ASSERT_EQ(vertex->OutEdges({2}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->OutEdges({2, 5}, storage::View::OLD).GetReturn().size(),
              1);

    acc.Commit();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid_vertex, storage::View::NEW);
    ASSERT_TRUE(vertex);

    // Check edges without filters
    ASSERT_EQ(vertex->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->InEdges({}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex->OutEdges({}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->OutEdges({}, storage::View::NEW).GetReturn().size(), 0);

    acc.Commit();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexDetachDeleteSingleCommit) {
  storage::Storage store;
  storage::Gid gid_from =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid_to =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create dataset
  {
    auto acc = store.Access();
    auto vertex_from = acc.CreateVertex();
    auto vertex_to = acc.CreateVertex();

    auto res = acc.CreateEdge(&vertex_from, &vertex_to, 5);
    ASSERT_TRUE(res.IsReturn());
    auto edge = res.GetReturn();
    ASSERT_EQ(edge.EdgeType(), 5);
    ASSERT_EQ(edge.FromVertex(), vertex_from);
    ASSERT_EQ(edge.ToVertex(), vertex_to);

    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();

    // Check edges
    ASSERT_EQ(vertex_from.InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_from.OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), vertex_from);
      ASSERT_EQ(e.ToVertex(), vertex_to);
    }
    {
      auto ret = vertex_to.InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), vertex_from);
      ASSERT_EQ(e.ToVertex(), vertex_to);
    }
    ASSERT_EQ(vertex_to.OutEdges({}, storage::View::NEW).GetReturn().size(), 0);

    acc.Commit();
  }

  // Detach delete vertex
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Delete must fail
    {
      auto ret = acc.DeleteVertex(&*vertex_from);
      ASSERT_TRUE(ret.IsError());
      ASSERT_EQ(ret.GetError(), storage::Error::VERTEX_HAS_EDGES);
    }

    // Detach delete vertex
    {
      auto ret = acc.DetachDeleteVertex(&*vertex_from);
      ASSERT_TRUE(ret.IsReturn());
      ASSERT_TRUE(ret.GetReturn());
    }

    // Check edges
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex_to->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    acc.Commit();
  }

  // Check dataset
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_FALSE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexDetachDeleteMultipleCommit) {
  storage::Storage store;
  storage::Gid gid_vertex1 =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid_vertex2 =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create dataset
  {
    auto acc = store.Access();
    auto vertex1 = acc.CreateVertex();
    auto vertex2 = acc.CreateVertex();

    gid_vertex1 = vertex1.Gid();
    gid_vertex2 = vertex2.Gid();

    auto res1 = acc.CreateEdge(&vertex1, &vertex2, 10);
    ASSERT_TRUE(res1.IsReturn());
    auto edge1 = res1.GetReturn();
    ASSERT_EQ(edge1.EdgeType(), 10);
    ASSERT_EQ(edge1.FromVertex(), vertex1);
    ASSERT_EQ(edge1.ToVertex(), vertex2);

    auto res2 = acc.CreateEdge(&vertex2, &vertex1, 20);
    ASSERT_TRUE(res2.IsReturn());
    auto edge2 = res2.GetReturn();
    ASSERT_EQ(edge2.EdgeType(), 20);
    ASSERT_EQ(edge2.FromVertex(), vertex2);
    ASSERT_EQ(edge2.ToVertex(), vertex1);

    auto res3 = acc.CreateEdge(&vertex1, &vertex1, 30);
    ASSERT_TRUE(res3.IsReturn());
    auto edge3 = res3.GetReturn();
    ASSERT_EQ(edge3.EdgeType(), 30);
    ASSERT_EQ(edge3.FromVertex(), vertex1);
    ASSERT_EQ(edge3.ToVertex(), vertex1);

    auto res4 = acc.CreateEdge(&vertex2, &vertex2, 40);
    ASSERT_TRUE(res4.IsReturn());
    auto edge4 = res4.GetReturn();
    ASSERT_EQ(edge4.EdgeType(), 40);
    ASSERT_EQ(edge4.FromVertex(), vertex2);
    ASSERT_EQ(edge4.ToVertex(), vertex2);

    // Check edges
    {
      auto ret = vertex1.InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 20);
        ASSERT_EQ(v, vertex2);
        ASSERT_EQ(e.EdgeType(), 20);
        ASSERT_EQ(e.FromVertex(), vertex2);
        ASSERT_EQ(e.ToVertex(), vertex1);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 30);
        ASSERT_EQ(v, vertex1);
        ASSERT_EQ(e.EdgeType(), 30);
        ASSERT_EQ(e.FromVertex(), vertex1);
        ASSERT_EQ(e.ToVertex(), vertex1);
      }
    }
    {
      auto ret = vertex1.OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 10);
        ASSERT_EQ(v, vertex2);
        ASSERT_EQ(e.EdgeType(), 10);
        ASSERT_EQ(e.FromVertex(), vertex1);
        ASSERT_EQ(e.ToVertex(), vertex2);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 30);
        ASSERT_EQ(v, vertex1);
        ASSERT_EQ(e.EdgeType(), 30);
        ASSERT_EQ(e.FromVertex(), vertex1);
        ASSERT_EQ(e.ToVertex(), vertex1);
      }
    }
    {
      auto ret = vertex2.InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 10);
        ASSERT_EQ(v, vertex1);
        ASSERT_EQ(e.EdgeType(), 10);
        ASSERT_EQ(e.FromVertex(), vertex1);
        ASSERT_EQ(e.ToVertex(), vertex2);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 40);
        ASSERT_EQ(v, vertex2);
        ASSERT_EQ(e.EdgeType(), 40);
        ASSERT_EQ(e.FromVertex(), vertex2);
        ASSERT_EQ(e.ToVertex(), vertex2);
      }
    }
    {
      auto ret = vertex2.OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 20);
        ASSERT_EQ(v, vertex1);
        ASSERT_EQ(e.EdgeType(), 20);
        ASSERT_EQ(e.FromVertex(), vertex2);
        ASSERT_EQ(e.ToVertex(), vertex1);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 40);
        ASSERT_EQ(v, vertex2);
        ASSERT_EQ(e.EdgeType(), 40);
        ASSERT_EQ(e.FromVertex(), vertex2);
        ASSERT_EQ(e.ToVertex(), vertex2);
      }
    }

    acc.Commit();
  }

  // Detach delete vertex
  {
    auto acc = store.Access();
    auto vertex1 = acc.FindVertex(gid_vertex1, storage::View::NEW);
    auto vertex2 = acc.FindVertex(gid_vertex2, storage::View::NEW);
    ASSERT_TRUE(vertex1);
    ASSERT_TRUE(vertex2);

    // Delete must fail
    {
      auto ret = acc.DeleteVertex(&*vertex1);
      ASSERT_TRUE(ret.IsError());
      ASSERT_EQ(ret.GetError(), storage::Error::VERTEX_HAS_EDGES);
    }

    // Detach delete vertex
    {
      auto ret = acc.DetachDeleteVertex(&*vertex1);
      ASSERT_TRUE(ret.IsReturn());
      ASSERT_TRUE(ret.GetReturn());
    }

    // Check edges
    {
      auto ret = vertex1->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 20);
        ASSERT_EQ(v, *vertex2);
        ASSERT_EQ(e.EdgeType(), 20);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 30);
        ASSERT_EQ(v, vertex1);
        ASSERT_EQ(e.EdgeType(), 30);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    ASSERT_EQ(vertex1->InEdges({}, storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex1->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 10);
        ASSERT_EQ(v, *vertex2);
        ASSERT_EQ(e.EdgeType(), 10);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 30);
        ASSERT_EQ(v, *vertex1);
        ASSERT_EQ(e.EdgeType(), 30);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    ASSERT_EQ(vertex1->OutEdges({}, storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex2->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 10);
        ASSERT_EQ(v, *vertex1);
        ASSERT_EQ(e.EdgeType(), 10);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 40);
        ASSERT_EQ(v, *vertex2);
        ASSERT_EQ(e.EdgeType(), 40);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 40);
      ASSERT_EQ(v, vertex2);
      ASSERT_EQ(e.EdgeType(), 40);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 20);
        ASSERT_EQ(v, *vertex1);
        ASSERT_EQ(e.EdgeType(), 20);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 40);
        ASSERT_EQ(v, *vertex2);
        ASSERT_EQ(e.EdgeType(), 40);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 40);
      ASSERT_EQ(v, vertex2);
      ASSERT_EQ(e.EdgeType(), 40);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }

    acc.Commit();
  }

  // Check dataset
  {
    auto acc = store.Access();
    auto vertex1 = acc.FindVertex(gid_vertex1, storage::View::NEW);
    auto vertex2 = acc.FindVertex(gid_vertex2, storage::View::NEW);
    ASSERT_FALSE(vertex1);
    ASSERT_TRUE(vertex2);

    // Check edges
    {
      auto ret = vertex2->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 40);
      ASSERT_EQ(v, vertex2);
      ASSERT_EQ(e.EdgeType(), 40);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 40);
      ASSERT_EQ(v, vertex2);
      ASSERT_EQ(e.EdgeType(), 40);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 40);
      ASSERT_EQ(v, vertex2);
      ASSERT_EQ(e.EdgeType(), 40);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 40);
      ASSERT_EQ(v, vertex2);
      ASSERT_EQ(e.EdgeType(), 40);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexDetachDeleteSingleAbort) {
  storage::Storage store;
  storage::Gid gid_from =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid_to =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create dataset
  {
    auto acc = store.Access();
    auto vertex_from = acc.CreateVertex();
    auto vertex_to = acc.CreateVertex();

    auto res = acc.CreateEdge(&vertex_from, &vertex_to, 5);
    ASSERT_TRUE(res.IsReturn());
    auto edge = res.GetReturn();
    ASSERT_EQ(edge.EdgeType(), 5);
    ASSERT_EQ(edge.FromVertex(), vertex_from);
    ASSERT_EQ(edge.ToVertex(), vertex_to);

    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();

    // Check edges
    ASSERT_EQ(vertex_from.InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_from.OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), vertex_from);
      ASSERT_EQ(e.ToVertex(), vertex_to);
    }
    {
      auto ret = vertex_to.InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), vertex_from);
      ASSERT_EQ(e.ToVertex(), vertex_to);
    }
    ASSERT_EQ(vertex_to.OutEdges({}, storage::View::NEW).GetReturn().size(), 0);

    acc.Commit();
  }

  // Detach delete vertex, but abort the transaction
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Delete must fail
    {
      auto ret = acc.DeleteVertex(&*vertex_from);
      ASSERT_TRUE(ret.IsError());
      ASSERT_EQ(ret.GetError(), storage::Error::VERTEX_HAS_EDGES);
    }

    // Detach delete vertex
    {
      auto ret = acc.DetachDeleteVertex(&*vertex_from);
      ASSERT_TRUE(ret.IsReturn());
      ASSERT_TRUE(ret.GetReturn());
    }

    // Check edges
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex_to->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    acc.Abort();
  }

  // Check dataset
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetReturn().size(),
              0);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    acc.Commit();
  }

  // Detach delete vertex
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Delete must fail
    {
      auto ret = acc.DeleteVertex(&*vertex_from);
      ASSERT_TRUE(ret.IsError());
      ASSERT_EQ(ret.GetError(), storage::Error::VERTEX_HAS_EDGES);
    }

    // Detach delete vertex
    {
      auto ret = acc.DetachDeleteVertex(&*vertex_from);
      ASSERT_TRUE(ret.IsReturn());
      ASSERT_TRUE(ret.GetReturn());
    }

    // Check edges
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_from->InEdges({}, storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex_from->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_to);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges({}, storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex_to->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 5);
      ASSERT_EQ(v, *vertex_from);
      ASSERT_EQ(e.EdgeType(), 5);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);

    acc.Commit();
  }

  // Check dataset
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(gid_from, storage::View::NEW);
    auto vertex_to = acc.FindVertex(gid_to, storage::View::NEW);
    ASSERT_FALSE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->InEdges({}, storage::View::NEW).GetReturn().size(), 0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::OLD).GetReturn().size(),
              0);
    ASSERT_EQ(vertex_to->OutEdges({}, storage::View::NEW).GetReturn().size(),
              0);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexDetachDeleteMultipleAbort) {
  storage::Storage store;
  storage::Gid gid_vertex1 =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid_vertex2 =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create dataset
  {
    auto acc = store.Access();
    auto vertex1 = acc.CreateVertex();
    auto vertex2 = acc.CreateVertex();

    gid_vertex1 = vertex1.Gid();
    gid_vertex2 = vertex2.Gid();

    auto res1 = acc.CreateEdge(&vertex1, &vertex2, 10);
    ASSERT_TRUE(res1.IsReturn());
    auto edge1 = res1.GetReturn();
    ASSERT_EQ(edge1.EdgeType(), 10);
    ASSERT_EQ(edge1.FromVertex(), vertex1);
    ASSERT_EQ(edge1.ToVertex(), vertex2);

    auto res2 = acc.CreateEdge(&vertex2, &vertex1, 20);
    ASSERT_TRUE(res2.IsReturn());
    auto edge2 = res2.GetReturn();
    ASSERT_EQ(edge2.EdgeType(), 20);
    ASSERT_EQ(edge2.FromVertex(), vertex2);
    ASSERT_EQ(edge2.ToVertex(), vertex1);

    auto res3 = acc.CreateEdge(&vertex1, &vertex1, 30);
    ASSERT_TRUE(res3.IsReturn());
    auto edge3 = res3.GetReturn();
    ASSERT_EQ(edge3.EdgeType(), 30);
    ASSERT_EQ(edge3.FromVertex(), vertex1);
    ASSERT_EQ(edge3.ToVertex(), vertex1);

    auto res4 = acc.CreateEdge(&vertex2, &vertex2, 40);
    ASSERT_TRUE(res4.IsReturn());
    auto edge4 = res4.GetReturn();
    ASSERT_EQ(edge4.EdgeType(), 40);
    ASSERT_EQ(edge4.FromVertex(), vertex2);
    ASSERT_EQ(edge4.ToVertex(), vertex2);

    // Check edges
    {
      auto ret = vertex1.InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 20);
        ASSERT_EQ(v, vertex2);
        ASSERT_EQ(e.EdgeType(), 20);
        ASSERT_EQ(e.FromVertex(), vertex2);
        ASSERT_EQ(e.ToVertex(), vertex1);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 30);
        ASSERT_EQ(v, vertex1);
        ASSERT_EQ(e.EdgeType(), 30);
        ASSERT_EQ(e.FromVertex(), vertex1);
        ASSERT_EQ(e.ToVertex(), vertex1);
      }
    }
    {
      auto ret = vertex1.OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 10);
        ASSERT_EQ(v, vertex2);
        ASSERT_EQ(e.EdgeType(), 10);
        ASSERT_EQ(e.FromVertex(), vertex1);
        ASSERT_EQ(e.ToVertex(), vertex2);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 30);
        ASSERT_EQ(v, vertex1);
        ASSERT_EQ(e.EdgeType(), 30);
        ASSERT_EQ(e.FromVertex(), vertex1);
        ASSERT_EQ(e.ToVertex(), vertex1);
      }
    }
    {
      auto ret = vertex2.InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 10);
        ASSERT_EQ(v, vertex1);
        ASSERT_EQ(e.EdgeType(), 10);
        ASSERT_EQ(e.FromVertex(), vertex1);
        ASSERT_EQ(e.ToVertex(), vertex2);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 40);
        ASSERT_EQ(v, vertex2);
        ASSERT_EQ(e.EdgeType(), 40);
        ASSERT_EQ(e.FromVertex(), vertex2);
        ASSERT_EQ(e.ToVertex(), vertex2);
      }
    }
    {
      auto ret = vertex2.OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 20);
        ASSERT_EQ(v, vertex1);
        ASSERT_EQ(e.EdgeType(), 20);
        ASSERT_EQ(e.FromVertex(), vertex2);
        ASSERT_EQ(e.ToVertex(), vertex1);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 40);
        ASSERT_EQ(v, vertex2);
        ASSERT_EQ(e.EdgeType(), 40);
        ASSERT_EQ(e.FromVertex(), vertex2);
        ASSERT_EQ(e.ToVertex(), vertex2);
      }
    }

    acc.Commit();
  }

  // Detach delete vertex, but abort the transaction
  {
    auto acc = store.Access();
    auto vertex1 = acc.FindVertex(gid_vertex1, storage::View::NEW);
    auto vertex2 = acc.FindVertex(gid_vertex2, storage::View::NEW);
    ASSERT_TRUE(vertex1);
    ASSERT_TRUE(vertex2);

    // Delete must fail
    {
      auto ret = acc.DeleteVertex(&*vertex1);
      ASSERT_TRUE(ret.IsError());
      ASSERT_EQ(ret.GetError(), storage::Error::VERTEX_HAS_EDGES);
    }

    // Detach delete vertex
    {
      auto ret = acc.DetachDeleteVertex(&*vertex1);
      ASSERT_TRUE(ret.IsReturn());
      ASSERT_TRUE(ret.GetReturn());
    }

    // Check edges
    {
      auto ret = vertex1->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 20);
        ASSERT_EQ(v, *vertex2);
        ASSERT_EQ(e.EdgeType(), 20);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 30);
        ASSERT_EQ(v, vertex1);
        ASSERT_EQ(e.EdgeType(), 30);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    ASSERT_EQ(vertex1->InEdges({}, storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex1->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 10);
        ASSERT_EQ(v, *vertex2);
        ASSERT_EQ(e.EdgeType(), 10);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 30);
        ASSERT_EQ(v, *vertex1);
        ASSERT_EQ(e.EdgeType(), 30);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    ASSERT_EQ(vertex1->OutEdges({}, storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex2->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 10);
        ASSERT_EQ(v, *vertex1);
        ASSERT_EQ(e.EdgeType(), 10);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 40);
        ASSERT_EQ(v, *vertex2);
        ASSERT_EQ(e.EdgeType(), 40);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 40);
      ASSERT_EQ(v, vertex2);
      ASSERT_EQ(e.EdgeType(), 40);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 20);
        ASSERT_EQ(v, *vertex1);
        ASSERT_EQ(e.EdgeType(), 20);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 40);
        ASSERT_EQ(v, *vertex2);
        ASSERT_EQ(e.EdgeType(), 40);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 40);
      ASSERT_EQ(v, vertex2);
      ASSERT_EQ(e.EdgeType(), 40);
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

    // Check edges
    {
      auto ret = vertex1->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 20);
        ASSERT_EQ(v, *vertex2);
        ASSERT_EQ(e.EdgeType(), 20);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 30);
        ASSERT_EQ(v, vertex1);
        ASSERT_EQ(e.EdgeType(), 30);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    {
      auto ret = vertex1->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 20);
        ASSERT_EQ(v, *vertex2);
        ASSERT_EQ(e.EdgeType(), 20);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 30);
        ASSERT_EQ(v, vertex1);
        ASSERT_EQ(e.EdgeType(), 30);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    {
      auto ret = vertex1->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 10);
        ASSERT_EQ(v, *vertex2);
        ASSERT_EQ(e.EdgeType(), 10);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 30);
        ASSERT_EQ(v, *vertex1);
        ASSERT_EQ(e.EdgeType(), 30);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    {
      auto ret = vertex1->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 10);
        ASSERT_EQ(v, *vertex2);
        ASSERT_EQ(e.EdgeType(), 10);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 30);
        ASSERT_EQ(v, *vertex1);
        ASSERT_EQ(e.EdgeType(), 30);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    {
      auto ret = vertex2->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 10);
        ASSERT_EQ(v, *vertex1);
        ASSERT_EQ(e.EdgeType(), 10);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 40);
        ASSERT_EQ(v, *vertex2);
        ASSERT_EQ(e.EdgeType(), 40);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 10);
        ASSERT_EQ(v, *vertex1);
        ASSERT_EQ(e.EdgeType(), 10);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 40);
        ASSERT_EQ(v, *vertex2);
        ASSERT_EQ(e.EdgeType(), 40);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 20);
        ASSERT_EQ(v, *vertex1);
        ASSERT_EQ(e.EdgeType(), 20);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 40);
        ASSERT_EQ(v, *vertex2);
        ASSERT_EQ(e.EdgeType(), 40);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 20);
        ASSERT_EQ(v, *vertex1);
        ASSERT_EQ(e.EdgeType(), 20);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 40);
        ASSERT_EQ(v, *vertex2);
        ASSERT_EQ(e.EdgeType(), 40);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }

    acc.Commit();
  }

  // Detach delete vertex
  {
    auto acc = store.Access();
    auto vertex1 = acc.FindVertex(gid_vertex1, storage::View::NEW);
    auto vertex2 = acc.FindVertex(gid_vertex2, storage::View::NEW);
    ASSERT_TRUE(vertex1);
    ASSERT_TRUE(vertex2);

    // Delete must fail
    {
      auto ret = acc.DeleteVertex(&*vertex1);
      ASSERT_TRUE(ret.IsError());
      ASSERT_EQ(ret.GetError(), storage::Error::VERTEX_HAS_EDGES);
    }

    // Detach delete vertex
    {
      auto ret = acc.DetachDeleteVertex(&*vertex1);
      ASSERT_TRUE(ret.IsReturn());
      ASSERT_TRUE(ret.GetReturn());
    }

    // Check edges
    {
      auto ret = vertex1->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 20);
        ASSERT_EQ(v, *vertex2);
        ASSERT_EQ(e.EdgeType(), 20);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 30);
        ASSERT_EQ(v, vertex1);
        ASSERT_EQ(e.EdgeType(), 30);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    ASSERT_EQ(vertex1->InEdges({}, storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex1->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 10);
        ASSERT_EQ(v, *vertex2);
        ASSERT_EQ(e.EdgeType(), 10);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 30);
        ASSERT_EQ(v, *vertex1);
        ASSERT_EQ(e.EdgeType(), 30);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    ASSERT_EQ(vertex1->OutEdges({}, storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex2->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 10);
        ASSERT_EQ(v, *vertex1);
        ASSERT_EQ(e.EdgeType(), 10);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 40);
        ASSERT_EQ(v, *vertex2);
        ASSERT_EQ(e.EdgeType(), 40);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 40);
      ASSERT_EQ(v, vertex2);
      ASSERT_EQ(e.EdgeType(), 40);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) < std::get<0>(b);
      });
      ASSERT_EQ(edges.size(), 2);
      {
        auto [et, v, e] = edges[0];
        ASSERT_EQ(et, 20);
        ASSERT_EQ(v, *vertex1);
        ASSERT_EQ(e.EdgeType(), 20);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto [et, v, e] = edges[1];
        ASSERT_EQ(et, 40);
        ASSERT_EQ(v, *vertex2);
        ASSERT_EQ(e.EdgeType(), 40);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 40);
      ASSERT_EQ(v, vertex2);
      ASSERT_EQ(e.EdgeType(), 40);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }

    acc.Commit();
  }

  // Check dataset
  {
    auto acc = store.Access();
    auto vertex1 = acc.FindVertex(gid_vertex1, storage::View::NEW);
    auto vertex2 = acc.FindVertex(gid_vertex2, storage::View::NEW);
    ASSERT_FALSE(vertex1);
    ASSERT_TRUE(vertex2);

    // Check edges
    {
      auto ret = vertex2->InEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 40);
      ASSERT_EQ(v, vertex2);
      ASSERT_EQ(e.EdgeType(), 40);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->InEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 40);
      ASSERT_EQ(v, vertex2);
      ASSERT_EQ(e.EdgeType(), 40);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->OutEdges({}, storage::View::OLD);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 40);
      ASSERT_EQ(v, vertex2);
      ASSERT_EQ(e.EdgeType(), 40);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->OutEdges({}, storage::View::NEW);
      ASSERT_TRUE(ret.IsReturn());
      auto edges = ret.GetReturn();
      ASSERT_EQ(edges.size(), 1);
      auto [et, v, e] = edges[0];
      ASSERT_EQ(et, 40);
      ASSERT_EQ(v, vertex2);
      ASSERT_EQ(e.EdgeType(), 40);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, EdgePropertyCommit) {
  storage::Storage store;
  storage::Gid gid =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    auto edge = acc.CreateEdge(&vertex, &vertex, 5).GetReturn();
    ASSERT_EQ(edge.EdgeType(), 5);
    ASSERT_EQ(edge.FromVertex(), vertex);
    ASSERT_EQ(edge.ToVertex(), vertex);

    ASSERT_TRUE(edge.GetProperty(5, storage::View::NEW).GetReturn().IsNull());
    ASSERT_EQ(edge.Properties(storage::View::NEW).GetReturn().size(), 0);

    {
      auto res = edge.SetProperty(5, storage::PropertyValue("temporary"));
      ASSERT_TRUE(res.IsReturn());
      ASSERT_FALSE(res.GetReturn());
    }

    ASSERT_EQ(edge.GetProperty(5, storage::View::NEW).GetReturn().ValueString(),
              "temporary");
    {
      auto properties = edge.Properties(storage::View::NEW).GetReturn();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "temporary");
    }

    {
      auto res = edge.SetProperty(5, storage::PropertyValue("nandare"));
      ASSERT_TRUE(res.IsReturn());
      ASSERT_TRUE(res.GetReturn());
    }

    ASSERT_EQ(edge.GetProperty(5, storage::View::NEW).GetReturn().ValueString(),
              "nandare");
    {
      auto properties = edge.Properties(storage::View::NEW).GetReturn();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    acc.Commit();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto [edge_type, other_vertex, edge] =
        vertex->OutEdges({}, storage::View::NEW).GetReturn()[0];

    ASSERT_EQ(edge.GetProperty(5, storage::View::OLD).GetReturn().ValueString(),
              "nandare");
    {
      auto properties = edge.Properties(storage::View::OLD).GetReturn();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    ASSERT_EQ(edge.GetProperty(5, storage::View::NEW).GetReturn().ValueString(),
              "nandare");
    {
      auto properties = edge.Properties(storage::View::NEW).GetReturn();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    ASSERT_TRUE(edge.GetProperty(10, storage::View::OLD).GetReturn().IsNull());
    ASSERT_TRUE(edge.GetProperty(10, storage::View::NEW).GetReturn().IsNull());

    acc.Abort();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto [edge_type, other_vertex, edge] =
        vertex->OutEdges({}, storage::View::NEW).GetReturn()[0];

    {
      auto res = edge.SetProperty(5, storage::PropertyValue());
      ASSERT_TRUE(res.IsReturn());
      ASSERT_TRUE(res.GetReturn());
    }

    ASSERT_EQ(edge.GetProperty(5, storage::View::OLD).GetReturn().ValueString(),
              "nandare");
    {
      auto properties = edge.Properties(storage::View::OLD).GetReturn();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    ASSERT_TRUE(edge.GetProperty(5, storage::View::NEW).GetReturn().IsNull());
    ASSERT_EQ(edge.Properties(storage::View::NEW).GetReturn().size(), 0);

    {
      auto res = edge.SetProperty(5, storage::PropertyValue());
      ASSERT_TRUE(res.IsReturn());
      ASSERT_FALSE(res.GetReturn());
    }

    acc.Commit();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto [edge_type, other_vertex, edge] =
        vertex->OutEdges({}, storage::View::NEW).GetReturn()[0];

    ASSERT_TRUE(edge.GetProperty(5, storage::View::OLD).GetReturn().IsNull());
    ASSERT_TRUE(edge.GetProperty(5, storage::View::NEW).GetReturn().IsNull());
    ASSERT_EQ(edge.Properties(storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(edge.Properties(storage::View::NEW).GetReturn().size(), 0);

    ASSERT_TRUE(edge.GetProperty(10, storage::View::OLD).GetReturn().IsNull());
    ASSERT_TRUE(edge.GetProperty(10, storage::View::NEW).GetReturn().IsNull());

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, EdgePropertyAbort) {
  storage::Storage store;
  storage::Gid gid =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create the vertex.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    auto edge = acc.CreateEdge(&vertex, &vertex, 5).GetReturn();
    ASSERT_EQ(edge.EdgeType(), 5);
    ASSERT_EQ(edge.FromVertex(), vertex);
    ASSERT_EQ(edge.ToVertex(), vertex);
    acc.Commit();
  }

  // Set property 5 to "nandare", but abort the transaction.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto [edge_type, other_vertex, edge] =
        vertex->OutEdges({}, storage::View::NEW).GetReturn()[0];

    ASSERT_TRUE(edge.GetProperty(5, storage::View::NEW).GetReturn().IsNull());
    ASSERT_EQ(edge.Properties(storage::View::NEW).GetReturn().size(), 0);

    {
      auto res = edge.SetProperty(5, storage::PropertyValue("temporary"));
      ASSERT_TRUE(res.IsReturn());
      ASSERT_FALSE(res.GetReturn());
    }

    ASSERT_EQ(edge.GetProperty(5, storage::View::NEW).GetReturn().ValueString(),
              "temporary");
    {
      auto properties = edge.Properties(storage::View::NEW).GetReturn();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "temporary");
    }

    {
      auto res = edge.SetProperty(5, storage::PropertyValue("nandare"));
      ASSERT_TRUE(res.IsReturn());
      ASSERT_TRUE(res.GetReturn());
    }

    ASSERT_EQ(edge.GetProperty(5, storage::View::NEW).GetReturn().ValueString(),
              "nandare");
    {
      auto properties = edge.Properties(storage::View::NEW).GetReturn();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    acc.Abort();
  }

  // Check that property 5 is null.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto [edge_type, other_vertex, edge] =
        vertex->OutEdges({}, storage::View::NEW).GetReturn()[0];

    ASSERT_TRUE(edge.GetProperty(5, storage::View::OLD).GetReturn().IsNull());
    ASSERT_TRUE(edge.GetProperty(5, storage::View::NEW).GetReturn().IsNull());
    ASSERT_EQ(edge.Properties(storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(edge.Properties(storage::View::NEW).GetReturn().size(), 0);

    ASSERT_TRUE(edge.GetProperty(10, storage::View::OLD).GetReturn().IsNull());
    ASSERT_TRUE(edge.GetProperty(10, storage::View::NEW).GetReturn().IsNull());

    acc.Abort();
  }

  // Set property 5 to "nandare".
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto [edge_type, other_vertex, edge] =
        vertex->OutEdges({}, storage::View::NEW).GetReturn()[0];

    ASSERT_TRUE(edge.GetProperty(5, storage::View::NEW).GetReturn().IsNull());
    ASSERT_EQ(edge.Properties(storage::View::NEW).GetReturn().size(), 0);

    {
      auto res = edge.SetProperty(5, storage::PropertyValue("temporary"));
      ASSERT_TRUE(res.IsReturn());
      ASSERT_FALSE(res.GetReturn());
    }

    ASSERT_EQ(edge.GetProperty(5, storage::View::NEW).GetReturn().ValueString(),
              "temporary");
    {
      auto properties = edge.Properties(storage::View::NEW).GetReturn();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "temporary");
    }

    {
      auto res = edge.SetProperty(5, storage::PropertyValue("nandare"));
      ASSERT_TRUE(res.IsReturn());
      ASSERT_TRUE(res.GetReturn());
    }

    ASSERT_EQ(edge.GetProperty(5, storage::View::NEW).GetReturn().ValueString(),
              "nandare");
    {
      auto properties = edge.Properties(storage::View::NEW).GetReturn();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    acc.Commit();
  }

  // Check that property 5 is "nandare".
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto [edge_type, other_vertex, edge] =
        vertex->OutEdges({}, storage::View::NEW).GetReturn()[0];

    ASSERT_EQ(edge.GetProperty(5, storage::View::OLD).GetReturn().ValueString(),
              "nandare");
    {
      auto properties = edge.Properties(storage::View::OLD).GetReturn();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    ASSERT_EQ(edge.GetProperty(5, storage::View::NEW).GetReturn().ValueString(),
              "nandare");
    {
      auto properties = edge.Properties(storage::View::NEW).GetReturn();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    ASSERT_TRUE(edge.GetProperty(10, storage::View::OLD).GetReturn().IsNull());
    ASSERT_TRUE(edge.GetProperty(10, storage::View::NEW).GetReturn().IsNull());

    acc.Abort();
  }

  // Set property 5 to null, but abort the transaction.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto [edge_type, other_vertex, edge] =
        vertex->OutEdges({}, storage::View::NEW).GetReturn()[0];

    ASSERT_EQ(edge.GetProperty(5, storage::View::OLD).GetReturn().ValueString(),
              "nandare");
    {
      auto properties = edge.Properties(storage::View::OLD).GetReturn();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    ASSERT_EQ(edge.GetProperty(5, storage::View::NEW).GetReturn().ValueString(),
              "nandare");
    {
      auto properties = edge.Properties(storage::View::NEW).GetReturn();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    {
      auto res = edge.SetProperty(5, storage::PropertyValue());
      ASSERT_TRUE(res.IsReturn());
      ASSERT_TRUE(res.GetReturn());
    }

    ASSERT_EQ(edge.GetProperty(5, storage::View::OLD).GetReturn().ValueString(),
              "nandare");
    {
      auto properties = edge.Properties(storage::View::OLD).GetReturn();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    ASSERT_TRUE(edge.GetProperty(5, storage::View::NEW).GetReturn().IsNull());
    ASSERT_EQ(edge.Properties(storage::View::NEW).GetReturn().size(), 0);

    acc.Abort();
  }

  // Check that property 5 is "nandare".
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto [edge_type, other_vertex, edge] =
        vertex->OutEdges({}, storage::View::NEW).GetReturn()[0];

    ASSERT_EQ(edge.GetProperty(5, storage::View::OLD).GetReturn().ValueString(),
              "nandare");
    {
      auto properties = edge.Properties(storage::View::OLD).GetReturn();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    ASSERT_EQ(edge.GetProperty(5, storage::View::NEW).GetReturn().ValueString(),
              "nandare");
    {
      auto properties = edge.Properties(storage::View::NEW).GetReturn();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    ASSERT_TRUE(edge.GetProperty(10, storage::View::OLD).GetReturn().IsNull());
    ASSERT_TRUE(edge.GetProperty(10, storage::View::NEW).GetReturn().IsNull());

    acc.Abort();
  }

  // Set property 5 to null.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto [edge_type, other_vertex, edge] =
        vertex->OutEdges({}, storage::View::NEW).GetReturn()[0];

    ASSERT_EQ(edge.GetProperty(5, storage::View::OLD).GetReturn().ValueString(),
              "nandare");
    {
      auto properties = edge.Properties(storage::View::OLD).GetReturn();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    ASSERT_EQ(edge.GetProperty(5, storage::View::NEW).GetReturn().ValueString(),
              "nandare");
    {
      auto properties = edge.Properties(storage::View::NEW).GetReturn();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    {
      auto res = edge.SetProperty(5, storage::PropertyValue());
      ASSERT_TRUE(res.IsReturn());
      ASSERT_TRUE(res.GetReturn());
    }

    ASSERT_EQ(edge.GetProperty(5, storage::View::OLD).GetReturn().ValueString(),
              "nandare");
    {
      auto properties = edge.Properties(storage::View::OLD).GetReturn();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    ASSERT_TRUE(edge.GetProperty(5, storage::View::NEW).GetReturn().IsNull());
    ASSERT_EQ(edge.Properties(storage::View::NEW).GetReturn().size(), 0);

    acc.Commit();
  }

  // Check that property 5 is null.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto [edge_type, other_vertex, edge] =
        vertex->OutEdges({}, storage::View::NEW).GetReturn()[0];

    ASSERT_TRUE(edge.GetProperty(5, storage::View::OLD).GetReturn().IsNull());
    ASSERT_TRUE(edge.GetProperty(5, storage::View::NEW).GetReturn().IsNull());
    ASSERT_EQ(edge.Properties(storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(edge.Properties(storage::View::NEW).GetReturn().size(), 0);

    ASSERT_TRUE(edge.GetProperty(10, storage::View::OLD).GetReturn().IsNull());
    ASSERT_TRUE(edge.GetProperty(10, storage::View::NEW).GetReturn().IsNull());

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, EdgePropertySerializationError) {
  storage::Storage store;
  storage::Gid gid =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    auto edge = acc.CreateEdge(&vertex, &vertex, 5).GetReturn();
    ASSERT_EQ(edge.EdgeType(), 5);
    ASSERT_EQ(edge.FromVertex(), vertex);
    ASSERT_EQ(edge.ToVertex(), vertex);
    acc.Commit();
  }

  auto acc1 = store.Access();
  auto acc2 = store.Access();

  // Set property 1 to 123 in accessor 1.
  {
    auto vertex = acc1.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto [edge_type, other_vertex, edge] =
        vertex->OutEdges({}, storage::View::NEW).GetReturn()[0];

    ASSERT_TRUE(edge.GetProperty(1, storage::View::OLD).GetReturn().IsNull());
    ASSERT_TRUE(edge.GetProperty(1, storage::View::NEW).GetReturn().IsNull());
    ASSERT_TRUE(edge.GetProperty(2, storage::View::OLD).GetReturn().IsNull());
    ASSERT_TRUE(edge.GetProperty(2, storage::View::NEW).GetReturn().IsNull());
    ASSERT_EQ(edge.Properties(storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(edge.Properties(storage::View::NEW).GetReturn().size(), 0);

    {
      auto res = edge.SetProperty(1, storage::PropertyValue(123));
      ASSERT_TRUE(res.IsReturn());
      ASSERT_FALSE(res.GetReturn());
    }

    ASSERT_TRUE(edge.GetProperty(1, storage::View::OLD).GetReturn().IsNull());
    ASSERT_EQ(edge.GetProperty(1, storage::View::NEW).GetReturn().ValueInt(),
              123);
    ASSERT_TRUE(edge.GetProperty(2, storage::View::OLD).GetReturn().IsNull());
    ASSERT_TRUE(edge.GetProperty(2, storage::View::NEW).GetReturn().IsNull());
    ASSERT_EQ(edge.Properties(storage::View::OLD).GetReturn().size(), 0);
    {
      auto properties = edge.Properties(storage::View::NEW).GetReturn();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[1].ValueInt(), 123);
    }
  }

  // Set property 2 to "nandare" in accessor 2.
  {
    auto vertex = acc2.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto [edge_type, other_vertex, edge] =
        vertex->OutEdges({}, storage::View::NEW).GetReturn()[0];

    ASSERT_TRUE(edge.GetProperty(1, storage::View::OLD).GetReturn().IsNull());
    ASSERT_TRUE(edge.GetProperty(1, storage::View::NEW).GetReturn().IsNull());
    ASSERT_TRUE(edge.GetProperty(2, storage::View::OLD).GetReturn().IsNull());
    ASSERT_TRUE(edge.GetProperty(2, storage::View::NEW).GetReturn().IsNull());
    ASSERT_EQ(edge.Properties(storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(edge.Properties(storage::View::NEW).GetReturn().size(), 0);

    {
      auto res = edge.SetProperty(2, storage::PropertyValue("nandare"));
      ASSERT_TRUE(res.IsError());
      ASSERT_EQ(res.GetError(), storage::Error::SERIALIZATION_ERROR);
    }
  }

  // Finalize both accessors.
  acc1.Commit();
  acc2.Abort();

  // Check which properties exist.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto [edge_type, other_vertex, edge] =
        vertex->OutEdges({}, storage::View::NEW).GetReturn()[0];

    ASSERT_EQ(edge.GetProperty(1, storage::View::OLD).GetReturn().ValueInt(),
              123);
    ASSERT_TRUE(edge.GetProperty(2, storage::View::OLD).GetReturn().IsNull());
    {
      auto properties = edge.Properties(storage::View::OLD).GetReturn();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[1].ValueInt(), 123);
    }

    ASSERT_EQ(edge.GetProperty(1, storage::View::NEW).GetReturn().ValueInt(),
              123);
    ASSERT_TRUE(edge.GetProperty(2, storage::View::NEW).GetReturn().IsNull());
    {
      auto properties = edge.Properties(storage::View::NEW).GetReturn();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[1].ValueInt(), 123);
    }

    acc.Abort();
  }
}
