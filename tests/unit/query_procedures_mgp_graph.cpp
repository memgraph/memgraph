#include <algorithm>
#include <iterator>
#include <list>
#include <memory>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "mg_procedure.h"
#include "query/db_accessor.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "storage_test_utils.hpp"
#include "utils/memory.hpp"

#define EXPECT_SUCCESS(...) EXPECT_NE(__VA_ARGS__, 0)
#define EXPECT_FAILURE(...) EXPECT_EQ(__VA_ARGS__, 0)
namespace {
struct MgpEdgeDeleter {
  void operator()(mgp_edge *e) {
    if (e != nullptr) {
      mgp_edge_destroy(e);
    }
  }
};

struct MgpEdgesIteratorDeleter {
  void operator()(mgp_edges_iterator *it) {
    if (it != nullptr) {
      mgp_edges_iterator_destroy(it);
    }
  }
};

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

struct MgpValueDeleter {
  void operator()(mgp_value *v) {
    if (v != nullptr) {
      mgp_value_destroy(v);
    }
  }
};

using MgpEdgePtr = std::unique_ptr<mgp_edge, MgpEdgeDeleter>;
using MgpEdgesIteratorPtr = std::unique_ptr<mgp_edges_iterator, MgpEdgesIteratorDeleter>;
using MgpVertexPtr = std::unique_ptr<mgp_vertex, MgpVertexDeleter>;
using MgpVerticesIteratorPtr = std::unique_ptr<mgp_vertices_iterator, MgpVerticesIteratorDeleter>;
using MgpValuePtr = std::unique_ptr<mgp_value, MgpValueDeleter>;

template <typename TMaybeIterable>
size_t CountMaybeIterables(TMaybeIterable &&maybe_iterable) {
  if (maybe_iterable.HasError()) {
    ADD_FAILURE() << static_cast<std::underlying_type_t<typename TMaybeIterable::ErrorType>>(maybe_iterable.GetError());
    return 0;
  }
  auto &iterable = maybe_iterable.GetValue();
  return std::distance(iterable.begin(), iterable.end());
}

void CheckEdgeCountBetween(const MgpVertexPtr &from, const MgpVertexPtr &to, const size_t number_of_edges_between) {
  EXPECT_EQ(CountMaybeIterables(from->impl.InEdges(storage::View::NEW)), 0);
  EXPECT_EQ(CountMaybeIterables(from->impl.OutEdges(storage::View::NEW)), number_of_edges_between);
  EXPECT_EQ(CountMaybeIterables(to->impl.InEdges(storage::View::NEW)), number_of_edges_between);
  EXPECT_EQ(CountMaybeIterables(to->impl.OutEdges(storage::View::NEW)), 0);
}
}  // namespace

struct MgpGraphTest : public ::testing::Test {
  mgp_graph CreateGraph(const storage::View view = storage::View::NEW) {
    // the execution context can be null as it shouldn't be used in these tests
    return mgp_graph{&CreateDbAccessor(storage::IsolationLevel::SNAPSHOT_ISOLATION), view, nullptr};
  }

  std::array<storage::Gid, 2> CreateEdge() {
    std::array<storage::Gid, 2> vertex_ids{};
    auto accessor = CreateDbAccessor(storage::IsolationLevel::SNAPSHOT_ISOLATION);
    for (auto i = 0; i < 2; ++i) {
      vertex_ids[i] = accessor.InsertVertex().Gid();
    }
    auto from = accessor.FindVertex(vertex_ids[0], storage::View::NEW);
    auto to = accessor.FindVertex(vertex_ids[1], storage::View::NEW);
    EXPECT_TRUE(accessor.InsertEdge(&from.value(), &to.value(), accessor.NameToEdgeType("EDGE")).HasValue());

    EXPECT_FALSE(accessor.Commit().HasError());

    return vertex_ids;
  }

  void GetFirstOutEdge(mgp_graph &graph, storage::Gid vertex_id, MgpEdgePtr &edge) {
    MgpVertexPtr from{mgp_graph_get_vertex_by_id(&graph, mgp_vertex_id{vertex_id.AsInt()}, &memory)};
    ASSERT_NE(from, nullptr);

    MgpEdgesIteratorPtr it{mgp_vertex_iter_out_edges(from.get(), &memory)};
    ASSERT_NE(it, nullptr);
    const auto *edge_from_it = mgp_edges_iterator_get(it.get());
    ASSERT_NE(edge_from_it, nullptr);
    // Copy is used to get a non const pointer because mgp_edges_iterator_get_mutable doesn't work with immutable graph
    edge.reset(mgp_edge_copy(edge_from_it, &memory));
    ASSERT_NE(edge, nullptr);
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
  EXPECT_SUCCESS(mgp_graph_remove_vertex(&graph, vertex.get()));
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
  EXPECT_FAILURE(mgp_graph_remove_vertex(&immutable_graph, vertex_to_remove.get()));
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
      EXPECT_TRUE(mgp_vertices_iterator_underlying_graph_is_mutable(vertices_iter.get()));
      EXPECT_NE(mgp_vertices_iterator_get_mutable(vertices_iter.get()), nullptr);
    } else {
      EXPECT_FALSE(mgp_vertices_iterator_underlying_graph_is_mutable(vertices_iter.get()));
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

TEST_F(MgpGraphTest, VertexIsMutable) {
  auto graph = CreateGraph(storage::View::NEW);
  MgpVertexPtr vertex{mgp_graph_create_vertex(&graph, &memory)};
  ASSERT_NE(vertex.get(), nullptr);
  EXPECT_NE(mgp_vertex_underlying_graph_is_mutable(vertex.get()), 0);
  graph.view = storage::View::OLD;
  EXPECT_EQ(mgp_vertex_underlying_graph_is_mutable(vertex.get()), 0);
}

TEST_F(MgpGraphTest, VertexSetProperty) {
  constexpr std::string_view property_to_update{"to_update"};
  constexpr std::string_view property_to_set{"to_set"};
  storage::Gid vertex_id{};
  {
    auto accessor = CreateDbAccessor(storage::IsolationLevel::SNAPSHOT_ISOLATION);
    auto vertex = accessor.InsertVertex();
    vertex_id = vertex.Gid();
    const auto result = vertex.SetProperty(accessor.NameToProperty(property_to_update), storage::PropertyValue(42));
    ASSERT_TRUE(result.HasValue());
    ASSERT_FALSE(accessor.Commit().HasError());
  }
  auto read_uncommited_accessor = storage.Access(storage::IsolationLevel::READ_UNCOMMITTED);
  EXPECT_EQ(CountVertices(read_uncommited_accessor, storage::View::NEW), 1);

  mgp_graph graph = CreateGraph(storage::View::NEW);
  MgpVertexPtr vertex{mgp_graph_get_vertex_by_id(&graph, mgp_vertex_id{vertex_id.AsInt()}, &memory)};
  ASSERT_NE(vertex, nullptr);

  auto vertex_acc = read_uncommited_accessor.FindVertex(vertex_id, storage::View::NEW);
  ASSERT_TRUE(vertex_acc.has_value());
  const auto property_id_to_update = read_uncommited_accessor.NameToProperty(property_to_update);

  {
    SCOPED_TRACE("Update the property");
    constexpr int64_t numerical_value_to_update_to{69};
    MgpValuePtr value_to_update_to{mgp_value_make_int(numerical_value_to_update_to, &memory)};
    ASSERT_NE(value_to_update_to, nullptr);
    EXPECT_SUCCESS(mgp_vertex_set_property(vertex.get(), property_to_update.data(), value_to_update_to.get()));

    const auto maybe_prop = vertex_acc->GetProperty(property_id_to_update, storage::View::NEW);
    ASSERT_TRUE(maybe_prop.HasValue());
    EXPECT_EQ(*maybe_prop, storage::PropertyValue{numerical_value_to_update_to});
  }
  {
    SCOPED_TRACE("Remove the property");
    MgpValuePtr null_value{mgp_value_make_null(&memory)};
    ASSERT_NE(null_value, nullptr);
    EXPECT_SUCCESS(mgp_vertex_set_property(vertex.get(), property_to_update.data(), null_value.get()));

    const auto maybe_prop = vertex_acc->GetProperty(property_id_to_update, storage::View::NEW);
    ASSERT_TRUE(maybe_prop.HasValue());
    EXPECT_EQ(*maybe_prop, storage::PropertyValue{});
  }
  {
    SCOPED_TRACE("Add a property");
    constexpr double numerical_value_to_set{3.5};
    MgpValuePtr value_to_set{mgp_value_make_double(numerical_value_to_set, &memory)};
    ASSERT_NE(value_to_set, nullptr);
    EXPECT_SUCCESS(mgp_vertex_set_property(vertex.get(), property_to_set.data(), value_to_set.get()));
    const auto maybe_prop =
        vertex_acc->GetProperty(read_uncommited_accessor.NameToProperty(property_to_set), storage::View::NEW);
    ASSERT_TRUE(maybe_prop.HasValue());
    EXPECT_EQ(*maybe_prop, storage::PropertyValue{numerical_value_to_set});
  }
}

TEST_F(MgpGraphTest, VertexAddLabel) {
  constexpr std::string_view label = "test_label";
  storage::Gid vertex_id{};
  {
    auto accessor = CreateDbAccessor(storage::IsolationLevel::SNAPSHOT_ISOLATION);
    const auto vertex = accessor.InsertVertex();
    vertex_id = vertex.Gid();
    ASSERT_FALSE(accessor.Commit().HasError());
  }

  mgp_graph graph = CreateGraph(storage::View::NEW);
  MgpVertexPtr vertex{mgp_graph_get_vertex_by_id(&graph, mgp_vertex_id{vertex_id.AsInt()}, &memory)};
  EXPECT_SUCCESS(mgp_vertex_add_label(vertex.get(), mgp_label{label.data()}));

  auto check_label = [&]() {
    EXPECT_NE(mgp_vertex_has_label_named(vertex.get(), label.data()), 0);

    auto read_uncommited_accessor = storage.Access(storage::IsolationLevel::READ_UNCOMMITTED);
    const auto maybe_vertex = read_uncommited_accessor.FindVertex(vertex_id, storage::View::NEW);
    ASSERT_TRUE(maybe_vertex.has_value());
    const auto label_ids = maybe_vertex->Labels(storage::View::NEW);
    ASSERT_TRUE(label_ids.HasValue());
    EXPECT_THAT(*label_ids, ::testing::ContainerEq(std::vector{read_uncommited_accessor.NameToLabel(label)}));
  };
  ASSERT_NO_FATAL_FAILURE(check_label());
  EXPECT_SUCCESS(mgp_vertex_add_label(vertex.get(), mgp_label{label.data()}));
  ASSERT_NO_FATAL_FAILURE(check_label());
}

TEST_F(MgpGraphTest, VertexRemoveLabel) {
  constexpr std::string_view label = "test_label";
  storage::Gid vertex_id{};
  {
    auto accessor = CreateDbAccessor(storage::IsolationLevel::SNAPSHOT_ISOLATION);
    auto vertex = accessor.InsertVertex();
    const auto result = vertex.AddLabel(accessor.NameToLabel(label));
    ASSERT_TRUE(result.HasValue());
    ASSERT_TRUE(*result);
    vertex_id = vertex.Gid();
    ASSERT_FALSE(accessor.Commit().HasError());
  }

  mgp_graph graph = CreateGraph(storage::View::NEW);
  MgpVertexPtr vertex{mgp_graph_get_vertex_by_id(&graph, mgp_vertex_id{vertex_id.AsInt()}, &memory)};
  EXPECT_SUCCESS(mgp_vertex_remove_label(vertex.get(), mgp_label{label.data()}));

  auto check_label = [&]() {
    EXPECT_EQ(mgp_vertex_has_label_named(vertex.get(), label.data()), 0);

    auto read_uncommited_accessor = storage.Access(storage::IsolationLevel::READ_UNCOMMITTED);
    const auto maybe_vertex = read_uncommited_accessor.FindVertex(vertex_id, storage::View::NEW);
    ASSERT_TRUE(maybe_vertex.has_value());
    const auto label_ids = maybe_vertex->Labels(storage::View::NEW);
    ASSERT_TRUE(label_ids.HasValue());
    EXPECT_EQ(label_ids->size(), 0);
  };
  ASSERT_NO_FATAL_FAILURE(check_label());
  EXPECT_SUCCESS(mgp_vertex_remove_label(vertex.get(), mgp_label{label.data()}));
  ASSERT_NO_FATAL_FAILURE(check_label());
}

TEST_F(MgpGraphTest, ModifyImmutableVertex) {
  constexpr std::string_view label_to_remove{"label_to_remove"};
  storage::Gid vertex_id{};
  {
    auto accessor = CreateDbAccessor(storage::IsolationLevel::SNAPSHOT_ISOLATION);
    auto vertex = accessor.InsertVertex();
    vertex_id = vertex.Gid();
    ASSERT_TRUE(vertex.AddLabel(accessor.NameToLabel(label_to_remove)).HasValue());
    ASSERT_FALSE(accessor.Commit().HasError());
  }
  const auto graph = CreateGraph(storage::View::OLD);
  MgpVertexPtr vertex{mgp_graph_get_vertex_by_id(&graph, mgp_vertex_id{vertex_id.AsInt()}, &memory)};
  EXPECT_EQ(mgp_vertex_underlying_graph_is_mutable(vertex.get()), 0);

  EXPECT_FAILURE(mgp_vertex_add_label(vertex.get(), mgp_label{"label"}));
  EXPECT_FAILURE(mgp_vertex_remove_label(vertex.get(), mgp_label{label_to_remove.data()}));
  MgpValuePtr value{mgp_value_make_int(4, &memory)};
  EXPECT_FAILURE(mgp_vertex_set_property(vertex.get(), "property", value.get()));
}
TEST_F(MgpGraphTest, CreateRemoveEdge) {
  std::array<storage::Gid, 2> vertex_ids{};
  {
    auto accessor = CreateDbAccessor(storage::IsolationLevel::SNAPSHOT_ISOLATION);
    for (auto i = 0; i < 2; ++i) {
      vertex_ids[i] = accessor.InsertVertex().Gid();
    }
    ASSERT_FALSE(accessor.Commit().HasError());
  }
  auto graph = CreateGraph();
  MgpVertexPtr from{mgp_graph_get_vertex_by_id(&graph, mgp_vertex_id{vertex_ids[0].AsInt()}, &memory)};
  MgpVertexPtr to{mgp_graph_get_vertex_by_id(&graph, mgp_vertex_id{vertex_ids[1].AsInt()}, &memory)};
  ASSERT_NE(from, nullptr);
  ASSERT_NE(to, nullptr);
  CheckEdgeCountBetween(from, to, 0);
  MgpEdgePtr edge{mgp_graph_create_edge(&graph, from.get(), to.get(), mgp_edge_type{"EDGE"}, &memory)};
  CheckEdgeCountBetween(from, to, 1);
  ASSERT_NE(edge, nullptr);
  EXPECT_SUCCESS(mgp_graph_remove_edge(&graph, edge.get()));
  CheckEdgeCountBetween(from, to, 0);
}

TEST_F(MgpGraphTest, CreateRemoveEdgeWithImmutableGraph) {
  storage::Gid from_id;
  storage::Gid to_id;
  {
    auto accessor = CreateDbAccessor(storage::IsolationLevel::SNAPSHOT_ISOLATION);
    auto from = accessor.InsertVertex();
    auto to = accessor.InsertVertex();
    from_id = from.Gid();
    to_id = to.Gid();
    ASSERT_TRUE(accessor.InsertEdge(&from, &to, accessor.NameToEdgeType("EDGE_TYPE_TO_REMOVE")).HasValue());
    ASSERT_FALSE(accessor.Commit().HasError());
  }
  auto graph = CreateGraph(storage::View::OLD);
  MgpVertexPtr from{mgp_graph_get_vertex_by_id(&graph, mgp_vertex_id{from_id.AsInt()}, &memory)};
  MgpVertexPtr to{mgp_graph_get_vertex_by_id(&graph, mgp_vertex_id{to_id.AsInt()}, &memory)};
  ASSERT_NE(from, nullptr);
  ASSERT_NE(to, nullptr);
  CheckEdgeCountBetween(from, to, 1);
  MgpEdgePtr edge{
      mgp_graph_create_edge(&graph, from.get(), to.get(), mgp_edge_type{"NEWLY_CREATED_EDGE_TYPE"}, &memory)};
  EXPECT_EQ(edge, nullptr);
  CheckEdgeCountBetween(from, to, 1);

  MgpEdgesIteratorPtr edges_it(mgp_vertex_iter_out_edges(from.get(), &memory));
  const auto *edge_from_it = mgp_edges_iterator_get(edges_it.get());
  ASSERT_NE(edge_from_it, nullptr);
  MgpEdgePtr non_const_edge{mgp_edge_copy(edge_from_it, &memory)};
  EXPECT_FAILURE(mgp_graph_remove_edge(&graph, non_const_edge.get()));
  CheckEdgeCountBetween(from, to, 1);
}

TEST_F(MgpGraphTest, EdgeIsMutable) {
  const auto vertex_ids = CreateEdge();
  auto graph = CreateGraph();
  MgpEdgePtr edge{};
  ASSERT_NO_FATAL_FAILURE(GetFirstOutEdge(graph, vertex_ids[0], edge));
  EXPECT_NE(mgp_edge_underlying_graph_is_mutable(edge.get()), 0);
  graph.view = storage::View::OLD;
  EXPECT_EQ(mgp_edge_underlying_graph_is_mutable(edge.get()), 0);
}

TEST_F(MgpGraphTest, MutableFromTo) {
  storage::Gid from_vertex_id{};
  {
    const auto vertex_ids = CreateEdge();
    from_vertex_id = vertex_ids[0];
  }
  auto check_edges_iterator = [this, from_vertex_id](const storage::View view) {
    mgp_graph graph = CreateGraph(view);

    MgpVertexPtr from{mgp_graph_get_vertex_by_id(&graph, mgp_vertex_id{from_vertex_id.AsInt()}, &memory)};
    ASSERT_NE(from, nullptr);

    MgpEdgePtr edge{};
    ASSERT_NO_FATAL_FAILURE(GetFirstOutEdge(graph, from_vertex_id, edge));
    if (view == storage::View::NEW) {
      ASSERT_NE(mgp_edge_underlying_graph_is_mutable(edge.get()), 0);
      auto *from = mgp_edge_get_mutable_from(edge.get());
      ASSERT_NE(from, nullptr);
      EXPECT_NE(mgp_vertex_underlying_graph_is_mutable(from), 0);
      auto *to = mgp_edge_get_mutable_from(edge.get());
      ASSERT_NE(to, nullptr);
      EXPECT_NE(mgp_vertex_underlying_graph_is_mutable(to), 0);
    } else {
      ASSERT_EQ(mgp_edge_underlying_graph_is_mutable(edge.get()), 0);
      EXPECT_EQ(mgp_edge_get_mutable_from(edge.get()), nullptr);
      EXPECT_EQ(mgp_edge_get_mutable_to(edge.get()), nullptr);
    }
  };
  {
    SCOPED_TRACE("View::OLD");
    check_edges_iterator(storage::View::OLD);
  }
  {
    SCOPED_TRACE("View::NEW");
    check_edges_iterator(storage::View::NEW);
  }
}

TEST_F(MgpGraphTest, EdgesIterator) {
  storage::Gid from_vertex_id{};
  {
    const auto vertex_ids = CreateEdge();
    from_vertex_id = vertex_ids[0];
  }
  auto check_edges_iterator = [this, from_vertex_id](const storage::View view) {
    mgp_graph graph = CreateGraph(view);

    MgpVertexPtr from{mgp_graph_get_vertex_by_id(&graph, mgp_vertex_id{from_vertex_id.AsInt()}, &memory)};
    ASSERT_NE(from, nullptr);
    MgpEdgesIteratorPtr iter{mgp_vertex_iter_out_edges(from.get(), &memory)};
    if (view == storage::View::NEW) {
      EXPECT_TRUE(mgp_edges_iterator_underlying_graph_is_mutable(iter.get()));
      ASSERT_NE(mgp_edges_iterator_get_mutable(iter.get()), nullptr);
    } else {
      EXPECT_FALSE(mgp_edges_iterator_underlying_graph_is_mutable(iter.get()));
      EXPECT_EQ(mgp_edges_iterator_get_mutable(iter.get()), nullptr);
    }
  };
  {
    SCOPED_TRACE("View::OLD");
    check_edges_iterator(storage::View::OLD);
  }
  {
    SCOPED_TRACE("View::NEW");
    check_edges_iterator(storage::View::NEW);
  }
}

TEST_F(MgpGraphTest, EdgeSetProperty) {
  constexpr std::string_view property_to_update{"to_update"};
  constexpr std::string_view property_to_set{"to_set"};

  storage::Gid from_vertex_id{};
  auto get_edge = [&from_vertex_id](storage::Storage::Accessor &accessor) -> storage::EdgeAccessor {
    auto from = accessor.FindVertex(from_vertex_id, storage::View::NEW);
    return from->OutEdges(storage::View::NEW).GetValue().front();
  };
  {
    const auto vertex_ids = CreateEdge();
    from_vertex_id = vertex_ids[0];
    auto accessor = storage.Access(storage::IsolationLevel::SNAPSHOT_ISOLATION);
    auto edge = get_edge(accessor);
    const auto result = edge.SetProperty(accessor.NameToProperty(property_to_update), storage::PropertyValue(42));
    ASSERT_TRUE(result.HasValue());
    ASSERT_FALSE(accessor.Commit().HasError());
  }
  auto read_uncommited_accessor = storage.Access(storage::IsolationLevel::READ_UNCOMMITTED);

  mgp_graph graph = CreateGraph(storage::View::NEW);
  MgpEdgePtr edge;
  ASSERT_NO_FATAL_FAILURE(GetFirstOutEdge(graph, from_vertex_id, edge));
  const auto edge_acc = get_edge(read_uncommited_accessor);

  const auto property_id_to_update = read_uncommited_accessor.NameToProperty(property_to_update);

  {
    SCOPED_TRACE("Update the property");
    constexpr int64_t numerical_value_to_update_to{69};
    MgpValuePtr value_to_update_to{mgp_value_make_int(numerical_value_to_update_to, &memory)};
    ASSERT_NE(value_to_update_to, nullptr);
    EXPECT_SUCCESS(mgp_edge_set_property(edge.get(), property_to_update.data(), value_to_update_to.get()));

    const auto maybe_prop = edge_acc.GetProperty(property_id_to_update, storage::View::NEW);
    ASSERT_TRUE(maybe_prop.HasValue());
    EXPECT_EQ(*maybe_prop, storage::PropertyValue{numerical_value_to_update_to});
  }
  {
    SCOPED_TRACE("Remove the property");
    MgpValuePtr null_value{mgp_value_make_null(&memory)};
    ASSERT_NE(null_value, nullptr);
    EXPECT_SUCCESS(mgp_edge_set_property(edge.get(), property_to_update.data(), null_value.get()));

    const auto maybe_prop = edge_acc.GetProperty(property_id_to_update, storage::View::NEW);
    ASSERT_TRUE(maybe_prop.HasValue());
    EXPECT_EQ(*maybe_prop, storage::PropertyValue{});
  }
  {
    SCOPED_TRACE("Add a property");
    constexpr double numerical_value_to_set{3.5};
    MgpValuePtr value_to_set{mgp_value_make_double(numerical_value_to_set, &memory)};
    ASSERT_NE(value_to_set, nullptr);
    EXPECT_SUCCESS(mgp_edge_set_property(edge.get(), property_to_set.data(), value_to_set.get()));
    const auto maybe_prop =
        edge_acc.GetProperty(read_uncommited_accessor.NameToProperty(property_to_set), storage::View::NEW);
    ASSERT_TRUE(maybe_prop.HasValue());
    EXPECT_EQ(*maybe_prop, storage::PropertyValue{numerical_value_to_set});
  }
}

TEST_F(MgpGraphTest, EdgeSetPropertyWithImmutableGraph) {
  storage::Gid from_vertex_id{};
  {
    const auto vertex_ids = CreateEdge();
    from_vertex_id = vertex_ids[0];
  }
  auto graph = CreateGraph(storage::View::OLD);
  MgpEdgePtr edge;
  ASSERT_NO_FATAL_FAILURE(GetFirstOutEdge(graph, from_vertex_id, edge));
  MgpValuePtr value{mgp_value_make_int(65, &memory)};
  ASSERT_NE(value, nullptr);
  EXPECT_EQ(mgp_edge_underlying_graph_is_mutable(edge.get()), 0);
  EXPECT_FAILURE(mgp_edge_set_property(edge.get(), "property", value.get()));
}
