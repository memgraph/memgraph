// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <algorithm>
#include <iterator>
#include <list>
#include <memory>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "disk_test_utils.hpp"
#include "mg_procedure.h"
#include "query/db_accessor.hpp"
#include "query/plan/operator.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "storage_test_utils.hpp"
#include "test_utils.hpp"
#include "utils/memory.hpp"
#include "utils/variant_helpers.hpp"

#define EXPECT_SUCCESS(...) EXPECT_EQ(__VA_ARGS__, mgp_error::MGP_ERROR_NO_ERROR)

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

template <typename TMaybeIterable, typename TIterableAccessor>
size_t CountMaybeIterables(TMaybeIterable &&maybe_iterable, TIterableAccessor func) {
  if (maybe_iterable.HasError()) {
    ADD_FAILURE() << static_cast<std::underlying_type_t<typename TMaybeIterable::ErrorType>>(maybe_iterable.GetError());
    return 0;
  }
  auto iterable = func(maybe_iterable.GetValue());
  return std::distance(iterable.begin(), iterable.end());
}

;

void CheckEdgeCountBetween(const MgpVertexPtr &from, const MgpVertexPtr &to, const size_t number_of_edges_between) {
  EXPECT_EQ(
      CountMaybeIterables(std::visit([](auto impl) { return impl.InEdges(memgraph::storage::View::NEW); }, from->impl),
                          [](const auto &edge_result) { return edge_result.edges; }),
      0);
  EXPECT_EQ(
      CountMaybeIterables(std::visit([](auto impl) { return impl.OutEdges(memgraph::storage::View::NEW); }, from->impl),
                          [](const auto &edge_result) { return edge_result.edges; }),
      number_of_edges_between);
  EXPECT_EQ(
      CountMaybeIterables(std::visit([](auto impl) { return impl.InEdges(memgraph::storage::View::NEW); }, to->impl),
                          [](const auto &edge_result) { return edge_result.edges; }),
      number_of_edges_between);
  EXPECT_EQ(
      CountMaybeIterables(std::visit([](auto impl) { return impl.OutEdges(memgraph::storage::View::NEW); }, to->impl),
                          [](const auto &edge_result) { return edge_result.edges; }),
      0);
}
}  // namespace

template <typename StorageType>
class MgpGraphTest : public ::testing::Test {
 public:
  mgp_graph CreateGraph(const memgraph::storage::View view = memgraph::storage::View::NEW) {
    // the execution context can be null as it shouldn't be used in these tests
    return mgp_graph{&CreateDbAccessor(memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION), view, ctx_.get()};
  }

  std::array<memgraph::storage::Gid, 2> CreateEdge() {
    std::array<memgraph::storage::Gid, 2> vertex_ids{};
    auto accessor = CreateDbAccessor(memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION);
    for (auto i = 0; i < 2; ++i) {
      vertex_ids[i] = accessor.InsertVertex().Gid();
    }
    auto from = accessor.FindVertex(vertex_ids[0], memgraph::storage::View::NEW);
    auto to = accessor.FindVertex(vertex_ids[1], memgraph::storage::View::NEW);
    EXPECT_TRUE(accessor.InsertEdge(&from.value(), &to.value(), accessor.NameToEdgeType("EDGE")).HasValue());

    EXPECT_FALSE(accessor.Commit().HasError());

    return vertex_ids;
  }

  void GetFirstOutEdge(mgp_graph &graph, memgraph::storage::Gid vertex_id, MgpEdgePtr &edge) {
    MgpVertexPtr from{EXPECT_MGP_NO_ERROR(mgp_vertex *, mgp_graph_get_vertex_by_id, &graph,
                                          mgp_vertex_id{vertex_id.AsInt()}, &this->memory)};
    ASSERT_NE(from, nullptr);

    MgpEdgesIteratorPtr it{
        EXPECT_MGP_NO_ERROR(mgp_edges_iterator *, mgp_vertex_iter_out_edges, from.get(), &this->memory)};
    ASSERT_NE(it, nullptr);
    auto *edge_from_it = EXPECT_MGP_NO_ERROR(mgp_edge *, mgp_edges_iterator_get, it.get());
    ASSERT_NE(edge_from_it, nullptr);
    // Copy is used to get a non const pointer because mgp_edges_iterator_get_mutable doesn't work with immutable graph
    edge.reset(EXPECT_MGP_NO_ERROR(mgp_edge *, mgp_edge_copy, edge_from_it, &this->memory));
    ASSERT_NE(edge, nullptr);
  }

  memgraph::query::DbAccessor &CreateDbAccessor(const memgraph::storage::IsolationLevel isolationLevel) {
    accessors_.push_back(storage->Access(isolationLevel));
    db_accessors_.emplace_back(accessors_.back().get());
    return db_accessors_.back();
  }

  void TearDown() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
  }

  const std::string testSuite = "query_procedures_mgp_graph";

  memgraph::storage::Config config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  std::unique_ptr<memgraph::storage::Storage> storage{new StorageType(config)};
  mgp_memory memory{memgraph::utils::NewDeleteResource()};

 private:
  std::list<std::unique_ptr<memgraph::storage::Storage::Accessor>> accessors_;
  std::list<memgraph::query::DbAccessor> db_accessors_;
  std::unique_ptr<memgraph::query::ExecutionContext> ctx_ = std::make_unique<memgraph::query::ExecutionContext>();
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_CASE(MgpGraphTest, StorageTypes);

TYPED_TEST(MgpGraphTest, IsMutable) {
  mgp_graph immutable_graph = this->CreateGraph(memgraph::storage::View::OLD);
  EXPECT_EQ(EXPECT_MGP_NO_ERROR(int, mgp_graph_is_mutable, &immutable_graph), 0);
  mgp_graph mutable_graph = this->CreateGraph(memgraph::storage::View::NEW);
  EXPECT_NE(EXPECT_MGP_NO_ERROR(int, mgp_graph_is_mutable, &mutable_graph), 0);
}

TYPED_TEST(MgpGraphTest, CreateVertex) {
  if (std::is_same<TypeParam, memgraph::storage::DiskStorage>::value) {
    // DiskStorage doesn't support READ_UNCOMMITTED isolation level
    return;
  }
  mgp_graph graph = this->CreateGraph();
  auto read_uncommited_accessor = this->storage->Access(memgraph::storage::IsolationLevel::READ_UNCOMMITTED);
  EXPECT_EQ(CountVertices(*read_uncommited_accessor, memgraph::storage::View::NEW), 0);
  MgpVertexPtr vertex{EXPECT_MGP_NO_ERROR(mgp_vertex *, mgp_graph_create_vertex, &graph, &this->memory)};
  EXPECT_NE(vertex, nullptr);
  EXPECT_EQ(CountVertices(*read_uncommited_accessor, memgraph::storage::View::NEW), 1);
  const auto vertex_id = EXPECT_MGP_NO_ERROR(mgp_vertex_id, mgp_vertex_get_id, vertex.get());
  EXPECT_TRUE(read_uncommited_accessor->FindVertex(memgraph::storage::Gid::FromInt(vertex_id.as_int),
                                                   memgraph::storage::View::NEW));
}

TYPED_TEST(MgpGraphTest, DeleteVertex) {
  if (std::is_same<TypeParam, memgraph::storage::DiskStorage>::value) {
    // DiskStorage doesn't support READ_UNCOMMITTED isolation level
    return;
  }
  memgraph::storage::Gid vertex_id{};
  {
    auto accessor = this->CreateDbAccessor(memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION);
    const auto vertex = accessor.InsertVertex();
    vertex_id = vertex.Gid();
    ASSERT_FALSE(accessor.Commit().HasError());
  }
  mgp_graph graph = this->CreateGraph();
  auto read_uncommited_accessor = this->storage->Access(memgraph::storage::IsolationLevel::READ_UNCOMMITTED);
  EXPECT_EQ(CountVertices(*read_uncommited_accessor, memgraph::storage::View::NEW), 1);
  MgpVertexPtr vertex{EXPECT_MGP_NO_ERROR(mgp_vertex *, mgp_graph_get_vertex_by_id, &graph,
                                          mgp_vertex_id{vertex_id.AsInt()}, &this->memory)};
  EXPECT_NE(vertex, nullptr);
  EXPECT_SUCCESS(mgp_graph_delete_vertex(&graph, vertex.get()));
  EXPECT_EQ(CountVertices(*read_uncommited_accessor, memgraph::storage::View::NEW), 0);
}

TYPED_TEST(MgpGraphTest, DetachDeleteVertex) {
  if (std::is_same<TypeParam, memgraph::storage::DiskStorage>::value) {
    // DiskStorage doesn't support READ_UNCOMMITTED isolation level
    return;
  }
  const auto vertex_ids = this->CreateEdge();
  auto graph = this->CreateGraph();
  auto read_uncommited_accessor = this->storage->Access(memgraph::storage::IsolationLevel::READ_UNCOMMITTED);
  EXPECT_EQ(CountVertices(*read_uncommited_accessor, memgraph::storage::View::NEW), 2);
  MgpVertexPtr vertex{EXPECT_MGP_NO_ERROR(mgp_vertex *, mgp_graph_get_vertex_by_id, &graph,
                                          mgp_vertex_id{vertex_ids.front().AsInt()}, &this->memory)};
  EXPECT_EQ(mgp_graph_delete_vertex(&graph, vertex.get()), mgp_error::MGP_ERROR_LOGIC_ERROR);
  EXPECT_EQ(CountVertices(*read_uncommited_accessor, memgraph::storage::View::NEW), 2);
  EXPECT_SUCCESS(mgp_graph_detach_delete_vertex(&graph, vertex.get()));
  EXPECT_EQ(CountVertices(*read_uncommited_accessor, memgraph::storage::View::NEW), 1);
}

TYPED_TEST(MgpGraphTest, CreateDeleteWithImmutableGraph) {
  if (std::is_same<TypeParam, memgraph::storage::DiskStorage>::value) {
    // DiskStorage doesn't support READ_UNCOMMITTED isolation level
    return;
  }
  memgraph::storage::Gid vertex_id{};
  {
    auto accessor = this->CreateDbAccessor(memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION);
    const auto vertex = accessor.InsertVertex();
    vertex_id = vertex.Gid();
    ASSERT_FALSE(accessor.Commit().HasError());
  }
  auto read_uncommited_accessor = this->storage->Access(memgraph::storage::IsolationLevel::READ_UNCOMMITTED);
  EXPECT_EQ(CountVertices(*read_uncommited_accessor, memgraph::storage::View::NEW), 1);

  mgp_graph immutable_graph = this->CreateGraph(memgraph::storage::View::OLD);
  mgp_vertex *raw_vertex{nullptr};
  EXPECT_EQ(mgp_graph_create_vertex(&immutable_graph, &this->memory, &raw_vertex),
            mgp_error::MGP_ERROR_IMMUTABLE_OBJECT);
  MgpVertexPtr created_vertex{raw_vertex};
  EXPECT_EQ(created_vertex, nullptr);
  EXPECT_EQ(CountVertices(*read_uncommited_accessor, memgraph::storage::View::NEW), 1);
  MgpVertexPtr vertex_to_delete{EXPECT_MGP_NO_ERROR(mgp_vertex *, mgp_graph_get_vertex_by_id, &immutable_graph,
                                                    mgp_vertex_id{vertex_id.AsInt()}, &this->memory)};
  ASSERT_NE(vertex_to_delete, nullptr);
  EXPECT_EQ(mgp_graph_delete_vertex(&immutable_graph, vertex_to_delete.get()), mgp_error::MGP_ERROR_IMMUTABLE_OBJECT);
  EXPECT_EQ(CountVertices(*read_uncommited_accessor, memgraph::storage::View::NEW), 1);
}

TYPED_TEST(MgpGraphTest, VerticesIterator) {
  {
    auto accessor = this->CreateDbAccessor(memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION);
    accessor.InsertVertex();
    ASSERT_FALSE(accessor.Commit().HasError());
  }
  auto check_vertices_iterator = [this](const memgraph::storage::View view) {
    mgp_graph graph = this->CreateGraph(view);
    MgpVerticesIteratorPtr vertices_iter{
        EXPECT_MGP_NO_ERROR(mgp_vertices_iterator *, mgp_graph_iter_vertices, &graph, &this->memory)};
    ASSERT_NE(vertices_iter, nullptr);
    EXPECT_MGP_NO_ERROR(mgp_vertex *, mgp_vertices_iterator_get, vertices_iter.get());
    if (view == memgraph::storage::View::NEW) {
      EXPECT_NE(EXPECT_MGP_NO_ERROR(int, mgp_vertices_iterator_underlying_graph_is_mutable, vertices_iter.get()), 0);
    } else {
      EXPECT_EQ(EXPECT_MGP_NO_ERROR(int, mgp_vertices_iterator_underlying_graph_is_mutable, vertices_iter.get()), 0);
    }
  };
  {
    SCOPED_TRACE("View::OLD");
    check_vertices_iterator(memgraph::storage::View::OLD);
  }
  {
    SCOPED_TRACE("View::NEW");
    check_vertices_iterator(memgraph::storage::View::NEW);
  }
}

TYPED_TEST(MgpGraphTest, VertexIsMutable) {
  auto graph = this->CreateGraph(memgraph::storage::View::NEW);
  MgpVertexPtr vertex{EXPECT_MGP_NO_ERROR(mgp_vertex *, mgp_graph_create_vertex, &graph, &this->memory)};
  ASSERT_NE(vertex.get(), nullptr);
  EXPECT_NE(EXPECT_MGP_NO_ERROR(int, mgp_vertex_underlying_graph_is_mutable, vertex.get()), 0);
  graph.view = memgraph::storage::View::OLD;
  EXPECT_EQ(EXPECT_MGP_NO_ERROR(int, mgp_vertex_underlying_graph_is_mutable, vertex.get()), 0);
}

TYPED_TEST(MgpGraphTest, VertexSetProperty) {
  if (std::is_same<TypeParam, memgraph::storage::DiskStorage>::value) {
    // DiskStorage doesn't support READ_UNCOMMITTED isolation level
    return;
  }
  static constexpr std::string_view property_to_update{"to_update"};
  static constexpr std::string_view property_to_set{"to_set"};
  memgraph::storage::Gid vertex_id{};
  {
    auto accessor = this->CreateDbAccessor(memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION);
    auto vertex = accessor.InsertVertex();
    vertex_id = vertex.Gid();
    const auto result =
        vertex.SetProperty(accessor.NameToProperty(property_to_update), memgraph::storage::PropertyValue(42));
    ASSERT_TRUE(result.HasValue());
    ASSERT_FALSE(accessor.Commit().HasError());
  }
  auto read_uncommited_accessor = this->storage->Access(memgraph::storage::IsolationLevel::READ_UNCOMMITTED);
  EXPECT_EQ(CountVertices(*read_uncommited_accessor, memgraph::storage::View::NEW), 1);

  mgp_graph graph = this->CreateGraph(memgraph::storage::View::NEW);
  MgpVertexPtr vertex{EXPECT_MGP_NO_ERROR(mgp_vertex *, mgp_graph_get_vertex_by_id, &graph,
                                          mgp_vertex_id{vertex_id.AsInt()}, &this->memory)};
  ASSERT_NE(vertex, nullptr);

  auto vertex_acc = read_uncommited_accessor->FindVertex(vertex_id, memgraph::storage::View::NEW);
  ASSERT_TRUE(vertex_acc);
  const auto property_id_to_update = read_uncommited_accessor->NameToProperty(property_to_update);

  {
    SCOPED_TRACE("Update the property");
    static constexpr int64_t numerical_value_to_update_to{69};
    MgpValuePtr value_to_update_to{
        EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_int, numerical_value_to_update_to, &this->memory)};
    ASSERT_NE(value_to_update_to, nullptr);
    EXPECT_SUCCESS(mgp_vertex_set_property(vertex.get(), property_to_update.data(), value_to_update_to.get()));

    const auto maybe_prop = vertex_acc->GetProperty(property_id_to_update, memgraph::storage::View::NEW);
    ASSERT_TRUE(maybe_prop.HasValue());
    EXPECT_EQ(*maybe_prop, memgraph::storage::PropertyValue{numerical_value_to_update_to});
  }
  {
    SCOPED_TRACE("Remove the property");
    MgpValuePtr null_value{EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_null, &this->memory)};
    ASSERT_NE(null_value, nullptr);
    EXPECT_SUCCESS(mgp_vertex_set_property(vertex.get(), property_to_update.data(), null_value.get()));

    const auto maybe_prop = vertex_acc->GetProperty(property_id_to_update, memgraph::storage::View::NEW);
    ASSERT_TRUE(maybe_prop.HasValue());
    EXPECT_EQ(*maybe_prop, memgraph::storage::PropertyValue{});
  }
  {
    SCOPED_TRACE("Add a property");
    static constexpr double numerical_value_to_set{3.5};
    MgpValuePtr value_to_set{
        EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_double, numerical_value_to_set, &this->memory)};
    ASSERT_NE(value_to_set, nullptr);
    EXPECT_SUCCESS(mgp_vertex_set_property(vertex.get(), property_to_set.data(), value_to_set.get()));
    const auto maybe_prop = vertex_acc->GetProperty(read_uncommited_accessor->NameToProperty(property_to_set),
                                                    memgraph::storage::View::NEW);
    ASSERT_TRUE(maybe_prop.HasValue());
    EXPECT_EQ(*maybe_prop, memgraph::storage::PropertyValue{numerical_value_to_set});
  }
}

TYPED_TEST(MgpGraphTest, VertexAddLabel) {
  if (std::is_same<TypeParam, memgraph::storage::DiskStorage>::value) {
    // DiskStorage doesn't support READ_UNCOMMITTED isolation level
    return;
  }
  static constexpr std::string_view label = "test_label";
  memgraph::storage::Gid vertex_id{};
  {
    auto accessor = this->CreateDbAccessor(memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION);
    const auto vertex = accessor.InsertVertex();
    vertex_id = vertex.Gid();
    ASSERT_FALSE(accessor.Commit().HasError());
  }

  mgp_graph graph = this->CreateGraph(memgraph::storage::View::NEW);
  MgpVertexPtr vertex{EXPECT_MGP_NO_ERROR(mgp_vertex *, mgp_graph_get_vertex_by_id, &graph,
                                          mgp_vertex_id{vertex_id.AsInt()}, &this->memory)};
  EXPECT_SUCCESS(mgp_vertex_add_label(vertex.get(), mgp_label{label.data()}));

  auto check_label = [&]() {
    EXPECT_NE(EXPECT_MGP_NO_ERROR(int, mgp_vertex_has_label_named, vertex.get(), label.data()), 0);

    auto read_uncommited_accessor = this->storage->Access(memgraph::storage::IsolationLevel::READ_UNCOMMITTED);
    const auto maybe_vertex = read_uncommited_accessor->FindVertex(vertex_id, memgraph::storage::View::NEW);
    ASSERT_TRUE(maybe_vertex);
    const auto label_ids = maybe_vertex->Labels(memgraph::storage::View::NEW);
    ASSERT_TRUE(label_ids.HasValue());
    EXPECT_THAT(*label_ids, ::testing::ContainerEq(std::vector{read_uncommited_accessor->NameToLabel(label)}));
  };
  ASSERT_NO_FATAL_FAILURE(check_label());
  EXPECT_SUCCESS(mgp_vertex_add_label(vertex.get(), mgp_label{label.data()}));
  ASSERT_NO_FATAL_FAILURE(check_label());
}

TYPED_TEST(MgpGraphTest, VertexRemoveLabel) {
  if (std::is_same<TypeParam, memgraph::storage::DiskStorage>::value) {
    // DiskStorage doesn't support READ_UNCOMMITTED isolation level
    return;
  }
  static constexpr std::string_view label = "test_label";
  memgraph::storage::Gid vertex_id{};
  {
    auto accessor = this->CreateDbAccessor(memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION);
    auto vertex = accessor.InsertVertex();
    const auto result = vertex.AddLabel(accessor.NameToLabel(label));
    ASSERT_TRUE(result.HasValue());
    ASSERT_TRUE(*result);
    vertex_id = vertex.Gid();
    ASSERT_FALSE(accessor.Commit().HasError());
  }

  mgp_graph graph = this->CreateGraph(memgraph::storage::View::NEW);
  MgpVertexPtr vertex{EXPECT_MGP_NO_ERROR(mgp_vertex *, mgp_graph_get_vertex_by_id, &graph,
                                          mgp_vertex_id{vertex_id.AsInt()}, &this->memory)};
  EXPECT_SUCCESS(mgp_vertex_remove_label(vertex.get(), mgp_label{label.data()}));

  auto check_label = [&]() {
    EXPECT_EQ(EXPECT_MGP_NO_ERROR(int, mgp_vertex_has_label_named, vertex.get(), label.data()), 0);

    auto read_uncommited_accessor = this->storage->Access(memgraph::storage::IsolationLevel::READ_UNCOMMITTED);
    const auto maybe_vertex = read_uncommited_accessor->FindVertex(vertex_id, memgraph::storage::View::NEW);
    ASSERT_TRUE(maybe_vertex);
    const auto label_ids = maybe_vertex->Labels(memgraph::storage::View::NEW);
    ASSERT_TRUE(label_ids.HasValue());
    EXPECT_EQ(label_ids->size(), 0);
  };
  ASSERT_NO_FATAL_FAILURE(check_label());
  EXPECT_SUCCESS(mgp_vertex_remove_label(vertex.get(), mgp_label{label.data()}));
  ASSERT_NO_FATAL_FAILURE(check_label());
}

TYPED_TEST(MgpGraphTest, ModifyImmutableVertex) {
  static constexpr std::string_view label_to_remove{"label_to_remove"};
  memgraph::storage::Gid vertex_id{};
  {
    auto accessor = this->CreateDbAccessor(memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION);
    auto vertex = accessor.InsertVertex();
    vertex_id = vertex.Gid();
    ASSERT_TRUE(vertex.AddLabel(accessor.NameToLabel(label_to_remove)).HasValue());
    ASSERT_FALSE(accessor.Commit().HasError());
  }
  auto graph = this->CreateGraph(memgraph::storage::View::OLD);
  MgpVertexPtr vertex{EXPECT_MGP_NO_ERROR(mgp_vertex *, mgp_graph_get_vertex_by_id, &graph,
                                          mgp_vertex_id{vertex_id.AsInt()}, &this->memory)};
  EXPECT_EQ(EXPECT_MGP_NO_ERROR(int, mgp_vertex_underlying_graph_is_mutable, vertex.get()), 0);

  EXPECT_EQ(mgp_vertex_add_label(vertex.get(), mgp_label{"label"}), mgp_error::MGP_ERROR_IMMUTABLE_OBJECT);
  EXPECT_EQ(mgp_vertex_remove_label(vertex.get(), mgp_label{label_to_remove.data()}),
            mgp_error::MGP_ERROR_IMMUTABLE_OBJECT);
  MgpValuePtr value{EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_int, 4, &this->memory)};
  EXPECT_EQ(mgp_vertex_set_property(vertex.get(), "property", value.get()), mgp_error::MGP_ERROR_IMMUTABLE_OBJECT);
}

TYPED_TEST(MgpGraphTest, CreateDeleteEdge) {
  std::array<memgraph::storage::Gid, 2> vertex_ids{};
  {
    auto accessor = this->CreateDbAccessor(memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION);
    for (auto i = 0; i < 2; ++i) {
      vertex_ids[i] = accessor.InsertVertex().Gid();
    }
    ASSERT_FALSE(accessor.Commit().HasError());
  }
  auto graph = this->CreateGraph();
  MgpVertexPtr from{EXPECT_MGP_NO_ERROR(mgp_vertex *, mgp_graph_get_vertex_by_id, &graph,
                                        mgp_vertex_id{vertex_ids[0].AsInt()}, &this->memory)};
  MgpVertexPtr to{EXPECT_MGP_NO_ERROR(mgp_vertex *, mgp_graph_get_vertex_by_id, &graph,
                                      mgp_vertex_id{vertex_ids[1].AsInt()}, &this->memory)};
  ASSERT_NE(from, nullptr);
  ASSERT_NE(to, nullptr);
  CheckEdgeCountBetween(from, to, 0);
  MgpEdgePtr edge{EXPECT_MGP_NO_ERROR(mgp_edge *, mgp_graph_create_edge, &graph, from.get(), to.get(),
                                      mgp_edge_type{"EDGE"}, &this->memory)};
  CheckEdgeCountBetween(from, to, 1);
  ASSERT_NE(edge, nullptr);
  EXPECT_SUCCESS(mgp_graph_delete_edge(&graph, edge.get()));
  CheckEdgeCountBetween(from, to, 0);
}

TYPED_TEST(MgpGraphTest, CreateDeleteEdgeWithImmutableGraph) {
  memgraph::storage::Gid from_id;
  memgraph::storage::Gid to_id;
  {
    auto accessor = this->CreateDbAccessor(memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION);
    auto from = accessor.InsertVertex();
    auto to = accessor.InsertVertex();
    from_id = from.Gid();
    to_id = to.Gid();
    ASSERT_TRUE(accessor.InsertEdge(&from, &to, accessor.NameToEdgeType("EDGE_TYPE_TO_REMOVE")).HasValue());
    ASSERT_FALSE(accessor.Commit().HasError());
  }
  auto graph = this->CreateGraph(memgraph::storage::View::OLD);
  MgpVertexPtr from{EXPECT_MGP_NO_ERROR(mgp_vertex *, mgp_graph_get_vertex_by_id, &graph,
                                        mgp_vertex_id{from_id.AsInt()}, &this->memory)};
  MgpVertexPtr to{EXPECT_MGP_NO_ERROR(mgp_vertex *, mgp_graph_get_vertex_by_id, &graph, mgp_vertex_id{to_id.AsInt()},
                                      &this->memory)};
  ASSERT_NE(from, nullptr);
  ASSERT_NE(to, nullptr);
  CheckEdgeCountBetween(from, to, 1);
  mgp_edge *edge{nullptr};
  EXPECT_EQ(mgp_graph_create_edge(&graph, from.get(), to.get(), mgp_edge_type{"NEWLY_CREATED_EDGE_TYPE"}, &this->memory,
                                  &edge),
            mgp_error::MGP_ERROR_IMMUTABLE_OBJECT);
  CheckEdgeCountBetween(from, to, 1);

  MgpEdgesIteratorPtr edges_it{
      EXPECT_MGP_NO_ERROR(mgp_edges_iterator *, mgp_vertex_iter_out_edges, from.get(), &this->memory)};
  auto *edge_from_it = EXPECT_MGP_NO_ERROR(mgp_edge *, mgp_edges_iterator_get, edges_it.get());
  ASSERT_NE(edge_from_it, nullptr);
  EXPECT_EQ(mgp_graph_delete_edge(&graph, edge_from_it), mgp_error::MGP_ERROR_IMMUTABLE_OBJECT);
  MgpEdgePtr edge_copy_of_immutable{EXPECT_MGP_NO_ERROR(mgp_edge *, mgp_edge_copy, edge_from_it, &this->memory)};
  EXPECT_EQ(mgp_graph_delete_edge(&graph, edge_copy_of_immutable.get()), mgp_error::MGP_ERROR_IMMUTABLE_OBJECT);
  CheckEdgeCountBetween(from, to, 1);
}

TYPED_TEST(MgpGraphTest, EdgeIsMutable) {
  const auto vertex_ids = this->CreateEdge();
  auto graph = this->CreateGraph();
  MgpEdgePtr edge{};
  ASSERT_NO_FATAL_FAILURE(this->GetFirstOutEdge(graph, vertex_ids[0], edge));
  EXPECT_NE(EXPECT_MGP_NO_ERROR(int, mgp_edge_underlying_graph_is_mutable, edge.get()), 0);
  graph.view = memgraph::storage::View::OLD;
  EXPECT_EQ(EXPECT_MGP_NO_ERROR(int, mgp_edge_underlying_graph_is_mutable, edge.get()), 0);
}

TYPED_TEST(MgpGraphTest, MutableFromTo) {
  memgraph::storage::Gid from_vertex_id{};
  {
    const auto vertex_ids = this->CreateEdge();
    from_vertex_id = vertex_ids[0];
  }
  auto check_edges_iterator = [this, from_vertex_id](const memgraph::storage::View view) {
    mgp_graph graph = this->CreateGraph(view);

    MgpEdgePtr edge{};
    ASSERT_NO_FATAL_FAILURE(this->GetFirstOutEdge(graph, from_vertex_id, edge));
    auto *from = EXPECT_MGP_NO_ERROR(mgp_vertex *, mgp_edge_get_from, edge.get());
    auto *to = EXPECT_MGP_NO_ERROR(mgp_vertex *, mgp_edge_get_from, edge.get());
    auto check_is_mutable = [&edge, from, to](bool is_mutable) {
      EXPECT_EQ(EXPECT_MGP_NO_ERROR(int, mgp_edge_underlying_graph_is_mutable, edge.get()) != 0, is_mutable);
      EXPECT_EQ(EXPECT_MGP_NO_ERROR(int, mgp_vertex_underlying_graph_is_mutable, from) != 0, is_mutable);
      EXPECT_EQ(EXPECT_MGP_NO_ERROR(int, mgp_vertex_underlying_graph_is_mutable, to) != 0, is_mutable);
    };
    if (view == memgraph::storage::View::NEW) {
      check_is_mutable(true);
    } else {
      check_is_mutable(false);
    }
  };
  {
    SCOPED_TRACE("View::OLD");
    check_edges_iterator(memgraph::storage::View::OLD);
  }
  {
    SCOPED_TRACE("View::NEW");
    check_edges_iterator(memgraph::storage::View::NEW);
  }
}

TYPED_TEST(MgpGraphTest, EdgesIterator) {
  memgraph::storage::Gid from_vertex_id{};
  {
    const auto vertex_ids = this->CreateEdge();
    from_vertex_id = vertex_ids[0];
  }
  auto check_edges_iterator = [this, from_vertex_id](const memgraph::storage::View view) {
    mgp_graph graph = this->CreateGraph(view);

    MgpVertexPtr from{EXPECT_MGP_NO_ERROR(mgp_vertex *, mgp_graph_get_vertex_by_id, &graph,
                                          mgp_vertex_id{from_vertex_id.AsInt()}, &this->memory)};
    MgpEdgesIteratorPtr iter{
        EXPECT_MGP_NO_ERROR(mgp_edges_iterator *, mgp_vertex_iter_out_edges, from.get(), &this->memory)};
    auto *edge = EXPECT_MGP_NO_ERROR(mgp_edge *, mgp_edges_iterator_get, iter.get());
    auto check_is_mutable = [&edge, &iter](bool is_mutable) {
      EXPECT_EQ(EXPECT_MGP_NO_ERROR(int, mgp_edges_iterator_underlying_graph_is_mutable, iter.get()) != 0, is_mutable);
      EXPECT_EQ(EXPECT_MGP_NO_ERROR(int, mgp_edge_underlying_graph_is_mutable, edge) != 0, is_mutable);
    };
    if (view == memgraph::storage::View::NEW) {
      check_is_mutable(true);
    } else {
      check_is_mutable(false);
    }
  };
  {
    SCOPED_TRACE("View::OLD");
    check_edges_iterator(memgraph::storage::View::OLD);
  }
  {
    SCOPED_TRACE("View::NEW");
    check_edges_iterator(memgraph::storage::View::NEW);
  }
}

TYPED_TEST(MgpGraphTest, EdgeSetProperty) {
  if (std::is_same<TypeParam, memgraph::storage::DiskStorage>::value) {
    // DiskStorage doesn't support READ_UNCOMMITTED isolation level
    return;
  }
  static constexpr std::string_view property_to_update{"to_update"};
  static constexpr std::string_view property_to_set{"to_set"};

  memgraph::storage::Gid from_vertex_id{};
  auto get_edge = [&from_vertex_id](memgraph::storage::Storage::Accessor *accessor) -> memgraph::storage::EdgeAccessor {
    auto from = accessor->FindVertex(from_vertex_id, memgraph::storage::View::NEW);
    return std::move(from->OutEdges(memgraph::storage::View::NEW).GetValue().edges.front());
  };
  {
    const auto vertex_ids = this->CreateEdge();
    from_vertex_id = vertex_ids[0];
    auto accessor = this->storage->Access(memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION);
    auto edge = get_edge(accessor.get());
    const auto result =
        edge.SetProperty(accessor->NameToProperty(property_to_update), memgraph::storage::PropertyValue(42));
    ASSERT_TRUE(result.HasValue());
    ASSERT_FALSE(accessor->Commit().HasError());
  }
  auto read_uncommited_accessor = this->storage->Access(memgraph::storage::IsolationLevel::READ_UNCOMMITTED);

  mgp_graph graph = this->CreateGraph(memgraph::storage::View::NEW);
  MgpEdgePtr edge;
  ASSERT_NO_FATAL_FAILURE(this->GetFirstOutEdge(graph, from_vertex_id, edge));
  const auto edge_acc = get_edge(read_uncommited_accessor.get());

  const auto property_id_to_update = read_uncommited_accessor->NameToProperty(property_to_update);

  {
    SCOPED_TRACE("Update the property");
    static constexpr int64_t numerical_value_to_update_to{69};
    MgpValuePtr value_to_update_to{
        EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_int, numerical_value_to_update_to, &this->memory)};
    ASSERT_NE(value_to_update_to, nullptr);
    EXPECT_SUCCESS(mgp_edge_set_property(edge.get(), property_to_update.data(), value_to_update_to.get()));

    const auto maybe_prop = edge_acc.GetProperty(property_id_to_update, memgraph::storage::View::NEW);
    ASSERT_TRUE(maybe_prop.HasValue());
    EXPECT_EQ(*maybe_prop, memgraph::storage::PropertyValue{numerical_value_to_update_to});
  }
  {
    SCOPED_TRACE("Remove the property");
    MgpValuePtr null_value{EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_null, &this->memory)};
    ASSERT_NE(null_value, nullptr);
    EXPECT_SUCCESS(mgp_edge_set_property(edge.get(), property_to_update.data(), null_value.get()));

    const auto maybe_prop = edge_acc.GetProperty(property_id_to_update, memgraph::storage::View::NEW);
    ASSERT_TRUE(maybe_prop.HasValue());
    EXPECT_EQ(*maybe_prop, memgraph::storage::PropertyValue{});
  }
  {
    SCOPED_TRACE("Add a property");
    static constexpr double numerical_value_to_set{3.5};
    MgpValuePtr value_to_set{
        EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_double, numerical_value_to_set, &this->memory)};
    ASSERT_NE(value_to_set, nullptr);
    EXPECT_SUCCESS(mgp_edge_set_property(edge.get(), property_to_set.data(), value_to_set.get()));
    const auto maybe_prop =
        edge_acc.GetProperty(read_uncommited_accessor->NameToProperty(property_to_set), memgraph::storage::View::NEW);
    ASSERT_TRUE(maybe_prop.HasValue());
    EXPECT_EQ(*maybe_prop, memgraph::storage::PropertyValue{numerical_value_to_set});
  }
}

TYPED_TEST(MgpGraphTest, EdgeSetPropertyWithImmutableGraph) {
  memgraph::storage::Gid from_vertex_id{};
  {
    const auto vertex_ids = this->CreateEdge();
    from_vertex_id = vertex_ids[0];
  }
  auto graph = this->CreateGraph(memgraph::storage::View::OLD);
  MgpEdgePtr edge;
  ASSERT_NO_FATAL_FAILURE(this->GetFirstOutEdge(graph, from_vertex_id, edge));
  MgpValuePtr value{EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_int, 65, &this->memory)};
  EXPECT_EQ(EXPECT_MGP_NO_ERROR(int, mgp_edge_underlying_graph_is_mutable, edge.get()), 0);
  EXPECT_EQ(mgp_edge_set_property(edge.get(), "property", value.get()), mgp_error::MGP_ERROR_IMMUTABLE_OBJECT);
}
