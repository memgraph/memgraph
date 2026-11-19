// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Pins the APOC call form of the refactor MAGE module: from/to advertise and
// write input/output/error beside the legacy relationship field, the five
// sibling procedures write the field their apoc.refactor.* alias advertises,
// and the failOnErrors channel is defined. The module sources are compiled into
// this target directly, so the registration and the bodies are the ones a
// module build ships.

#include <cstdint>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

#include <gtest/gtest.h>

#include "mgp.hpp"
#include "query/db_accessor.hpp"
#include "query/graph.hpp"
#include "query/procedure/cypher_types.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/access_type.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/view.hpp"
#include "storage_test_utils.hpp"
#include "test_utils.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/memory.hpp"

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory);

namespace {

using memgraph::query::TypedValue;
using memgraph::storage::Gid;
using memgraph::storage::View;

struct MgpEdgeDeleter {
  void operator()(mgp_edge *e) const {
    if (e != nullptr) mgp_edge_destroy(e);
  }
};

struct MgpVertexDeleter {
  void operator()(mgp_vertex *v) const {
    if (v != nullptr) mgp_vertex_destroy(v);
  }
};

struct MgpEdgesIteratorDeleter {
  void operator()(mgp_edges_iterator *it) const {
    if (it != nullptr) mgp_edges_iterator_destroy(it);
  }
};

using MgpEdgePtr = std::unique_ptr<mgp_edge, MgpEdgeDeleter>;
using MgpVertexPtr = std::unique_ptr<mgp_vertex, MgpVertexDeleter>;
using MgpEdgesIteratorPtr = std::unique_ptr<mgp_edges_iterator, MgpEdgesIteratorDeleter>;

const mgp_proc *FindProc(mgp_module &module, std::string_view name) {
  auto it = module.procedures.find(memgraph::utils::pmr::string{name, module.procedures.get_allocator()});
  if (it == module.procedures.end()) {
    ADD_FAILURE() << "procedure '" << name << "' is not registered";
    return nullptr;
  }
  return &it->second;
}

std::string DeclaredResultFields(const mgp_proc &proc) {
  std::string declared;
  for (const auto &[name, meta] : proc.results) {
    if (!declared.empty()) declared += ", ";
    declared += name;
  }
  return declared;
}

std::string DeclaredResultFields(const mgp_result &result) {
  std::string declared;
  for (const auto &[name, meta] : result.signature) {
    if (!declared.empty()) declared += ", ";
    declared += name;
  }
  return declared;
}

void ExpectResult(const mgp_proc &proc, std::string_view name, std::string_view presentable_type) {
  auto it = proc.results.find(memgraph::utils::pmr::string{name, proc.results.get_allocator()});
  ASSERT_NE(it, proc.results.end()) << "procedure '" << proc.name << "' does not declare result field '" << name
                                    << "'; declared: " << DeclaredResultFields(proc);
  EXPECT_EQ(it->second.first->GetPresentableName(), presentable_type)
      << "procedure '" << proc.name << "' declares '" << name << "' with the wrong type";
}

void ExpectArg(const mgp_proc &proc, size_t index, std::string_view name, std::string_view presentable_type) {
  ASSERT_LT(index, proc.args.size()) << "procedure '" << proc.name << "' has fewer required arguments than expected";
  EXPECT_EQ(proc.args[index].first, name);
  EXPECT_EQ(proc.args[index].second->GetPresentableName(), presentable_type);
}

void ExpectOptionalArg(const mgp_proc &proc, size_t index, std::string_view name, std::string_view presentable_type) {
  ASSERT_LT(index, proc.opt_args.size()) << "procedure '" << proc.name << "' has no optional argument at " << index;
  EXPECT_EQ(std::get<0>(proc.opt_args[index]), name);
  EXPECT_EQ(std::get<1>(proc.opt_args[index])->GetPresentableName(), presentable_type);
}

/// The value a procedure wrote into the first row's field, or nullptr when the
/// module never declared the field (which is exactly the defect under test).
const TypedValue *RowField(const mgp_result &result, std::string_view name) {
  auto it = result.signature.find(memgraph::utils::pmr::string{name, result.signature.get_allocator()});
  if (it == result.signature.end()) {
    ADD_FAILURE() << "no result field named '" << name << "'; declared: " << DeclaredResultFields(result);
    return nullptr;
  }
  if (result.rows.empty()) {
    ADD_FAILURE() << "no row produced for field '" << name << "'";
    return nullptr;
  }
  return &result.rows.front().values[it->second.field_id];
}

class RefactorModuleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    storage_ = std::make_unique<memgraph::storage::InMemoryStorage>(config_);
    acc_ = &CreateDbAccessor();
    guard_.emplace(&memory_);
  }

  void TearDown() override {
    graph_.reset();
    guard_.reset();
    db_accessors_.clear();
    accessors_.clear();
    storage_.reset();
  }

  memgraph::query::DbAccessor &CreateDbAccessor() {
    accessors_.push_back(storage_->Access(memgraph::storage::StorageAccessType::WRITE,
                                          memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION,
                                          std::nullopt));
    db_accessors_.emplace_back(accessors_.back().get());
    return db_accessors_.back();
  }

  Gid InsertNode() { return acc_->InsertVertex().Gid(); }

  Gid InsertEdge(const Gid &from, const Gid &to, const std::string &type) {
    auto from_vertex = acc_->FindVertex(from, View::NEW);
    auto to_vertex = acc_->FindVertex(to, View::NEW);
    EXPECT_TRUE(from_vertex.has_value());
    EXPECT_TRUE(to_vertex.has_value());
    auto edge = acc_->InsertEdge(&*from_vertex, &*to_vertex, acc_->NameToEdgeType(type));
    EXPECT_TRUE(edge.has_value());
    return edge->Gid();
  }

  void AddLabel(const Gid &node, const std::string &label) {
    auto vertex = acc_->FindVertex(node, View::NEW);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(vertex->AddLabel(acc_->NameToLabel(label)).has_value());
  }

  void SetNodeProperty(const Gid &node, const std::string &name, int64_t value) {
    auto vertex = acc_->FindVertex(node, View::NEW);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(vertex->SetProperty(acc_->NameToProperty(name), memgraph::storage::PropertyValue{value}).has_value());
  }

  void SetEdgeProperty(const Gid &edge, const std::string &name, int64_t value) {
    auto edge_accessor = acc_->FindEdge(edge, View::NEW);
    ASSERT_TRUE(edge_accessor.has_value());
    ASSERT_TRUE(
        edge_accessor->SetProperty(acc_->NameToProperty(name), memgraph::storage::PropertyValue{value}).has_value());
  }

  /// Commits the setup and reopens the graph the procedure under test runs on.
  /// A View::OLD graph is immutable, which makes the mutating bodies fail after
  /// they have read their arguments.
  void CommitAndOpen(View view) {
    ASSERT_TRUE(acc_->Commit(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc_ = &CreateDbAccessor();
    graph_.emplace(&*acc_, view, ctx_.get(), memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL);
  }

  mgp_graph &graph() {
    EXPECT_TRUE(graph_.has_value());
    return *graph_;
  }

  MgpVertexPtr VertexById(const Gid &node) {
    return MgpVertexPtr{EXPECT_MGP_NO_ERROR(mgp_vertex *,
                                            mgp_graph_get_vertex_by_id,
                                            &graph(),
                                            mgp_vertex_id{static_cast<int64_t>(node.AsInt())},
                                            &memory_)};
  }

  mgp::Node NodeArg(const Gid &node) { return mgp::Node{VertexById(node).get()}; }

  /// Copies the single outgoing edge of `node` into an owned mgp_edge, the
  /// shape the query engine hands to a procedure argument.
  mgp::Relationship RelationshipArg(const Gid &from) {
    auto vertex = VertexById(from);
    EXPECT_NE(vertex, nullptr);
    MgpEdgesIteratorPtr it{
        EXPECT_MGP_NO_ERROR(mgp_edges_iterator *, mgp_vertex_iter_out_edges, vertex.get(), &memory_)};
    EXPECT_NE(it, nullptr);
    auto *edge = EXPECT_MGP_NO_ERROR(mgp_edge *, mgp_edges_iterator_get, it.get());
    EXPECT_NE(edge, nullptr);
    MgpEdgePtr copy{EXPECT_MGP_NO_ERROR(mgp_edge *, mgp_edge_copy, edge, &memory_)};
    return mgp::Relationship{copy.get()};
  }

  /// Builds the fully declared result signature the way the procedure runner
  /// does, so a body can be called and its row inspected.
  static mgp_result MakeResult(const mgp_proc &proc) {
    mgp_result result{memgraph::utils::NewDeleteResource()};
    uint32_t field_id = 0;
    for (const auto &[name, meta] : proc.results) {
      result.signature.emplace(name, ResultsMetadata{meta.first, false, field_id++});
    }
    return result;
  }

  void Call(const mgp_proc &proc, mgp::List &args, mgp_result &result) {
    proc.cb(args.GetPtr(), &graph(), &result, &memory_);
  }

  memgraph::storage::Config config_;
  std::unique_ptr<memgraph::storage::InMemoryStorage> storage_;
  mgp_memory memory_{memgraph::utils::NewDeleteResource()};
  std::list<std::unique_ptr<memgraph::storage::Storage::Accessor>> accessors_;
  std::list<memgraph::query::DbAccessor> db_accessors_;
  memgraph::query::DbAccessor *acc_{nullptr};
  std::unique_ptr<memgraph::query::ExecutionContext> ctx_ = std::make_unique<memgraph::query::ExecutionContext>();
  std::optional<mgp_graph> graph_;
  std::optional<mgp::MemoryDispatcherGuard> guard_;
};

std::unique_ptr<mgp_module> InitializedModule(mgp_memory &memory) {
  auto module = std::make_unique<mgp_module>(memgraph::utils::NewDeleteResource());
  EXPECT_EQ(mgp_init_module(module.get(), &memory), 0);
  return module;
}

}  // namespace

TEST_F(RefactorModuleTest, Signatures) {
  auto module = InitializedModule(memory_);

  const auto *from = FindProc(*module, "from");
  ASSERT_NE(from, nullptr);
  ASSERT_EQ(from->args.size(), 2U);
  ExpectArg(*from, 0, "relationship", "RELATIONSHIP");
  ExpectArg(*from, 1, "new_from", "NODE");
  ASSERT_EQ(from->opt_args.size(), 1U);
  ExpectOptionalArg(*from, 0, "config", "MAP");
  ExpectResult(*from, "relationship", "RELATIONSHIP");
  ExpectResult(*from, "input", "INTEGER");
  ExpectResult(*from, "output", "RELATIONSHIP");
  ExpectResult(*from, "error", "STRING");
  EXPECT_EQ(from->results.size(), 4U);

  const auto *to = FindProc(*module, "to");
  ASSERT_NE(to, nullptr);
  ASSERT_EQ(to->args.size(), 2U);
  ExpectArg(*to, 0, "relationship", "RELATIONSHIP");
  ExpectArg(*to, 1, "new_to", "NODE");
  ASSERT_EQ(to->opt_args.size(), 1U);
  ExpectOptionalArg(*to, 0, "config", "MAP");
  ExpectResult(*to, "relationship", "RELATIONSHIP");
  ExpectResult(*to, "input", "INTEGER");
  ExpectResult(*to, "output", "RELATIONSHIP");
  ExpectResult(*to, "error", "STRING");
  EXPECT_EQ(to->results.size(), 4U);

  const auto *collapse_node = FindProc(*module, "collapse_node");
  ASSERT_NE(collapse_node, nullptr);
  ExpectResult(*collapse_node, "id_collapsed", "INTEGER");
  ExpectResult(*collapse_node, "new_relationship", "RELATIONSHIP");
  ExpectResult(*collapse_node, "input", "INTEGER");
  ExpectResult(*collapse_node, "output", "RELATIONSHIP");
  ExpectResult(*collapse_node, "error", "STRING");

  for (const auto *name : {"rename_label", "rename_node_property"}) {
    const auto *proc = FindProc(*module, name);
    ASSERT_NE(proc, nullptr);
    ExpectResult(*proc, "nodes_changed", "INTEGER");
    ExpectResult(*proc, "batches", "INTEGER");
  }

  for (const auto *name : {"rename_type", "rename_type_property"}) {
    const auto *proc = FindProc(*module, name);
    ASSERT_NE(proc, nullptr);
    ExpectResult(*proc, "relationships_changed", "INTEGER");
    ExpectResult(*proc, "total", "INTEGER");
  }
}

TEST_F(RefactorModuleTest, FromToSuccessRow) {
  // a -> b, b -> c: the "to" case uses b -> c, a relationship the "from" call
  // did not remove.
  const auto a = InsertNode();
  const auto b = InsertNode();
  const auto c = InsertNode();
  const auto edge_ab = InsertEdge(a, b, "TYPE");
  const auto edge_bc = InsertEdge(b, c, "TYPE");
  CommitAndOpen(View::NEW);

  auto module = InitializedModule(memory_);
  const auto *from = FindProc(*module, "from");
  const auto *to = FindProc(*module, "to");
  ASSERT_NE(from, nullptr);
  ASSERT_NE(to, nullptr);

  {
    auto rel = RelationshipArg(a);
    auto new_from = NodeArg(c);
    mgp::Map config;
    mgp::List args{mgp::Value(rel), mgp::Value(new_from), mgp::Value(config)};
    auto result = MakeResult(*from);
    Call(*from, args, result);

    ASSERT_FALSE(result.error_msg.has_value()) << *result.error_msg;
    ASSERT_EQ(result.rows.size(), 1U);
    const auto *input = RowField(result, "input");
    ASSERT_NE(input, nullptr);
    const auto *output = RowField(result, "output");
    ASSERT_NE(output, nullptr);
    const auto *legacy = RowField(result, "relationship");
    ASSERT_NE(legacy, nullptr);
    const auto *error = RowField(result, "error");
    ASSERT_NE(error, nullptr);

    ASSERT_TRUE(input->IsInt());
    EXPECT_EQ(input->ValueInt(), static_cast<int64_t>(edge_ab.AsInt()));
    ASSERT_TRUE(output->IsEdge());
    EXPECT_NE(output->ValueEdge().Gid(), edge_ab);
    ASSERT_TRUE(legacy->IsEdge());
    EXPECT_EQ(legacy->ValueEdge().Gid(), output->ValueEdge().Gid());
    EXPECT_TRUE(error->IsNull());
  }

  {
    auto rel = RelationshipArg(b);
    auto new_to = NodeArg(a);
    mgp::Map config;
    mgp::List args{mgp::Value(rel), mgp::Value(new_to), mgp::Value(config)};
    auto result = MakeResult(*to);
    Call(*to, args, result);

    ASSERT_FALSE(result.error_msg.has_value()) << *result.error_msg;
    ASSERT_EQ(result.rows.size(), 1U);
    const auto *input = RowField(result, "input");
    ASSERT_NE(input, nullptr);
    const auto *output = RowField(result, "output");
    ASSERT_NE(output, nullptr);
    const auto *legacy = RowField(result, "relationship");
    ASSERT_NE(legacy, nullptr);
    const auto *error = RowField(result, "error");
    ASSERT_NE(error, nullptr);

    ASSERT_TRUE(input->IsInt());
    EXPECT_EQ(input->ValueInt(), static_cast<int64_t>(edge_bc.AsInt()));
    ASSERT_TRUE(output->IsEdge());
    EXPECT_NE(output->ValueEdge().Gid(), edge_bc);
    ASSERT_TRUE(legacy->IsEdge());
    EXPECT_EQ(legacy->ValueEdge().Gid(), output->ValueEdge().Gid());
    EXPECT_TRUE(error->IsNull());
  }
}

TEST_F(RefactorModuleTest, FromToFailureChannels) {
  const auto a = InsertNode();
  const auto b = InsertNode();
  const auto edge_ab = InsertEdge(a, b, "TYPE");
  // An immutable view makes the create fail after the arguments were read.
  CommitAndOpen(View::OLD);

  auto module = InitializedModule(memory_);
  const auto *from = FindProc(*module, "from");
  ASSERT_NE(from, nullptr);

  auto expect_soft_row = [&](const mgp::Map &config) {
    auto rel = RelationshipArg(a);
    auto new_from = NodeArg(b);
    mgp::List args{mgp::Value(rel), mgp::Value(new_from), mgp::Value(config)};
    auto result = MakeResult(*from);
    Call(*from, args, result);

    EXPECT_FALSE(result.error_msg.has_value()) << *result.error_msg;
    ASSERT_EQ(result.rows.size(), 1U);
    const auto *input = RowField(result, "input");
    ASSERT_NE(input, nullptr);
    const auto *error = RowField(result, "error");
    ASSERT_NE(error, nullptr);
    const auto *output = RowField(result, "output");
    ASSERT_NE(output, nullptr);
    EXPECT_TRUE(input->IsInt());
    EXPECT_EQ(input->ValueInt(), static_cast<int64_t>(edge_ab.AsInt()));
    EXPECT_TRUE(error->IsString());
    EXPECT_FALSE(error->ValueString().empty());
    EXPECT_TRUE(output->IsNull());
  };

  // A literal boolean true selects the hard failure: the call itself fails.
  {
    auto rel = RelationshipArg(a);
    auto new_from = NodeArg(b);
    mgp::Map config{{"failOnErrors", mgp::Value(true)}};
    mgp::List args{mgp::Value(rel), mgp::Value(new_from), mgp::Value(config)};
    auto result = MakeResult(*from);
    Call(*from, args, result);

    EXPECT_TRUE(result.error_msg.has_value());
    EXPECT_TRUE(result.rows.empty());
  }

  expect_soft_row(mgp::Map{{"failOnErrors", mgp::Value(false)}});
  expect_soft_row(mgp::Map{});
  expect_soft_row(mgp::Map{{"failOnErrors", mgp::Value(int64_t{1})}});
  expect_soft_row(mgp::Map{{"unknownKey", mgp::Value("ignored")}});
}

TEST_F(RefactorModuleTest, NullArguments) {
  const auto a = InsertNode();
  const auto b = InsertNode();
  InsertEdge(a, b, "TYPE");
  CommitAndOpen(View::NEW);

  auto module = InitializedModule(memory_);
  const auto *from = FindProc(*module, "from");
  ASSERT_NE(from, nullptr);

  auto new_from = NodeArg(b);
  mgp::Map config;
  mgp::List args{mgp::Value(), mgp::Value(new_from), mgp::Value(config)};
  auto result = MakeResult(*from);
  Call(*from, args, result);

  // A null relationship is a soft error row, never a hard failure and never an
  // empty stream.
  EXPECT_FALSE(result.error_msg.has_value()) << *result.error_msg;
  ASSERT_EQ(result.rows.size(), 1U);
  const auto *error = RowField(result, "error");
  ASSERT_NE(error, nullptr);
  const auto *output = RowField(result, "output");
  ASSERT_NE(output, nullptr);
  EXPECT_TRUE(error->IsString());
  EXPECT_FALSE(error->ValueString().empty());
  EXPECT_TRUE(output->IsNull());
}

TEST_F(RefactorModuleTest, SiblingBodies) {
  // n0 -> n1 -> n2 for collapse_node; n3 -> n4 -> n5 for the rename_type calls.
  const auto n0 = InsertNode();
  const auto n1 = InsertNode();
  const auto n2 = InsertNode();
  InsertEdge(n0, n1, "CHAIN");
  InsertEdge(n1, n2, "CHAIN");
  const auto n3 = InsertNode();
  const auto n4 = InsertNode();
  const auto n5 = InsertNode();
  const auto e34 = InsertEdge(n3, n4, "OldType");
  const auto e45 = InsertEdge(n4, n5, "OldType");
  SetEdgeProperty(e34, "old_edge", 1);
  SetEdgeProperty(e45, "old_edge", 1);
  AddLabel(n0, "OLD");
  AddLabel(n2, "OLD");
  AddLabel(n3, "OLD");
  SetNodeProperty(n0, "old_prop", 1);
  SetNodeProperty(n2, "old_prop", 1);
  SetNodeProperty(n3, "old_prop", 1);
  CommitAndOpen(View::NEW);

  auto module = InitializedModule(memory_);

  {
    const auto *proc = FindProc(*module, "rename_label");
    ASSERT_NE(proc, nullptr);
    mgp::List nodes{mgp::Value(NodeArg(n0)), mgp::Value(NodeArg(n2)), mgp::Value(NodeArg(n3))};
    mgp::List args{mgp::Value("OLD"), mgp::Value("NEW"), mgp::Value(nodes)};
    auto result = MakeResult(*proc);
    Call(*proc, args, result);
    ASSERT_FALSE(result.error_msg.has_value()) << *result.error_msg;
    ASSERT_EQ(result.rows.size(), 1U);
    const auto *batches = RowField(result, "batches");
    ASSERT_NE(batches, nullptr);
    const auto *legacy = RowField(result, "nodes_changed");
    ASSERT_NE(legacy, nullptr);
    ASSERT_TRUE(batches->IsInt());
    EXPECT_EQ(batches->ValueInt(), 3);
    ASSERT_TRUE(legacy->IsInt());
    EXPECT_EQ(legacy->ValueInt(), 3);
  }

  {
    const auto *proc = FindProc(*module, "rename_node_property");
    ASSERT_NE(proc, nullptr);
    mgp::List nodes{mgp::Value(NodeArg(n0)), mgp::Value(NodeArg(n2)), mgp::Value(NodeArg(n3))};
    mgp::List args{mgp::Value("old_prop"), mgp::Value("new_prop"), mgp::Value(nodes)};
    auto result = MakeResult(*proc);
    Call(*proc, args, result);
    ASSERT_FALSE(result.error_msg.has_value()) << *result.error_msg;
    ASSERT_EQ(result.rows.size(), 1U);
    const auto *batches = RowField(result, "batches");
    ASSERT_NE(batches, nullptr);
    const auto *legacy = RowField(result, "nodes_changed");
    ASSERT_NE(legacy, nullptr);
    ASSERT_TRUE(batches->IsInt());
    EXPECT_EQ(batches->ValueInt(), 3);
    ASSERT_TRUE(legacy->IsInt());
    EXPECT_EQ(legacy->ValueInt(), 3);
  }

  {
    const auto *proc = FindProc(*module, "rename_type_property");
    ASSERT_NE(proc, nullptr);
    mgp::List rels{mgp::Value(RelationshipArg(n3)), mgp::Value(RelationshipArg(n4))};
    mgp::List args{mgp::Value("old_edge"), mgp::Value("new_edge"), mgp::Value(rels)};
    auto result = MakeResult(*proc);
    Call(*proc, args, result);
    ASSERT_FALSE(result.error_msg.has_value()) << *result.error_msg;
    ASSERT_EQ(result.rows.size(), 1U);
    const auto *total = RowField(result, "total");
    ASSERT_NE(total, nullptr);
    const auto *legacy = RowField(result, "relationships_changed");
    ASSERT_NE(legacy, nullptr);
    ASSERT_TRUE(total->IsInt());
    EXPECT_EQ(total->ValueInt(), 2);
    ASSERT_TRUE(legacy->IsInt());
    EXPECT_EQ(legacy->ValueInt(), 2);
  }

  {
    const auto *proc = FindProc(*module, "rename_type");
    ASSERT_NE(proc, nullptr);
    mgp::List rels{mgp::Value(RelationshipArg(n3)), mgp::Value(RelationshipArg(n4))};
    mgp::List args{mgp::Value("OldType"), mgp::Value("NewType"), mgp::Value(rels)};
    auto result = MakeResult(*proc);
    Call(*proc, args, result);
    ASSERT_FALSE(result.error_msg.has_value()) << *result.error_msg;
    ASSERT_EQ(result.rows.size(), 1U);
    const auto *total = RowField(result, "total");
    ASSERT_NE(total, nullptr);
    const auto *legacy = RowField(result, "relationships_changed");
    ASSERT_NE(legacy, nullptr);
    ASSERT_TRUE(total->IsInt());
    EXPECT_EQ(total->ValueInt(), 2);
    ASSERT_TRUE(legacy->IsInt());
    EXPECT_EQ(legacy->ValueInt(), 2);
  }

  {
    const auto *proc = FindProc(*module, "collapse_node");
    ASSERT_NE(proc, nullptr);
    mgp::List args{mgp::Value(NodeArg(n1)), mgp::Value("COLLAPSED")};
    auto result = MakeResult(*proc);
    Call(*proc, args, result);
    ASSERT_FALSE(result.error_msg.has_value()) << *result.error_msg;
    ASSERT_EQ(result.rows.size(), 1U);
    const auto *input = RowField(result, "input");
    ASSERT_NE(input, nullptr);
    const auto *output = RowField(result, "output");
    ASSERT_NE(output, nullptr);
    const auto *error = RowField(result, "error");
    ASSERT_NE(error, nullptr);
    const auto *legacy_id = RowField(result, "id_collapsed");
    ASSERT_NE(legacy_id, nullptr);
    const auto *legacy_rel = RowField(result, "new_relationship");
    ASSERT_NE(legacy_rel, nullptr);
    ASSERT_TRUE(input->IsInt());
    EXPECT_EQ(input->ValueInt(), static_cast<int64_t>(n1.AsInt()));
    ASSERT_TRUE(legacy_id->IsInt());
    EXPECT_EQ(legacy_id->ValueInt(), static_cast<int64_t>(n1.AsInt()));
    ASSERT_TRUE(output->IsEdge());
    ASSERT_TRUE(legacy_rel->IsEdge());
    EXPECT_EQ(output->ValueEdge().Gid(), legacy_rel->ValueEdge().Gid());
    EXPECT_TRUE(error->IsNull());
  }
}
