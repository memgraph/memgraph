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

// The refactor module's sources are compiled into this target, so these tests
// observe the registration and bodies a module build ships.

#include <gtest/gtest.h>
#include <cstdint>
#include <list>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include "mgp.hpp"
#include "query/db_accessor.hpp"
#include "query/procedure/cypher_types.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/memory.hpp"

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory);

namespace {
using memgraph::query::TypedValue;
using memgraph::storage::View;
using Fields = std::initializer_list<std::pair<std::string_view, std::string_view>>;
const mgp_proc *FindProc(mgp_module &module, std::string_view name) {
  auto it = module.procedures.find(memgraph::utils::pmr::string{name, module.procedures.get_allocator()});
  if (it == module.procedures.end()) {
    ADD_FAILURE() << "procedure '" << name << "' is not registered";
    return nullptr;
  }
  return &it->second;
}

const TypedValue &Field(const mgp_result &result, std::string_view name) {
  auto it = result.signature.find(memgraph::utils::pmr::string{name, result.signature.get_allocator()});
  if (it == result.signature.end() || result.rows.size() != 1U) {
    throw std::logic_error{"no single row carrying '" + std::string{name} + "'"};
  }
  return result.rows.front().values[it->second.field_id];
}

void ExpectInt(const mgp_result &result, std::string_view name, int64_t value) {
  EXPECT_TRUE(Field(result, name).IsInt()) << name;
  EXPECT_EQ(Field(result, name).ValueInt(), value) << name;
}

void ExpectNonNullString(const mgp_result &result, std::string_view name) {
  EXPECT_TRUE(Field(result, name).IsString()) << name;
  EXPECT_FALSE(Field(result, name).ValueString().empty()) << name;
}

void ExpectResults(const mgp_proc &proc, Fields fields) {
  EXPECT_EQ(proc.results.size(), fields.size()) << proc.name;
  for (auto [name, type] : fields) {
    auto it = proc.results.find(memgraph::utils::pmr::string{name, proc.results.get_allocator()});
    ASSERT_NE(it, proc.results.end()) << proc.name << " does not declare '" << name << "'";
    EXPECT_EQ(it->second.first->GetPresentableName(), type) << proc.name << " '" << name << "'";
  }
}

void ExpectResults(mgp_module &module, std::string_view name, Fields fields) {
  const auto *proc = FindProc(module, name);
  ASSERT_NE(proc, nullptr);
  ExpectResults(*proc, fields);
}

void ExpectArg(const mgp_proc &proc, size_t index, std::string_view name, std::string_view type) {
  ASSERT_LT(index, proc.args.size()) << proc.name;
  EXPECT_EQ(proc.args[index].first, name) << proc.name;
  EXPECT_EQ(proc.args[index].second->GetPresentableName(), type) << proc.name;
}

void ExpectFromToSignature(mgp_module &module, std::string_view name, std::string_view second_arg) {
  const auto *proc = FindProc(module, name);
  ASSERT_NE(proc, nullptr);
  ASSERT_EQ(proc->args.size(), 2U);
  ExpectArg(*proc, 0, "relationship", "RELATIONSHIP");
  ExpectArg(*proc, 1, second_arg, "NODE");
  ASSERT_EQ(proc->opt_args.size(), 1U);
  EXPECT_EQ(std::get<0>(proc->opt_args[0]), "config");
  EXPECT_EQ(std::get<1>(proc->opt_args[0])->GetPresentableName(), "MAP");
  ExpectResults(
      *proc, {{"relationship", "RELATIONSHIP"}, {"input", "INTEGER"}, {"output", "RELATIONSHIP"}, {"error", "STRING"}});
}

std::unique_ptr<mgp_module> InitializedModule(mgp_memory &memory) {
  auto module = std::make_unique<mgp_module>(memgraph::utils::NewDeleteResource());
  EXPECT_EQ(mgp_init_module(module.get(), &memory), 0);
  return module;
}
class RefactorModuleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    storage_ = std::make_unique<memgraph::storage::InMemoryStorage>(config_);
    acc_ = &Access();
    guard_.emplace(&memory_);
  }
  void TearDown() override {
    graph_.reset();
    guard_.reset();
    db_accessors_.clear();
    accessors_.clear();
    storage_.reset();
  }

  memgraph::query::DbAccessor &Access() {
    accessors_.push_back(storage_->Access(memgraph::storage::StorageAccessType::WRITE,
                                          memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION,
                                          std::nullopt));
    db_accessors_.emplace_back(accessors_.back().get());
    return db_accessors_.back();
  }

  /// Commits the setup and reopens the graph the procedure runs on. A View::OLD
  /// graph is immutable, so the mutating bodies fail after reading their arguments.
  void Open(View view) {
    ASSERT_TRUE(acc_->Commit(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc_ = &Access();
    graph_.emplace(&*acc_, view, ctx_.get(), memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL);
  }

  mgp_graph &Graph() { return *graph_; }

  int64_t InsertNode() { return static_cast<int64_t>(acc_->InsertVertex().Gid().AsInt()); }

  int64_t InsertEdge(int64_t from, int64_t to, const std::string &type) {
    auto from_vertex = acc_->FindVertex(memgraph::storage::Gid::FromInt(from), View::NEW);
    auto to_vertex = acc_->FindVertex(memgraph::storage::Gid::FromInt(to), View::NEW);
    EXPECT_TRUE(from_vertex.has_value());
    EXPECT_TRUE(to_vertex.has_value());
    auto edge = acc_->InsertEdge(&*from_vertex, &*to_vertex, acc_->NameToEdgeType(type));
    EXPECT_TRUE(edge.has_value());
    return static_cast<int64_t>(edge->Gid().AsInt());
  }

  void AddLabel(int64_t node, const std::string &label) {
    auto vertex = acc_->FindVertex(memgraph::storage::Gid::FromInt(node), View::NEW);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(vertex->AddLabel(acc_->NameToLabel(label)).has_value());
  }

  void SetNodeProperty(int64_t node, const std::string &name, int64_t value) {
    auto vertex = acc_->FindVertex(memgraph::storage::Gid::FromInt(node), View::NEW);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(vertex->SetProperty(acc_->NameToProperty(name), memgraph::storage::PropertyValue{value}).has_value());
  }

  void SetEdgeProperty(int64_t edge, const std::string &name, int64_t value) {
    auto edge_accessor = acc_->FindEdge(memgraph::storage::Gid::FromInt(edge), View::NEW);
    ASSERT_TRUE(edge_accessor.has_value());
    ASSERT_TRUE(
        edge_accessor->SetProperty(acc_->NameToProperty(name), memgraph::storage::PropertyValue{value}).has_value());
  }

  mgp::Node By(int64_t id) { return mgp::Graph{&Graph()}.GetNodeById(mgp::Id::FromInt(id)); }

  mgp::Relationship FirstOut(int64_t id) {
    for (auto rel : By(id).OutRelationships()) return rel;
    throw std::logic_error{"node has no outgoing relationship"};
  }
  static mgp_result MakeResult(const mgp_proc &proc) {
    mgp_result result{memgraph::utils::NewDeleteResource()};
    uint32_t field_id = 0;
    for (const auto &[name, meta] : proc.results)
      result.signature.emplace(name, ResultsMetadata{meta.first, false, field_id++});
    return result;
  }
  void Call(const mgp_proc &proc, mgp::List &args, mgp_result &result) {
    proc.cb(args.GetPtr(), &Graph(), &result, &memory_);
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
}  // namespace

TEST_F(RefactorModuleTest, Signatures) {
  auto module = InitializedModule(memory_);
  ExpectFromToSignature(*module, "from", "new_from");
  ExpectFromToSignature(*module, "to", "new_to");
  ExpectResults(*module,
                "collapse_node",
                {{"id_collapsed", "INTEGER"},
                 {"new_relationship", "RELATIONSHIP"},
                 {"input", "INTEGER"},
                 {"output", "RELATIONSHIP"},
                 {"error", "STRING"}});
  for (const auto *name : {"rename_label", "rename_node_property"})
    ExpectResults(*module, name, {{"nodes_changed", "INTEGER"}, {"batches", "INTEGER"}});
  for (const auto *name : {"rename_type", "rename_type_property"})
    ExpectResults(*module, name, {{"relationships_changed", "INTEGER"}, {"total", "INTEGER"}});
}
TEST_F(RefactorModuleTest, FromToSuccessRow) {
  const auto a = InsertNode(), b = InsertNode(), c = InsertNode();
  const auto edge_ab = InsertEdge(a, b, "TYPE"), edge_bc = InsertEdge(b, c, "TYPE");
  Open(View::NEW);
  auto module = InitializedModule(memory_);
  auto check = [&](std::string_view name, int64_t rel_from, int64_t new_node, int64_t original) {
    const auto *proc = FindProc(*module, name);
    ASSERT_NE(proc, nullptr);
    auto rel = FirstOut(rel_from);
    auto target = By(new_node);
    mgp::List args{mgp::Value(rel), mgp::Value(target), mgp::Value(mgp::Map{})};
    auto result = MakeResult(*proc);
    Call(*proc, args, result);
    ASSERT_FALSE(result.error_msg.has_value()) << *result.error_msg;
    ASSERT_EQ(result.rows.size(), 1U);
    ExpectInt(result, "input", original);
    EXPECT_TRUE(Field(result, "output").IsEdge());
    EXPECT_NE(Field(result, "output").ValueEdge().Gid().AsInt(), original);
    EXPECT_TRUE(Field(result, "relationship").IsEdge());
    EXPECT_EQ(Field(result, "relationship").ValueEdge().Gid().AsInt(),
              Field(result, "output").ValueEdge().Gid().AsInt());
    EXPECT_TRUE(Field(result, "error").IsNull());
  };
  check("from", a, c, edge_ab);
  check("to", b, a, edge_bc);
}
TEST_F(RefactorModuleTest, FromToFailureChannels) {
  const auto a = InsertNode(), b = InsertNode();
  const auto edge_ab = InsertEdge(a, b, "TYPE");
  Open(View::OLD);
  auto module = InitializedModule(memory_);
  const auto *from = FindProc(*module, "from");
  ASSERT_NE(from, nullptr);
  auto call = [&](mgp::Value rel, const mgp::Map &config) {
    mgp::List args{std::move(rel), mgp::Value(By(b)), mgp::Value(config)};
    auto result = MakeResult(*from);
    Call(*from, args, result);
    return result;
  };
  auto expect_soft = [&](const mgp_result &result, bool has_input) {
    EXPECT_FALSE(result.error_msg.has_value()) << *result.error_msg;
    ASSERT_EQ(result.rows.size(), 1U);
    if (has_input) ExpectInt(result, "input", edge_ab);
    ExpectNonNullString(result, "error");
    EXPECT_TRUE(Field(result, "output").IsNull());
  };
  auto hard = call(mgp::Value(FirstOut(a)), mgp::Map{{"failOnErrors", mgp::Value(true)}});
  EXPECT_TRUE(hard.error_msg.has_value());
  EXPECT_TRUE(hard.rows.empty());
  expect_soft(call(mgp::Value(FirstOut(a)), mgp::Map{{"failOnErrors", mgp::Value(false)}}), true);
  expect_soft(call(mgp::Value(FirstOut(a)), mgp::Map{}), true);
  expect_soft(call(mgp::Value(FirstOut(a)), mgp::Map{{"failOnErrors", mgp::Value(int64_t{1})}}), true);
  expect_soft(call(mgp::Value(FirstOut(a)), mgp::Map{{"unknownKey", mgp::Value("ignored")}}), true);
  // A null relationship is a soft error row, never a hard failure or empty stream.
  expect_soft(call(mgp::Value(), mgp::Map{}), false);
}
TEST_F(RefactorModuleTest, SiblingBodies) {
  // n0 -> n1 -> n2 for collapse_node; n3 -> n4 -> n5 for the rename_type calls.
  const auto n0 = InsertNode(), n1 = InsertNode(), n2 = InsertNode();
  const auto n3 = InsertNode(), n4 = InsertNode(), n5 = InsertNode();
  InsertEdge(n0, n1, "CHAIN");
  InsertEdge(n1, n2, "CHAIN");
  const auto e34 = InsertEdge(n3, n4, "OldType");
  const auto e45 = InsertEdge(n4, n5, "OldType");
  SetEdgeProperty(e34, "old_edge", 1);
  SetEdgeProperty(e45, "old_edge", 1);
  for (const auto node : {n0, n2, n3}) {
    AddLabel(node, "OLD");
    SetNodeProperty(node, "old_prop", 1);
  }
  Open(View::NEW);
  auto module = InitializedModule(memory_);
  auto run = [&](std::string_view name, mgp::List &args) {
    const auto *proc = FindProc(*module, name);
    EXPECT_NE(proc, nullptr);
    if (proc == nullptr) throw std::logic_error{"procedure is not registered"};
    auto result = MakeResult(*proc);
    Call(*proc, args, result);
    EXPECT_FALSE(result.error_msg.has_value()) << *result.error_msg;
    EXPECT_EQ(result.rows.size(), 1U);
    return result;
  };
  mgp::List nodes{mgp::Value(By(n0)), mgp::Value(By(n2)), mgp::Value(By(n3))};
  auto rename = [&](std::string_view name, std::string_view old_name, std::string_view new_name) {
    mgp::List args{mgp::Value(old_name), mgp::Value(new_name), mgp::Value(nodes)};
    auto result = run(name, args);
    ExpectInt(result, "batches", 3);
    ExpectInt(result, "nodes_changed", 3);
  };
  rename("rename_label", "OLD", "NEW");
  rename("rename_node_property", "old_prop", "new_prop");
  mgp::List rels{mgp::Value(FirstOut(n3)), mgp::Value(FirstOut(n4))};
  auto rename_type = [&](std::string_view name, std::string_view old_name, std::string_view new_name) {
    mgp::List args{mgp::Value(old_name), mgp::Value(new_name), mgp::Value(rels)};
    auto result = run(name, args);
    ExpectInt(result, "total", 2);
    ExpectInt(result, "relationships_changed", 2);
  };
  rename_type("rename_type_property", "old_edge", "new_edge");
  rename_type("rename_type", "OldType", "NewType");
  mgp::List collapse_args{mgp::Value(By(n1)), mgp::Value("COLLAPSED")};
  auto collapse = run("collapse_node", collapse_args);
  ExpectInt(collapse, "input", n1);
  ExpectInt(collapse, "id_collapsed", n1);
  EXPECT_TRUE(Field(collapse, "output").IsEdge());
  EXPECT_TRUE(Field(collapse, "new_relationship").IsEdge());
  EXPECT_EQ(Field(collapse, "output").ValueEdge().Gid().AsInt(),
            Field(collapse, "new_relationship").ValueEdge().Gid().AsInt());
  EXPECT_TRUE(Field(collapse, "error").IsNull());
}
