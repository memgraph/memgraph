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

// What fine-grained access control costs a property read.
//
// A property read asks whether the caller may see that property. The question is reached once per
// read and per row, so a query reading a property over a large expansion asks it as often as it
// reads anything.
//
// The four arms separate the cost of the feature existing from the cost of using it. `NoChecker`
// is a query with no fine-grained checker attached at all. `Unrestricted` attaches one for a user
// who restricts nothing, which is what most deployments with the feature enabled actually run, and
// is the arm that should match `NoChecker`: a caller who restricts nothing should pay nothing.
// `RestrictedElsewhere` restricts a different property, so the check is live but this read is
// allowed, and `Denied` restricts the property being read, which is the masking path.

#include <benchmark/benchmark.h>

#include <optional>
#include <string>

#include "auth/models.hpp"
#include "glue/auth_checker.hpp"
#include "license/license.hpp"
#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/interpret/eval.hpp"
#include "query/interpret/frame.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"

namespace {

using memgraph::query::AstStorage;
using memgraph::query::DbAccessor;
using memgraph::query::EvaluationContext;
using memgraph::query::ExecutionContext;
using memgraph::query::ExpressionEvaluator;
using memgraph::query::Frame;
using memgraph::query::SymbolTable;
using memgraph::query::TypedValue;

constexpr auto kLabel = "Item";
constexpr auto kProperty = "value";
constexpr auto kOtherProperty = "other";

/// Which property rule the user under measurement carries.
enum class Restriction { kNone, kElsewhere, kOnTheProperty };

memgraph::auth::User MakeUser(std::string const &db_name, Restriction restriction) {
  auto user = memgraph::auth::User{"bench"};
  user.db_access().Grant(db_name);
  user.fine_grained_access_handler().label_permissions().GrantGlobal(memgraph::auth::kAllLabelPermissions);
  // Unrestricted access is a global grant over every property, not the absence of a rule.
  user.property_access_handler().label_properties().GrantGlobal("*", memgraph::auth::kAllPropertyPermissionTypes);
  user.property_access_handler().edge_type_properties().GrantGlobal("*", memgraph::auth::kAllPropertyPermissionTypes);

  switch (restriction) {
    case Restriction::kNone:
      break;
    case Restriction::kElsewhere:
      user.property_access_handler().label_properties().Deny(
          {kLabel}, kOtherProperty, memgraph::auth::kAllPropertyPermissionTypes);
      break;
    case Restriction::kOnTheProperty:
      user.property_access_handler().label_properties().Deny(
          {kLabel}, kProperty, memgraph::auth::kAllPropertyPermissionTypes);
      break;
  }
  return user;
}

/// One vertex carrying the property, and an evaluator reading it, arranged so the loop below does
/// nothing but the read being measured.
struct Fixture {
  std::unique_ptr<memgraph::storage::Storage> db = std::make_unique<memgraph::storage::InMemoryStorage>();
  std::unique_ptr<memgraph::storage::Storage::Accessor> storage_dba = db->Access(memgraph::storage::WRITE);
  DbAccessor dba{storage_dba.get()};
  AstStorage ast;
  SymbolTable symbol_table;

  explicit Fixture() {
    auto label = dba.NameToLabel(kLabel);
    auto vertex = dba.InsertVertex();
    MG_ASSERT(vertex.AddLabel(label).has_value());
    MG_ASSERT(vertex.SetProperty(dba.NameToProperty(kProperty), memgraph::storage::PropertyValue(42)).has_value());
    MG_ASSERT(vertex.SetProperty(dba.NameToProperty(kOtherProperty), memgraph::storage::PropertyValue(7)).has_value());
    dba.AdvanceCommand();
    vertex_ = vertex;
  }

  memgraph::query::VertexAccessor Vertex() const { return *vertex_; }

 private:
  std::optional<memgraph::query::VertexAccessor> vertex_;
};

void ReadOneProperty(benchmark::State &state, bool attach_checker, Restriction restriction) {
  memgraph::license::global_license_checker.EnableTesting();

  Fixture fixture;
  auto symbol = fixture.symbol_table.CreateSymbol("n", true);
  auto *identifier = fixture.ast.Create<memgraph::query::Identifier>("n")->MapTo(symbol);
  auto *lookup = fixture.ast.Create<memgraph::query::PropertyLookup>(identifier, fixture.ast.GetPropertyIx(kProperty));

  auto user = MakeUser(fixture.dba.DatabaseName(), restriction);
  auto checker = std::optional<memgraph::glue::FineGrainedAuthChecker>{};
  if (attach_checker) checker.emplace(std::move(user), &fixture.dba);

  Frame frame(fixture.symbol_table.max_position());
  frame.GetFrameWriter(nullptr, memgraph::utils::NewDeleteResource()).Write(symbol, fixture.Vertex());

  ExecutionContext ctx;
  ctx.db_accessor = &fixture.dba;
  ctx.symbol_table = fixture.symbol_table;
  ctx.evaluation_context = EvaluationContext{};
  ctx.evaluation_context.properties = memgraph::query::NamesToProperties(fixture.ast.properties_, &fixture.dba);
  if (attach_checker) ctx.auth_checker = &*checker;

  // The evaluator resolves whether any property is restricted once, on construction, so it is built
  // outside the loop exactly as a query builds one per pull rather than per read.
  ExpressionEvaluator evaluator(&frame, ctx, memgraph::storage::View::NEW);

  for (auto _ : state) {
    benchmark::DoNotOptimize(lookup->Accept(evaluator));
  }
  state.SetItemsProcessed(state.iterations());

  memgraph::license::global_license_checker.DisableTesting();
}

void NoChecker(benchmark::State &state) { ReadOneProperty(state, false, Restriction::kNone); }

void Unrestricted(benchmark::State &state) { ReadOneProperty(state, true, Restriction::kNone); }

void RestrictedElsewhere(benchmark::State &state) { ReadOneProperty(state, true, Restriction::kElsewhere); }

void Denied(benchmark::State &state) { ReadOneProperty(state, true, Restriction::kOnTheProperty); }

BENCHMARK(NoChecker)->Unit(benchmark::kNanosecond);
BENCHMARK(Unrestricted)->Unit(benchmark::kNanosecond);
BENCHMARK(RestrictedElsewhere)->Unit(benchmark::kNanosecond);
BENCHMARK(Denied)->Unit(benchmark::kNanosecond);

}  // namespace

BENCHMARK_MAIN();
