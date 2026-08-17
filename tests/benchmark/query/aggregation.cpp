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

// The supernode aggregation shapes, in process.
//
// These are the queries a separate client-server benchmark runs against a ten million item graph and
// a second engine. That one answers "are we faster than them", takes half an hour and needs two
// servers; it cannot say which change moved which query. This runs the same shapes over a smaller
// graph in one process, so a single commit can be held against a single number.
//
// The graph is the one those queries were written for: a hub of degree N, whose neighbours carry a
// skewed category, a uniform region and bucket, and the numbers the aggregations add up, with the
// first of them linked to each other in a ring with chords for the queries that walk paths. It is
// built once and shared, because building it costs far more than running any query over it.
//
// The distributions are the large graph's, not convenient ones, because they decide the answers:
// how skewed the category is decides the size of a group, and how long the tail of the values is
// decides how much a filter on them keeps. The two edge types are held at the same ratio to each
// other as well, since which end of a two-hop pattern is cheaper to start from follows from that
// ratio and not from the size of the graph.
//
// Measured against the large graph, most shapes cost within a few percent of the same per row.
//
// Only execution is measured. Parsing and planning happen once, outside the loop, as they do for a
// real query whose plan is cached.
//
// The flags the server takes are parsed here too, so a change behind one can be held against itself:
//
//   for f in false true; do
//     ./aggregation --query-cache-properties=$f --benchmark_min_time=0.8s \
//       --benchmark_out=$f.json --benchmark_out_format=json
//   done
//
// Two things about reading the result. Compare a build against a build in one sitting rather than
// against a number written down earlier - the absolute figures drift between sessions by more than
// most of the effects here. And pin to a core: `taskset -c 0`, because an unpinned run on a machine
// with simultaneous multithreading measures the scheduler as much as the code.

#include <cstdlib>
#include <random>
#include <string>
#include <vector>

#include "query/frontend/ast/cypher_main_visitor.hpp"

#include <benchmark/benchmark.h>
#include <gflags/gflags.h>

//////////////////////////////////////////////////////
// THIS INCLUDE SHOULD ALWAYS COME BEFORE THE
// OTHER INCLUDES
// "planner.hpp" includes json.hpp which uses libc's
// EOF macro while in the other includes
// <antlr4-runtime.h> is included which contains a static
// variable of the same name, EOF.
// This hides the definition of the macro which causes
// the compilation to fail.
#include "query/interpret/frame.hpp"
#include "query/parameters.hpp"
#include "query/plan/planner.hpp"
//////////////////////////////////////////////////////
#include "metrics/prometheus_metrics.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/interpret/eval.hpp"
#include "query/interpreter.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/memory.hpp"

namespace {

namespace ms = memgraph::storage;
namespace mq = memgraph::query;

memgraph::metrics::DatabaseMetricHandles &BenchmarkMetricHandles() {
  static memgraph::metrics::DatabaseMetricHandles handles;
  return handles;
}

/// Neighbours of the hub. Large enough that per-row costs dominate the fixed cost of a pull, small
/// enough that the whole graph builds in a couple of seconds. `MG_AGG_BENCH_ITEMS` overrides it, for
/// checking that a result holds at another size rather than for routine runs.
int64_t ItemCount() {
  static int64_t const count = [] {
    auto const *env = std::getenv("MG_AGG_BENCH_ITEMS");
    return env != nullptr ? std::strtoll(env, nullptr, 10) : 250'000;
  }();
  return count;
}

/// Items that are also linked to each other. Held at the same proportion of the hub's degree as the
/// large graph uses, because which end of a two-hop pattern a planner starts from is decided by the
/// ratio between the two edge types, not by their absolute size.
int64_t LinkCoreSize() { return ItemCount() / 50; }

/// The value distributions the queries were written against: category is skewed so a few groups hold
/// most of the rows, region and bucket are uniform, and the grouping keys span four orders of
/// magnitude of cardinality across the query set.
constexpr int kCategoryCount = 16;
constexpr int kRegionCount = 8;
constexpr int kBucketCount = 1000;
constexpr int64_t kTsMin = 1'420'070'400;
constexpr int64_t kTsMax = 1'767'225'599;

struct Graph {
  std::unique_ptr<ms::Storage> db;
};

/// One hub, `ItemCount()` items hanging off it, built once for the whole run.
///
/// Analytical mode, because that is what the workload these queries come from runs in, and because a
/// record with no delta chain is the case the property reads are shaped for.
Graph const &TheGraph() {
  static Graph const graph = [] {
    auto config = ms::Config{};
    config.salient.items.properties_on_edges = true;
    auto db = std::make_unique<ms::InMemoryStorage>(config);
    db->SetStorageMode(ms::StorageMode::IN_MEMORY_ANALYTICAL);

    auto const item_count = ItemCount();
    {
      auto dba = db->Access(ms::WRITE);
      auto const hub_label = dba->NameToLabel("Hub");
      auto const item_label = dba->NameToLabel("Item");
      auto const has_type = dba->NameToEdgeType("HAS");

      auto const p_id = dba->NameToProperty("id");
      auto const p_category = dba->NameToProperty("category");
      auto const p_region = dba->NameToProperty("region");
      auto const p_bucket = dba->NameToProperty("bucket");
      auto const p_value = dba->NameToProperty("value");
      auto const p_quantity = dba->NameToProperty("quantity");
      auto const p_ts = dba->NameToProperty("ts");
      auto const p_active = dba->NameToProperty("active");
      auto const p_weight = dba->NameToProperty("weight");
      auto const p_amount = dba->NameToProperty("amount");
      auto const p_year = dba->NameToProperty("year");

      auto const p_cost = dba->NameToProperty("cost");
      auto const link_type = dba->NameToEdgeType("LINK");

      auto hub = dba->CreateVertex();
      MG_ASSERT(hub.AddLabel(hub_label).has_value());
      MG_ASSERT(hub.SetProperty(p_id, ms::PropertyValue(int64_t{0})).has_value());

      // The first items are also wired to each other, which is what the path queries walk.
      auto core = std::vector<ms::VertexAccessor>{};
      core.reserve(static_cast<size_t>(LinkCoreSize()));

      // NOLINTNEXTLINE(cert-msc32-c,cert-msc51-cpp)
      auto rng = std::mt19937_64{42};
      auto uniform = std::uniform_real_distribution<double>{0.0, 1.0};
      // Long tailed rather than flat, which is what decides how much a filter on it keeps.
      auto value_of = std::lognormal_distribution<double>{3.0, 1.0};

      auto categories = std::vector<std::string>{};
      for (int i = 0; i != kCategoryCount; ++i) {
        categories.push_back("cat_" + std::string(i < 10 ? "0" : "") + std::to_string(i));
      }
      auto regions = std::vector<std::string>{};
      for (int i = 0; i != kRegionCount; ++i) regions.push_back("region_" + std::to_string(i));

      for (int64_t i = 0; i != item_count; ++i) {
        auto item = dba->CreateVertex();
        MG_ASSERT(item.AddLabel(item_label).has_value());

        // Skewed towards the low categories: the square of a uniform draw puts about a fifth of the
        // items in the first one, which is what makes the grouped aggregations non-trivial.
        auto const skew = uniform(rng);
        auto const category = static_cast<int>(skew * skew * kCategoryCount) % kCategoryCount;

        MG_ASSERT(item.SetProperty(p_id, ms::PropertyValue(i)).has_value());
        MG_ASSERT(item.SetProperty(p_category, ms::PropertyValue(categories[category])).has_value());
        MG_ASSERT(item.SetProperty(p_region, ms::PropertyValue(regions[rng() % kRegionCount])).has_value());
        MG_ASSERT(
            item.SetProperty(p_bucket, ms::PropertyValue(static_cast<int64_t>(rng() % kBucketCount))).has_value());
        MG_ASSERT(item.SetProperty(p_value, ms::PropertyValue(value_of(rng))).has_value());
        MG_ASSERT(item.SetProperty(p_quantity, ms::PropertyValue(static_cast<int64_t>(1 + rng() % 100))).has_value());
        MG_ASSERT(item.SetProperty(p_ts, ms::PropertyValue(kTsMin + static_cast<int64_t>(rng() % (kTsMax - kTsMin))))
                      .has_value());
        MG_ASSERT(item.SetProperty(p_active, ms::PropertyValue(uniform(rng) < 0.7)).has_value());

        if (i < LinkCoreSize()) core.push_back(item);

        auto edge = dba->CreateEdge(&hub, &item, has_type);
        MG_ASSERT(edge.has_value());
        MG_ASSERT(edge->SetProperty(p_weight, ms::PropertyValue(uniform(rng))).has_value());
        MG_ASSERT(edge->SetProperty(p_amount, ms::PropertyValue(uniform(rng) * 1000.0)).has_value());
        MG_ASSERT(
            edge->SetProperty(p_year, ms::PropertyValue(int64_t{2015} + static_cast<int64_t>(rng() % 11))).has_value());
      }

      // A ring, so every core item can reach every other, plus two random chords each. That is what
      // makes the shortest paths short and the depth-limited walks branch.
      auto const core_size = static_cast<int64_t>(core.size());
      auto link = [&](ms::VertexAccessor &from, ms::VertexAccessor &to) {
        auto edge = dba->CreateEdge(&from, &to, link_type);
        MG_ASSERT(edge.has_value());
        MG_ASSERT(edge->SetProperty(p_cost, ms::PropertyValue(1.0 + uniform(rng) * 9.0)).has_value());
        MG_ASSERT(edge->SetProperty(p_weight, ms::PropertyValue(uniform(rng))).has_value());
      };
      for (int64_t k = 0; k != core_size; ++k) {
        link(core[k], core[(k + 1) % core_size]);
        for (int chord = 0; chord != 2; ++chord) link(core[k], core[rng() % core_size]);
      }

      MG_ASSERT(dba->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    {
      auto unique_acc = db->UniqueAccess();
      MG_ASSERT(unique_acc->CreateIndex(db->NameToLabel("Hub")).has_value());
      MG_ASSERT(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    {
      auto unique_acc = db->UniqueAccess();
      MG_ASSERT(unique_acc->CreateIndex(db->NameToLabel("Item")).has_value());
      MG_ASSERT(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    {
      auto unique_acc = db->UniqueAccess();
      MG_ASSERT(
          unique_acc->CreateIndex(db->NameToLabel("Item"), {ms::PropertyPath{db->NameToProperty("id")}}).has_value());
      MG_ASSERT(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    return Graph{.db = std::move(db)};
  }();
  return graph;
}

mq::CypherQuery *ParseCypherQuery(std::string const &query_string, mq::AstStorage *ast) {
  mq::frontend::ParsingContext parsing_context;
  mq::Parameters parameters;
  parsing_context.is_query_cached = false;
  mq::frontend::opencypher::Parser parser(query_string);
  mq::frontend::CypherMainVisitor cypher_visitor(parsing_context, ast, &parameters);
  cypher_visitor.visit(parser.tree());
  return memgraph::utils::Downcast<mq::CypherQuery>(cypher_visitor.query());
}

/// Plans `query` once, then pulls it to completion for each iteration.
///
/// Everything a real query does before the first row - parsing, symbol generation, planning and the
/// plan rewrites - happens once here, because a served query finds its plan in the cache. What is
/// left inside the loop is the cursor tree and the rows through it.
void RunQuery(benchmark::State &state, std::string const &query) {
  auto const &graph = TheGraph();
  auto storage_dba = graph.db->Access(ms::READ);
  mq::DbAccessor dba(storage_dba.get());

  mq::AstStorage ast;
  mq::Parameters parameters;
  auto *cypher_query = ParseCypherQuery(query, &ast);
  auto symbol_table = mq::MakeSymbolTable(cypher_query);
  auto planning_context = mq::plan::MakePlanningContext(&ast, &symbol_table, cypher_query, &dba);
  auto plan_and_cost = mq::plan::MakeLogicalPlan(&planning_context, parameters, false);

  memgraph::utils::MonotonicBufferResource per_pull_memory{mq::kExecutionMemoryBlockSize};
  mq::EvaluationContext evaluation_context{&per_pull_memory};
  // The interned label and property indices an expression carries are resolved against the database
  // once, exactly as the interpreter resolves them before executing a cached plan. Without this a
  // label test indexes an empty vector.
  evaluation_context.properties = mq::NamesToProperties(ast.properties_, &dba);
  evaluation_context.labels = mq::NamesToLabels(ast.labels_, &dba);

  int64_t rows = 0;
  for (auto _ : state) {
    mq::ExecutionContext execution_context{.db_accessor = &dba,
                                           .symbol_table = symbol_table,
                                           .evaluation_context = evaluation_context,
                                           .metric_handles = &BenchmarkMetricHandles()};
    memgraph::utils::MonotonicBufferResource memory{mq::kExecutionMemoryBlockSize};
    mq::Frame frame(symbol_table.max_position(), &memory);
    auto cursor = plan_and_cost.plan->MakeCursor(&memory, BenchmarkMetricHandles());
    while (cursor->Pull(frame, execution_context)) {
      ++rows;
      per_pull_memory.Release();
    }
  }
  // Rows through the aggregation, not rows out of it: what these queries cost is per input row, and
  // most of them return a handful.
  state.SetItemsProcessed(state.iterations() * ItemCount());
  state.counters["out_rows"] = static_cast<double>(rows) / static_cast<double>(state.iterations());
}

// ------------------------------------------------------------------ no properties at all ----

// The control for everything that touches a property: it expands the whole supernode and reads
// nothing off it. A property-side change that moves this has done something unintended.
void CountNoGroupKey(benchmark::State &state) { RunQuery(state, "MATCH (:Hub)-[r:HAS]->() RETURN count(r) AS degree"); }

// COUNT over a bound identifier rather than an expression: the frame slot answers whether there is a
// value, so nothing needs building to ask.
void CountIdentifier(benchmark::State &state) { RunQuery(state, "MATCH (:Hub)-[:HAS]->(i:Item) RETURN count(i) AS n"); }

// ------------------------------------------------------------------------- one property ----

// One property, read five times a row. This is the shape a per-row property cache exists for, and
// the largest single win the operator has.
void RepeatedPropertyRead(benchmark::State &state) {
  RunQuery(state,
           "MATCH (:Hub)-[:HAS]->(i:Item) "
           "RETURN sum(i.value) AS total, avg(i.value) AS mean, min(i.value) AS lo, "
           "max(i.value) AS hi, count(i) AS n");
}

// One property, read once. Nothing to batch and nothing to repeat, so a property cache must leave
// this alone - it is the control that says a change is not simply moving everything.
//
// Note it has to be *one* property, not two distinct ones: the bar for caching is two lookups
// counted as mentions, so reading two different properties once each is still one batched pass
// instead of two separate reads, and does get faster. `TwoDistinctProperties` below is that case.
void SinglePropertyRead(benchmark::State &state) {
  RunQuery(state, "MATCH (:Hub)-[:HAS]->(i:Item) RETURN min(i.value) AS lo");
}

// Two properties, each read once. No repeat, but still two lookups, so one pass replaces two.
void TwoDistinctProperties(benchmark::State &state) {
  RunQuery(state, "MATCH (:Hub)-[:HAS]->(i:Item) RETURN min(i.category) AS c, min(i.region) AS r");
}

// Adding into a running total rather than beside it, over a property read once per row.
void SumAvgNumeric(benchmark::State &state) {
  RunQuery(state, "MATCH (:Hub)-[:HAS]->(i:Item) RETURN sum(i.value) AS total, avg(i.value) AS mean");
}

// ------------------------------------------------------------------------- grouping keys ----

// Sixteen groups, skewed, three properties per row.
void GroupBySingleKey(benchmark::State &state) {
  RunQuery(state,
           "MATCH (:Hub)-[:HAS]->(i:Item) "
           "RETURN i.category AS category, count(*) AS n, avg(i.value) AS mean_value, "
           "sum(i.quantity) AS total_quantity ORDER BY category");
}

// A hundred and twenty eight groups from two keys.
void GroupByComposite(benchmark::State &state) {
  RunQuery(state,
           "MATCH (:Hub)-[:HAS]->(i:Item) "
           "RETURN i.category AS category, i.region AS region, count(*) AS n, "
           "sum(i.value) AS total_value ORDER BY category, region");
}

// A thousand groups: the hash aggregation rather than the property read is what this stresses.
void GroupByHighCardinality(benchmark::State &state) {
  RunQuery(state,
           "MATCH (:Hub)-[:HAS]->(i:Item) "
           "RETURN i.bucket AS bucket, count(*) AS n, avg(i.value) AS mean_value ORDER BY bucket");
}

// Two DISTINCT sets side by side, one of a thousand values and one of sixteen.
void CountDistinct(benchmark::State &state) {
  RunQuery(state,
           "MATCH (:Hub)-[:HAS]->(i:Item) "
           "RETURN count(DISTINCT i.bucket) AS b, count(DISTINCT i.category) AS c");
}

// Aggregating the groups of an aggregation.
void ChainedAggregate(benchmark::State &state) {
  RunQuery(state,
           "MATCH (:Hub)-[:HAS]->(i:Item) "
           "WITH i.category AS category, i.region AS region, sum(i.value) AS region_total, "
           "count(*) AS region_items "
           "WITH category, max(region_total) AS best, sum(region_total) AS total, "
           "sum(region_items) AS items "
           "RETURN category, items, total, best ORDER BY total DESC");
}

// --------------------------------------------------------------- expressions and filters ----

// CASE conditions are evaluated for every row, so the properties they test are read every row - and
// the branches are not, which is why only the condition is worth caching.
void ConditionalAgg(benchmark::State &state) {
  RunQuery(state,
           "MATCH (:Hub)-[:HAS]->(i:Item) "
           "RETURN sum(CASE WHEN i.active THEN i.value ELSE 0.0 END) AS active_value, "
           "sum(CASE WHEN i.active THEN 0.0 ELSE i.value END) AS inactive_value, "
           "count(CASE WHEN i.quantity > 50 THEN 1 END) AS bulk");
}

// A filter that throws away most rows, with an `IN` over a short literal list. The membership test
// runs on every row; the aggregation only on the survivors.
void FilteredAggregate(benchmark::State &state) {
  RunQuery(state,
           "MATCH (:Hub)-[:HAS]->(i:Item) "
           "WHERE i.value > 50.0 AND i.region IN ['region_1', 'region_2'] "
           "RETURN count(*) AS n, sum(i.value * i.quantity) AS revenue, avg(i.value) AS mean_value");
}

// A grouping key computed with arithmetic, over a measure that is itself computed, plus one property
// off the relationship.
void ComputedGroupKey(benchmark::State &state) {
  RunQuery(state,
           "MATCH (:Hub)-[r:HAS]->(i:Item) "
           "WITH i.ts / 2592000 AS month_bucket, i.value * i.quantity AS revenue, r.amount AS amount "
           "WHERE revenue > 500.0 "
           "RETURN month_bucket, count(*) AS n, sum(revenue) AS total_revenue, avg(amount) AS mean_amount "
           "ORDER BY month_bucket");
}

// ----------------------------------------------------------------------- edge properties ----

// The same shape against properties of the relationship rather than the node. An edge reads a record
// the same way a vertex does, so anything that helps one should help the other.
void EdgePropertyAggregate(benchmark::State &state) {
  RunQuery(state,
           "MATCH (:Hub)-[r:HAS]->() "
           "RETURN r.year AS year, count(*) AS n, sum(r.amount) AS total_amount, "
           "avg(r.weight) AS mean_weight ORDER BY year");
}

// ------------------------------------------------------------------ non-primitive results ----

// Aggregating into a list of maps: the values built per group are not scalars.
void CollectListOfMaps(benchmark::State &state) {
  RunQuery(state,
           "MATCH (:Hub)-[:HAS]->(i:Item) "
           "WITH i.category AS category, i.region AS region, count(*) AS n, sum(i.value) AS total "
           "WITH category, collect({region: region, items: n, total_value: total}) AS breakdown, "
           "sum(n) AS items "
           "RETURN category, items, size(breakdown) AS regions ORDER BY items DESC");
}

// A set aggregation: a thousand-element list per group.
void CollectDistinct(benchmark::State &state) {
  RunQuery(state,
           "MATCH (:Hub)-[:HAS]->(i:Item) "
           "WITH i.category AS category, collect(DISTINCT i.bucket) AS buckets "
           "RETURN category, size(buckets) AS distinct_buckets ORDER BY category");
}

// --------------------------------------------------------------------------- no expansion ----

// Every item by label, touching no edge at all. The other engine answers this from a count store in
// about a millisecond, which is a difference in metadata bookkeeping rather than in aggregation, so
// it is here as a floor rather than as a comparison.
//
// This is also the one shape whose cost per row does not carry over from the large graph: a quarter
// million labelled vertices are walked out of cache, ten million are not, so this reads about twice
// as fast here. It is still a floor to compare a build against itself on.
void CountAllItems(benchmark::State &state) { RunQuery(state, "MATCH (i:Item) RETURN count(i) AS items"); }

// ------------------------------------------------------------- grouping keys, wider still ----

// Eight thousand groups from a uniform key crossed with a small one.
void GroupByBucketRegion(benchmark::State &state) {
  RunQuery(state,
           "MATCH (:Hub)-[:HAS]->(i:Item) "
           "RETURN i.bucket AS bucket, i.region AS region, count(*) AS n, sum(i.value) AS total_value, "
           "max(i.quantity) AS max_quantity ORDER BY bucket, region");
}

// Sixteen thousand groups, a skewed key crossed with a uniform one.
void GroupByCategoryBucket(benchmark::State &state) {
  RunQuery(state,
           "MATCH (:Hub)-[:HAS]->(i:Item) "
           "RETURN i.category AS category, i.bucket AS bucket, count(*) AS n, avg(i.value) AS mean_value "
           "ORDER BY category, bucket");
}

// ------------------------------------------------------------------------------ traversal ----
//
// These walk the linked core rather than the hub's neighbours. The ids are the ones the large-graph
// runs use, and are written as literals because a benchmark has no parameters to bind.

// Depth-limited expansion with no path built: what a branching walk costs.
void VarLengthPaths(benchmark::State &state) {
  RunQuery(state,
           "MATCH (a:Item {id: 1})-[:LINK *1..6]->(b:Item) "
           "RETURN count(*) AS paths, count(DISTINCT b) AS distinct_endpoints");
}

// The same reachability question asked breadth first, which visits each node once.
void BfsReachability(benchmark::State &state) {
  RunQuery(state, "MATCH (a:Item {id: 1})-[:LINK *BFS 1..12]->(b:Item) RETURN count(b) AS reached");
}

// One path between two fixed items.
void ShortestPath(benchmark::State &state) {
  RunQuery(state,
           "MATCH p = (a:Item {id: 1})-[:LINK *BFS 1..20]->(b:Item {id: 1338}) "
           "RETURN size(relationships(p)) AS hops");
}

// The cheapest path by an edge property rather than the shortest by hop count.
void WeightedShortestPath(benchmark::State &state) {
  RunQuery(state,
           "MATCH p = (a:Item {id: 1})-[:LINK *WSHORTEST 20 (e, n | e.cost) total_cost]->(b:Item {id: 1338}) "
           "RETURN size(relationships(p)) AS hops, total_cost AS cost");
}

// Aggregating over whole paths: a path object per row, reduced over its relationships.
void PathAggregation(benchmark::State &state) {
  RunQuery(state,
           "MATCH p = (a:Item {id: 1})-[:LINK *1..6]->(b:Item) "
           "WITH size(relationships(p)) AS hops, reduce(c = 0.0, r IN relationships(p) | c + r.cost) AS path_cost "
           "RETURN hops, count(*) AS paths, min(path_cost) AS cheapest, avg(path_cost) AS mean_cost, "
           "max(path_cost) AS dearest ORDER BY hops");
}

// Two hops off the supernode, aggregated over the path. Which end this starts from decides it: there
// are fifty times as many edges of the first type as of the second, and the ratio is held at what the
// large graph has for exactly that reason.
void TwoHopPathAggregate(benchmark::State &state) {
  RunQuery(state,
           "MATCH p = (:Hub)-[:HAS]->(i:Item)-[:LINK]->(j:Item) "
           "WITH i.category AS category, reduce(w = 0.0, r IN relationships(p) | w + r.weight) AS path_weight, "
           "j.value AS endpoint_value "
           "RETURN category, count(*) AS paths, avg(path_weight) AS mean_path_weight, "
           "max(path_weight) AS max_path_weight, sum(endpoint_value) AS endpoint_value_total "
           "ORDER BY paths DESC");
}

constexpr auto kUnit = benchmark::kMillisecond;

BENCHMARK(CountNoGroupKey)->Unit(kUnit);
BENCHMARK(CountIdentifier)->Unit(kUnit);
BENCHMARK(RepeatedPropertyRead)->Unit(kUnit);
BENCHMARK(SinglePropertyRead)->Unit(kUnit);
BENCHMARK(TwoDistinctProperties)->Unit(kUnit);
BENCHMARK(SumAvgNumeric)->Unit(kUnit);
BENCHMARK(GroupBySingleKey)->Unit(kUnit);
BENCHMARK(GroupByComposite)->Unit(kUnit);
BENCHMARK(GroupByHighCardinality)->Unit(kUnit);
BENCHMARK(CountDistinct)->Unit(kUnit);
BENCHMARK(ChainedAggregate)->Unit(kUnit);
BENCHMARK(ConditionalAgg)->Unit(kUnit);
BENCHMARK(FilteredAggregate)->Unit(kUnit);
BENCHMARK(ComputedGroupKey)->Unit(kUnit);
BENCHMARK(EdgePropertyAggregate)->Unit(kUnit);
BENCHMARK(CollectListOfMaps)->Unit(kUnit);
BENCHMARK(CollectDistinct)->Unit(kUnit);
BENCHMARK(CountAllItems)->Unit(kUnit);
BENCHMARK(GroupByBucketRegion)->Unit(kUnit);
BENCHMARK(GroupByCategoryBucket)->Unit(kUnit);
BENCHMARK(VarLengthPaths)->Unit(kUnit);
BENCHMARK(BfsReachability)->Unit(kUnit);
BENCHMARK(ShortestPath)->Unit(kUnit);
BENCHMARK(WeightedShortestPath)->Unit(kUnit);
BENCHMARK(PathAggregation)->Unit(kUnit);
BENCHMARK(TwoHopPathAggregate)->Unit(kUnit);

}  // namespace

// Google Benchmark's own main does not know about gflags, and the flag that turns the property cache
// on and off is one, so the A/B this file exists for needs both parsed. Reparsing is allowed so each
// parser ignores the other's arguments.
int main(int argc, char **argv) {
  // Benchmark first, so it takes its own `--benchmark_*` arguments out of the way; gflags then has
  // only the flags this binary shares with the server left to look at.
  benchmark::Initialize(&argc, argv);
  gflags::AllowCommandLineReparsing();
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
