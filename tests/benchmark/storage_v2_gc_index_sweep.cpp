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

// Demonstrates that a long index-cleanup sweep starves delta reclamation.
//
// The index-cleanup sweep is O(index-size), not O(garbage): it examines every entry
// of every armed index regardless of how few are obsolete. utils::Scheduler runs its
// job synchronously and never overlaps runs, so once a sweep exceeds the GC interval
// the whole of CollectGarbage runs at sweep rate -- including the undo-buffer drain,
// which is what releases delta memory. Deltas the writers produce in the meantime
// accumulate.
//
// Stands up `num_indexes` label-property indexes over `num_vertices` vertices, times
// one synchronous GC pass to establish the sweep cost against the GC interval, then
// runs continuous label-writing traffic and samples unreleased_delta_objects.
//
// Reading the output: `sweep/interval` well above 1 is the precondition. The signal is
// the standing backlog in `unreleased_delta_objects` once it reaches steady state.
//
// `--num_churn_properties=K` sets how many of the indexes the traffic actually dirties, which
// is the fraction a sweep has any reason to visit. It exists because the sweep is armed per
// index family rather than per index, so today the cost is the same at every K; a run at a low
// K next to one at K = index count is what shows whether that is still true. K=0 keeps the
// writers on a property no index covers.
//
// MEMORY: run this under a hard cgroup cap. A transaction allocates a page slab for its
// deltas whatever it puts in it, and these writers commit one delta per transaction, so a
// starved GC parks a whole slab per outstanding transaction and resident memory is
// governed by transaction count rather than by delta size. At the configurations that
// demonstrate the starvation this consumes system memory, and without a cap that is an
// OS-level OOM kill rather than a test failure:
//   systemd-run --user --scope -q -p MemoryMax=<limit> -p MemorySwapMax=0 -- <this binary>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

#include <gflags/gflags.h>

#include "metrics/prometheus_metrics.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/resource_lock.hpp"
#include "utils/timer.hpp"
#include "utils/uuid.hpp"

DEFINE_int32(num_indexes, 200, "number of label-property indexes to create");
DEFINE_int32(num_vertices, 20'000, "number of vertices, each carrying every indexed property");
DEFINE_int32(num_properties, -1,
             "properties per vertex; defaults to num_indexes. Separate from num_indexes so that "
             "per-write costs that scale with how many properties a vertex carries can be told "
             "apart from the sweep cost that index count drives");
DEFINE_int32(num_edge_indexes, 0,
             "edge-type-property indexes, each over one property of a chain of edges. Vertex writes "
             "cannot invalidate an edge index entry, so a workload that only touches vertices should "
             "not pay to sweep these");
DEFINE_string(churn, "label",
              "label | property | edge_property. What the writers modify. A label write names the "
              "vertex side on its own; a property write does not say which side it belongs to, so the "
              "vertex and edge cases are separable here. Note that `label` touches no index at all: "
              "every index sits on the single indexed label, and the churn labels are disjoint from it");
DEFINE_int32(num_churn_properties, 0,
             "how many of the *indexed* properties the writers churn, which is the fraction of "
             "indexes a sweep has any reason to visit. 0 keeps the writers on a non-indexed "
             "property, so no index is dirtied. Set it equal to the index count to reproduce the "
             "whole-family sweep -- that configuration is the control: a saving measured at a low "
             "value only means something if the high value still costs what it costs today");
DEFINE_int32(num_label_indexes, 0,
             "plain label indexes, over the labels the writers churn. Separate from num_indexes "
             "because a label index turns on a label alone, so it is the one an --churn=label "
             "workload can leave with nothing to collect");
DEFINE_int32(num_churn_labels, 64,
             "how many distinct labels the writers toggle. Below num_label_indexes it is the "
             "fraction of label indexes a sweep has any reason to visit, the same dial "
             "num_churn_properties gives on the property side");
DEFINE_int32(num_writers, 4, "number of threads writing labels continuously");
DEFINE_int32(duration_seconds, 30, "how long to run the write traffic");
DEFINE_int32(gc_interval_ms, 100, "periodic GC interval");
DEFINE_int32(sample_ms, 250, "how often to sample unreleased_delta_objects");
DEFINE_int32(vertices_per_txn, 2'000, "batch size when building the vertex set");
DEFINE_int32(deltas_per_txn, 1,
             "label toggles committed per write transaction. 1 models many small transactions, where "
             "each one parks a whole delta slab; a large value models batched ingestion, where a slab "
             "is shared by many deltas and the backlog reaches memory through delta volume instead");
DEFINE_string(acquirer, "none",
              "none | read_only | unique. A background thread asking for that access on a timeout, the "
              "way automatic index creation does. READ_ONLY gates writers only. UNIQUE needs the lock "
              "fully unlocked, so it cannot be granted while GC holds its shared hold for a whole "
              "sweep, and once pending it gates every new reader and writer");
DEFINE_int32(readonly_period_ms, 2'000, "how often the acquirer tries");
DEFINE_int32(acquirer_hold_ms, 0,
             "how long the acquirer holds once granted. Real index creation holds for the length of "
             "the build, which scales with graph size; releasing immediately understates the effect");
DEFINE_int32(readonly_timeout_ms, 1'000, "READ_ONLY acquisition timeout");
DEFINE_int32(num_readers, 2, "threads doing point lookups, to measure how long a reader is stalled");

namespace ms = memgraph::storage;

namespace {

struct LatencyStats {
  std::atomic<uint64_t> ops{0};
  std::atomic<uint64_t> max_us{0};
  std::atomic<uint64_t> over_1s{0};

  void Record(double seconds) {
    auto const us = static_cast<uint64_t>(seconds * 1e6);
    ops.fetch_add(1, std::memory_order_relaxed);
    if (seconds > 1.0) over_1s.fetch_add(1, std::memory_order_relaxed);
    auto prev = max_us.load(std::memory_order_relaxed);
    while (us > prev && !max_us.compare_exchange_weak(prev, us, std::memory_order_relaxed)) {
    }
  }
};

struct Sample {
  double at_seconds;
  uint64_t unreleased_deltas;
  uint64_t commits;  // cumulative, so the report can show writers stayed live throughout
};

void WriterFunc(int thread_id, ms::Storage *storage, std::vector<ms::Gid> const &vertices,
                std::vector<ms::LabelId> const &churn_labels, std::atomic<bool> const &stop,
                std::vector<ms::Gid> const &edges, std::vector<ms::PropertyId> const &churn_properties,
                std::atomic<uint64_t> &commits, std::atomic<uint64_t> &conflicts, LatencyStats &stats) {
  std::mt19937 gen(thread_id);
  std::uniform_int_distribution<uint64_t> vertex_dist(0, vertices.size() - 1);
  std::uniform_int_distribution<uint64_t> label_dist(0, FLAGS_num_churn_labels - 1);
  std::uniform_int_distribution<uint64_t> property_dist(0, churn_properties.size() - 1);

  while (!stop.load(std::memory_order_relaxed)) {
    memgraph::utils::Timer timer;
    auto acc = storage->Access(ms::StorageAccessType::WRITE);
    bool aborted = false;

    // Distinct vertices within a transaction: toggling the same one twice would undo the
    // first delta rather than adding a second.
    auto touched = std::vector<uint64_t>{};
    touched.reserve(FLAGS_deltas_per_txn);

    for (int i = 0; i < FLAGS_deltas_per_txn && !aborted; ++i) {
      auto index = vertex_dist(gen);
      while (std::ranges::contains(touched, index)) index = vertex_dist(gen);
      touched.push_back(index);

      auto vertex = acc->FindVertex(vertices[index], ms::View::OLD);
      MG_ASSERT(vertex.has_value(), "vertex disappeared");
      auto const churn_property = churn_properties[property_dist(gen)];
      if (FLAGS_churn == "edge_property") {
        auto edge = acc->FindEdge(edges[index % edges.size()], ms::View::OLD);
        MG_ASSERT(edge.has_value(), "edge disappeared");
        if (!edge->SetProperty(churn_property, ms::PropertyValue{static_cast<int64_t>(gen())}).has_value()) {
          aborted = true;
        }
        continue;
      }
      if (FLAGS_churn == "property") {
        // A property write on a vertex. It can invalidate a vertex index entry and no edge index
        // entry, which is the distinction being exercised.
        if (!vertex->SetProperty(churn_property, ms::PropertyValue{static_cast<int64_t>(gen())}).has_value()) {
          aborted = true;
        }
        continue;
      }
      auto const label = churn_labels[label_dist(gen)];
      // Toggle rather than add: with a bounded label pool every vertex soon carries every
      // label, and an AddLabel that finds one present produces no delta. The traffic has to
      // keep producing deltas for the whole run or there is nothing for reclamation to fall
      // behind on.
      auto const added = vertex->AddLabel(label);
      if (!added.has_value() || (!*added && !vertex->RemoveLabel(label).has_value())) aborted = true;
    }

    // Writers picking the same vertex conflict, which is expected traffic rather than a
    // harness failure; abort and move on, but count it so the report can say how much of
    // the run actually committed.
    if (aborted || !acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) {
      acc->Abort();
      conflicts.fetch_add(1, std::memory_order_relaxed);
      stats.Record(timer.Elapsed().count());
      continue;
    }
    commits.fetch_add(1, std::memory_order_relaxed);
    stats.Record(timer.Elapsed().count());
  }
}

// Point lookups, timed. This is the reader whose stall the freeze reports describe: it
// touches no index the sweep is walking and should be unaffected by GC.
void ReaderProbeFunc(int thread_id, ms::Storage *storage, std::vector<ms::Gid> const &vertices,
                     std::atomic<bool> const &stop, LatencyStats &stats) {
  std::mt19937 gen(thread_id + 9973);
  std::uniform_int_distribution<uint64_t> vertex_dist(0, vertices.size() - 1);

  while (!stop.load(std::memory_order_relaxed)) {
    memgraph::utils::Timer timer;
    {
      auto acc = storage->Access(ms::StorageAccessType::READ);
      auto vertex = acc->FindVertex(vertices[vertex_dist(gen)], ms::View::NEW);
      MG_ASSERT(vertex.has_value(), "vertex disappeared");
      acc->Abort();
    }
    stats.Record(timer.Elapsed().count());
  }
}

// Asks for READ_ONLY access on a timeout and gives up when it cannot get it, which is what
// the automatic index creation path does. Acquiring it is not the point: registering as a
// pending exclusive acquirer is, because that is what blocks every new shared acquirer for
// as long as the sweep holds its own shared hold.
void AcquirerFunc(ms::Storage *storage, bool unique, std::atomic<bool> const &stop, std::atomic<uint64_t> &granted,
                  std::atomic<uint64_t> &timed_out) {
  auto const timeout = std::chrono::milliseconds(FLAGS_readonly_timeout_ms);
  while (!stop.load(std::memory_order_relaxed)) {
    try {
      auto acc = unique ? storage->UniqueAccess(std::nullopt, timeout) : storage->ReadOnlyAccess(std::nullopt, timeout);
      granted.fetch_add(1, std::memory_order_relaxed);
      if (FLAGS_acquirer_hold_ms > 0) std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_acquirer_hold_ms));
      acc->Abort();
    } catch (ms::ReadOnlyAccessTimeout const &) {
      timed_out.fetch_add(1, std::memory_order_relaxed);
    } catch (ms::UniqueAccessTimeout const &) {
      timed_out.fetch_add(1, std::memory_order_relaxed);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_readonly_period_ms));
  }
}

// A full synchronous GC pass. Handing FreeMemory a UNIQUE hold makes it adopt rather
// than acquire, which is what forces the pass to run here rather than being skipped.
double TimeSynchronousGcPass(ms::Storage *storage) {
  memgraph::utils::Timer timer;
  {
    auto guard = memgraph::utils::ResourceLockGuard{storage->main_lock_, memgraph::utils::ResourceLockGuard::UNIQUE};
    storage->FreeMemory(std::move(guard), false);
  }
  return timer.Elapsed().count();
}

}  // namespace

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  auto const gc_interval = std::chrono::milliseconds(FLAGS_gc_interval_ms);
  auto const num_properties = FLAGS_num_properties < 0 ? FLAGS_num_indexes : FLAGS_num_properties;
  MG_ASSERT(num_properties >= FLAGS_num_indexes, "each index needs a property to index");
  MG_ASSERT(num_properties >= FLAGS_num_edge_indexes, "each edge index needs a property to index");
  MG_ASSERT(FLAGS_churn == "label" || FLAGS_churn == "property" || FLAGS_churn == "edge_property",
            "--churn must be label, property or edge_property");
  MG_ASSERT(FLAGS_num_churn_properties >= 0, "--num_churn_properties cannot be negative");
  MG_ASSERT(FLAGS_num_churn_labels > 0, "the writers need at least one label to toggle");
  // Asserted rather than ignored: a run that asked for a churn fraction and silently got none
  // would report the unarmed cost under the name of the armed one.
  MG_ASSERT(FLAGS_churn != "label" || FLAGS_num_churn_properties == 0,
            "--num_churn_properties needs --churn=property or --churn=edge_property");
  MG_ASSERT(FLAGS_churn != "property" || FLAGS_num_churn_properties <= FLAGS_num_indexes,
            "--num_churn_properties cannot exceed --num_indexes");
  MG_ASSERT(FLAGS_churn != "edge_property" || FLAGS_num_churn_properties <= FLAGS_num_edge_indexes,
            "--num_churn_properties cannot exceed --num_edge_indexes");

  constexpr std::string_view kDbName = "gc_index_sweep";

  auto config = ms::Config{};
  config.salient.name = kDbName;
  config.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = gc_interval};

  auto const uuid = memgraph::utils::UUID{};
  auto handles = memgraph::metrics::Metrics().AddDatabase(uuid, kDbName);

  auto storage = std::make_unique<ms::InMemoryStorage>(
      config, std::nullopt, std::make_unique<ms::PlanInvalidatorDefault>(), handles);

  // --- Build the indexed data set -------------------------------------------------
  std::vector<ms::Gid> vertices;
  std::vector<ms::Gid> edges;
  std::vector<ms::PropertyId> properties;
  std::vector<ms::LabelId> churn_labels;
  ms::LabelId indexed_label;
  std::vector<ms::PropertyId> churn_properties;

  {
    auto acc = storage->Access(ms::StorageAccessType::WRITE);
    indexed_label = acc->NameToLabel("Indexed");
    properties.reserve(num_properties);
    for (int i = 0; i < num_properties; ++i) {
      properties.push_back(acc->NameToProperty("p" + std::to_string(i)));
    }
    // What the writers target. Drawn from the front of the indexed set when a churn fraction
    // is asked for, so the traffic dirties exactly that many indexes and leaves the rest with
    // nothing to collect; otherwise a property no index covers, which dirties none. The vertex
    // and edge indexes are both built over a prefix of `properties`, so the same slice serves
    // either churn mode.
    if (FLAGS_num_churn_properties > 0) {
      churn_properties.assign(properties.begin(), properties.begin() + FLAGS_num_churn_properties);
    } else {
      churn_properties.push_back(acc->NameToProperty("churn"));
    }
    auto const label_pool = std::max(FLAGS_num_churn_labels, FLAGS_num_label_indexes);
    churn_labels.reserve(label_pool);
    for (int i = 0; i < label_pool; ++i) {
      churn_labels.push_back(acc->NameToLabel("Churn" + std::to_string(i)));
    }
    acc->Abort();
  }

  {
    memgraph::utils::Timer timer;
    vertices.reserve(FLAGS_num_vertices);
    for (int base = 0; base < FLAGS_num_vertices; base += FLAGS_vertices_per_txn) {
      auto const batch_end = std::min(base + FLAGS_vertices_per_txn, FLAGS_num_vertices);
      auto acc = storage->Access(ms::StorageAccessType::WRITE);
      for (int i = base; i < batch_end; ++i) {
        auto vertex = acc->CreateVertex();
        MG_ASSERT(vertex.AddLabel(indexed_label).has_value());
        // The label indexes are built over these, so every vertex has to carry them or the
        // indexes stand empty and a sweep of them measures nothing.
        for (int l = 0; l < FLAGS_num_label_indexes; ++l) {
          MG_ASSERT(vertex.AddLabel(churn_labels[l]).has_value());
        }
        for (int p = 0; p < num_properties; ++p) {
          MG_ASSERT(vertex.SetProperty(properties[p], ms::PropertyValue{static_cast<int64_t>(i)}).has_value());
        }
        vertices.push_back(vertex.Gid());
      }
      MG_ASSERT(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    std::cout << "built " << vertices.size() << " vertices x " << num_properties << " properties in "
              << timer.Elapsed().count() << "s\n";
  }

  if (FLAGS_num_edge_indexes > 0) {
    memgraph::utils::Timer timer;
    auto const edge_type = std::invoke([&] {
      auto acc = storage->Access(ms::StorageAccessType::WRITE);
      auto type = acc->NameToEdgeType("RELATES_TO");
      acc->Abort();
      return type;
    });
    for (size_t base = 0; base + 1 < vertices.size(); base += FLAGS_vertices_per_txn) {
      auto const batch_end = std::min(base + FLAGS_vertices_per_txn, vertices.size() - 1);
      auto acc = storage->Access(ms::StorageAccessType::WRITE);
      for (size_t i = base; i < batch_end; ++i) {
        auto from = acc->FindVertex(vertices[i], ms::View::OLD);
        auto to = acc->FindVertex(vertices[i + 1], ms::View::OLD);
        auto edge = acc->CreateEdge(&*from, &*to, edge_type);
        MG_ASSERT(edge.has_value());
        edges.push_back(edge->Gid());
        for (int p = 0; p < FLAGS_num_edge_indexes; ++p) {
          MG_ASSERT(edge->SetProperty(properties[p], ms::PropertyValue{static_cast<int64_t>(i)}).has_value());
        }
      }
      MG_ASSERT(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    for (int i = 0; i < FLAGS_num_edge_indexes; ++i) {
      auto acc = storage->ReadOnlyAccess();
      MG_ASSERT(acc->CreateIndex(edge_type, properties[i]).has_value());
      MG_ASSERT(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    std::cout << "created " << FLAGS_num_edge_indexes << " edge-type-property indexes over " << (vertices.size() - 1)
              << " edges in " << timer.Elapsed().count() << "s\n";
  }

  if (FLAGS_num_label_indexes > 0) {
    memgraph::utils::Timer timer;
    for (int i = 0; i < FLAGS_num_label_indexes; ++i) {
      auto acc = storage->ReadOnlyAccess();
      MG_ASSERT(acc->CreateIndex(churn_labels[i]).has_value());
      MG_ASSERT(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    std::cout << "created " << FLAGS_num_label_indexes << " label indexes in " << timer.Elapsed().count() << "s\n";
  }

  {
    memgraph::utils::Timer timer;
    for (int i = 0; i < FLAGS_num_indexes; ++i) {
      auto acc = storage->ReadOnlyAccess();
      MG_ASSERT(acc->CreateIndex(indexed_label, {ms::PropertyPath{properties[i]}}).has_value());
      MG_ASSERT(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    std::cout << "created " << FLAGS_num_indexes << " label-property indexes in " << timer.Elapsed().count() << "s\n";
  }

  auto const index_entries = static_cast<uint64_t>(FLAGS_num_indexes) * static_cast<uint64_t>(FLAGS_num_vertices);

  // --- Establish the sweep cost against the GC interval ----------------------------
  // The sweep is armed by the delta actions a cycle sees, so an
  // unarmed pass skips it entirely and times nothing. Drain the build's garbage first,
  // then arm and time a pass whose cost is the scan itself.
  // The build wrote edge properties, so this pass is armed for the edge side as well as the
  // vertex side; the timed pass below is armed only by whatever --churn writes.
  auto const drain_seconds = TimeSynchronousGcPass(storage.get());
  {
    auto acc = storage->Access(ms::StorageAccessType::WRITE);
    auto vertex = acc->FindVertex(vertices.front(), ms::View::OLD);
    MG_ASSERT(vertex.has_value());
    // Touch everything the writers touch, not one of them: the pass being timed should be armed
    // the way a pass during the run is, and arming is per index.
    if (FLAGS_churn == "edge_property") {
      auto edge = acc->FindEdge(edges.front(), ms::View::OLD);
      MG_ASSERT(edge.has_value());
      for (auto const property : churn_properties) {
        MG_ASSERT(edge->SetProperty(property, ms::PropertyValue{int64_t{1}}).has_value());
      }
    } else if (FLAGS_churn == "property") {
      for (auto const property : churn_properties) {
        MG_ASSERT(vertex->SetProperty(property, ms::PropertyValue{int64_t{1}}).has_value());
      }
    } else {
      // Toggle, as the writers do: with the label indexes built over labels every vertex already
      // carries, an AddLabel that finds one present produces no delta and arms nothing.
      for (int l = 0; l < FLAGS_num_churn_labels; ++l) {
        auto const added = vertex->AddLabel(churn_labels[l]);
        MG_ASSERT(added.has_value());
        if (!*added) MG_ASSERT(vertex->RemoveLabel(churn_labels[l]).has_value());
      }
    }
    MG_ASSERT(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  auto const sweep_seconds = TimeSynchronousGcPass(storage.get());
  auto const interval_seconds = std::chrono::duration<double>(gc_interval).count();

  auto const armed_indexes = FLAGS_churn == "edge_property" ? FLAGS_num_edge_indexes : FLAGS_num_indexes;
  if (FLAGS_num_label_indexes > 0) {
    std::cout << "\nlabel indexes:   " << FLAGS_num_label_indexes
              << ", of which churned: " << std::min(FLAGS_num_churn_labels, FLAGS_num_label_indexes) << "\n";
  }
  std::cout << "\nindex entries:   " << index_entries << "\n"
            << "indexes dirtied: " << FLAGS_num_churn_properties << " of " << armed_indexes << "\n"
            << "drain pass:      " << drain_seconds << "s\n"
            << "gc pass:         " << sweep_seconds << "s\n"
            << "gc interval:     " << interval_seconds << "s\n"
            << "sweep/interval:  " << (sweep_seconds / interval_seconds) << "x\n\n";

  // --- Run the traffic and watch delta reclamation ---------------------------------
  std::atomic<bool> stop{false};
  std::atomic<uint64_t> commits{0};
  std::atomic<uint64_t> conflicts{0};
  std::atomic<uint64_t> ro_granted{0};
  std::atomic<uint64_t> ro_timed_out{0};
  LatencyStats reader_stats;
  LatencyStats writer_stats;

  std::vector<std::thread> threads;
  threads.reserve(FLAGS_num_writers + FLAGS_num_readers + 1);
  for (int i = 0; i < FLAGS_num_writers; ++i) {
    threads.emplace_back(WriterFunc,
                         i,
                         storage.get(),
                         std::cref(vertices),
                         std::cref(churn_labels),
                         std::cref(stop),
                         std::cref(edges),
                         std::cref(churn_properties),
                         std::ref(commits),
                         std::ref(conflicts),
                         std::ref(writer_stats));
  }
  for (int i = 0; i < FLAGS_num_readers; ++i) {
    threads.emplace_back(
        ReaderProbeFunc, i, storage.get(), std::cref(vertices), std::cref(stop), std::ref(reader_stats));
  }
  MG_ASSERT(FLAGS_acquirer == "none" || FLAGS_acquirer == "read_only" || FLAGS_acquirer == "unique",
            "--acquirer must be none, read_only or unique");
  if (FLAGS_acquirer != "none") {
    threads.emplace_back(AcquirerFunc,
                         storage.get(),
                         FLAGS_acquirer == "unique",
                         std::cref(stop),
                         std::ref(ro_granted),
                         std::ref(ro_timed_out));
  }

  std::vector<Sample> samples;
  {
    memgraph::utils::Timer timer;
    auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds(FLAGS_duration_seconds);
    while (std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_sample_ms));
      samples.push_back({timer.Elapsed().count(),
                         static_cast<uint64_t>(handles.unreleased_delta_objects.Value()),
                         commits.load(std::memory_order_relaxed)});
    }
  }

  stop.store(true, std::memory_order_relaxed);
  for (auto &thread : threads) thread.join();

  // --- Report ----------------------------------------------------------------------
  // commits_in_interval is what shows GC and the writers actually overlapped: a stalled
  // writer set would flatten the delta curve for a reason that has nothing to do with
  // reclamation.
  std::cout << "seconds,unreleased_delta_objects,commits_in_interval\n";
  uint64_t previous_commits = 0;
  for (auto const &sample : samples) {
    std::cout << sample.at_seconds << "," << sample.unreleased_deltas << "," << (sample.commits - previous_commits)
              << "\n";
    previous_commits = sample.commits;
  }

  MG_ASSERT(samples.size() >= 4, "not enough samples to judge a trend; raise --duration_seconds");

  // The signal is the standing backlog, not a growth trend. Delta production is
  // self-limiting -- writers slow as chains lengthen, and the pass does eventually run --
  // so the backlog plateaus rather than growing without bound. What the sweep controls is
  // the height of that plateau: a pass costs `sweep` seconds and drains once per pass, so
  // the backlog settles near one pass worth of commits. Reported over the last three
  // quarters, the first being the ramp to steady state.
  auto const steady_begin = samples.size() / 4;
  std::vector<uint64_t> steady;
  steady.reserve(samples.size() - steady_begin);
  for (size_t i = steady_begin; i < samples.size(); ++i) steady.push_back(samples[i].unreleased_deltas);
  std::ranges::sort(steady);
  auto const steady_median = steady[steady.size() / 2];
  auto const steady_max = steady.back();

  auto const elapsed = samples.back().at_seconds;
  std::cout << "\ncommits:                   " << commits.load() << "\n"
            << "conflicts:                 " << conflicts.load() << "\n"
            << "commit rate:               " << static_cast<uint64_t>(commits.load() / elapsed) << "/s\n"
            << "sweep/interval:            " << (sweep_seconds / interval_seconds) << "x\n"
            << "steady-state deltas (med): " << steady_median << "\n"
            << "steady-state deltas (max): " << steady_max << "\n"
            << "reader lookups:            " << reader_stats.ops.load() << "\n"
            << "reader worst stall:        " << (static_cast<double>(reader_stats.max_us.load()) / 1e6) << "s\n"
            << "reader lookups over 1s:    " << reader_stats.over_1s.load() << "\n"
            << "writer worst stall:        " << (static_cast<double>(writer_stats.max_us.load()) / 1e6) << "s\n"
            << "writer txns over 1s:       " << writer_stats.over_1s.load() << "\n";
  if (FLAGS_acquirer != "none") {
    std::cout << FLAGS_acquirer << " granted:              " << ro_granted.load() << "\n"
              << FLAGS_acquirer << " timed out:            " << ro_timed_out.load() << "\n";
  }

  storage.reset();
  memgraph::metrics::Metrics().RemoveDatabase(uuid);
  return 0;
}
