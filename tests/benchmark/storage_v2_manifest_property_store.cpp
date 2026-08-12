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

// Compares the manifest-based store against the current one on the record shape the
// supernode aggregation workload uses: eight properties, two of them strings, read one at a
// time by a scan. Reading by position is the point of the comparison, because the current
// store scans from the front of the record and so pays for a property's position.

#include <benchmark/benchmark.h>

#include <map>
#include <string>
#include <vector>

#include "storage/v2/manifest_property_store.hpp"
#include "storage/v2/property_store.hpp"

namespace {

using memgraph::storage::ManifestPropertyStore;
using memgraph::storage::ManifestRegistry;
using memgraph::storage::PropertyId;
using memgraph::storage::PropertyStore;
using memgraph::storage::PropertyValue;

// The :Item record: id, category, region, bucket, value, quantity, ts, active. Property ids
// are assigned in that order, so index 0 is the first property in the record and 7 the last.
auto ItemProperties() -> std::vector<std::pair<PropertyId, PropertyValue>> {
  return {
      {PropertyId::FromUint(0), PropertyValue(int64_t{7'000'000})},
      {PropertyId::FromUint(1), PropertyValue(std::string{"cat_07"})},
      {PropertyId::FromUint(2), PropertyValue(std::string{"region_3"})},
      {PropertyId::FromUint(3), PropertyValue(int64_t{512})},
      {PropertyId::FromUint(4), PropertyValue(33.14907)},
      {PropertyId::FromUint(5), PropertyValue(int64_t{57})},
      {PropertyId::FromUint(6), PropertyValue(int64_t{1'600'000'000})},
      {PropertyId::FromUint(7), PropertyValue(true)},
  };
}

PropertyStore MakeCurrent() {
  PropertyStore store;
  for (auto const &[id, value] : ItemProperties()) store.SetProperty(id, value);
  return store;
}

ManifestPropertyStore MakeManifest(ManifestRegistry &registry) {
  ManifestPropertyStore store;
  for (auto const &[id, value] : ItemProperties()) store.SetProperty(registry, id, value);
  return store;
}

// ---------------------------------------------------------------- read one property ----

void CurrentGetByPosition(benchmark::State &state) {
  auto const store = MakeCurrent();
  auto const property = PropertyId::FromUint(static_cast<uint32_t>(state.range(0)));
  for (auto _ : state) {
    benchmark::DoNotOptimize(store.GetProperty(property));
  }
  state.SetItemsProcessed(state.iterations());
}

void ManifestGetByPosition(benchmark::State &state) {
  ManifestRegistry registry;
  auto const store = MakeManifest(registry);
  auto const property = PropertyId::FromUint(static_cast<uint32_t>(state.range(0)));
  for (auto _ : state) {
    benchmark::DoNotOptimize(store.GetProperty(registry, property));
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(CurrentGetByPosition)->DenseRange(0, 7, 1)->Unit(benchmark::kNanosecond);
BENCHMARK(ManifestGetByPosition)->DenseRange(0, 7, 1)->Unit(benchmark::kNanosecond);

// ------------------------------------------------- read the properties a query needs ----

// q04 reads category, value and quantity per row: one string and two numbers, spread across
// the record.
void CurrentGetQueryProperties(benchmark::State &state) {
  auto const store = MakeCurrent();
  for (auto _ : state) {
    benchmark::DoNotOptimize(store.GetProperty(PropertyId::FromUint(1)));
    benchmark::DoNotOptimize(store.GetProperty(PropertyId::FromUint(4)));
    benchmark::DoNotOptimize(store.GetProperty(PropertyId::FromUint(5)));
  }
  state.SetItemsProcessed(state.iterations() * 3);
}

void ManifestGetQueryProperties(benchmark::State &state) {
  ManifestRegistry registry;
  auto const store = MakeManifest(registry);
  for (auto _ : state) {
    benchmark::DoNotOptimize(store.GetProperty(registry, PropertyId::FromUint(1)));
    benchmark::DoNotOptimize(store.GetProperty(registry, PropertyId::FromUint(4)));
    benchmark::DoNotOptimize(store.GetProperty(registry, PropertyId::FromUint(5)));
  }
  state.SetItemsProcessed(state.iterations() * 3);
}

BENCHMARK(CurrentGetQueryProperties)->Unit(benchmark::kNanosecond);
BENCHMARK(ManifestGetQueryProperties)->Unit(benchmark::kNanosecond);

// ------------------------------------------------------------------ read all of them ----

void CurrentProperties(benchmark::State &state) {
  auto const store = MakeCurrent();
  for (auto _ : state) {
    benchmark::DoNotOptimize(store.Properties());
  }
  state.SetItemsProcessed(state.iterations());
}

void ManifestProperties(benchmark::State &state) {
  ManifestRegistry registry;
  auto const store = MakeManifest(registry);
  for (auto _ : state) {
    benchmark::DoNotOptimize(store.Properties(registry));
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(CurrentProperties)->Unit(benchmark::kNanosecond);
BENCHMARK(ManifestProperties)->Unit(benchmark::kNanosecond);

// ------------------------------------------------------------------------- has/type ----

// The manifest answers this without touching the record at all.
void CurrentHasProperty(benchmark::State &state) {
  auto const store = MakeCurrent();
  for (auto _ : state) {
    benchmark::DoNotOptimize(store.HasProperty(PropertyId::FromUint(7)));
  }
  state.SetItemsProcessed(state.iterations());
}

void ManifestHasProperty(benchmark::State &state) {
  ManifestRegistry registry;
  auto const store = MakeManifest(registry);
  for (auto _ : state) {
    benchmark::DoNotOptimize(store.HasProperty(registry, PropertyId::FromUint(7)));
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(CurrentHasProperty)->Unit(benchmark::kNanosecond);
BENCHMARK(ManifestHasProperty)->Unit(benchmark::kNanosecond);

// ------------------------------------------------- a scan over records of one shape ----

// What a scan does: read one property from record after record. The current store has to
// find the property in every record; the manifest store can work out where it lives once.
constexpr size_t kScanRecords = 4096;

void CurrentScanOneProperty(benchmark::State &state) {
  std::vector<PropertyStore> records(kScanRecords);
  for (auto &record : records) {
    for (auto const &[id, value] : ItemProperties()) record.SetProperty(id, value);
  }
  auto const property = PropertyId::FromUint(4);
  for (auto _ : state) {
    for (auto const &record : records) benchmark::DoNotOptimize(record.GetProperty(property));
  }
  state.SetItemsProcessed(state.iterations() * kScanRecords);
}

void ManifestScanOneProperty(benchmark::State &state) {
  ManifestRegistry registry;
  auto const properties = ItemProperties();
  std::vector<ManifestPropertyStore> records(kScanRecords);
  for (auto &record : records) record.InitProperties(registry, properties);
  auto const property = PropertyId::FromUint(4);
  for (auto _ : state) {
    for (auto const &record : records) benchmark::DoNotOptimize(record.GetProperty(registry, property));
  }
  state.SetItemsProcessed(state.iterations() * kScanRecords);
}

void ManifestScanOnePropertyResolvedOnce(benchmark::State &state) {
  ManifestRegistry registry;
  auto const properties = ItemProperties();
  std::vector<ManifestPropertyStore> records(kScanRecords);
  for (auto &record : records) record.InitProperties(registry, properties);

  auto const shape = records.front().manifest();
  auto const &manifest = registry.Resolve(shape);
  auto const location = *manifest.Find(PropertyId::FromUint(4));

  for (auto _ : state) {
    for (auto const &record : records) {
      // A scan checks the shape it resolved against and only then reads at the known place.
      if (record.manifest() != shape) continue;
      benchmark::DoNotOptimize(record.GetProperty(manifest, location));
    }
  }
  state.SetItemsProcessed(state.iterations() * kScanRecords);
}

BENCHMARK(CurrentScanOneProperty)->Unit(benchmark::kMicrosecond);
BENCHMARK(ManifestScanOneProperty)->Unit(benchmark::kMicrosecond);
BENCHMARK(ManifestScanOnePropertyResolvedOnce)->Unit(benchmark::kMicrosecond);

// ----------------------------------------------------------------------------- write ----

// Overwriting a number in place: the update the manifest store is meant to keep cheap.
void CurrentSetInPlace(benchmark::State &state) {
  auto store = MakeCurrent();
  int64_t counter = 0;
  for (auto _ : state) {
    store.SetProperty(PropertyId::FromUint(5), PropertyValue(++counter % 100));
  }
  state.SetItemsProcessed(state.iterations());
}

void ManifestSetInPlace(benchmark::State &state) {
  ManifestRegistry registry;
  auto store = MakeManifest(registry);
  int64_t counter = 0;
  for (auto _ : state) {
    store.SetProperty(registry, PropertyId::FromUint(5), PropertyValue(++counter % 100));
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(CurrentSetInPlace)->Unit(benchmark::kNanosecond);
BENCHMARK(ManifestSetInPlace)->Unit(benchmark::kNanosecond);

// Building a whole record, which is what an import does.
void CurrentBuildRecord(benchmark::State &state) {
  auto const properties = ItemProperties();
  for (auto _ : state) {
    PropertyStore store;
    for (auto const &[id, value] : properties) store.SetProperty(id, value);
    benchmark::DoNotOptimize(store);
  }
  state.SetItemsProcessed(state.iterations());
}

void ManifestBuildRecord(benchmark::State &state) {
  ManifestRegistry registry;
  auto const properties = ItemProperties();
  for (auto _ : state) {
    ManifestPropertyStore store;
    for (auto const &[id, value] : properties) store.SetProperty(registry, id, value);
    benchmark::DoNotOptimize(store);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(CurrentBuildRecord)->Unit(benchmark::kNanosecond);
BENCHMARK(ManifestBuildRecord)->Unit(benchmark::kNanosecond);

// The same record built one property at a time, but laid out up front for the properties it
// is about to be given, which is what a planner knows from the query text.
void ManifestBuildRecordReserved(benchmark::State &state) {
  ManifestRegistry registry;
  auto const properties = ItemProperties();

  auto fields = std::vector<memgraph::storage::ManifestEntry>{};
  {
    // What the planner would work out once: the fields, and how wide each one will be.
    ManifestPropertyStore probe;
    probe.InitProperties(registry, properties);
    auto const &manifest = registry.Resolve(probe.manifest());
    fields.assign(manifest.entries().begin(), manifest.entries().end());
  }

  for (auto _ : state) {
    ManifestPropertyStore store;
    store.ReserveFields(registry, fields);
    for (auto const &[id, value] : properties) store.SetProperty(registry, id, value);
    benchmark::DoNotOptimize(store);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(ManifestBuildRecordReserved)->Unit(benchmark::kNanosecond);

// The same again with only the fixed-width properties, to separate the cost of filling a
// reserved slot from the cost of a value whose width is not known until it arrives.
void ManifestBuildRecordReservedFixedOnly(benchmark::State &state) {
  ManifestRegistry registry;
  auto all = ItemProperties();
  auto properties = std::vector<std::pair<PropertyId, PropertyValue>>{};
  for (auto const &pair : all) {
    if (!pair.second.IsString()) properties.push_back(pair);
  }

  auto fields = std::vector<memgraph::storage::ManifestEntry>{};
  {
    ManifestPropertyStore probe;
    probe.InitProperties(registry, properties);
    auto const &manifest = registry.Resolve(probe.manifest());
    fields.assign(manifest.entries().begin(), manifest.entries().end());
  }

  for (auto _ : state) {
    ManifestPropertyStore store;
    store.ReserveFields(registry, fields);
    for (auto const &[id, value] : properties) store.SetProperty(registry, id, value);
    benchmark::DoNotOptimize(store);
  }
  state.SetItemsProcessed(state.iterations());
}

void ManifestBuildRecordFixedOnly(benchmark::State &state) {
  ManifestRegistry registry;
  auto all = ItemProperties();
  auto properties = std::vector<std::pair<PropertyId, PropertyValue>>{};
  for (auto const &pair : all) {
    if (!pair.second.IsString()) properties.push_back(pair);
  }
  for (auto _ : state) {
    ManifestPropertyStore store;
    for (auto const &[id, value] : properties) store.SetProperty(registry, id, value);
    benchmark::DoNotOptimize(store);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(ManifestBuildRecordFixedOnly)->Unit(benchmark::kNanosecond);
BENCHMARK(ManifestBuildRecordReservedFixedOnly)->Unit(benchmark::kNanosecond);

// Building a record the way a load does, with every property known up front.
void CurrentInitRecord(benchmark::State &state) {
  auto const properties = std::map<PropertyId, PropertyValue>{ItemProperties().begin(), ItemProperties().end()};
  for (auto _ : state) {
    PropertyStore store;
    store.InitProperties(properties);
    benchmark::DoNotOptimize(store);
  }
  state.SetItemsProcessed(state.iterations());
}

void ManifestInitRecord(benchmark::State &state) {
  ManifestRegistry registry;
  auto const properties = std::map<PropertyId, PropertyValue>{ItemProperties().begin(), ItemProperties().end()};
  for (auto _ : state) {
    ManifestPropertyStore store;
    store.InitProperties(registry, properties);
    benchmark::DoNotOptimize(store);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(CurrentInitRecord)->Unit(benchmark::kNanosecond);
BENCHMARK(ManifestInitRecord)->Unit(benchmark::kNanosecond);

// The same thing from a caller that already holds the properties in order, which is the
// shape of the API both stores really want; the map overloads above pay for a conversion.
void CurrentInitRecordOrdered(benchmark::State &state) {
  auto const properties = ItemProperties();
  for (auto _ : state) {
    PropertyStore store;
    store.InitProperties(properties);
    benchmark::DoNotOptimize(store);
  }
  state.SetItemsProcessed(state.iterations());
}

void ManifestInitRecordOrdered(benchmark::State &state) {
  ManifestRegistry registry;
  auto const properties = ItemProperties();
  for (auto _ : state) {
    ManifestPropertyStore store;
    store.InitProperties(registry, properties);
    benchmark::DoNotOptimize(store);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(CurrentInitRecordOrdered)->Unit(benchmark::kNanosecond);
BENCHMARK(ManifestInitRecordOrdered)->Unit(benchmark::kNanosecond);

// A load builds records from every thread at once, all of them interning the same shape.
// Interning has to scale, or it becomes the load's bottleneck rather than the encoding.
ManifestRegistry &SharedRegistry() {
  static ManifestRegistry registry;
  return registry;
}

void CurrentInitRecordThreaded(benchmark::State &state) {
  auto const properties = std::map<PropertyId, PropertyValue>{ItemProperties().begin(), ItemProperties().end()};
  for (auto _ : state) {
    PropertyStore store;
    store.InitProperties(properties);
    benchmark::DoNotOptimize(store);
  }
  state.SetItemsProcessed(state.iterations());
}

void ManifestInitRecordThreaded(benchmark::State &state) {
  auto &registry = SharedRegistry();
  auto const properties = std::map<PropertyId, PropertyValue>{ItemProperties().begin(), ItemProperties().end()};
  for (auto _ : state) {
    ManifestPropertyStore store;
    store.InitProperties(registry, properties);
    benchmark::DoNotOptimize(store);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(CurrentInitRecordThreaded)->ThreadRange(1, 8)->Unit(benchmark::kNanosecond)->UseRealTime();
BENCHMARK(ManifestInitRecordThreaded)->ThreadRange(1, 8)->Unit(benchmark::kNanosecond)->UseRealTime();

}  // namespace

int main(int argc, char **argv) {
  // Bytes per record decides as much as the timings do, so report it before running.
  ManifestRegistry registry;
  auto const manifest_store = MakeManifest(registry);
  auto const current_store = MakeCurrent();
  std::cout << "record bytes: current " << current_store.StringBuffer().size() << ", manifest "
            << manifest_store.buffer_size() << " (+ " << registry.size() << " shared shape(s))\n"
            << "object bytes: current " << sizeof(PropertyStore) << ", manifest " << sizeof(ManifestPropertyStore)
            << "\n\n";

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  return 0;
}
